import json
import logging
import os
import threading
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler

from ml_capacity.formatting import fmt_mb
from ml_capacity.snapshot import (
    SNAPSHOT_DIR, collect_snapshot, save_snapshot, prune_snapshots, load_snapshots,
)
from ml_capacity.prometheus import snapshot_to_prometheus, push_otlp

log = logging.getLogger("mlca")

_BUILD = os.environ.get("MLCA_BUILD", "dev")[:12]


def run_service(client, databases, interval_sec, port, otlp_endpoint=None,
                retention_days=30, api_token=None):
    """Run MLCA as a persistent service with HTTP endpoints.

    Collects snapshots on schedule, serves /metrics, /api/*, and web UI.
    """

    # Shared state
    latest_snapshots = {}  # database -> latest snap
    lock = threading.Lock()

    def collect_all():
        """Collect snapshots for all monitored databases."""
        for db in databases:
            try:
                snap = collect_snapshot(client, db)
                save_snapshot(snap)
                with lock:
                    latest_snapshots[db] = snap
                log.info("Collected %s: %s docs, forest=%s",
                         db, f"{snap['totals']['documents']:,}",
                         fmt_mb(snap['totals']['host_forest_mb']))
            except Exception as e:
                log.error("Error collecting %s: %s", db, e)

    def schedule_loop():
        """Run collection on a repeating interval."""
        while True:
            collect_all()
            # Prune old snapshots
            removed = prune_snapshots(retention_days)
            if removed:
                log.info("Pruned %d snapshot(s) older than %d days",
                         removed, retention_days)
            # Push to OTLP if configured
            if otlp_endpoint:
                with lock:
                    for db, snap in latest_snapshots.items():
                        try:
                            push_otlp(snap, otlp_endpoint)
                        except Exception as e:
                            log.error("OTLP push error: %s", e)
            threading.Event().wait(interval_sec)

    class MLCAHandler(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            log.info("HTTP %s", format % args)

        def _check_auth(self):
            """Return True if request is authorized, False otherwise."""
            if not api_token:
                return True
            # /health is always accessible (for load balancers / probes)
            path, _ = self._parse_request()
            if path == "/health":
                return True
            auth = self.headers.get("Authorization", "")
            if auth == f"Bearer {api_token}":
                return True
            self.send_response(401)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"error":"Unauthorized - provide Authorization: Bearer <token>"}')
            return False

        def _add_cors_headers(self):
            """Add CORS headers — restricted to same-origin when no token configured."""
            origin = self.headers.get("Origin", "")
            if api_token:
                # When auth is enabled, allow the requesting origin (token provides security)
                if origin:
                    self.send_header("Access-Control-Allow-Origin", origin)
                    self.send_header("Vary", "Origin")
            else:
                # No auth — only allow same-origin (no CORS header = browser blocks cross-origin)
                pass
            self.send_header("Access-Control-Allow-Methods",
                             "GET, POST, DELETE, OPTIONS")
            self.send_header("Access-Control-Allow-Headers",
                             "Content-Type, Authorization")

        def _parse_request(self):
            """Parse URL into path and query params."""
            from urllib.parse import urlparse, parse_qs
            parsed = urlparse(self.path)
            params = parse_qs(parsed.query)
            return parsed.path, params

        def _get_db_filter(self, params):
            """Extract optional database filter from query params."""
            vals = params.get("database", [])
            return vals[0] if vals else None

        def do_GET(self):
            if not self._check_auth():
                return
            path, params = self._parse_request()
            db_filter = self._get_db_filter(params)

            if path == "/metrics":
                self._serve_metrics()
            elif path == "/api/snapshot":
                self._serve_json_snapshot(db_filter)
            elif path == "/api/snapshots":
                self._serve_json_snapshots_list(db_filter)
            elif path == "/api/trend":
                self._serve_json_trend(db_filter)
            elif path == "/api/databases":
                self._serve_json_databases()
            elif path.startswith("/api/snapshot/"):
                filename = path[len("/api/snapshot/"):]
                self._serve_json_snapshot_file(filename)
            elif path == "/":
                self._serve_ui()
            elif path == "/health":
                self._respond(200, "application/json", '{"status":"ok"}')
            elif path == "/api/info":
                self._respond(200, "application/json",
                              json.dumps({"build": _BUILD}))
            else:
                self._respond(404, "text/plain", "Not Found")

        def do_POST(self):
            if not self._check_auth():
                return
            path, params = self._parse_request()
            if path == "/api/snapshot":
                self._handle_take_snapshot()
            else:
                self._respond(404, "text/plain", "Not Found")

        def do_DELETE(self):
            if not self._check_auth():
                return
            path, params = self._parse_request()
            if path.startswith("/api/snapshots/"):
                filename = path[len("/api/snapshots/"):]
                self._handle_delete_snapshot(filename)
            else:
                self._respond(404, "text/plain", "Not Found")

        def do_OPTIONS(self):
            """Handle CORS preflight for DELETE/POST."""
            self.send_response(200)
            self._add_cors_headers()
            self.end_headers()

        def _serve_metrics(self):
            with lock:
                all_metrics = []
                for db, snap in latest_snapshots.items():
                    all_metrics.append(snapshot_to_prometheus(snap))
            body = "\n".join(all_metrics) if all_metrics else \
                "# No data collected yet\n"
            self._respond(200,
                          "text/plain; version=0.0.4; charset=utf-8", body)

        def _serve_json_snapshot(self, db_filter=None):
            with lock:
                if db_filter and db_filter in latest_snapshots:
                    snap = latest_snapshots[db_filter]
                elif not db_filter and latest_snapshots:
                    snap = list(latest_snapshots.values())[0]
                elif db_filter:
                    # Collect on-demand for a database we aren't
                    # actively monitoring
                    try:
                        snap = collect_snapshot(client, db_filter)
                        latest_snapshots[db_filter] = snap
                    except Exception as e:
                        self._respond(500, "application/json",
                                      json.dumps({"error": str(e)}))
                        return
                else:
                    self._respond(503, "application/json",
                                  '{"error":"No data collected yet"}')
                    return
                self._respond(200, "application/json",
                              json.dumps(snap, indent=2, default=str))

        def _serve_json_snapshots_list(self, db_filter=None):
            all_snaps = load_snapshots(database=db_filter)
            summary = []
            for s in all_snaps:
                t = s.get("totals", {})
                summary.append({
                    "timestamp": s.get("timestamp"),
                    "database": s.get("database"),
                    "documents": t.get("documents", 0),
                    "forest_disk_mb": t.get("forest_disk_mb", 0),
                    "host_forest_mb": t.get("host_forest_mb", 0),
                    "host_rss_mb": t.get("host_rss_mb", 0),
                    "file": s.get("_file"),
                })
            self._respond(200, "application/json",
                          json.dumps(summary, indent=2, default=str))

        def _serve_json_trend(self, db_filter=None):
            all_snaps = load_snapshots(database=db_filter)
            points = []
            for s in all_snaps:
                t = s.get("totals", {})
                points.append({
                    "timestamp": s.get("timestamp"),
                    "database": s.get("database"),
                    "documents": t.get("documents", 0),
                    "forest_disk_mb": t.get("forest_disk_mb", 0),
                    "host_forest_mb": t.get("host_forest_mb", 0),
                    "host_rss_mb": t.get("host_rss_mb", 0),
                    "active_fragments": t.get("active_fragments", 0),
                    "deleted_fragments": t.get("deleted_fragments", 0),
                    "system_total_mb": t.get("system_total_mb", 0),
                    "host_cache_mb": t.get("host_cache_mb", 0),
                    "host_base_mb": t.get("host_base_mb", 0),
                    "host_file_mb": t.get("host_file_mb", 0),
                })
            self._respond(200, "application/json",
                          json.dumps(points, indent=2, default=str))

        def _serve_json_databases(self):
            _SYSTEM_DBS = {
                "App-Services", "Extensions", "Fab", "Last-Login",
                "Meters", "Modules", "Schemas", "Security", "Triggers",
            }
            db_names = set(latest_snapshots.keys())
            # Fetch all databases from the cluster
            try:
                results = client.eval_javascript(
                    'Array.from(xdmp.databases()).map('
                    'function(id){return xdmp.databaseName(id)})')
                if results and isinstance(results[0], list):
                    for name in results[0]:
                        db_names.add(name)
            except Exception:
                pass  # fall back to local knowledge
            # Also include databases from saved snapshots
            for s in load_snapshots():
                db_name = s.get("database")
                if db_name:
                    db_names.add(db_name)
            # Filter out system databases
            db_names -= _SYSTEM_DBS
            # Ensure Documents is first if present
            result = sorted(db_names)
            if "Documents" in result:
                result.remove("Documents")
                result.insert(0, "Documents")
            self._respond(200, "application/json",
                          json.dumps(result, default=str))

        def _serve_json_snapshot_file(self, filename):
            """Serve full snapshot JSON for a specific file."""
            if ".." in filename or "/" in filename \
                    or not filename.endswith(".json"):
                self._respond(400, "application/json",
                              '{"error":"Invalid filename"}')
                return
            fpath = SNAPSHOT_DIR / filename
            if not fpath.exists():
                self._respond(404, "application/json",
                              '{"error":"Snapshot not found"}')
                return
            try:
                with open(fpath) as f:
                    data = json.load(f)
                self._respond(200, "application/json",
                              json.dumps(data, indent=2, default=str))
            except (json.JSONDecodeError, OSError) as e:
                self._respond(500, "application/json",
                              json.dumps({"error": str(e)}))

        def _handle_delete_snapshot(self, filename):
            """Delete a snapshot file."""
            if ".." in filename or "/" in filename \
                    or not filename.endswith(".json"):
                self._respond(400, "application/json",
                              '{"error":"Invalid filename"}')
                return
            fpath = SNAPSHOT_DIR / filename
            if not fpath.exists():
                self._respond(404, "application/json",
                              '{"error":"Snapshot not found"}')
                return
            try:
                fpath.unlink()
                self._respond(200, "application/json",
                              '{"status":"deleted"}')
            except OSError as e:
                self._respond(500, "application/json",
                              json.dumps({"error": str(e)}))

        def _handle_take_snapshot(self):
            """Trigger an immediate snapshot collection."""
            try:
                content_len = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(content_len) if content_len else b"{}"
                req = json.loads(body) if body else {}
            except (json.JSONDecodeError, ValueError):
                req = {}

            db = req.get("database") or (
                list(latest_snapshots.keys())[0] if latest_snapshots
                else databases[0] if databases else "Documents"
            )

            try:
                snap = collect_snapshot(client, db)
                save_snapshot(snap)
                with lock:
                    latest_snapshots[db] = snap
                t = snap.get("totals", {})
                summary = {
                    "status": "ok",
                    "timestamp": snap.get("timestamp"),
                    "database": db,
                    "documents": t.get("documents", 0),
                }
                print(f"  [{datetime.now(timezone.utc).strftime('%H:%M:%S')}] "
                      f"Manual snapshot {db}: "
                      f"{t.get('documents', 0):,} docs")
                self._respond(200, "application/json",
                              json.dumps(summary, indent=2, default=str))
            except Exception as e:
                self._respond(500, "application/json",
                              json.dumps({"error": str(e)}))

        def _serve_ui(self):
            self._respond(200, "text/html", _WEB_UI_HTML)

        def _respond(self, code, content_type, body):
            self.send_response(code)
            self.send_header("Content-Type", content_type)
            self._add_cors_headers()
            self.end_headers()
            self.wfile.write(body.encode() if isinstance(body, str) else body)

    # Initial collection
    print(f"\n  MLCA Service starting on port {port}")
    print(f"  Monitoring: {', '.join(databases)}")
    print(f"  Collection interval: {interval_sec}s")
    print(f"  Endpoints:")
    print(f"    http://localhost:{port}/          Web UI")
    print(f"    http://localhost:{port}/metrics   Prometheus metrics")
    print(f"    http://localhost:{port}/api/      JSON API")
    print(f"    http://localhost:{port}/health    Health check")
    if otlp_endpoint:
        print(f"  OTLP push: {otlp_endpoint}")
    print()

    collect_all()

    # Start background collector
    collector = threading.Thread(target=schedule_loop, daemon=True)
    collector.start()

    # Start HTTP server
    server = HTTPServer(("", port), MLCAHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  MLCA Service stopped.")
        server.shutdown()



_WEB_UI_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>MLCA — MarkLogic Capacity Analysis</title>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body { background: #0d1117; color: #c9d1d9; font-family: 'SF Mono', 'Consolas', monospace; font-size: 14px; }
.container { max-width: 1200px; margin: 0 auto; padding: 20px; }
h1 { color: #58a6ff; font-size: 20px; margin-bottom: 4px; }
h3 { color: #58a6ff; font-size: 16px; margin: 24px 0 12px; border-bottom: 1px solid #21262d; padding-bottom: 8px; }

.header { display: flex; align-items: center; gap: 16px; margin-bottom: 4px; flex-wrap: wrap; }
.header h1 { margin-bottom: 0; }
.db-select { background: #161b22; color: #c9d1d9; border: 1px solid #30363d; border-radius: 6px;
  padding: 6px 10px; font-family: inherit; font-size: 13px; cursor: pointer; }
.db-select:focus { border-color: #58a6ff; outline: none; }
#subtitle { color: #8b949e; font-size: 14px; margin-bottom: 12px; }

.tabs { display: flex; gap: 0; border-bottom: 1px solid #21262d; margin-bottom: 20px; }
.tab { padding: 10px 20px; cursor: pointer; color: #8b949e; border-bottom: 2px solid transparent;
  font-family: inherit; font-size: 14px; background: none; border-top: none; border-left: none; border-right: none; }
.tab:hover { color: #c9d1d9; }
.tab.active { color: #58a6ff; border-bottom-color: #58a6ff; }
.tab-content { display: none; }
.tab-content.active { display: block; }

.hero { display: flex; gap: 20px; margin-bottom: 24px; flex-wrap: wrap; }
.hero-card { flex: 1; min-width: 200px; background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 20px; text-align: center; }
.hero-card.warn { border-color: #d29922; }
.hero-card.crit { border-color: #f85149; }
.hero-value { font-size: 36px; font-weight: bold; color: #58a6ff; }
.hero-card.warn .hero-value { color: #d29922; }
.hero-card.crit .hero-value { color: #f85149; }
.hero-label { color: #8b949e; font-size: 12px; margin-top: 4px; }

.bar-container { background: #21262d; border-radius: 4px; height: 20px; margin: 4px 0; }
.bar-fill { height: 100%; border-radius: 4px; transition: width 0.5s; }
.bar-fill.green { background: #3fb950; }
.bar-fill.yellow { background: #d29922; }
.bar-fill.red { background: #f85149; }

.grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(350px, 1fr)); gap: 16px; }
.card { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 16px; }
.card-title { color: #58a6ff; font-size: 13px; margin-bottom: 8px; }

.metric { display: flex; justify-content: space-between; padding: 4px 0; border-bottom: 1px solid #21262d; }
.metric:last-child { border-bottom: none; }
.metric-key { color: #8b949e; }
.metric-val { color: #c9d1d9; font-weight: bold; }
.metric-val.good { color: #3fb950; }
.metric-val.warn { color: #d29922; }
.metric-val.crit { color: #f85149; }

table { width: 100%; border-collapse: collapse; font-size: 13px; }
th { text-align: left; color: #58a6ff; border-bottom: 1px solid #30363d; padding: 6px 8px; }
td { padding: 6px 8px; border-bottom: 1px solid #21262d; }
td.num { text-align: right; font-variant-numeric: tabular-nums; }

.btn { background: #21262d; color: #c9d1d9; border: 1px solid #30363d; border-radius: 6px;
  padding: 5px 12px; cursor: pointer; font-family: inherit; font-size: 12px; }
.btn:hover { background: #30363d; border-color: #8b949e; }
.btn-primary { background: #238636; border-color: #238636; color: #fff; }
.btn-primary:hover { background: #2ea043; }
.btn-danger { color: #f85149; }
.btn-danger:hover { background: #da3633; color: #fff; border-color: #da3633; }
.btn:disabled { opacity: 0.5; cursor: not-allowed; }

.snap-toolbar { display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px; }

.chart-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
.chart-card { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 16px; }
.chart-card canvas { width: 100%; height: 220px; }
.chart-title { color: #58a6ff; font-size: 13px; margin-bottom: 10px; }
.chart-tooltip { position: fixed; background: #161b22; border: 1px solid #58a6ff; border-radius: 6px;
  padding: 8px 12px; font-size: 12px; pointer-events: none; z-index: 100; display: none; color: #c9d1d9; }

.modal-overlay { position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0,0,0,0.7);
  z-index: 200; display: none; justify-content: center; align-items: center; }
.modal-overlay.show { display: flex; }
.modal { background: #161b22; border: 1px solid #30363d; border-radius: 8px; max-width: 800px;
  width: 90%; max-height: 80vh; display: flex; flex-direction: column; }
.modal-header { display: flex; justify-content: space-between; align-items: center; padding: 16px;
  border-bottom: 1px solid #21262d; }
.modal-header h3 { margin: 0; border: none; padding: 0; }
.modal-close { background: none; border: none; color: #8b949e; font-size: 20px; cursor: pointer; padding: 4px 8px; }
.modal-close:hover { color: #c9d1d9; }
.modal-body { padding: 16px; overflow-y: auto; flex: 1; }
.modal-body pre { white-space: pre-wrap; word-break: break-all; font-size: 12px; line-height: 1.5; }

.alert-banner { background: #3d1a1a; border: 1px solid #f85149; border-radius: 8px;
  padding: 12px 16px; margin-bottom: 16px; color: #f85149; font-size: 13px;
  display: flex; align-items: center; gap: 10px; }
.alert-banner .alert-icon { font-size: 18px; flex-shrink: 0; }
.alert-banner.warn { background: #2d2008; border-color: #d29922; color: #d29922; }

.footer { margin-top: 24px; color: #484f58; font-size: 12px; text-align: center; }
.loading { color: #8b949e; text-align: center; padding: 40px; }

@media (max-width: 768px) {
  .chart-grid { grid-template-columns: 1fr; }
  .hero { flex-direction: column; }
}
</style>
</head>
<body>
<div class="container">
  <div class="header">
    <h1>MLCA</h1>
    <select id="dbSelect" class="db-select" title="Select database"></select>
  </div>
  <div id="subtitle">Loading...</div>

  <div id="alert-banner" style="display:none" class="alert-banner">
    <span class="alert-icon">&#9888;</span>
    <span id="alert-text"></span>
  </div>

  <div class="tabs">
    <button class="tab active" data-tab="dashboard">Dashboard</button>
    <button class="tab" data-tab="trends">Trends</button>
    <button class="tab" data-tab="snapshots">Snapshots</button>
  </div>

  <div id="tab-dashboard" class="tab-content active">
    <div class="hero" id="hero"></div>
    <div class="grid" id="grid"></div>
    <div id="projections-section" style="display:none">
      <h3>Capacity Projections</h3>
      <div class="grid" id="projections"></div>
    </div>
    <h3>Index Memory Usage</h3>
    <div class="card" id="indexes"><div class="loading">Loading...</div></div>
  </div>

  <div id="tab-trends" class="tab-content">
    <div class="chart-grid" id="charts">
      <div class="chart-card"><div class="chart-title">Documents</div><canvas id="chart-docs"></canvas></div>
      <div class="chart-card"><div class="chart-title">Forest Memory</div><canvas id="chart-forest"></canvas></div>
      <div class="chart-card"><div class="chart-title">Disk Usage</div><canvas id="chart-disk"></canvas></div>
      <div class="chart-card"><div class="chart-title">Memory Ceiling %</div><canvas id="chart-ceiling"></canvas></div>
    </div>
    <div id="trend-empty" class="loading" style="display:none">Not enough snapshots for trend charts. Take at least 2 snapshots.</div>
  </div>

  <div id="tab-snapshots" class="tab-content">
    <div class="snap-toolbar">
      <h3 style="margin:0;border:none;padding:0">Saved Snapshots</h3>
      <button class="btn btn-primary" id="takeSnapshotBtn" onclick="takeSnapshot()">Take Snapshot</button>
    </div>
    <div class="card" id="snap-list"><div class="loading">Loading...</div></div>
  </div>

  <div class="footer">MLCA &mdash; refreshes every 30s &mdash; build <span id="build-sha">...</span></div>
</div>

<div class="modal-overlay" id="modal">
  <div class="modal">
    <div class="modal-header">
      <h3 id="modal-title">Snapshot Detail</h3>
      <button class="modal-close" onclick="closeModal()">&times;</button>
    </div>
    <div class="modal-body"><pre id="modal-body"></pre></div>
  </div>
</div>

<div class="chart-tooltip" id="tooltip"></div>

<script>
var selectedDb = null;
var activeTab = 'dashboard';

function fmt(mb) {
  if (mb == null) return 'N/A';
  mb = Number(mb);
  if (mb >= 1024) return (mb/1024).toFixed(2) + ' GB';
  return mb.toFixed(1) + ' MB';
}
function fmtNum(n) { return (n||0).toLocaleString(); }
function barClass(p) { return p >= 90 ? 'red' : p >= 70 ? 'yellow' : 'green'; }
function renderBar(v) {
  return '<div class="bar-container"><div class="bar-fill ' + barClass(v) +
         '" style="width:' + Math.min(100, v) + '%"></div></div>';
}
function metric(key, val, cls) {
  return '<div class="metric"><span class="metric-key">' + key +
         '</span><span class="metric-val' + (cls ? ' '+cls : '') + '">' + val + '</span></div>';
}
function dbParam() { return selectedDb ? '?database=' + encodeURIComponent(selectedDb) : ''; }

// Tabs
document.querySelectorAll('.tab').forEach(function(tab) {
  tab.addEventListener('click', function() {
    document.querySelectorAll('.tab').forEach(function(t) { t.classList.remove('active'); });
    document.querySelectorAll('.tab-content').forEach(function(c) { c.classList.remove('active'); });
    tab.classList.add('active');
    activeTab = tab.dataset.tab;
    document.getElementById('tab-' + activeTab).classList.add('active');
    refreshActiveTab();
  });
});

// Database selector
async function loadDatabases() {
  try {
    var dbs = await (await fetch('/api/databases')).json();
    var sel = document.getElementById('dbSelect');
    sel.innerHTML = '';
    dbs.forEach(function(db) {
      var opt = document.createElement('option');
      opt.value = db; opt.textContent = db;
      if (db === selectedDb) opt.selected = true;
      sel.appendChild(opt);
    });
    if (!selectedDb && dbs.length > 0) selectedDb = dbs[0];
  } catch(e) {}
}
document.getElementById('dbSelect').addEventListener('change', function() {
  selectedDb = this.value;
  refreshActiveTab();
});

// Dashboard
async function refreshDashboard() {
  try {
    var snap = await (await fetch('/api/snapshot' + dbParam())).json();
    if (snap.error) { document.getElementById('subtitle').textContent = snap.error; return; }
    var t = snap.totals || {};
    var db = snap.database || '?';
    var ts = (snap.timestamp || '').substring(0,19).replace('T',' ');
    document.getElementById('subtitle').textContent = db + ' \\u2014 ' + ts;

    var sysTot = t.system_total_mb || 0;
    var cache = t.host_cache_mb || 0, base = t.host_base_mb || 0;
    var file = t.host_file_mb || 0, forest = t.host_forest_mb || 0;
    var fixed = cache + base + file;
    var ceiling = sysTot * 0.8;
    var headroom = ceiling - fixed - forest;
    var memPct = ceiling > 0 ? ((fixed + forest) / ceiling * 100) : 0;

    // Severity helpers for hero cards
    var memCard = memPct >= 90 ? 'crit' : memPct >= 70 ? 'warn' : '';
    var headroomCard = headroom < 512 ? 'crit' : headroom < 1024 ? 'warn' : '';

    var heroHTML = '';
    heroHTML += '<div class="hero-card"><div class="hero-value">' +
                fmtNum(t.documents) + '</div><div class="hero-label">Documents</div></div>';
    heroHTML += '<div class="hero-card"><div class="hero-value">' +
                fmt(forest) + '</div><div class="hero-label">Forest Memory</div></div>';
    heroHTML += '<div class="hero-card ' + headroomCard + '"><div class="hero-value">' +
                fmt(headroom) + '</div><div class="hero-label">Memory Headroom</div></div>';
    heroHTML += '<div class="hero-card ' + memCard + '"><div class="hero-value">' +
                memPct.toFixed(1) + '%</div><div class="hero-label">Memory Ceiling</div>' +
                renderBar(memPct) + '</div>';
    document.getElementById('hero').innerHTML = heroHTML;

    // Alert banner
    var banner = document.getElementById('alert-banner');
    var alertText = document.getElementById('alert-text');
    if (headroom <= 0) {
      banner.className = 'alert-banner';
      alertText.textContent = 'CRITICAL: Memory ceiling exceeded — ' + db +
        ' is using ' + memPct.toFixed(1) + '% of the 80% ML guardrail. Risk of OOM.';
      banner.style.display = 'flex';
    } else if (memPct >= 90) {
      banner.className = 'alert-banner';
      alertText.textContent = 'WARNING: Memory at ' + memPct.toFixed(1) + '% of ceiling — only ' +
        fmt(headroom) + ' headroom remaining in ' + db + '.';
      banner.style.display = 'flex';
    } else if (memPct >= 70) {
      banner.className = 'alert-banner warn';
      alertText.textContent = 'Memory at ' + memPct.toFixed(1) + '% of ceiling — ' +
        fmt(headroom) + ' headroom remaining.';
      banner.style.display = 'flex';
    } else {
      banner.style.display = 'none';
    }

    var grid = '';
    // Identify dominant growth driver (base_mb typically grows fastest with data)
    var baseNote = base > forest * 1.5 ? ' \u2605 primary driver' : '';
    grid += '<div class="card"><div class="card-title">Memory Breakdown</div>' +
      metric('Cache (list+tree)', fmt(cache)) +
      metric('Forest stands', fmt(forest)) +
      metric('Forest disk / host-size' + baseNote, fmt(base), baseNote ? 'warn' : '') +
      metric('File cache', fmt(file)) +
      metric('Fixed total', fmt(fixed)) +
      metric('Ceiling (80% RAM)', fmt(ceiling)) +
      metric('Headroom', fmt(headroom), headroom < 512 ? 'crit' : headroom < 1024 ? 'warn' : 'good') +
    '</div>';

    var hosts = snap.hosts || [];
    if (hosts.length > 0) {
      var h = hosts[0];
      var rss = h['memory-process-rss-mb'] || 0;
      var swap = h['memory-process-swap-mb'] || 0;
      grid += '<div class="card"><div class="card-title">Host: ' + (h.hostname||'?') + '</div>' +
        metric('System RAM', fmt(h['memory-system-total-mb'])) +
        metric('ML RSS', fmt(rss)) +
        metric('RSS peak', fmt(h['memory-process-rss-hwm-mb'])) +
        metric('Swap', fmt(swap), swap > 0 ? 'crit' : 'good') +
        metric('Page-out rate', (h['memory-system-pageout-rate']||0).toFixed(1) + ' MB/s') +
      '</div>';
    }

    var dbStat = snap.database_status || {};
    var diskUsed = t.forest_disk_mb || 0;
    var diskRemaining = Number(dbStat.least_remaining_mb || 0);
    var diskTotal = diskUsed + diskRemaining;
    var diskPct = diskTotal > 0 ? (diskUsed / diskTotal * 100) : 0;
    grid += '<div class="card"><div class="card-title">Disk</div>' +
      metric('Data on disk', fmt(diskUsed)) +
      metric('Remaining', fmt(diskRemaining)) +
      metric('Utilization', diskPct.toFixed(1) + '%') + renderBar(diskPct) +
      (t.documents > 0 ? metric('Bytes/doc', Math.round(diskUsed*1048576/t.documents).toLocaleString()) : '') +
    '</div>';

    var active = t.active_fragments || 0, deleted = t.deleted_fragments || 0;
    var fragTotal = active + deleted;
    var fragPct = fragTotal > 0 ? (deleted/fragTotal*100) : 0;
    grid += '<div class="card"><div class="card-title">Fragments</div>' +
      metric('Active', fmtNum(active)) +
      metric('Deleted', fmtNum(deleted), deleted > 0 ? 'warn' : '') +
      metric('Fragmentation', fragPct.toFixed(1) + '%', fragPct > 25 ? 'crit' : fragPct > 10 ? 'warn' : 'good') +
      (fragPct >= 25 ? renderBar(fragPct) : '') +
    '</div>';
    document.getElementById('grid').innerHTML = grid;

    // Index table
    var im = snap.index_memory || {};
    var indexes = im.indexes || [];
    if (indexes.length > 0) {
      indexes.sort(function(a,b) { return (b.totalMemoryBytes||0) - (a.totalMemoryBytes||0); });
      var tbl = '<table><tr><th>Index</th><th>Type</th><th>Memory</th><th>Disk</th></tr>';
      indexes.forEach(function(i) {
        var name = i.localname || i.pathExpression || i.indexType || '?';
        var mem = i.totalMemoryBytes || 0;
        var disk = i.totalOnDiskBytes || 0;
        var memStr = mem > 0 ? fmt(mem/1048576) : '<span style="color:#484f58">not cached</span>';
        var diskStr = disk > 0 ? fmt(disk/1048576) : '<span style="color:#484f58">not cached</span>';
        tbl += '<tr><td>' + name + '</td><td>' + (i.scalarType||i.indexType||'') +
               '</td><td class="num">' + memStr +
               '</td><td class="num">' + diskStr + '</td></tr>';
      });
      tbl += '</table>';

      var sums = (im.standSummaries || []);
      if (sums.length > 0) {
        var totalRange = 0;
        sums.forEach(function(s) { totalRange += (s.summary||{}).rangeIndexesBytes || 0; });
        if (totalRange > 0) {
          tbl += '<div style="margin-top:12px;padding:8px;background:#21262d;border-radius:4px">' +
            '<span class="metric-key">Total range index memory (stand-level): </span>' +
            '<span class="metric-val" style="color:#58a6ff">' + fmt(totalRange/1048576) + '</span>' +
            '<div style="color:#484f58;font-size:12px;margin-top:4px">' +
            'Per-index values show cache-warmed data only. Stand-level total includes all resident pages. ' +
            'Use --index-impact for measured per-index costs.</div></div>';
        }
      }

      document.getElementById('indexes').innerHTML = tbl;
    } else {
      document.getElementById('indexes').innerHTML = '<div class="loading">No index memory data available</div>';
    }

    // Capacity projections from trend data
    // Methodology: use linear regression on disk (stable, monotonic) as
    // primary growth signal.  Forest memory is volatile due to ML stand
    // merges/flushes, so we use regression to smooth it.  Per-doc cost
    // uses marginal cost from recent snapshots (amortisation means average
    // cost drops as the collection grows — marginal is more predictive).
    try {
      var trend = await (await fetch('/api/trend' + dbParam())).json();
      var projSec = document.getElementById('projections-section');

      // ── Improved dedup: for each run of identical doc counts, keep first
      // and last snapshot. "First" captures the moment loading stopped;
      // "last" captures the settled (post-merge) state. This preserves both
      // the inflection point and the steady-state value for regression quality.
      var dedupTrend = [];
      var i = 0;
      while (i < trend.length) {
        var runStart = i;
        while (i < trend.length && trend[i].documents === trend[runStart].documents) i++;
        dedupTrend.push(trend[runStart]);
        if (i - 1 > runStart) dedupTrend.push(trend[i - 1]);
      }
      var rawCount = trend.length;
      trend = dedupTrend;

      if (trend.length >= 3) {
        var first = trend[0], last = trend[trend.length - 1];
        var t0 = new Date(first.timestamp).getTime(), t1 = new Date(last.timestamp).getTime();
        var spanDays = (t1 - t0) / 86400000;

        // Linear regression helper: returns {slope, intercept, r2}
        function linReg(xs, ys) {
          var n = xs.length, sx = 0, sy = 0, sxy = 0, sx2 = 0, sy2 = 0;
          for (var i = 0; i < n; i++) {
            sx += xs[i]; sy += ys[i]; sxy += xs[i]*ys[i];
            sx2 += xs[i]*xs[i]; sy2 += ys[i]*ys[i];
          }
          var denom = n*sx2 - sx*sx;
          if (denom === 0) return { slope: 0, intercept: 0, r2: 0 };
          var slope = (n*sxy - sx*sy) / denom;
          var intercept = (sy - slope*sx) / n;
          var ssRes = 0, ssTot = 0, yMean = sy/n;
          for (var i = 0; i < n; i++) {
            var yHat = slope*xs[i] + intercept;
            ssRes += (ys[i]-yHat)*(ys[i]-yHat);
            ssTot += (ys[i]-yMean)*(ys[i]-yMean);
          }
          return { slope: slope, intercept: intercept, r2: ssTot > 0 ? 1 - ssRes/ssTot : 0 };
        }

        // Build regression inputs
        var xDocs = trend.map(function(p) { return p.documents; });
        var yDisk = trend.map(function(p) { return p.forest_disk_mb; });
        var yForest = trend.map(function(p) { return p.host_forest_mb; });
        // Total non-cache memory = forest + base + file (all grow with data)
        var yTotalMem = trend.map(function(p) {
          return (p.host_forest_mb||0) + (p.host_base_mb||0) + (p.host_file_mb||0);
        });

        // ── Distinct doc levels (regression quality signal) ──
        var distinctLevels = new Set(xDocs).size;

        // ── Forest spike detection: exclude mid-merge snapshots from regression ──
        // A merge spike occurs when forest_mb is substantially above the doc-level
        // expectation. We detect it by comparing each point to the overall trend:
        // sort forest values, use median as baseline, flag points > 1.5× median.
        var sortedForest = yForest.slice().sort(function(a,b){return a-b;});
        var medForest = sortedForest[Math.floor(sortedForest.length / 2)];
        var spikeThreshold = medForest * 1.6;
        var nonSpike = trend.filter(function(p) {
          return (p.host_forest_mb||0) <= spikeThreshold;
        });
        var spikeCount = trend.length - nonSpike.length;
        // Use spike-filtered set for regression if we still have enough points
        var regPoints = nonSpike.length >= 3 ? nonSpike : trend;
        var xDocsR = regPoints.map(function(p) { return p.documents; });
        var yDiskR  = regPoints.map(function(p) { return p.forest_disk_mb; });
        var yTotalMemR = regPoints.map(function(p) {
          return (p.host_forest_mb||0) + (p.host_base_mb||0) + (p.host_file_mb||0);
        });

        // Regression: docs as x-axis (stable, monotonic, independent of loading rate)
        var diskReg = linReg(xDocsR, yDiskR);
        var totalMemReg = linReg(xDocsR, yTotalMemR);

        // Marginal cost per doc from regression slope
        var marginalMemBytes = totalMemReg.slope * 1048576;  // MB/doc -> bytes/doc
        var marginalDiskBytes = diskReg.slope * 1048576;

        // Time-based rates using regression on time
        var xTimes = trend.map(function(p) { return (new Date(p.timestamp).getTime() - t0) / 86400000; });
        var diskTimeReg = linReg(xTimes, yDisk);
        var docsTimeReg = linReg(xTimes, xDocs);
        var totalMemTimeReg = linReg(xTimes, yTotalMem);

        // Current memory state
        var curBase = last.host_base_mb || 0;
        var curFile = last.host_file_mb || 0;
        var effectiveHeadroom = ceiling - cache - curBase - curFile - forest;

        // Warn if data spans a very short window (bulk loading skews time-based rates)
        var shortWindow = spanDays < 0.5;  // less than 12 hours

        var proj = '';

        // ── Memory capacity: doc-based (primary — immune to loading rate) ──
        if (marginalMemBytes > 0 && effectiveHeadroom > 0) {
          var docsUntilCeiling = Math.round((effectiveHeadroom * 1048576) / marginalMemBytes);
          var memConfidence = totalMemReg.r2;
          var memConfLabel = memConfidence > 0.9 ? 'high' : memConfidence > 0.7 ? 'medium' : 'low';
          var memConfClass = memConfidence > 0.9 ? 'good' : memConfidence > 0.7 ? 'warn' : 'crit';
          var docClass = docsUntilCeiling < 500000 ? 'crit' : docsUntilCeiling < 2000000 ? 'warn' : 'good';

          proj += '<div class="card"><div class="card-title">Memory Capacity</div>' +
            metric('Current headroom', fmt(effectiveHeadroom)) +
            metric('Marginal memory/doc', Math.round(marginalMemBytes).toLocaleString() + ' bytes') +
            metric('Docs until ceiling', '<span class="metric-val ' + docClass + '">' + fmtNum(docsUntilCeiling) + '</span>', '');

          // Time-based ETA: only show if window is long enough AND growth is positive
          if (!shortWindow && totalMemTimeReg.slope > 0) {
            var daysUntilMem = effectiveHeadroom / totalMemTimeReg.slope;
            var etaDate = new Date(Date.now() + daysUntilMem * 86400000);
            var etaStr = etaDate.getFullYear() + '-' + String(etaDate.getMonth()+1).padStart(2,'0') + '-' + String(etaDate.getDate()).padStart(2,'0');
            var runwayClass = daysUntilMem < 30 ? 'crit' : daysUntilMem < 90 ? 'warn' : 'good';
            proj += metric('Days until ceiling', '<span class="metric-val ' + runwayClass + '">' + Math.round(daysUntilMem) + ' days</span>', '') +
                    metric('ETA', etaStr);
          } else if (shortWindow) {
            proj += metric('Time-based ETA', '<span style="color:#8b949e">Need &gt;12h of data</span>');
          }

          proj += metric('Regression fit (R\u00b2)', '<span class="metric-val ' + memConfClass + '">' + memConfLabel + ' (' + memConfidence.toFixed(2) + ')</span>', '');
          // Data quality indicators
          var dataQuality = rawCount + ' snapshots \u2192 ' + trend.length + ' deduped, ' + distinctLevels + ' distinct doc level' + (distinctLevels !== 1 ? 's' : '');
          if (spikeCount > 0) dataQuality += ', ' + spikeCount + ' merge spike' + (spikeCount !== 1 ? 's' : '') + ' excluded';
          proj += metric('Data quality', '<span style="color:#8b949e">' + dataQuality + '</span>');
          if (distinctLevels < 3) {
            proj += '<div style="color:#d29922;font-size:11px;padding:4px 0">Low distinct doc levels — add more snapshots across different doc counts for better regression quality.</div>';
          }
          proj += metric('Window', spanDays < 1 ? (spanDays*24).toFixed(1) + 'h' : spanDays.toFixed(1) + 'd') + '</div>';

          // ── Model accuracy: track projected ceiling vs reality ──
          var projKey = 'mlca_proj_' + (selectedDb || 'Documents');
          var projCeiling = last.documents + docsUntilCeiling;
          // If we are past a previously stored ceiling, show accuracy
          var storedProj = null;
          try { storedProj = JSON.parse(localStorage.getItem(projKey)); } catch(e) {}
          if (storedProj && storedProj.ceiling && last.documents >= storedProj.ceiling) {
            var overBy = last.documents - storedProj.ceiling;
            var pctOff = Math.abs(overBy / storedProj.ceiling * 100);
            proj += '<div class="card" style="border-color:#3fb950"><div class="card-title">Model Accuracy</div>' +
              metric('Projected ceiling', fmtNum(storedProj.ceiling) + ' docs') +
              metric('Actual at ceiling', fmtNum(last.documents) + ' docs') +
              metric('Error', (overBy >= 0 ? '+' : '') + fmtNum(overBy) + ' (' + pctOff.toFixed(1) + '% ' + (overBy >= 0 ? 'optimistic' : 'pessimistic') + ')') +
              '</div>';
          }
          // Always persist current projection for future comparison
          localStorage.setItem(projKey, JSON.stringify({ ceiling: projCeiling, ts: Date.now() }));
        }

        // ── Disk capacity ──
        var diskRemain = Number((snap.database_status || {}).least_remaining_mb || 0);
        if (marginalDiskBytes > 0 && diskRemain > 0) {
          var docsUntilDiskFull = Math.round((diskRemain * 1048576) / marginalDiskBytes);
          var diskDocClass = docsUntilDiskFull < 500000 ? 'crit' : docsUntilDiskFull < 2000000 ? 'warn' : 'good';
          proj += '<div class="card"><div class="card-title">Disk Capacity</div>' +
            metric('Remaining', fmt(diskRemain)) +
            metric('Disk/doc (regressed)', Math.round(marginalDiskBytes).toLocaleString() + ' bytes') +
            metric('Docs until full', '<span class="metric-val ' + diskDocClass + '">' + fmtNum(docsUntilDiskFull) + '</span>', '');

          if (!shortWindow && diskTimeReg.slope > 0) {
            var daysUntilDisk = diskRemain / diskTimeReg.slope;
            var diskEta = new Date(Date.now() + daysUntilDisk * 86400000);
            var diskEtaStr = diskEta.getFullYear() + '-' + String(diskEta.getMonth()+1).padStart(2,'0') + '-' + String(diskEta.getDate()).padStart(2,'0');
            var diskClass = daysUntilDisk < 30 ? 'crit' : daysUntilDisk < 90 ? 'warn' : 'good';
            proj += metric('Days until full', '<span class="metric-val ' + diskClass + '">' + Math.round(daysUntilDisk) + ' days</span>', '') +
                    metric('ETA', diskEtaStr);
          } else if (shortWindow) {
            proj += metric('Time-based ETA', '<span style="color:#8b949e">Need &gt;12h of data</span>');
          }

          proj += metric('Regression fit (R\u00b2)', diskReg.r2.toFixed(2)) + '</div>';
        }

        // ── Document growth rate ──
        if (docsTimeReg.slope > 0) {
          var growthNote = shortWindow ? ' <span style="color:#8b949e;font-size:11px">(bulk load window)</span>' : '';
          proj += '<div class="card"><div class="card-title">Document Growth</div>' +
            metric('Current count', fmtNum(last.documents)) +
            metric('Growth rate (regressed)', fmtNum(Math.round(docsTimeReg.slope)) + '/day' + growthNote) +
            metric('Total growth observed', '+' + fmtNum(last.documents - first.documents)) +
            '</div>';
        }

        if (proj) {
          document.getElementById('projections').innerHTML = proj;
          projSec.style.display = 'block';
        } else { projSec.style.display = 'none'; }
      } else { projSec.style.display = 'none'; }
    } catch(e2) { /* projections are optional */ }
  } catch(e) {
    document.getElementById('subtitle').textContent = 'Error: ' + e.message;
  }
}

// Snapshots tab
async function refreshSnapshots() {
  try {
    var snaps = await (await fetch('/api/snapshots' + dbParam())).json();
    if (snaps.length === 0) {
      document.getElementById('snap-list').innerHTML = '<div class="loading">No snapshots found</div>';
      return;
    }
    snaps.reverse();
    var tbl = '<table><tr><th>#</th><th>Timestamp</th><th>Database</th>' +
              '<th style="text-align:right">Documents</th><th style="text-align:right">Forest Disk</th>' +
              '<th style="text-align:right">RSS</th><th>Actions</th></tr>';
    snaps.forEach(function(s, i) {
      var ts = (s.timestamp||'').substring(0,19).replace('T',' ');
      var file = (s.file || '').replace(/[^A-Za-z0-9_.\\-]/g, '');
      tbl += '<tr><td>' + (snaps.length - i) + '</td><td>' + ts + '</td><td>' + (s.database||'') + '</td>' +
             '<td class="num">' + fmtNum(s.documents) + '</td>' +
             '<td class="num">' + fmt(s.forest_disk_mb) + '</td>' +
             '<td class="num">' + fmt(s.host_rss_mb) + '</td>' +
             '<td><button class="btn" onclick="viewSnapshot(\\'' + file + '\\')">View</button> ' +
             '<button class="btn btn-danger" onclick="deleteSnapshot(\\'' + file + '\\')">Delete</button></td></tr>';
    });
    tbl += '</table>';
    document.getElementById('snap-list').innerHTML = tbl;
  } catch(e) {
    document.getElementById('snap-list').innerHTML = '<div class="loading">Error: ' + e.message + '</div>';
  }
}

async function viewSnapshot(filename) {
  try {
    var data = await (await fetch('/api/snapshot/' + encodeURIComponent(filename))).json();
    document.getElementById('modal-title').textContent = filename;
    document.getElementById('modal-body').textContent = JSON.stringify(data, null, 2);
    document.getElementById('modal').classList.add('show');
  } catch(e) { alert('Failed to load snapshot: ' + e.message); }
}

function closeModal() { document.getElementById('modal').classList.remove('show'); }
document.addEventListener('keydown', function(e) { if (e.key === 'Escape') closeModal(); });
document.getElementById('modal').addEventListener('click', function(e) {
  if (e.target === this) closeModal();
});

async function deleteSnapshot(filename) {
  if (!confirm('Delete snapshot ' + filename + '?')) return;
  try {
    var resp = await fetch('/api/snapshots/' + encodeURIComponent(filename), { method: 'DELETE' });
    if (resp.ok) refreshSnapshots();
    else alert('Delete failed: ' + (await resp.json()).error);
  } catch(e) { alert('Delete error: ' + e.message); }
}

async function takeSnapshot() {
  var btn = document.getElementById('takeSnapshotBtn');
  btn.disabled = true; btn.textContent = 'Collecting...';
  try {
    var body = selectedDb ? JSON.stringify({database: selectedDb}) : '{}';
    var resp = await fetch('/api/snapshot', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: body
    });
    var result = await resp.json();
    if (resp.ok) {
      refreshSnapshots();
      loadDatabases();
    } else {
      alert('Snapshot failed: ' + (result.error || 'Unknown error'));
    }
  } catch(e) { alert('Snapshot error: ' + e.message); }
  btn.disabled = false; btn.textContent = 'Take Snapshot';
}

// Charts
function drawChart(canvasId, points, yKey, color, yFmt) {
  var canvas = document.getElementById(canvasId);
  if (!canvas) return;
  var rect = canvas.parentElement.getBoundingClientRect();
  var dpr = window.devicePixelRatio || 1;
  canvas.width = (rect.width - 32) * dpr;
  canvas.height = 220 * dpr;
  canvas.style.width = (rect.width - 32) + 'px';
  canvas.style.height = '220px';
  var ctx = canvas.getContext('2d');
  ctx.scale(dpr, dpr);
  var W = rect.width - 32, H = 220;
  var pad = {top: 10, right: 16, bottom: 35, left: 65};
  var plotW = W - pad.left - pad.right, plotH = H - pad.top - pad.bottom;

  if (points.length < 2) return;

  var vals = points.map(function(p) { return p[yKey] || 0; });
  var times = points.map(function(p) { return new Date(p.timestamp).getTime(); });
  var yMin = Math.min.apply(null, vals), yMax = Math.max.apply(null, vals);
  if (yMin === yMax) { yMin = yMin * 0.9; yMax = yMax * 1.1 || 1; }
  var yRange = yMax - yMin;
  var tMin = Math.min.apply(null, times), tMax = Math.max.apply(null, times);
  var tRange = tMax - tMin || 1;

  function xPos(t) { return pad.left + (t - tMin) / tRange * plotW; }
  function yPos(v) { return pad.top + plotH - (v - yMin) / yRange * plotH; }

  ctx.fillStyle = '#161b22';
  ctx.fillRect(0, 0, W, H);

  // Gridlines and Y labels
  ctx.strokeStyle = '#21262d'; ctx.lineWidth = 1;
  var ySteps = 5;
  for (var i = 0; i <= ySteps; i++) {
    var yy = pad.top + plotH * i / ySteps;
    ctx.beginPath(); ctx.moveTo(pad.left, yy); ctx.lineTo(W - pad.right, yy); ctx.stroke();
    ctx.fillStyle = '#8b949e'; ctx.font = '11px monospace'; ctx.textAlign = 'right';
    var yVal = yMax - yRange * i / ySteps;
    ctx.fillText(yFmt ? yFmt(yVal) : yVal.toFixed(0), pad.left - 6, yy + 4);
  }

  // X labels
  ctx.textAlign = 'center'; ctx.fillStyle = '#8b949e';
  var xSteps = Math.min(points.length - 1, 6);
  for (var i = 0; i <= xSteps; i++) {
    var idx = Math.round(i * (points.length - 1) / xSteps);
    var d = new Date(points[idx].timestamp);
    var label = (d.getMonth()+1) + '/' + d.getDate() + ' ' +
      String(d.getHours()).padStart(2,'0') + ':' + String(d.getMinutes()).padStart(2,'0');
    ctx.fillText(label, xPos(times[idx]), H - 5);
  }

  // Line
  ctx.strokeStyle = color; ctx.lineWidth = 2;
  ctx.beginPath();
  for (var i = 0; i < points.length; i++) {
    var x = xPos(times[i]), y = yPos(vals[i]);
    if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
  }
  ctx.stroke();

  // Dots
  ctx.fillStyle = color;
  for (var i = 0; i < points.length; i++) {
    ctx.beginPath();
    ctx.arc(xPos(times[i]), yPos(vals[i]), 3, 0, Math.PI * 2);
    ctx.fill();
  }

  // Area fill
  ctx.globalAlpha = 0.1; ctx.fillStyle = color;
  ctx.beginPath(); ctx.moveTo(xPos(times[0]), yPos(vals[0]));
  for (var i = 1; i < points.length; i++) ctx.lineTo(xPos(times[i]), yPos(vals[i]));
  ctx.lineTo(xPos(times[points.length-1]), pad.top + plotH);
  ctx.lineTo(xPos(times[0]), pad.top + plotH);
  ctx.closePath(); ctx.fill(); ctx.globalAlpha = 1.0;

  canvas._chartData = { points: points, times: times, vals: vals, yKey: yKey, yFmt: yFmt,
    xPos: xPos, yPos: yPos, pad: pad, W: W, H: H };
}

// Tooltip
document.addEventListener('mousemove', function(e) {
  var tooltip = document.getElementById('tooltip');
  var canvas = e.target;
  if (canvas.tagName !== 'CANVAS' || !canvas._chartData) { tooltip.style.display = 'none'; return; }
  var cd = canvas._chartData;
  var rect = canvas.getBoundingClientRect();
  var mx = e.clientX - rect.left, my = e.clientY - rect.top;
  if (mx < cd.pad.left || mx > cd.W - cd.pad.right) { tooltip.style.display = 'none'; return; }

  var closest = -1, closestDist = Infinity;
  for (var i = 0; i < cd.times.length; i++) {
    var dx = Math.abs(cd.xPos(cd.times[i]) - mx);
    if (dx < closestDist) { closestDist = dx; closest = i; }
  }
  if (closest < 0 || closestDist > 30) { tooltip.style.display = 'none'; return; }
  var p = cd.points[closest];
  var d = new Date(p.timestamp);
  var dateStr = d.getFullYear() + '-' + String(d.getMonth()+1).padStart(2,'0') + '-' +
    String(d.getDate()).padStart(2,'0') + ' ' + String(d.getHours()).padStart(2,'0') + ':' +
    String(d.getMinutes()).padStart(2,'0');
  var valStr = cd.yFmt ? cd.yFmt(cd.vals[closest]) : fmtNum(cd.vals[closest]);
  tooltip.innerHTML = '<div>' + dateStr + '</div><div style="color:#58a6ff;font-weight:bold">' + valStr + '</div>';
  tooltip.style.display = 'block';
  tooltip.style.left = (e.clientX + 12) + 'px';
  tooltip.style.top = (e.clientY - 10) + 'px';
});

async function refreshTrends() {
  try {
    var points = await (await fetch('/api/trend' + dbParam())).json();
    var empty = document.getElementById('trend-empty');
    var charts = document.getElementById('charts');
    if (points.length < 2) {
      empty.style.display = 'block'; charts.style.display = 'none'; return;
    }
    empty.style.display = 'none'; charts.style.display = 'grid';

    points.forEach(function(p) {
      var sysTot = p.system_total_mb || 0;
      var ceiling = sysTot * 0.8;
      var fixed = (p.host_cache_mb||0) + (p.host_base_mb||0) + (p.host_file_mb||0);
      p.ceiling_pct = ceiling > 0 ? ((fixed + (p.host_forest_mb||0)) / ceiling * 100) : 0;
    });

    drawChart('chart-docs', points, 'documents', '#58a6ff', fmtNum);
    drawChart('chart-forest', points, 'host_forest_mb', '#3fb950', fmt);
    drawChart('chart-disk', points, 'forest_disk_mb', '#d29922', fmt);
    drawChart('chart-ceiling', points, 'ceiling_pct', '#f85149', function(v) { return v.toFixed(1) + '%'; });
  } catch(e) {}
}

// Refresh
function refreshActiveTab() {
  if (activeTab === 'dashboard') refreshDashboard();
  else if (activeTab === 'trends') refreshTrends();
  else if (activeTab === 'snapshots') refreshSnapshots();
}

fetch('/api/info').then(function(r){return r.json();}).then(function(info){
  document.getElementById('build-sha').textContent = info.build || 'dev';
}).catch(function(){});

loadDatabases().then(function() { refreshActiveTab(); });
setInterval(refreshActiveTab, 30000);
window.addEventListener('resize', function() { if (activeTab === 'trends') refreshTrends(); });
</script>
</body>
</html>
"""
