import json
import logging
import os
import socket
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from importlib import resources
from typing import Optional
from urllib.parse import urlparse, parse_qs

from ml_capacity.formatting import fmt_mb
from ml_capacity.snapshot import (
    SNAPSHOT_DIR, collect_snapshot, save_snapshot, prune_snapshots, load_snapshots,
    import_snapshot_data,
)
from ml_capacity.prometheus import snapshot_to_prometheus, push_otlp

log = logging.getLogger("mlca")

_BUILD = os.environ.get("MLCA_BUILD", "dev")[:12]

_SYSTEM_DBS = {
    "App-Services", "Extensions", "Fab", "Last-Login",
    "Meters", "Modules", "Schemas", "Security", "Triggers",
}


def _load_ui_html():
    return resources.files("ml_capacity.static").joinpath("ui.html").read_text(encoding="utf-8")


_WEB_UI_HTML = _load_ui_html()


@dataclass
class ServiceContext:
    """Shared state passed into the HTTP handler via the server instance."""
    client: object
    databases: list
    api_token: Optional[str] = None
    latest_snapshots: dict = field(default_factory=dict)
    lock: threading.Lock = field(default_factory=threading.Lock)


class MLCAServer(ThreadingHTTPServer):
    """Threaded HTTP server that carries a ServiceContext for handlers to read.

    Binds dual-stack (IPv4 + IPv6) when possible so reverse proxies resolving
    ``localhost`` to ``::1`` don't get ECONNREFUSED. Uses a larger listen
    backlog than ``HTTPServer``'s default of 5 so concurrent UI requests
    don't overflow the kernel accept queue.
    """

    request_queue_size = 128
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, server_address, handler_class, ctx: ServiceContext):
        host, port = server_address
        if host in ("", "0.0.0.0", "::") and socket.has_ipv6:
            self.address_family = socket.AF_INET6
            server_address = ("::", port)
        super().__init__(server_address, handler_class)
        self.ctx = ctx

    def server_bind(self):
        if self.address_family == socket.AF_INET6:
            try:
                self.socket.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
            except (OSError, AttributeError):
                pass
        super().server_bind()


class MLCAHandler(BaseHTTPRequestHandler):
    """HTTP handler serving MLCA's web UI, Prometheus metrics, and JSON API.

    State is pulled from ``self.server.ctx`` (a :class:`ServiceContext`), so the
    handler is module-scoped and directly testable against a real HTTPServer.
    """

    @property
    def ctx(self) -> ServiceContext:
        return self.server.ctx

    def log_message(self, format, *args):
        log.info("HTTP %s", format % args)

    def _check_auth(self):
        """Return True if request is authorized, False otherwise."""
        token = self.ctx.api_token
        if not token:
            return True
        path, _ = self._parse_request()
        if path == "/health":
            return True
        auth = self.headers.get("Authorization", "")
        if auth == f"Bearer {token}":
            return True
        self.send_response(401)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"error":"Unauthorized - provide Authorization: Bearer <token>"}')
        return False

    def _add_cors_headers(self):
        """Add CORS headers — restricted to same-origin when no token configured."""
        origin = self.headers.get("Origin", "")
        if self.ctx.api_token and origin:
            self.send_header("Access-Control-Allow-Origin", origin)
            self.send_header("Vary", "Origin")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")

    def _parse_request(self):
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)
        return parsed.path, params

    def _get_db_filter(self, params):
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
            self._respond(200, "application/json", json.dumps({"build": _BUILD}))
        else:
            self._respond(404, "text/plain", "Not Found")

    def do_POST(self):
        if not self._check_auth():
            return
        path, _ = self._parse_request()
        if path == "/api/snapshot":
            self._handle_take_snapshot()
        elif path == "/api/snapshots/import":
            self._handle_import_snapshot()
        else:
            self._respond(404, "text/plain", "Not Found")

    def do_DELETE(self):
        if not self._check_auth():
            return
        path, _ = self._parse_request()
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
        with self.ctx.lock:
            all_metrics = [snapshot_to_prometheus(snap)
                           for snap in self.ctx.latest_snapshots.values()]
        body = "\n".join(all_metrics) if all_metrics else "# No data collected yet\n"
        self._respond(200, "text/plain; version=0.0.4; charset=utf-8", body)

    def _serve_json_snapshot(self, db_filter=None):
        ctx = self.ctx
        with ctx.lock:
            if db_filter and db_filter in ctx.latest_snapshots:
                snap = ctx.latest_snapshots[db_filter]
            elif not db_filter and ctx.latest_snapshots:
                snap = next(iter(ctx.latest_snapshots.values()))
            else:
                snap = None

        if snap is None:
            if not db_filter:
                self._respond(503, "application/json",
                              '{"error":"No data collected yet"}')
                return
            try:
                snap = collect_snapshot(ctx.client, db_filter)
            except Exception as e:
                self._respond(500, "application/json",
                              json.dumps({"error": str(e)}))
                return
            with ctx.lock:
                ctx.latest_snapshots[db_filter] = snap

        self._respond(200, "application/json",
                      json.dumps(snap, indent=2, default=str))

    def _serve_json_snapshots_list(self, db_filter=None):
        all_snaps = load_snapshots(database=db_filter)
        summary = [{
            "timestamp": s.get("timestamp"),
            "database": s.get("database"),
            "documents": s.get("totals", {}).get("documents", 0),
            "forest_disk_mb": s.get("totals", {}).get("forest_disk_mb", 0),
            "host_forest_mb": s.get("totals", {}).get("host_forest_mb", 0),
            "host_rss_mb": s.get("totals", {}).get("host_rss_mb", 0),
            "file": s.get("_file"),
        } for s in all_snaps]
        self._respond(200, "application/json",
                      json.dumps(summary, indent=2, default=str))

    def _serve_json_trend(self, db_filter=None):
        all_snaps = load_snapshots(database=db_filter)
        points = [{
            "timestamp": s.get("timestamp"),
            "database": s.get("database"),
            "documents": s.get("totals", {}).get("documents", 0),
            "forest_disk_mb": s.get("totals", {}).get("forest_disk_mb", 0),
            "host_forest_mb": s.get("totals", {}).get("host_forest_mb", 0),
            "host_rss_mb": s.get("totals", {}).get("host_rss_mb", 0),
            "active_fragments": s.get("totals", {}).get("active_fragments", 0),
            "deleted_fragments": s.get("totals", {}).get("deleted_fragments", 0),
            "system_total_mb": s.get("totals", {}).get("system_total_mb", 0),
            "host_cache_mb": s.get("totals", {}).get("host_cache_mb", 0),
            "host_base_mb": s.get("totals", {}).get("host_base_mb", 0),
            "host_file_mb": s.get("totals", {}).get("host_file_mb", 0),
        } for s in all_snaps]
        self._respond(200, "application/json",
                      json.dumps(points, indent=2, default=str))

    def _serve_json_databases(self):
        ctx = self.ctx
        db_names = set(ctx.latest_snapshots.keys())
        try:
            results = ctx.client.eval_javascript(
                'Array.from(xdmp.databases()).map('
                'function(id){return xdmp.databaseName(id)})')
            if results and isinstance(results[0], list):
                db_names.update(results[0])
        except Exception as e:
            log.debug("Falling back to local knowledge for /api/databases: %s", e)
        for s in load_snapshots():
            db_name = s.get("database")
            if db_name:
                db_names.add(db_name)
        db_names -= _SYSTEM_DBS
        result = sorted(db_names)
        if "Documents" in result:
            result.remove("Documents")
            result.insert(0, "Documents")
        self._respond(200, "application/json", json.dumps(result, default=str))

    def _serve_json_snapshot_file(self, filename):
        """Serve full snapshot JSON for a specific file."""
        if ".." in filename or "/" in filename or not filename.endswith(".json"):
            self._respond(400, "application/json", '{"error":"Invalid filename"}')
            return
        fpath = SNAPSHOT_DIR / filename
        if not fpath.exists():
            self._respond(404, "application/json", '{"error":"Snapshot not found"}')
            return
        try:
            with open(fpath) as f:
                data = json.load(f)
            self._respond(200, "application/json",
                          json.dumps(data, indent=2, default=str))
        except (json.JSONDecodeError, OSError) as e:
            self._respond(500, "application/json", json.dumps({"error": str(e)}))

    def _handle_delete_snapshot(self, filename):
        """Delete a snapshot file."""
        if ".." in filename or "/" in filename or not filename.endswith(".json"):
            self._respond(400, "application/json", '{"error":"Invalid filename"}')
            return
        fpath = SNAPSHOT_DIR / filename
        if not fpath.exists():
            self._respond(404, "application/json", '{"error":"Snapshot not found"}')
            return
        try:
            fpath.unlink()
            self._respond(200, "application/json", '{"status":"deleted"}')
        except OSError as e:
            self._respond(500, "application/json", json.dumps({"error": str(e)}))

    def _handle_take_snapshot(self):
        """Trigger an immediate snapshot collection."""
        ctx = self.ctx
        try:
            content_len = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_len) if content_len else b"{}"
            req = json.loads(body) if body else {}
        except (json.JSONDecodeError, ValueError):
            req = {}

        db = req.get("database") or (
            next(iter(ctx.latest_snapshots), None)
            or (ctx.databases[0] if ctx.databases else "Documents")
        )

        try:
            snap = collect_snapshot(ctx.client, db)
            save_snapshot(snap)
            with ctx.lock:
                ctx.latest_snapshots[db] = snap
            t = snap.get("totals", {})
            summary = {
                "status": "ok",
                "timestamp": snap.get("timestamp"),
                "database": db,
                "documents": t.get("documents", 0),
            }
            log.info("[%s] Manual snapshot %s: %s docs",
                     datetime.now(timezone.utc).strftime('%H:%M:%S'),
                     db, f"{t.get('documents', 0):,}")
            self._respond(200, "application/json",
                          json.dumps(summary, indent=2, default=str))
        except Exception as e:
            self._respond(500, "application/json", json.dumps({"error": str(e)}))

    def _handle_import_snapshot(self):
        """Import a snapshot JSON from the request body."""
        try:
            content_len = int(self.headers.get("Content-Length", 0))
            if content_len == 0:
                self._respond(400, "application/json", '{"error":"Empty request body"}')
                return
            body = self.rfile.read(content_len)
            snap = json.loads(body)
        except (json.JSONDecodeError, ValueError) as e:
            self._respond(400, "application/json",
                          json.dumps({"error": f"Invalid JSON: {e}"}))
            return

        result = import_snapshot_data(snap)
        if result.get("error"):
            self._respond(400, "application/json", json.dumps(result))
            return

        self._respond(200, "application/json", json.dumps(result, default=str))

    def _serve_ui(self):
        self._respond(200, "text/html", _WEB_UI_HTML)

    def _respond(self, code, content_type, body):
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self._add_cors_headers()
        self.end_headers()
        self.wfile.write(body.encode() if isinstance(body, str) else body)


def _collect_all(ctx: ServiceContext):
    """Collect snapshots for all monitored databases."""
    for db in ctx.databases:
        try:
            snap = collect_snapshot(ctx.client, db)
            save_snapshot(snap)
            with ctx.lock:
                ctx.latest_snapshots[db] = snap
            log.info("Collected %s: %s docs, forest=%s",
                     db, f"{snap['totals']['documents']:,}",
                     fmt_mb(snap['totals']['host_forest_mb']))
        except Exception as e:
            log.error("Error collecting %s: %s", db, e)


def _schedule_loop(ctx: ServiceContext, interval_sec, retention_days, otlp_endpoint):
    """Run collection on a repeating interval, prune, and optionally push to OTLP."""
    while True:
        _collect_all(ctx)
        removed = prune_snapshots(retention_days)
        if removed:
            log.info("Pruned %d snapshot(s) older than %d days", removed, retention_days)
        if otlp_endpoint:
            with ctx.lock:
                for _, snap in ctx.latest_snapshots.items():
                    try:
                        push_otlp(snap, otlp_endpoint)
                    except Exception as e:
                        log.error("OTLP push error: %s", e)
        threading.Event().wait(interval_sec)


def run_service(client, databases, interval_sec, port, otlp_endpoint=None,
                retention_days=30, api_token=None):
    """Run MLCA as a persistent service with HTTP endpoints.

    Collects snapshots on schedule, serves /metrics, /api/*, and web UI.
    """
    ctx = ServiceContext(client=client, databases=databases, api_token=api_token)

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

    _collect_all(ctx)

    collector = threading.Thread(
        target=_schedule_loop,
        args=(ctx, interval_sec, retention_days, otlp_endpoint),
        daemon=True,
    )
    collector.start()

    server = MLCAServer(("", port), MLCAHandler, ctx)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  MLCA Service stopped.")
        server.shutdown()
