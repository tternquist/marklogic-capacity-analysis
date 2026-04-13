#!/usr/bin/env python3
"""
MLCA Ceiling Validation Harness — Telemetry Document Schema

Cross-validates the capacity model with a document corpus that is
meaningfully different from the original generic-content harness:

  * Domain:   IoT / observability telemetry events
  * Schema:   event_id, metric, severity, value, recorded_at, …
  * Indexes:  four range indexes installed *before* loading begins
              (double, int, string + string) — so index overhead is
              baked into every snapshot, not isolated as a delta.
  * Loader:   Flux, via the flux-runner HTTP API at --flux-url.
              Each batch is generated in Python, served over a
              short-lived local HTTP server, and fetched by the
              runner via its --http-url support — no docker cp needed.
  * Target:   load until the model's "docs-to-ceiling" estimate is
              exhausted or a safety threshold fires.

Range indexes installed at startup:
  /value     double   (no collation)          high-cardinality numeric
  /severity  int      (no collation)          very-low-cardinality (1-5)
  /metric    string   (ML default collation)  ~12 unique values
  /event_id  string   (ML default collation)  unique per document

Prerequisites:
  # Start test MarkLogic (4 GB memory limit)
  cd test && docker compose up -d

  # Verify the flux-runner is accessible
  curl -s -X POST http://localhost:8082/run -d '{}'

Usage:
    python3 test/test_ceiling.py
    python3 test/test_ceiling.py --phase-size 150000
    python3 test/test_ceiling.py --safety-pct 85 --no-cleanup
"""

import argparse
import http.server
import json
import os
import random
import shutil
import socketserver
import sys
import time
import urllib.error
import urllib.request
import threading
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from ml_capacity import (
    MarkLogicClient, collect_snapshot, save_snapshot, load_snapshots,
    check_config_drift, wait_for_reindex,
    fmt_mb, color, header, sub_header, kv, bar, status_badge,
    BOLD, CYAN, GREEN, RED, YELLOW, DIM, RESET,
)

# ── Collection / URI namespace ──────────────────────────────────────────

COLLECTION = "ceiling-test"
URI_PREFIX = "/ceiling-test/"

# ── Range indexes installed before any loading ──────────────────────────

INDEXES = [
    {
        "label":                 "value (double, high-card)",
        "scalar-type":           "double",
        "path-expression":       "/value",
        "range-value-positions": False,
        "invalid-values":        "ignore",
    },
    {
        "label":                 "severity (int, 1-5)",
        "scalar-type":           "int",
        "path-expression":       "/severity",
        "range-value-positions": False,
        "invalid-values":        "ignore",
    },
    {
        "label":                 "metric (string, ~12 values)",
        "scalar-type":           "string",
        "path-expression":       "/metric",
        "collation":             "http://marklogic.com/collation/",
        "range-value-positions": False,
        "invalid-values":        "ignore",
    },
    {
        "label":                 "event_id (string, unique per doc)",
        "scalar-type":           "string",
        "path-expression":       "/event_id",
        "collation":             "http://marklogic.com/collation/",
        "range-value-positions": False,
        "invalid-values":        "ignore",
    },
]

# ── Vocabulary pools ────────────────────────────────────────────────────

_METRICS    = ['cpu_usage','mem_usage','disk_io','net_in','net_out',
               'latency_p50','latency_p99','error_rate','request_rate',
               'queue_depth','gc_pause','cache_hit_rate']
_REGIONS    = ['us-east-1','us-east-2','us-west-1','us-west-2',
               'eu-west-1','eu-central-1','ap-southeast-1','ap-northeast-1',
               'ap-south-1','sa-east-1','ca-central-1','eu-north-1',
               'af-south-1','me-south-1','ap-east-1','eu-south-1',
               'us-gov-west-1','us-gov-east-1','cn-north-1','cn-northwest-1']
_DEV_CLS    = ['server','container','vm','edge-node',
               'gateway','sensor','appliance','cloud-function']
_TAG_POOL   = ['production','staging','canary','critical','monitored',
               'sla-tier1','sla-tier2','infra','app','database','network']
_TRENDS     = ['rising','falling','stable','oscillating']
_BASE_TS    = datetime(2022, 1, 1)
_SPAN_SECS  = 3 * 365 * 24 * 3600
_CHARS      = 'abcdefghijklmnopqrstuvwxyz0123456789'


def _rand_ts():
    return (_BASE_TS + timedelta(
        seconds=random.randint(0, _SPAN_SECS)
    )).strftime('%Y-%m-%dT%H:%M:%S')


def _rand_str(n):
    return ''.join(random.choices(_CHARS, k=n))


def _metric_value(metric):
    if metric in ('cpu_usage', 'mem_usage', 'cache_hit_rate'):
        return round(random.uniform(0, 100), 4)
    if 'latency' in metric or 'pause' in metric:
        return round(random.uniform(0.1, 5000), 3)
    if 'rate' in metric:
        return round(random.uniform(0, 10000), 2)
    return round(random.uniform(0, 1e9), 1)


def _metric_unit(metric):
    if 'rate' in metric:         return 'req/s'
    if 'latency' in metric or 'pause' in metric: return 'ms'
    if 'io' in metric or 'net' in metric:        return 'bytes'
    if 'usage' in metric:        return '%'
    return 'count'


# ── JSONL generation (pure Python) ─────────────────────────────────────

def generate_jsonl_batch(count, offset):
    """Generate *count* telemetry event documents as a JSONL string."""
    lines = []
    for i in range(count):
        doc_id   = offset + i
        metric   = random.choice(_METRICS)
        severity = random.randint(1, 5)
        value    = _metric_value(metric)
        roll     = random.random()
        size     = 'small' if roll < 0.30 else ('medium' if roll < 0.80 else 'large')

        doc = {
            'event_id':     f'evt-{doc_id:012d}',
            'sensor_id':    f'sensor-{random.randint(1, 1000):04d}',
            'metric':       metric,
            'severity':     severity,
            'value':        value,
            'recorded_at':  _rand_ts(),
            'region':       random.choice(_REGIONS),
            'device_class': random.choice(_DEV_CLS),
            'acknowledged': random.random() > 0.7,
        }

        if size in ('medium', 'large'):
            doc['threshold']    = round(value * random.uniform(0.5, 2.0), 2)
            doc['baseline']     = round(value * random.uniform(0.3, 1.5), 2)
            doc['tags']         = [random.choice(_TAG_POOL), random.choice(_TAG_POOL)]
            doc['sample_count'] = random.randint(1, 300)
            doc['unit']         = _metric_unit(metric)
            doc['host_fqdn']    = (f'{_rand_str(8)}.internal'
                                   f'.{doc["region"]}.example.com')

        if size == 'large':
            doc['notes'] = (
                f'Automated alert for {metric} on {doc["sensor_id"]}. '
                f'Severity {severity}. Value {value}. '
                f'Runbook section {random.randint(1,50)}.{random.randint(1,10)}.'
            )
            doc['context'] = {
                'prev_value': round(value * random.uniform(0.7, 1.3), 4),
                'delta_pct':  round(random.uniform(-50, 50), 2),
                'trend':      random.choice(_TRENDS),
                'alert_id':   f'alrt-{_rand_str(8)}',
                'runbook_url': f'https://runbooks.example.com/{metric}/sev{severity}',
            }
            doc['history'] = [
                {'ts':    _rand_ts(),
                 'value': round(value * random.uniform(0.5, 1.5), 4),
                 'sev':   random.randint(1, 5)}
                for _ in range(random.randint(3, 8))
            ]

        lines.append(json.dumps(doc))

    return '\n'.join(lines)


# ── Embedded HTTP server (serves one JSONL payload then stops) ──────────

class _JSOHLServer:
    """Serve a JSONL payload once over HTTP, then shut down.

    Usage:
        with _JSOHLServer(jsonl_str, port=19999) as url:
            # url == 'http://localhost:19999/batch.jsonl'
            call_flux(url)
    """

    def __init__(self, content_str, port=19999):
        self._body = content_str.encode('utf-8')
        self._port = port
        self._served = threading.Event()
        self._server = None

    def _make_handler(self):
        body = self._body
        served = self._served

        class _H(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200)
                self.send_header('Content-Type', 'application/x-ndjson')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                served.set()           # signal that we've served the payload

            def log_message(self, *a):
                pass                   # suppress access log noise

        return _H

    def __enter__(self):
        handler = self._make_handler()
        # Subclass to set allow_reuse_address *before* bind (class-level attr)
        class _ReuseServer(socketserver.TCPServer):
            allow_reuse_address = True
        self._server = _ReuseServer(('', self._port), handler)
        t = threading.Thread(target=self._server.serve_forever, daemon=True)
        t.start()
        return f'http://localhost:{self._port}/batch.jsonl'

    def __exit__(self, *_):
        if self._server:
            self._server.shutdown()


# ── Flux loader ─────────────────────────────────────────────────────────

def flux_import_phase(flux_url, jsonl_str, ml_host, ml_app_port,
                      ml_user, ml_password, auth_type,
                      collection=COLLECTION,
                      uri_template='/ceiling-test/{event_id}.json',
                      batch_size=500, thread_count=4,
                      serve_port=19999):
    """Serve *jsonl_str* via a local HTTP server and import via the
    flux-runner HTTP API using its --http-url support.

    The runner (on host networking) fetches the JSONL from localhost
    and passes it to Flux as a local --path — no docker cp required.

    Returns the number of documents imported (line count of the JSONL).
    """
    conn_str = f'{ml_user}:{ml_password}@{ml_host}:{ml_app_port}'

    with _JSOHLServer(jsonl_str, port=serve_port) as serve_url:
        args = [
            'import-aggregate-json-files',
            '--connection-string', conn_str,
            '--auth-type',         auth_type.upper(),
            '--http-url',          serve_url,
            '--json-lines',
            '--collections',       collection,
            '--permissions',       'rest-reader,read,rest-writer,update',
            '--uri-template',      uri_template,
            '--batch-size',        str(batch_size),
            '--thread-count',      str(thread_count),
        ]

        body = json.dumps({'args': args}).encode('utf-8')
        req  = urllib.request.Request(
            f'{flux_url}/run',
            data=body,
            headers={'Content-Type': 'application/json'},
            method='POST',
        )
        try:
            with urllib.request.urlopen(req, timeout=600) as resp:
                resp_body = resp.read().decode('utf-8')
        except urllib.error.HTTPError as e:
            resp_body = e.read().decode('utf-8')
            raise RuntimeError(f'Flux runner HTTP {e.code}: {resp_body}')

    # Parse runner response — it returns JSON with exitCode + output
    try:
        rj = json.loads(resp_body)
        exit_code = rj.get('exitCode', 0)
        output    = rj.get('output', '')
        if exit_code != 0:
            raise RuntimeError(
                f'Flux exited {exit_code}.\n{output}'
            )
    except json.JSONDecodeError:
        pass  # some runner versions return plain text on success

    return jsonl_str.count('\n') + 1


# ── Cleanup ─────────────────────────────────────────────────────────────

_CLEANUP_JS = """
declareUpdate();
let deleted = 0;
const uris = cts.uriMatch(prefix + '*').toArray();
for (let i = 0; i < uris.length; i++) {
  xdmp.documentDelete(uris[i]);
  deleted++;
}
deleted;
"""


def cleanup_docs(client, database):
    total = 0
    while True:
        result = client.eval_javascript(
            _CLEANUP_JS, database=database,
            vars={'prefix': URI_PREFIX},
        )
        n = (result[0] if isinstance(result, list) else result) or 0
        total += n
        if n == 0:
            break
    return total


# ── Index management ────────────────────────────────────────────────────

def setup_indexes(client, database):
    """Add the four telemetry range indexes; return paths of newly added ones."""
    props    = client.get_json(f'/manage/v2/databases/{database}/properties?format=json')
    existing = props.get('range-path-index', [])
    have     = {ix.get('path-expression') for ix in existing}
    to_add   = [ix for ix in INDEXES if ix['path-expression'] not in have]

    if not to_add:
        print(f"    {color('All 4 indexes already present.', GREEN)}")
        return []

    new_list = existing + [
        {k: v for k, v in ix.items() if k != 'label'}
        for ix in to_add
    ]
    client.put_json(f'/manage/v2/databases/{database}/properties',
                    {'range-path-index': new_list})

    for ix in to_add:
        print(f"    {color('Added:', GREEN)} {ix['label']}  ({ix['path-expression']})")

    # Only wait for reindex if documents already exist — check via db status
    try:
        db_status = client.get_json(
            f'/manage/v2/databases/{database}?view=counts&format=json'
        )
        doc_count = (db_status.get('database-counts', {})
                               .get('count-properties', {})
                               .get('documents', {})
                               .get('value', 0))
        has_docs = int(doc_count or 0) > 0
    except Exception:
        has_docs = False  # if we can't tell, skip the wait (safe for empty DB)

    if has_docs:
        print('    Waiting for reindex (existing docs detected)...')
        if not wait_for_reindex(client, database, timeout=600):
            print(f"    {color('WARNING: Reindex timed out', YELLOW)}")
        else:
            print(f"    {color('Reindex complete.', GREEN)}")

    return [ix['path-expression'] for ix in to_add]


def remove_indexes(client, database, paths):
    if not paths:
        return
    props    = client.get_json(f'/manage/v2/databases/{database}/properties?format=json')
    existing = props.get('range-path-index', [])
    kept     = [ix for ix in existing if ix.get('path-expression') not in paths]
    client.put_json(f'/manage/v2/databases/{database}/properties',
                    {'range-path-index': kept})
    print(f"    Removed {len(paths)} index(es): {', '.join(paths)}")
    wait_for_reindex(client, database, timeout=300)


# ── Runway / projection ─────────────────────────────────────────────────

def compute_runway(snaps):
    if len(snaps) < 2:
        return None
    try:
        ts_first = datetime.fromisoformat(snaps[0]['timestamp'])
        ts_last  = datetime.fromisoformat(snaps[-1]['timestamp'])
    except (ValueError, TypeError):
        return None
    days = (ts_last - ts_first).total_seconds() / 86400
    if days <= 0:
        return None

    ft = snaps[0]['totals']
    lt = snaps[-1]['totals']

    forest_first = ft.get('host_forest_mb', 0)
    forest_last  = lt.get('host_forest_mb', 0)
    forest_delta = forest_last - forest_first
    sys_total    = lt.get('system_total_mb', 0)
    fixed        = lt.get('host_cache_mb',0) + lt.get('host_base_mb',0) + lt.get('host_file_mb',0)
    ceiling      = sys_total * 0.80
    headroom     = ceiling - fixed - forest_last
    doc_delta    = lt.get('documents', 0) - ft.get('documents', 0)

    result = {
        'days':           days,
        'forest_last_mb': forest_last,
        'forest_delta_mb': forest_delta,
        'fixed_mb':       fixed,
        'ceiling_mb':     ceiling,
        'headroom_mb':    headroom,
        'doc_delta':      doc_delta,
        'docs_total':     lt.get('documents', 0),
    }

    if forest_delta > 0:
        daily_growth = forest_delta / days
        result['growth_rate_mb_day'] = daily_growth
        result['runway_days']        = headroom / daily_growth if headroom > 0 else 0
        if doc_delta > 0:
            result['forest_bytes_per_doc'] = (forest_delta * 1024 * 1024) / doc_delta
            result['docs_until_ceiling']   = int(
                (headroom * 1024 * 1024) / result['forest_bytes_per_doc']
            ) if headroom > 0 else 0

    return result


def wait_for_ml(client, timeout=120):
    start = time.time()
    while time.time() - start < timeout:
        try:
            client.get_json('/manage/v2?format=json')
            return True
        except Exception:
            time.sleep(2)
    return False


# ── Main ─────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='MLCA Ceiling Validation — Telemetry Schema, Flux loader'
    )
    parser.add_argument('--host',         default='localhost')
    parser.add_argument('--port',         type=int, default=8102,
                        help='Management API port (default: 8102)')
    parser.add_argument('--app-port',     type=int, default=8100,
                        help='App Services port Flux writes to (default: 8100)')
    parser.add_argument('--user',         default='admin')
    parser.add_argument('--password',     default='admin')
    parser.add_argument('--database',     default='Documents')
    parser.add_argument('--auth-type',    default='digest',
                        choices=['digest', 'basic'])
    parser.add_argument('--flux-url',     default='http://localhost:8082',
                        help='Flux runner HTTP API base URL (default: localhost:8082)')
    parser.add_argument('--serve-port',   type=int, default=19999,
                        help='Local port for serving JSONL batches to the runner '
                             '(default: 19999)')
    parser.add_argument('--phase-size',   type=int, default=100000,
                        help='Documents per phase (default: 100,000)')
    parser.add_argument('--max-phases',   type=int, default=20,
                        help='Hard cap on loading phases (default: 20)')
    parser.add_argument('--safety-pct',   type=int, default=88,
                        help='Stop when forest+fixed reaches this %% of ceiling '
                             '(default: 88)')
    parser.add_argument('--flux-threads', type=int, default=4)
    parser.add_argument('--flux-batch',   type=int, default=500)
    parser.add_argument('--snapshot-dir', default=None)
    parser.add_argument('--no-cleanup',   action='store_true',
                        help='Leave test docs and indexes after run')
    parser.add_argument('--no-index-cleanup', action='store_true',
                        help='Leave the 4 range indexes on the database after run')
    args = parser.parse_args()

    # ── Snapshot directory ──────────────────────────────────────────
    test_snap_dir = args.snapshot_dir or os.path.join(
        os.path.dirname(__file__), '.ml-capacity-ceiling'
    )
    import ml_capacity
    ml_capacity.SNAPSHOT_DIR = type(ml_capacity.SNAPSHOT_DIR)(test_snap_dir)

    if os.path.exists(test_snap_dir):
        shutil.rmtree(test_snap_dir)
    os.makedirs(test_snap_dir, exist_ok=True)

    client      = MarkLogicClient(args.host, args.port, args.user,
                                  args.password, args.auth_type)
    safety_frac = args.safety_pct / 100.0

    print(color("""
    ╔══════════════════════════════════════════════════════════════╗
    ║  MLCA Ceiling Validation — Telemetry Schema (Flux loader)    ║
    ║  Drives load toward the estimated memory ceiling             ║
    ╚══════════════════════════════════════════════════════════════╝
    """, CYAN))

    # ── Connectivity ────────────────────────────────────────────────
    header('SETUP')
    print(f"    Connecting to {args.host}:{args.port}...")
    if not wait_for_ml(client, timeout=30):
        print(f"    {color('ERROR: Cannot connect to MarkLogic', RED)}")
        sys.exit(1)
    print(f"    {color('MarkLogic connected.', GREEN)}")

    # Verify flux runner (expect "Invalid request body" for empty POST)
    try:
        req = urllib.request.Request(
            f'{args.flux_url}/run',
            data=b'{}',
            headers={'Content-Type': 'application/json'},
            method='POST',
        )
        with urllib.request.urlopen(req, timeout=5) as r:
            r.read()
    except Exception as e:
        if 'Invalid request body' not in str(e) and '400' not in str(e):
            print(f"    {color(f'WARNING: Flux runner check failed: {e}', YELLOW)}")
    print(f"    {color('Flux runner reachable.', GREEN)}")

    print()
    kv('Host',          f"{args.host}:{args.port}")
    kv('App port',      f"{args.app_port}  (Flux writes here)")
    kv('Flux runner',   args.flux_url)
    kv('Serve port',    f"{args.serve_port}  (local JSONL HTTP server)")
    kv('Database',      args.database)
    kv('Phase size',    f"{args.phase_size:,} docs")
    kv('Max phases',    args.max_phases)
    kv('Safety stop',   f"{args.safety_pct}% of ceiling")
    kv('Snapshot dir',  test_snap_dir)

    # ── Index setup ─────────────────────────────────────────────────
    sub_header('Range Index Setup')
    print(f"    Installing {len(INDEXES)} range indexes for telemetry schema...")
    added_indexes = setup_indexes(client, args.database)
    print()

    # ── Baseline snapshot ────────────────────────────────────────────
    header('PHASE 0: Baseline (indexes installed, no telemetry docs yet)')
    snap0      = collect_snapshot(client, args.database)
    save_snapshot(snap0)
    t0v        = snap0['totals']
    ceiling_mb = t0v['system_total_mb'] * 0.80
    fixed_mb0  = t0v['host_cache_mb'] + t0v['host_base_mb'] + t0v['host_file_mb']
    headroom0  = ceiling_mb - fixed_mb0 - t0v['host_forest_mb']

    kv('Documents',            f"{t0v['documents']:,}")
    kv('Forest memory',        fmt_mb(t0v['host_forest_mb']))
    kv('System RAM',           fmt_mb(t0v['system_total_mb']))
    kv('Memory ceiling (80%)', fmt_mb(ceiling_mb))
    kv('Initial headroom',     fmt_mb(headroom0))

    # ── Loading phases ───────────────────────────────────────────────
    phase_results  = []
    inserted_total = 0
    stop_reason    = None

    for phase in range(1, args.max_phases + 1):
        header(f'PHASE {phase}: +{args.phase_size:,} telemetry events  [Flux]')

        # Generate JSONL in Python
        t_gen  = time.time()
        jsonl  = generate_jsonl_batch(args.phase_size, inserted_total)
        gen_ms = (time.time() - t_gen) * 1000
        print(f"    Generated {args.phase_size:,} docs in {gen_ms:.0f}ms  "
              f"({len(jsonl):,} bytes)")

        # Import via Flux runner (serve locally, runner fetches via http-url)
        t_flux = time.time()
        try:
            n = flux_import_phase(
                flux_url     = args.flux_url,
                jsonl_str    = jsonl,
                ml_host      = args.host,
                ml_app_port  = args.app_port,
                ml_user      = args.user,
                ml_password  = args.password,
                auth_type    = args.auth_type,
                collection   = COLLECTION,
                uri_template = f'{URI_PREFIX}{{event_id}}.json',
                batch_size   = args.flux_batch,
                thread_count = args.flux_threads,
                serve_port   = args.serve_port,
            )
        except Exception as e:
            print(f"    {color(f'Flux import failed: {e}', RED)}")
            stop_reason = f'Flux import error at phase {phase}: {e}'
            break

        elapsed        = time.time() - t_flux
        inserted_total += n
        rate           = n / elapsed if elapsed > 0 else 0
        print(f"    Flux imported {n:,} docs in {elapsed:.1f}s  ({rate:,.0f}/s)")
        time.sleep(2)  # let pending merges settle

        snap = collect_snapshot(client, args.database)
        save_snapshot(snap)
        tv   = snap['totals']

        kv('Total documents', f"{tv['documents']:,}")
        kv('Forest memory',   fmt_mb(tv['host_forest_mb']))
        kv('RSS',             fmt_mb(tv['host_rss_mb']))

        # ── Runway ───────────────────────────────────────────────────
        all_snaps = load_snapshots(args.database)
        runway    = compute_runway(all_snaps)

        pr = {
            'phase':     phase,
            'docs':      tv['documents'],
            'forest_mb': tv['host_forest_mb'],
            'rss_mb':    tv['host_rss_mb'],
        }
        if runway:
            pr.update(runway)
            if 'forest_bytes_per_doc' in runway:
                kv('Bytes/doc (forest)', f"{runway['forest_bytes_per_doc']:,.0f}")
            if 'docs_until_ceiling' in runway:
                kv('Docs until ceiling', f"{runway['docs_until_ceiling']:,}")
            if 'runway_days' in runway:
                rdays = f"{runway['runway_days']:.1f} days"
                kv('Memory runway',
                   f"{color(rdays, BOLD)}  "
                   f"(growing {fmt_mb(runway['growth_rate_mb_day'])}/day)")

        drift = check_config_drift(all_snaps)
        if drift:
            print(f"    {color('WARNING: Config drift detected!', RED)}")
        else:
            kv('Config stability', color('STABLE', GREEN))

        fixed_now = tv['host_cache_mb'] + tv['host_base_mb'] + tv['host_file_mb']
        used_mb   = fixed_now + tv['host_forest_mb']
        used_pct  = used_mb / ceiling_mb * 100 if ceiling_mb else 0
        kv('Ceiling usage', f"{used_pct:.1f}%  {bar(used_pct)}")

        phase_results.append(pr)

        # ── Stopping conditions ──────────────────────────────────────
        swap_mb = tv.get('host_swap_mb', 0) or 0
        free_mb = tv.get('system_free_mb', 0) or 0
        sys_tot = tv.get('system_total_mb', 0)

        if swap_mb > 0:
            stop_reason = f'SWAP detected ({fmt_mb(swap_mb)}) — hard stop'
            break

        if tv['host_forest_mb'] + fixed_now >= ceiling_mb * safety_frac:
            stop_reason = (f'Forest+fixed reached {args.safety_pct}% of ceiling '
                           f'({fmt_mb(used_mb)} / {fmt_mb(ceiling_mb)})')
            break

        if sys_tot and free_mb > 0 and free_mb < sys_tot * 0.08:
            # free_mb == 0 means the metric is unavailable (container env) — skip
            stop_reason = (f'System free memory critically low '
                           f'({fmt_mb(free_mb)} of {fmt_mb(sys_tot)})')
            break

        # Soft stop: model says next phase would overshoot
        if runway and runway.get('docs_until_ceiling') is not None:
            d_left = runway['docs_until_ceiling']
            if d_left < args.phase_size * 0.25:
                stop_reason = (f'Model projects only {d_left:,} docs remaining '
                               f'(< 25% of phase size) — ceiling reached')
                break

    if stop_reason:
        print()
        print(f"    {color('STOPPED: ' + stop_reason, RED + BOLD)}")

    # ── Results ──────────────────────────────────────────────────────
    header('RESULTS: Convergence & Projection Accuracy')

    sub_header('Phase-by-Phase Summary')
    print()
    hdr = (f"  {'Ph':>3}  {'Docs':>10}  {'Forest':>9}  "
           f"{'B/doc':>9}  {'DocsLeft':>10}  {'%Ceil':>6}")
    print(color(hdr, BOLD))
    print(color('  ' + '-' * (len(hdr) - 2), DIM))

    for pr in phase_results:
        bpd     = pr.get('forest_bytes_per_doc')
        dleft   = pr.get('docs_until_ceiling')
        fmb_v   = pr['forest_mb']
        fp      = pr.get('fixed_mb', 0)
        c_mb    = pr.get('ceiling_mb', ceiling_mb)
        upct    = (fp + fmb_v) / c_mb * 100 if c_mb else 0

        bpd_s   = f"{bpd:>9,.0f}" if bpd else f"{'—':>9}"
        dleft_s = f"{dleft:>10,}" if dleft is not None else f"{'—':>10}"

        print(f"  {pr['phase']:>3}  {pr['docs']:>10,}  {fmt_mb(fmb_v):>9}  "
              f"{bpd_s}  {dleft_s}  {upct:>5.1f}%")

    # ── Convergence analysis ─────────────────────────────────────────
    sub_header('Convergence Analysis')

    projections = [pr for pr in phase_results if pr.get('forest_bytes_per_doc')]

    if len(projections) >= 3:
        bpd_vals  = [pr['forest_bytes_per_doc'] for pr in projections]
        last3     = bpd_vals[-3:]
        mean_bpd  = sum(last3) / 3
        max_dev   = max(abs(v - mean_bpd) / mean_bpd * 100 for v in last3)

        kv('Bytes/doc — last 3 phases',
           f"{', '.join(f'{v:,.0f}' for v in last3)}")
        kv('Mean bytes/doc', f"{mean_bpd:,.0f}")
        kv('Max deviation',
           f"{max_dev:.1f}%  {status_badge(max_dev < 20, 'CONVERGED', 'NOT YET')}")

        ceil_vals = [pr.get('docs_until_ceiling', 0) for pr in projections[-3:]]
        if any(ceil_vals):
            mean_ceil = sum(ceil_vals) / len(ceil_vals)
            max_cdv   = (max(abs(v - mean_ceil) / mean_ceil * 100 for v in ceil_vals)
                         if mean_ceil else 100)
            kv('Docs-to-ceiling (last 3)',
               f"{', '.join(f'{v:,}' for v in ceil_vals)}")
            kv('Ceiling est. stability',
               f"{max_cdv:.1f}%  {status_badge(max_cdv < 20, 'STABLE', 'FLUCTUATING')}")

        print()
        if max_dev < 20:
            print(f"    {color('PASS', GREEN + BOLD)}: Model converged.")
            print(f"    Telemetry events cost ~{mean_bpd:,.0f} bytes/doc of forest memory.")
        elif max_dev < 40:
            print(f"    {color('PARTIAL', YELLOW + BOLD)}: Stabilizing — "
                  f"more phases would tighten the estimate.")
        else:
            print(f"    {color('FAIL', RED + BOLD)}: Not converged "
                  f"({max_dev:.0f}% variance).")
    elif projections:
        print(f"    Need 3+ phases with growth data. Got {len(projections)}.")
    else:
        print('    No growth data — check document insertion.')

    # ── Final memory state ───────────────────────────────────────────
    sub_header('Final Memory State')
    all_snaps = load_snapshots(args.database)
    if all_snaps:
        ft      = all_snaps[-1]['totals']
        fixed_f = ft['host_cache_mb'] + ft['host_base_mb'] + ft['host_file_mb']
        used_f  = fixed_f + ft['host_forest_mb']
        c_f     = ft['system_total_mb'] * 0.80
        u_pct   = used_f / c_f * 100 if c_f else 0

        kv('Total documents',      f"{ft['documents']:,}")
        kv('Forest memory',        fmt_mb(ft['host_forest_mb']))
        kv('RSS',                  fmt_mb(ft['host_rss_mb']))
        kv('Fixed components',     fmt_mb(fixed_f))
        kv('Memory ceiling (80%)', fmt_mb(c_f))
        kv('Used vs ceiling',      f"{u_pct:.1f}%  {bar(u_pct)}")
        reached = u_pct >= args.safety_pct
        kv('Ceiling reached',
           color('YES' if reached else 'NO (safety stopped early)',
                 GREEN if reached else YELLOW))

    # ── Cleanup ──────────────────────────────────────────────────────
    if not args.no_cleanup:
        header('CLEANUP')
        print(f"    Removing {inserted_total:,} ceiling-test documents...")
        n_del = cleanup_docs(client, args.database)
        kv('Deleted', f'{n_del:,}')

        if not args.no_index_cleanup and added_indexes:
            print(f"    Removing {len(added_indexes)} range index(es)...")
            remove_indexes(client, args.database, added_indexes)
    else:
        header('CLEANUP SKIPPED')
        print(f"    {inserted_total:,} docs remain in collection '{COLLECTION}'.")
        if added_indexes:
            print(f"    {len(added_indexes)} range index(es) remain on the database.")

    print()
    kv('Snapshots saved',
       f"{len(load_snapshots(args.database))} in {test_snap_dir}")
    print()
    print(f"    To review trends:  "
          f"python3 ../ml_capacity.py --trend --database {args.database}")
    print()


if __name__ == '__main__':
    main()
