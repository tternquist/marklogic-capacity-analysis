#!/usr/bin/env python3
"""
In-Memory Stand Overhead Monitor

Runs a Flux bulk load in a background thread while polling MarkLogic
memory metrics every 2 seconds. Produces a timeline showing:

  - memory-forest-size  (includes in-memory stand data)
  - memory-process-rss
  - host-size (disk)
  - per-forest stand count and memory

This captures the *peak* memory pressure during ingestion — when
in-memory stands are full but not yet flushed to disk — vs the
settled post-load state. The ratio (peak / settled) tells us the
"burst headroom" factor needed in capacity planning.

Usage:
    python3 test/test_inmemory_stands.py
    python3 test/test_inmemory_stands.py --docs 200000 --poll-interval 1
    python3 test/test_inmemory_stands.py --no-cleanup

Prerequisites:
    cd test && docker compose up -d
    curl -s -X POST http://localhost:8082/run -d '{}'
"""

import argparse
import json
import os
import sys
import time
import threading
import urllib.error
import urllib.request

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from ml_capacity import MarkLogicClient
from test_ceiling import generate_jsonl_batch, _JSOHLServer, COLLECTION

DEFAULT_HOST      = 'localhost'
DEFAULT_PORT      = 8102
DEFAULT_APP_PORT  = 8100
DEFAULT_FLUX_URL  = 'http://localhost:8082'
DEFAULT_USER      = 'admin'
DEFAULT_PASSWORD  = 'admin'
DEFAULT_AUTH_TYPE = 'digest'
DATABASE          = 'Documents'
DEFAULT_DOCS      = 200_000
DEFAULT_INTERVAL  = 2.0   # seconds between polls
SERVE_PORT        = 19999


# ── Host/forest metrics (single poll) ────────────────────────────────────

def _host_id(client):
    hl = client.get_json('/manage/v2/hosts?format=json')
    return hl['host-default-list']['list-items']['list-item'][0]['idref']


def _find(obj, key):
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            r = _find(v, key)
            if r is not None:
                return r
    elif isinstance(obj, list):
        for item in obj:
            r = _find(item, key)
            if r is not None:
                return r
    return None


def _find_val(obj, key):
    v = _find(obj, key)
    if v is None:
        return None
    return v['value'] if isinstance(v, dict) and 'value' in v else v


def _sum_values(obj, key):
    total = 0.0
    if isinstance(obj, dict):
        if key in obj:
            v = obj[key]
            total += float(v['value'] if isinstance(v, dict) and 'value' in v else (v or 0))
        for k, v in obj.items():
            if k != key:
                total += _sum_values(v, key)
    elif isinstance(obj, list):
        for item in obj:
            total += _sum_values(item, key)
    return total


def _count_stands(fsp):
    sd = _find(fsp, 'stand')
    if sd is None: return 0
    return len(sd) if isinstance(sd, list) else 1


def poll_once(client, hid):
    """Return a metrics dict for the current instant."""
    h  = client.get_json(f'/manage/v2/hosts/{hid}?view=status&format=json')
    sp = h['host-status']['status-properties']

    # Per-Documents forest detail
    doc_forest_mem   = 0.0
    doc_forest_disk  = 0.0
    doc_forest_stands= 0

    fl = client.get_json('/manage/v2/forests?format=json')
    for item in fl['forest-default-list']['list-items']['list-item']:
        name = item['nameref']
        if 'Documents' not in name:
            continue
        fs  = client.get_json(f'/manage/v2/forests/{name}?view=status&format=json')
        fsp = fs['forest-status']['status-properties']
        doc_forest_mem   += _sum_values(fsp, 'memory-size')
        doc_forest_disk  += _sum_values(fsp, 'disk-size')
        doc_forest_stands+= _count_stands(fsp)

    return {
        'ts':               time.time(),
        'rss_mb':           _find_val(sp, 'memory-process-rss')   or 0,
        'proc_mb':          _find_val(sp, 'memory-process-size')  or 0,
        'forest_mb':        _find_val(sp, 'memory-forest-size')   or 0,
        'host_size_mb':     _find_val(sp, 'host-size')            or 0,
        'sys_free_mb':      _find_val(sp, 'memory-system-free')   or 0,
        'doc_forest_mem_mb':  doc_forest_mem,
        'doc_forest_disk_mb': doc_forest_disk,
        'doc_stands':         doc_forest_stands,
    }


# ── Flux loader (runs in background thread) ───────────────────────────────

def _flux_load(flux_url, jsonl_str, ml_host, ml_app_port,
               ml_user, ml_password, auth_type, result_box):
    """Run Flux import; store result/exception in result_box[0]."""
    conn_str = f'{ml_user}:{ml_password}@{ml_host}:{ml_app_port}'
    with _JSOHLServer(jsonl_str, port=SERVE_PORT) as serve_url:
        args = [
            'import-aggregate-json-files',
            '--connection-string', conn_str,
            '--auth-type',         auth_type.upper(),
            '--http-url',          serve_url,
            '--json-lines',
            '--collections',       COLLECTION,
            '--permissions',       'rest-reader,read,rest-writer,update',
            '--uri-template',      '/inmem-test/{event_id}.json',
            '--batch-size',        '500',
            '--thread-count',      '4',
        ]
        body = json.dumps({'args': args}).encode()
        req  = urllib.request.Request(
            f'{flux_url}/run', data=body,
            headers={'Content-Type': 'application/json'}, method='POST',
        )
        try:
            with urllib.request.urlopen(req, timeout=900) as resp:
                result_box[0] = ('ok', resp.read().decode())
        except Exception as exc:
            result_box[0] = ('err', str(exc))


# ── Reporting ─────────────────────────────────────────────────────────────

def _hdr():
    return (f"{'t(s)':>5}  {'forest_mb':>9}  {'doc_fmem':>8}  "
            f"{'rss_mb':>7}  {'doc_disk':>8}  {'stands':>6}  {'sys_free':>8}")


def _row(elapsed, m, mark=''):
    return (f"{elapsed:5.0f}  {m['forest_mb']:9.1f}  {m['doc_forest_mem_mb']:8.1f}  "
            f"{m['rss_mb']:7.1f}  {m['doc_forest_disk_mb']:8.1f}  "
            f"{m['doc_stands']:6d}  {m['sys_free_mb']:8.1f}  {mark}")


def _print_analysis(baseline, timeline, settled):
    if not timeline:
        return

    peak_forest = max(timeline, key=lambda m: m['forest_mb'])
    peak_rss    = max(timeline, key=lambda m: m['rss_mb'])

    base_forest = baseline['forest_mb']
    base_rss    = baseline['rss_mb']
    base_disk   = baseline['host_size_mb']

    settle_forest = settled['forest_mb']
    settle_rss    = settled['rss_mb']
    settle_disk   = settled['host_size_mb']

    doc_count = int(round(  # approximate from disk delta and prior calibration
        (settle_disk - base_disk) * 1024 * 1024 / 1206
    ))

    print("\n" + "=" * 70)
    print("  ANALYSIS")
    print("=" * 70)

    def row(label, base, peak, settle, unit='MB'):
        peak_over_settle = ((peak - settle) / settle * 100) if settle else 0
        print(f"  {label:<22s}  base={base:7.1f}  peak={peak:7.1f}  "
              f"settled={settle:7.1f}  peak_excess={peak_over_settle:+.1f}%  [{unit}]")

    row('forest-mem (all)',  base_forest, peak_forest['forest_mb'], settle_forest)
    row('doc forest-mem',   baseline['doc_forest_mem_mb'],
                            peak_forest['doc_forest_mem_mb'],
                            settled['doc_forest_mem_mb'])
    row('process-rss',      base_rss,    peak_rss['rss_mb'],        settle_rss)
    row('host-size (disk)', base_disk,   max(m['host_size_mb'] for m in timeline),
                            settle_disk)

    delta_forest  = settle_forest - base_forest
    delta_rss     = settle_rss    - base_rss
    delta_disk    = settle_disk   - base_disk
    peak_delta_f  = peak_forest['forest_mb'] - base_forest
    peak_delta_rss= peak_rss['rss_mb']       - base_rss

    print()
    if delta_disk > 0:
        docs_loaded = (settle_disk - base_disk) * 1024 * 1024 / 1206
        print(f"  Estimated docs loaded : {docs_loaded:,.0f}  "
              f"(from disk delta {delta_disk:.1f} MB @ 1206 bytes/doc)")
        if delta_forest > 0:
            settled_forest_per_doc = delta_forest * 1024 * 1024 / docs_loaded
            print(f"  Settled forest-mem/doc: {settled_forest_per_doc:,.0f} bytes  "
                  f"(total delta {delta_forest:.1f} MB)")
        if peak_delta_f > 0 and delta_forest > 0:
            burst = peak_delta_f / delta_forest
            print(f"  Peak / settled forest : {burst:.2f}x  "
                  f"(burst factor for in-memory stands)")
        if peak_delta_rss > 0 and delta_rss > 0:
            burst_rss = peak_delta_rss / delta_rss
            print(f"  Peak / settled RSS    : {burst_rss:.2f}x")

    peak_ts   = peak_forest['ts'] - timeline[0]['ts']
    settle_ts = settled['ts']     - timeline[-1]['ts']
    print(f"\n  Peak forest-mem at t+{peak_ts:.0f}s into load")
    print(f"  Settled {abs(settle_ts):.0f}s after load completed")
    print()


# ── Main ──────────────────────────────────────────────────────────────────

def run(args):
    client = MarkLogicClient(
        args.host, args.port, args.user, args.password, args.auth_type
    )
    hid = _host_id(client)

    print(f"\n=== In-Memory Stand Monitor ===")
    print(f"  {args.host}:{args.port}  flux={args.flux_url}  "
          f"docs={args.docs:,}  poll={args.poll_interval}s")

    # ── Clear + baseline ──────────────────────────────────────────────────
    print("\n[Setup] Clearing Documents and taking baseline…")
    client.post_json(f'/manage/v2/databases/{DATABASE}', {'operation': 'clear-database'})
    time.sleep(8)
    baseline = poll_once(client, hid)
    print(f"  Baseline: forest={baseline['forest_mb']:.1f} MB  "
          f"rss={baseline['rss_mb']:.1f} MB  disk={baseline['host_size_mb']:.1f} MB")

    # ── Generate JSONL ────────────────────────────────────────────────────
    print(f"\n[Generate] Building {args.docs:,} doc JSONL…", end='', flush=True)
    t0 = time.time()
    jsonl = generate_jsonl_batch(args.docs, 0)
    print(f" {len(jsonl)/1e6:.1f} MB in {time.time()-t0:.1f}s")

    # ── Start Flux in background thread ───────────────────────────────────
    result_box = [None]
    flux_thread = threading.Thread(
        target=_flux_load,
        args=(args.flux_url, jsonl, args.host, args.app_port,
              args.user, args.password, args.auth_type, result_box),
        daemon=True,
    )

    print(f"\n[Load] Starting Flux import…  polling every {args.poll_interval}s\n")
    print(_hdr())
    print('-' * 75)

    timeline    = []
    load_start  = time.time()
    flux_thread.start()

    # Give Flux a moment to start before first poll
    time.sleep(1)

    while flux_thread.is_alive():
        try:
            m = poll_once(client, hid)
            elapsed = m['ts'] - load_start
            # Mark stand flushes (disk grows) and stand count changes
            mark = ''
            if timeline:
                prev = timeline[-1]
                if m['doc_stands'] < prev['doc_stands']:
                    mark = '<-- merge'
                elif m['doc_forest_disk_mb'] > prev['doc_forest_disk_mb'] + 4:
                    mark = '<-- flush'
                elif m['doc_stands'] > prev['doc_stands']:
                    mark = '<-- new stand'
            timeline.append(m)
            print(_row(elapsed, m, mark))
        except Exception as exc:
            print(f"  poll error: {exc}")
        time.sleep(args.poll_interval)

    flux_thread.join()
    load_elapsed = time.time() - load_start
    status, detail = result_box[0] or ('?', '')
    print(f"\n[Load done]  {load_elapsed:.1f}s  status={status}")
    if status == 'err':
        print(f"  ERROR: {detail}")

    # ── Settle and final snapshot ─────────────────────────────────────────
    print(f"\n[Settle] Waiting 15s for in-memory stands to flush…")
    settle_snapshots = []
    for i in range(8):
        time.sleep(2)
        m = poll_once(client, hid)
        elapsed = m['ts'] - load_start
        print(_row(elapsed, m, '<-- settling' if i == 0 else ''))
        settle_snapshots.append(m)

    settled = settle_snapshots[-1]

    # ── Analysis ──────────────────────────────────────────────────────────
    _print_analysis(baseline, timeline, settled)

    # ── Cleanup ───────────────────────────────────────────────────────────
    if not args.no_cleanup:
        print("[Cleanup] Clearing Documents…")
        client.post_json(f'/manage/v2/databases/{DATABASE}', {'operation': 'clear-database'})
        time.sleep(5)
        print("  Done.")
    else:
        print("[--no-cleanup] Documents left as-is.")


def main():
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument('--host',          default=DEFAULT_HOST)
    p.add_argument('--port',          default=DEFAULT_PORT,     type=int)
    p.add_argument('--app-port',      default=DEFAULT_APP_PORT, type=int)
    p.add_argument('--user',          default=DEFAULT_USER)
    p.add_argument('--password',      default=DEFAULT_PASSWORD)
    p.add_argument('--auth-type',     default=DEFAULT_AUTH_TYPE)
    p.add_argument('--flux-url',      default=DEFAULT_FLUX_URL)
    p.add_argument('--docs',          default=DEFAULT_DOCS, type=int)
    p.add_argument('--poll-interval', default=DEFAULT_INTERVAL, type=float)
    p.add_argument('--no-cleanup',    action='store_true')
    args = p.parse_args()
    run(args)


if __name__ == '__main__':
    main()
