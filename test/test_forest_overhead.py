#!/usr/bin/env python3
"""
Per-Forest and Per-Stand Overhead Experiment

Measures fixed memory and disk overhead introduced by each additional
forest attached to the Documents database. Compares:

  Phase A: baseline — 1 forest, empty Documents database
  Phase B: 3 forests — add Documents-2 and Documents-3, still empty
  Phase C: 3 forests — load 100 K docs and measure per-doc costs

Key metrics captured at each phase:
  host-size              (disk: total forest disk on host)
  memory-process-rss     (RSS of the MarkLogic process)
  memory-forest-size     (stand + list memory for all forests)
  per-forest: disk-size, memory-size, stand count

Usage:
    python3 test/test_forest_overhead.py
    python3 test/test_forest_overhead.py --host localhost --port 8102
    python3 test/test_forest_overhead.py --no-cleanup   # leave extra forests

Prerequisites:
    cd test && docker compose up -d
    curl -s -X POST http://localhost:8082/run -d '{}'   # flux-runner up
"""

import argparse
import json
import os
import sys
import time
import urllib.error

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from ml_capacity import MarkLogicClient
from test_ceiling import (
    generate_jsonl_batch,
    flux_import_phase,
    COLLECTION,
)

# ── Defaults ────────────────────────────────────────────────────────────

DEFAULT_HOST      = 'localhost'
DEFAULT_PORT      = 8102   # management API
DEFAULT_APP_PORT  = 8100   # app services (for Flux connection-string)
DEFAULT_FLUX_URL  = 'http://localhost:8082'
DEFAULT_USER      = 'admin'
DEFAULT_PASSWORD  = 'admin'
DEFAULT_AUTH_TYPE = 'digest'
DATABASE          = 'Documents'
EXTRA_FORESTS     = ['Documents-2', 'Documents-3']
LOAD_DOCS         = 100_000
BATCH_SIZE        = 10_000   # docs per Flux call


# ── Snapshot ─────────────────────────────────────────────────────────────

def snapshot(client, label):
    """Capture host-level and per-forest metrics; print and return as dict."""
    h = client.get_json(
        f'/manage/v2/hosts/{_host_id(client)}?view=status&format=json'
    )
    sp = h['host-status']['status-properties']

    host_size  = _find_val(sp, 'host-size')
    rss        = _find_val(sp, 'memory-process-rss')
    proc_size  = _find_val(sp, 'memory-process-size')
    forest_mem = _find_val(sp, 'memory-forest-size')
    sys_total  = _find_val(sp, 'memory-system-total')
    sys_free   = _find_val(sp, 'memory-system-free')

    # Per-forest details
    fl = client.get_json('/manage/v2/forests?format=json')
    forest_names = [
        i['nameref']
        for i in fl['forest-default-list']['list-items']['list-item']
    ]
    forests = {}
    for name in forest_names:
        fs  = client.get_json(f'/manage/v2/forests/{name}?view=status&format=json')
        fsp = fs['forest-status']['status-properties']
        forests[name] = {
            'disk_mb': _sum_values(fsp, 'disk-size'),
            'mem_mb':  _sum_values(fsp, 'memory-size'),
            'stands':  _count_stands(fsp),
        }

    # Doc count
    db = client.get_json(f'/manage/v2/databases/{DATABASE}?view=counts&format=json')
    doc_count = int(
        db.get('database-counts', {})
          .get('count-properties', {})
          .get('documents', {})
          .get('value', 0) or 0
    )

    result = {
        'label':        label,
        'doc_count':    doc_count,
        'host_size_mb': host_size or 0,
        'rss_mb':       rss       or 0,
        'proc_mb':      proc_size or 0,
        'forest_mb':    forest_mem or 0,
        'sys_total_mb': sys_total or 0,
        'sys_free_mb':  sys_free  or 0,
        'forests':      forests,
    }
    _print_snapshot(result)
    return result


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
    if isinstance(v, dict) and 'value' in v:
        return v['value']
    return v


def _sum_values(obj, key):
    """Sum all numeric occurrences of key (handles multi-stand forests)."""
    total = 0.0
    if isinstance(obj, dict):
        if key in obj:
            v = obj[key]
            if isinstance(v, (int, float)):
                total += v
            elif isinstance(v, dict) and 'value' in v:
                total += float(v['value'] or 0)
        for k, v in obj.items():
            if k != key:
                total += _sum_values(v, key)
    elif isinstance(obj, list):
        for item in obj:
            total += _sum_values(item, key)
    return total


def _count_stands(fsp):
    """Count stand elements in a forest status payload."""
    stand_data = _find(fsp, 'stand')
    if stand_data is None:
        return 0
    if isinstance(stand_data, list):
        return len(stand_data)
    if isinstance(stand_data, dict):
        return 1
    return 0


def _print_snapshot(s):
    hr = '-' * 64
    print(f"\n{hr}")
    print(f"  SNAPSHOT: {s['label']}  (docs={s['doc_count']:,})")
    print(hr)
    print(f"  host-size  (disk) : {s['host_size_mb']:>8.1f} MB")
    print(f"  process-rss       : {s['rss_mb']:>8.1f} MB")
    print(f"  process-size      : {s['proc_mb']:>8.1f} MB")
    print(f"  forest-mem        : {s['forest_mb']:>8.1f} MB")
    if s['sys_free_mb'] is not None:
        print(f"  sys-free          : {s['sys_free_mb']:>8.1f} MB / {s['sys_total_mb']} MB")
    doc_forests = [n for n in s['forests'] if 'Documents' in n]
    if doc_forests:
        print(f"  --- Documents forests ---")
        for name in doc_forests:
            f = s['forests'][name]
            print(f"  {name:<22s}  disk={f['disk_mb']:>6.1f} MB"
                  f"  mem={f['mem_mb']:>6.1f} MB  stands={f['stands']}")
    print(hr)


def _delta(a, b):
    n_forests = len(EXTRA_FORESTS)
    print(f"\n  DELTA: {a['label']!r} → {b['label']!r}")
    for k, label in [('host_size_mb','host-size (disk)'),
                      ('rss_mb','rss'),
                      ('proc_mb','proc-size'),
                      ('forest_mb','forest-mem')]:
        diff = (b.get(k) or 0) - (a.get(k) or 0)
        print(f"    {label:<20s}: {diff:+.1f} MB")
    doc_diff = (b['doc_count'] or 0) - (a['doc_count'] or 0)
    if doc_diff > 0:
        for k, label in [('host_size_mb','disk bytes/doc'),
                          ('rss_mb','rss bytes/doc'),
                          ('forest_mb','forest bytes/doc')]:
            diff_bytes = ((b.get(k) or 0) - (a.get(k) or 0)) * 1024 * 1024
            if diff_bytes > 0:
                print(f"    {label:<20s}: {diff_bytes/doc_diff:,.0f}")


# ── Forest management ─────────────────────────────────────────────────────

def create_forest(client, name):
    """Create a new forest on this host via POST /manage/v2/forests."""
    host_id = _host_id(client)
    try:
        status = client.post_json('/manage/v2/forests', {'forest-name': name, 'host': host_id})
        print(f"  Created forest {name!r} (HTTP {status})")
        return True
    except urllib.error.HTTPError as e:
        body_txt = e.read().decode()
        if 'already exists' in body_txt.lower() or e.code == 201:
            print(f"  Forest {name!r} already exists")
            return True
        print(f"  ERROR creating forest {name!r}: HTTP {e.code}\n  {body_txt[:300]}")
        return False


def get_db_forest_list(client, db_name):
    """Return the current list of forest names attached to a database."""
    props = client.get_json(f'/manage/v2/databases/{db_name}/properties?format=json')
    forests = props.get('forest', [])
    if isinstance(forests, str):
        forests = [forests]
    return list(forests)


def attach_forest(client, db_name, forest_name):
    """Attach forest to database by PUTting updated database properties."""
    current = get_db_forest_list(client, db_name)
    if forest_name in current:
        print(f"  {forest_name!r} already in {db_name!r} forest list")
        return True
    updated = current + [forest_name]
    try:
        status = client.put_json(
            f'/manage/v2/databases/{db_name}/properties',
            {'forest': updated},
        )
        print(f"  Attached {forest_name!r} to {db_name!r}  (forests now: {updated})")
        return True
    except urllib.error.HTTPError as e:
        body_txt = e.read().decode()
        print(f"  ERROR attaching {forest_name!r}: HTTP {e.code}\n  {body_txt[:300]}")
        return False


def detach_delete_forest(client, db_name, forest_name):
    """Detach forest from db (via db properties PUT) then delete the forest."""
    # 1. Remove from database forest list
    current = get_db_forest_list(client, db_name)
    if forest_name in current:
        updated = [f for f in current if f != forest_name]
        try:
            client.put_json(
                f'/manage/v2/databases/{db_name}/properties',
                {'forest': updated},
            )
            print(f"  Detached {forest_name!r} from {db_name!r}")
        except urllib.error.HTTPError as e:
            print(f"  WARNING: detach {forest_name!r} HTTP {e.code}: {e.read().decode()[:200]}")

    time.sleep(2)  # let forest go offline before deleting

    # 2. Delete the forest itself
    try:
        status = client.delete_resource(f'/manage/v2/forests/{forest_name}?level=full')
        print(f"  Deleted forest {forest_name!r} (HTTP {status})")
        return True
    except urllib.error.HTTPError as e:
        body_txt = e.read().decode()
        print(f"  ERROR deleting {forest_name!r}: HTTP {e.code}\n  {body_txt[:300]}")
        return False


def clear_database(client, db_name):
    """Clear all documents via POST /manage/v2/databases/{db} operation=clear-database."""
    try:
        status = client.post_json(
            f'/manage/v2/databases/{db_name}',
            {'operation': 'clear-database'},
        )
        print(f"  Cleared {db_name!r} (HTTP {status})")
        return True
    except urllib.error.HTTPError as e:
        body_txt = e.read().decode()
        print(f"  ERROR clearing {db_name!r}: HTTP {e.code}\n  {body_txt[:300]}")
        return False


def wait_settle(secs=5, msg='Settling...'):
    print(f"  {msg} ({secs}s)")
    time.sleep(secs)


# ── Main experiment ───────────────────────────────────────────────────────

def run(args):
    client = MarkLogicClient(
        args.host, args.port, args.user, args.password, args.auth_type
    )

    print(f"\n=== Per-Forest Overhead Experiment ===")
    print(f"  Manage: {args.host}:{args.port}  App: {args.host}:{args.app_port}")
    print(f"  Flux:   {args.flux_url}")

    # ── Phase A: 1 forest, empty ──────────────────────────────────────────
    print("\n\n[Phase A] Baseline — 1 forest, empty Documents")
    clear_database(client, DATABASE)
    wait_settle(8, 'Waiting for clear to settle')
    snap_a = snapshot(client, 'A: 1 forest, empty')

    # ── Phase B: 3 forests, empty ─────────────────────────────────────────
    print(f"\n\n[Phase B] Adding {len(EXTRA_FORESTS)} extra forests to {DATABASE!r}")
    for f in EXTRA_FORESTS:
        if create_forest(client, f):
            attach_forest(client, DATABASE, f)
    wait_settle(10, 'Waiting for forests to open and stabilise')
    snap_b = snapshot(client, f'B: {1+len(EXTRA_FORESTS)} forests, empty')

    print()
    _delta(snap_a, snap_b)
    n_added = len(EXTRA_FORESTS)
    for k, label in [('host_size_mb','disk/forest'),
                      ('rss_mb','rss/forest'),
                      ('forest_mb','forest-mem/forest')]:
        diff = (snap_b.get(k) or 0) - (snap_a.get(k) or 0)
        print(f"  Per-forest overhead  {label:<18s}: {diff/n_added:+.1f} MB")

    # ── Phase C: 3 forests, loaded ───────────────────────────────────────
    print(f"\n\n[Phase C] Loading {args.load_docs:,} docs ({1+n_added} forests)")
    total_loaded = 0
    offset = 0
    while total_loaded < args.load_docs:
        batch = min(BATCH_SIZE, args.load_docs - total_loaded)
        print(f"  Batch {total_loaded+1:,}–{total_loaded+batch:,}…", end='', flush=True)
        jsonl = generate_jsonl_batch(batch, offset)
        flux_import_phase(
            flux_url=args.flux_url,
            jsonl_str=jsonl,
            ml_host=args.host,
            ml_app_port=args.app_port,
            ml_user=args.user,
            ml_password=args.password,
            auth_type=args.auth_type.upper(),
            collection=COLLECTION,
            uri_template='/forest-test/{event_id}.json',
        )
        total_loaded += batch
        offset       += batch
        print(' done')
    wait_settle(10, 'Waiting for post-load settle')
    snap_c = snapshot(client, f'C: {1+n_added} forests, {args.load_docs:,} docs')

    # ── Summary ──────────────────────────────────────────────────────────
    print("\n\n=== Summary ===")
    print("\n  Per-forest overhead (empty forest addition):")
    for k, label in [('host_size_mb','disk'),
                      ('rss_mb','rss'),
                      ('forest_mb','forest-mem')]:
        diff = (snap_b.get(k) or 0) - (snap_a.get(k) or 0)
        print(f"    {label:<12s}: {diff:.1f} MB total  ({diff/n_added:.1f} MB/forest)")

    print(f"\n  Per-doc cost  ({1+n_added} forests, docs A→C):")
    doc_diff = (snap_c['doc_count'] or 0) - (snap_a['doc_count'] or 0)
    if doc_diff > 0:
        for k, label in [('host_size_mb','disk'),
                          ('rss_mb','rss'),
                          ('forest_mb','forest-mem')]:
            diff_bytes = ((snap_c.get(k) or 0) - (snap_b.get(k) or 0)) * 1024 * 1024
            if diff_bytes > 0:
                print(f"    {label:<12s}: {diff_bytes/doc_diff:,.0f} bytes/doc")

    print()
    _delta(snap_b, snap_c)

    # ── Cleanup ───────────────────────────────────────────────────────────
    if not args.no_cleanup:
        print("\n\n[Cleanup] Removing extra forests")
        clear_database(client, DATABASE)
        wait_settle(5)
        for f in EXTRA_FORESTS:
            detach_delete_forest(client, DATABASE, f)
        wait_settle(5, 'Post-cleanup settle')
        snap_final = snapshot(client, 'Final: 1 forest, empty (post-cleanup)')
        print()
        _delta(snap_a, snap_final)
    else:
        print("\n  --no-cleanup: extra forests left in place.")

    print("\n=== Done ===\n")


# ── CLI ───────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument('--host',      default=DEFAULT_HOST)
    p.add_argument('--port',      default=DEFAULT_PORT,     type=int,
                   help='Management API port (default 8102)')
    p.add_argument('--app-port',  default=DEFAULT_APP_PORT, type=int,
                   help='App Services port for Flux (default 8100)')
    p.add_argument('--user',      default=DEFAULT_USER)
    p.add_argument('--password',  default=DEFAULT_PASSWORD)
    p.add_argument('--auth-type', default=DEFAULT_AUTH_TYPE)
    p.add_argument('--flux-url',  default=DEFAULT_FLUX_URL)
    p.add_argument('--load-docs', default=LOAD_DOCS, type=int)
    p.add_argument('--no-cleanup', action='store_true',
                   help='Leave extra forests attached after experiment')
    args = p.parse_args()
    run(args)


if __name__ == '__main__':
    main()
