#!/usr/bin/env python3
"""
MLCA Memory Projection Test Harness

Validates that MLCA's trend-based memory projections converge on the
actual memory ceiling by loading documents in phases against a clean
MarkLogic instance (Docker Compose, 4 GB memory limit).

What it tests:
  1. Load documents in phases (e.g., 5 phases of 100K docs)
  2. Take a snapshot after each phase
  3. Run trend analysis after each phase (starting from phase 2)
  4. Record the memory runway projection at each phase
  5. Validate that projections converge as more data points are added
  6. Report projection accuracy and stability

Expected behavior:
  - After 2 snapshots, the memory runway estimate may be rough
  - After 3-4 snapshots, it should stabilize
  - The estimate should converge toward the actual ceiling experience
  - Configuration drift checks should remain STABLE throughout

Prerequisites:
  - Docker Compose ML instance running (see docker-compose.yml)
  - Default: localhost:8102 (Management API), admin/admin, basic auth

Usage:
    # Start the test instance
    docker compose up -d
    # Wait for healthcheck to pass
    docker compose ps

    # Run the harness
    python3 test_harness.py

    # Or with custom settings
    python3 test_harness.py --phases 8 --phase-size 50000

    # Tear down
    docker compose down -v
"""

import argparse
import getpass
import json
import os
import shutil
import subprocess
import sys
import time

# Add parent dir to path for ml_capacity imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from ml_capacity import (
    MarkLogicClient, collect_snapshot, save_snapshot, load_snapshots,
    check_config_drift, extract_config_fingerprint, SNAPSHOT_DIR,
    diff_index_memory, _index_label, wait_for_reindex,
    fmt_mb, color, header, sub_header, kv, bar, status_badge,
    BOLD, CYAN, GREEN, RED, YELLOW, DIM, RESET,
)

# ── Document generation (server-side, same as stress test) ──────────

BATCH_INSERT_JS = """
declareUpdate();
const words = [
  'cluster','forest','index','fragment','stand','merge','shard',
  'document','query','search','cache','memory','disk','scale',
  'capacity','replication','backup','restore','journal','lock',
  'transaction','database','schema','field','element','attribute',
  'namespace','collection','permission','role','user','host'
];
const categories = ['alpha','beta','gamma','delta','epsilon','zeta'];
const statuses   = ['active','inactive','pending','archived','draft'];

function pick(arr) { return arr[Math.floor(Math.random() * arr.length)]; }
function randStr(n) {
  let  s = '';
  const c = 'abcdefghijklmnopqrstuvwxyz';
  for (let i = 0; i < n; i++) s += c[Math.floor(Math.random() * c.length)];
  return s;
}
function randSentence(min, max) {
  const n = min + Math.floor(Math.random() * (max - min + 1));
  const out = [];
  for (let i = 0; i < n; i++) out.push(pick(words));
  return out.join(' ');
}
function randDate() {
  const y = 2018 + Math.floor(Math.random() * 9);
  const m = 1 + Math.floor(Math.random() * 12);
  const d = 1 + Math.floor(Math.random() * 28);
  return y + '-' + String(m).padStart(2,'0') + '-' + String(d).padStart(2,'0');
}

let inserted = 0;
const perms = [
  xdmp.permission('rest-reader', 'read'),
  xdmp.permission('rest-writer', 'update')
];

for (let i = 0; i < count; i++) {
  const id = offset + i;
  const sizeRoll = Math.random();
  const sizeClass = sizeRoll < 0.25 ? 'small' : sizeRoll < 0.85 ? 'medium' : 'large';

  const doc = {
    id:        'harness-' + String(id).padStart(10, '0'),
    category:  pick(categories),
    status:    pick(statuses),
    created:   randDate(),
    score:     Math.round(Math.random() * 100000) / 1000,
    sizeClass: sizeClass
  };

  if (sizeClass === 'medium' || sizeClass === 'large') {
    doc.title       = randSentence(3, 8);
    doc.description = randSentence(10, 25);
    doc.tags        = [pick(words), pick(words), pick(words)];
    doc.count       = Math.floor(Math.random() * 10000);
    doc.enabled     = Math.random() > 0.5;
    doc.region      = randStr(6);
    doc.updated     = randDate();
    doc.priority    = 1 + Math.floor(Math.random() * 10);
    doc.ratio       = Math.round(Math.random() * 1000000) / 1000000;
  }

  if (sizeClass === 'large') {
    doc.notes   = randSentence(30, 60);
    doc.author  = randStr(6) + ' ' + randStr(8);
    doc.source  = 'https://' + randStr(8) + '.example.com/' + randStr(4);
    doc.version = (1+Math.floor(Math.random()*4))+'.'
                  +Math.floor(Math.random()*10)+'.'
                  +Math.floor(Math.random()*20);
    doc.attrs = {};
    const nA = 3 + Math.floor(Math.random()*6);
    for (let a = 0; a < nA; a++) doc.attrs[randStr(5)] = randStr(10);
    doc.history = [];
    const nH = 2 + Math.floor(Math.random()*5);
    for (let h = 0; h < nH; h++) {
      doc.history.push({ date: randDate(), action: pick(statuses) });
    }
  }

  xdmp.documentInsert(
    uriPrefix + doc.id + '.json', doc,
    { permissions: perms, collections: [collection] }
  );
  inserted++;
}
inserted;
"""

COLLECTION = "harness-test"
URI_PREFIX = "/harness-test/"


def insert_phase(client, database, count, offset):
    result = client.eval_javascript(
        BATCH_INSERT_JS, database=database,
        vars={"count": count, "offset": offset,
              "uriPrefix": URI_PREFIX, "collection": COLLECTION},
    )
    return result[0] if result else 0


def compute_memory_runway(snaps):
    """Compute memory runway from a list of snapshots (same logic as --trend).

    Returns dict with runway_days, growth_rate_mb_day, headroom_mb,
    forest_bytes_per_doc, or None if insufficient data.
    """
    if len(snaps) < 2:
        return None

    from datetime import datetime

    first_t = snaps[0].get("totals", {})
    last_t  = snaps[-1].get("totals", {})

    try:
        ts_first = datetime.fromisoformat(snaps[0]["timestamp"])
        ts_last  = datetime.fromisoformat(snaps[-1]["timestamp"])
    except (ValueError, TypeError):
        return None

    days = (ts_last - ts_first).total_seconds() / 86400
    if days <= 0:
        return None

    forest_first = first_t.get("host_forest_mb", 0)
    forest_last  = last_t.get("host_forest_mb", 0)
    forest_delta = forest_last - forest_first

    sys_total = last_t.get("system_total_mb", 0)
    cache_mb  = last_t.get("host_cache_mb", 0)
    base_mb   = last_t.get("host_base_mb", 0)
    file_mb   = last_t.get("host_file_mb", 0)
    fixed     = cache_mb + base_mb + file_mb
    ceiling   = sys_total * 0.80
    headroom  = ceiling - fixed - forest_last

    doc_delta = last_t.get("documents", 0) - first_t.get("documents", 0)

    result = {
        "days": days,
        "forest_first_mb": forest_first,
        "forest_last_mb": forest_last,
        "forest_delta_mb": forest_delta,
        "fixed_mb": fixed,
        "ceiling_mb": ceiling,
        "headroom_mb": headroom,
        "doc_delta": doc_delta,
        "docs_total": last_t.get("documents", 0),
    }

    if forest_delta > 0:
        daily_growth = forest_delta / days
        result["growth_rate_mb_day"] = daily_growth
        result["runway_days"] = headroom / daily_growth if headroom > 0 else 0

        if doc_delta > 0:
            result["forest_bytes_per_doc"] = (forest_delta * 1024 * 1024) / doc_delta
            result["docs_until_ceiling"] = int((headroom * 1024 * 1024) / result["forest_bytes_per_doc"]) if headroom > 0 else 0

    return result


def wait_for_ml(client, timeout=120):
    """Wait for MarkLogic to be responsive."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            client.get_json("/manage/v2?format=json")
            return True
        except Exception:
            time.sleep(2)
    return False


def _add_index_and_measure(client, database, path_expr, scalar_type, label,
                           collation=None):
    """Add a range-path-index, merge, warm cache, snapshot, then remove.

    Returns dict with measurement results or None on failure.
    """
    sub_header(f"Testing: {label}")
    kv("Path expression", path_expr)
    kv("Scalar type", scalar_type)

    # Snapshot before
    before_snap = collect_snapshot(client, database)
    save_snapshot(before_snap)
    before_docs = before_snap["totals"]["documents"]

    # Get current indexes to restore later
    current_props = client.get_json(
        f"/manage/v2/databases/{database}/properties?format=json"
    )
    existing_path_indexes = current_props.get("range-path-index", [])

    # Add the index
    new_index = {
        "scalar-type": scalar_type,
        "path-expression": path_expr,
        "range-value-positions": False,
        "invalid-values": "ignore"
    }
    if collation:
        new_index["collation"] = collation

    client.put_json(
        f"/manage/v2/databases/{database}/properties",
        {"range-path-index": existing_path_indexes + [new_index]}
    )
    print(f"    {color('Index added.', GREEN)} Waiting for reindex...")

    if not wait_for_reindex(client, database, timeout=300):
        print(f"    {color('WARNING: Reindex timed out', YELLOW)}")
    else:
        print(f"    {color('Reindex complete.', GREEN)}")

    # Force merge to consolidate
    print(f"    Forcing merge...")
    try:
        client.eval_xquery(
            'xdmp:merge(<options xmlns="xdmp:merge"><forests><forest>'
            '{xdmp:database-forests(xdmp:database("' + database + '"))}'
            '</forest></forests></options>)'
        )
        for _ in range(60):
            data = client.get_json(f"/manage/v2/databases/{database}?view=status&format=json")
            mc = data.get("database-status", {}).get("status-properties", {}).get("merge-count", {}).get("value", 0)
            if mc == 0:
                break
            time.sleep(2)
        print(f"    {color('Merge complete.', GREEN)}")
    except Exception as e:
        print(f"    {color(f'Merge warning: {e}', YELLOW)}")

    # Warm cache
    try:
        client.eval_xquery(
            f'cts:values(cts:path-reference("{path_expr}"))',
            database=database
        )
    except Exception:
        pass

    time.sleep(3)

    # Snapshot after
    after_snap = collect_snapshot(client, database)
    save_snapshot(after_snap)

    # Measure via diff
    diff = diff_index_memory(before_snap, after_snap)

    # Stand-level aggregate
    def sum_range_bytes(snap):
        im = snap.get("index_memory") or {}
        return sum(
            ss.get("summary", {}).get("rangeIndexesBytes", 0)
            for ss in im.get("standSummaries", [])
        )

    range_delta = sum_range_bytes(after_snap) - sum_range_bytes(before_snap)
    disk_delta_mb = (after_snap.get("totals", {}).get("forest_disk_mb", 0) -
                     before_snap.get("totals", {}).get("forest_disk_mb", 0))

    # Per-index detail for the new index
    per_index_mem = 0
    for idx in diff["added"]:
        if path_expr in _index_label(idx):
            per_index_mem = idx.get("totalMemoryBytes", 0) or 0

    # Remove the index (restore original)
    client.put_json(
        f"/manage/v2/databases/{database}/properties",
        {"range-path-index": existing_path_indexes}
    )
    print(f"    {color('Index removed.', GREEN)} Waiting for reindex...")
    wait_for_reindex(client, database, timeout=300)
    time.sleep(2)

    # Best available signal for per-doc cost:
    # 1. range_delta if positive (stand-level aggregate, most granular)
    # 2. disk_delta if positive (forest-level, includes merge compression noise)
    # 3. per_index_mem if positive (cache-level, only warmed data)
    if range_delta > 0 and before_docs > 0:
        bytes_per_doc = range_delta / before_docs
        cost_source = "stand aggregate"
    elif disk_delta_mb > 0 and before_docs > 0:
        bytes_per_doc = (disk_delta_mb * 1024 * 1024) / before_docs
        cost_source = "disk delta"
    elif per_index_mem > 0 and before_docs > 0:
        bytes_per_doc = per_index_mem / before_docs
        cost_source = "cache detail"
    else:
        bytes_per_doc = 0
        cost_source = "no signal"

    result = {
        "label": label,
        "path": path_expr,
        "scalar_type": scalar_type,
        "docs": before_docs,
        "range_delta_bytes": range_delta,
        "disk_delta_mb": disk_delta_mb,
        "per_index_cache_bytes": per_index_mem,
        "found_in_added": any(path_expr in _index_label(i) for i in diff["added"]),
        "bytes_per_doc": bytes_per_doc,
        "cost_source": cost_source,
    }

    kv("Range index aggregate delta", f"{range_delta:,} bytes"
       + (f"  {color('(negative — merges compressed existing data)', DIM)}" if range_delta < 0 else ""))
    kv("Forest disk delta", fmt_mb(disk_delta_mb))
    if bytes_per_doc > 0:
        kv(f"Bytes/doc ({cost_source})", f"{bytes_per_doc:,.0f}")
    print()

    return result


def run_index_impact_test(client, database):
    """Validate index impact assessment with low and high cardinality indexes.

    Tests two indexes to show how cardinality affects per-doc cost:
      - /category (string, ~6 unique values)  — low cardinality
      - /id       (string, unique per doc)    — high cardinality

    This demonstrates that index cost is NOT constant per scalar type —
    it depends heavily on value cardinality and size.
    """
    header("INDEX IMPACT VALIDATION")

    before_snap = collect_snapshot(client, database)
    before_docs = before_snap["totals"]["documents"]
    kv("Documents", f"{before_docs:,}")

    if before_docs == 0:
        print(f"    {color('SKIP: No documents loaded — cannot measure index cost', YELLOW)}")
        return True

    # ── Test 1: Low cardinality (/category — ~6 unique values) ────
    low_card = _add_index_and_measure(
        client, database,
        path_expr="/category",
        scalar_type="string",
        label="Low cardinality: /category (~6 unique values)",
        collation="http://marklogic.com/collation/",
    )

    # ── Test 2: High cardinality (/id — unique per document) ──────
    high_card = _add_index_and_measure(
        client, database,
        path_expr="/id",
        scalar_type="string",
        label="High cardinality: /id (unique per document)",
        collation="http://marklogic.com/collation/",
    )

    # ── Test 3: Numeric (/score — float, ~100K unique values) ─────
    numeric = _add_index_and_measure(
        client, database,
        path_expr="/score",
        scalar_type="float",
        label="Numeric: /score (float, ~100K unique values)",
    )

    # ── Results comparison ────────────────────────────────────────
    sub_header("Index Cost Comparison")

    results = [r for r in [low_card, high_card, numeric] if r]

    print()
    hdr = f"  {'Index':40}  {'Type':8}  {'B/doc':>10}  {'Source':>16}  {'Disk Δ':>10}"
    print(color(hdr, BOLD))
    print(color("  " + "-" * (len(hdr) - 2), DIM))

    for r in results:
        bpd = f"{r['bytes_per_doc']:,.0f}" if r["bytes_per_doc"] > 0 else "—"
        src = r.get("cost_source", "—")
        print(
            f"  {r['label'][:40]:40}  {r['scalar_type']:8}  "
            f"{bpd:>10}  {src:>16}  {fmt_mb(r['disk_delta_mb']):>10}"
        )

    # ── Validation checks ─────────────────────────────────────────
    sub_header("Validation Checks")
    checks = []

    for r in results:
        # Check: index was detected
        checks.append((
            r["found_in_added"],
            f"{r['path']} index detected in snapshot diff",
            f"{r['path']} index NOT found in diff"
        ))
        # Check: measurable impact
        checks.append((
            r["range_delta_bytes"] > 0 or r["disk_delta_mb"] > 0,
            f"{r['path']} has measurable impact (range Δ={r['range_delta_bytes']:,} bytes)",
            f"{r['path']} has no measurable impact"
        ))

    # Check: high cardinality costs more than low cardinality
    if low_card and high_card and low_card["bytes_per_doc"] > 0 and high_card["bytes_per_doc"] > 0:
        ratio = high_card["bytes_per_doc"] / low_card["bytes_per_doc"]
        checks.append((
            ratio > 1.5,
            f"High cardinality costs {ratio:.1f}x more than low cardinality per doc",
            f"High cardinality ({high_card['bytes_per_doc']:,.0f} B/doc) not significantly "
            f"more than low ({low_card['bytes_per_doc']:,.0f} B/doc) — ratio {ratio:.1f}x"
        ))

    all_pass = True
    for ok, pass_msg, fail_msg in checks:
        if ok:
            print(f"    {color('PASS', GREEN)}  {pass_msg}")
        else:
            print(f"    {color('FAIL', RED)}  {fail_msg}")
            all_pass = False

    if all_pass:
        print(f"\n    {color('All index impact checks passed.', GREEN + BOLD)}")
    return all_pass


def main():
    parser = argparse.ArgumentParser(
        description="MLCA Memory Projection Test Harness"
    )
    parser.add_argument("--host",       default="localhost")
    parser.add_argument("--port",       type=int, default=8102,
                        help="Management API port (default: 8102 for compose)")
    parser.add_argument("--user",       default="admin")
    parser.add_argument("--password",   default="admin")
    parser.add_argument("--database",   default="Documents")
    parser.add_argument("--auth-type",  default="digest")
    parser.add_argument("--phases",     type=int, default=6,
                        help="Number of loading phases (default: 6)")
    parser.add_argument("--phase-size", type=int, default=100000,
                        help="Documents per phase (default: 100,000)")
    parser.add_argument("--snapshot-dir", default=None,
                        help="Override snapshot directory (default: test/.ml-capacity-harness/)")
    parser.add_argument("--index-impact", action="store_true",
                        help="Run index impact validation after loading phases")

    args = parser.parse_args()

    # Use a separate snapshot directory for the test harness
    test_snap_dir = args.snapshot_dir or os.path.join(os.path.dirname(__file__), ".ml-capacity-harness")

    # Monkey-patch the snapshot dir for this run
    import ml_capacity
    original_snap_dir = ml_capacity.SNAPSHOT_DIR
    ml_capacity.SNAPSHOT_DIR = type(original_snap_dir)(test_snap_dir)

    # Clear any previous harness snapshots
    if os.path.exists(test_snap_dir):
        shutil.rmtree(test_snap_dir)
    os.makedirs(test_snap_dir, exist_ok=True)

    client = MarkLogicClient(args.host, args.port, args.user, args.password, args.auth_type)

    total_docs = args.phases * args.phase_size

    print(color("""
    ╔══════════════════════════════════════════════════════════╗
    ║  MLCA Memory Projection Test Harness                     ║
    ║  Validates trend-based memory runway convergence          ║
    ╚══════════════════════════════════════════════════════════╝
    """, CYAN))

    # ── Check connectivity ───────────────────────────────────────────
    header("SETUP")
    print(f"    Connecting to {args.host}:{args.port}...")
    if not wait_for_ml(client, timeout=30):
        print(f"    {color('ERROR: Cannot connect to MarkLogic', RED)}")
        print(f"    Run: docker compose up -d && docker compose ps")
        sys.exit(1)
    print(f"    {color('Connected.', GREEN)}")
    print()

    kv("Host",           f"{args.host}:{args.port}")
    kv("Database",       args.database)
    kv("Phases",         args.phases)
    kv("Docs per phase", f"{args.phase_size:,}")
    kv("Total docs",     f"{total_docs:,}")
    kv("Snapshot dir",   test_snap_dir)

    # ── Baseline snapshot ────────────────────────────────────────────
    header("PHASE 0: Baseline")
    snap = collect_snapshot(client, args.database)
    save_snapshot(snap)
    t = snap["totals"]
    kv("Documents",      f"{t['documents']:,}")
    kv("Forest memory",  fmt_mb(t["host_forest_mb"]))
    kv("RSS",            fmt_mb(t["host_rss_mb"]))
    kv("System RAM",     fmt_mb(t["system_total_mb"]))

    # ── Loading phases ───────────────────────────────────────────────
    phase_results = []
    inserted_total = 0

    for phase in range(1, args.phases + 1):
        header(f"PHASE {phase}/{args.phases}: Loading {args.phase_size:,} documents")

        t0 = time.time()
        n = insert_phase(client, args.database, args.phase_size, inserted_total)
        elapsed = time.time() - t0
        inserted_total += n
        rate = n / elapsed if elapsed > 0 else 0

        print(f"    Inserted {n:,} docs in {elapsed:.1f}s ({rate:,.0f}/s)")

        # Brief pause to let merges settle and memory stabilize
        time.sleep(2)

        # Take snapshot
        snap = collect_snapshot(client, args.database)
        save_snapshot(snap)
        t = snap["totals"]

        kv("Total documents", f"{t['documents']:,}")
        kv("Forest memory",   fmt_mb(t["host_forest_mb"]))
        kv("RSS",             fmt_mb(t["host_rss_mb"]))

        # Compute memory runway from all snapshots so far
        all_snaps = load_snapshots(args.database)
        runway = compute_memory_runway(all_snaps)

        if runway:
            pr = {
                "phase": phase,
                "docs": t["documents"],
                "forest_mb": t["host_forest_mb"],
                "rss_mb": t["host_rss_mb"],
            }
            pr.update(runway)

            if "runway_days" in runway:
                rwy_days = runway["runway_days"]
                grow_rate = runway["growth_rate_mb_day"]
                kv("Memory runway",
                   f"{color(f'{rwy_days:.1f} days', BOLD)}  "
                   f"(forest growing {fmt_mb(grow_rate)}/day)")
            if "docs_until_ceiling" in runway:
                d_left = runway["docs_until_ceiling"]
                fbpd = runway["forest_bytes_per_doc"]
                kv("Est. docs to ceiling",
                   f"{d_left:,}  ({fbpd:,.0f} bytes/doc)")

            # Config drift check
            drift = check_config_drift(all_snaps)
            if drift:
                print(f"    {color('WARNING: Config drift detected!', RED)}")
            else:
                kv("Config stability", color("STABLE", GREEN))

            phase_results.append(pr)
        else:
            kv("Memory runway", "insufficient data (need 2+ snapshots)")
            phase_results.append({
                "phase": phase,
                "docs": t["documents"],
                "forest_mb": t["host_forest_mb"],
                "rss_mb": t["host_rss_mb"],
            })

    # ── Results ──────────────────────────────────────────────────────
    header("RESULTS: Projection Convergence")

    sub_header("Phase-by-Phase Projections")
    print()
    hdr = (f"  {'Phase':>6}  {'Docs':>10}  {'Forest':>10}  "
           f"{'MB/day':>10}  {'B/doc':>10}  {'Runway':>10}  {'DocsLeft':>12}")
    print(color(hdr, BOLD))
    print(color("  " + "-" * (len(hdr) - 2), DIM))

    for pr in phase_results:
        phase = pr["phase"]
        docs  = pr["docs"]
        fmb   = pr["forest_mb"]
        rate  = pr.get("growth_rate_mb_day")
        bpd   = pr.get("forest_bytes_per_doc")
        rwy   = pr.get("runway_days")
        dleft = pr.get("docs_until_ceiling")

        rate_s  = f"{rate:>10.1f}" if rate else f"{'—':>10}"
        bpd_s   = f"{bpd:>10,.0f}" if bpd else f"{'—':>10}"
        rwy_s   = f"{rwy:>10.1f}" if rwy else f"{'—':>10}"
        dleft_s = f"{dleft:>12,}" if dleft else f"{'—':>12}"

        print(f"  {phase:>6}  {docs:>10,}  {fmt_mb(fmb):>10}  "
              f"{rate_s}  {bpd_s}  {rwy_s}  {dleft_s}")

    # ── Convergence analysis ─────────────────────────────────────────
    sub_header("Convergence Analysis")

    projections_with_bpd = [pr for pr in phase_results if pr.get("forest_bytes_per_doc")]

    if len(projections_with_bpd) >= 3:
        bpd_values = [pr["forest_bytes_per_doc"] for pr in projections_with_bpd]
        ceiling_values = [pr.get("docs_until_ceiling", 0) for pr in projections_with_bpd]

        # Check if bytes/doc is stabilizing (last 3 values within 20% of each other)
        last_3_bpd = bpd_values[-3:]
        mean_bpd = sum(last_3_bpd) / len(last_3_bpd)
        max_deviation = max(abs(v - mean_bpd) / mean_bpd * 100 for v in last_3_bpd) if mean_bpd else 100

        kv("Bytes/doc (last 3 phases)",
           f"{', '.join(f'{v:,.0f}' for v in last_3_bpd)}")
        kv("Mean bytes/doc",
           f"{mean_bpd:,.0f}")
        kv("Max deviation from mean",
           f"{max_deviation:.1f}%  {status_badge(max_deviation < 20, 'CONVERGED', 'NOT YET')}")

        if ceiling_values:
            last_3_ceil = ceiling_values[-3:]
            mean_ceil = sum(last_3_ceil) / len(last_3_ceil)
            max_ceil_dev = max(abs(v - mean_ceil) / mean_ceil * 100 for v in last_3_ceil) if mean_ceil else 100
            kv("Docs-to-ceiling (last 3)",
               f"{', '.join(f'{v:,}' for v in last_3_ceil)}")
            kv("Ceiling est. deviation",
               f"{max_ceil_dev:.1f}%  {status_badge(max_ceil_dev < 20, 'STABLE', 'FLUCTUATING')}")

        print()
        if max_deviation < 20:
            print(f"    {color('PASS', GREEN + BOLD)}: Memory projection has converged.")
            print(f"    Marginal cost is ~{mean_bpd:,.0f} bytes/doc of forest memory.")
            if mean_ceil > 0:
                print(f"    Estimated {mean_ceil:,.0f} additional docs before memory ceiling.")
        elif max_deviation < 40:
            print(f"    {color('PARTIAL', YELLOW + BOLD)}: Projection is stabilizing but not fully converged.")
            print(f"    Consider more phases or a longer time between snapshots.")
        else:
            print(f"    {color('FAIL', RED + BOLD)}: Projection has not converged.")
            print(f"    Bytes/doc varies too much ({max_deviation:.0f}%) — merges may be")
            print(f"    dominating memory behavior. Need more loading phases.")

    elif len(projections_with_bpd) > 0:
        print(f"    Need at least 3 phases with growth data for convergence analysis.")
        print(f"    Got {len(projections_with_bpd)}. Run more phases.")

    else:
        print(f"    No growth data collected. Check that documents are being inserted.")

    # ── Index impact test (optional) ─────────────────────────────────
    if args.index_impact:
        index_pass = run_index_impact_test(client, args.database)
    else:
        index_pass = None

    # ── Final summary ────────────────────────────────────────────────
    sub_header("Test Summary")
    kv("Total docs loaded",  f"{inserted_total:,}")
    final_snap = load_snapshots(args.database)[-1] if load_snapshots(args.database) else None
    if final_snap:
        ft = final_snap["totals"]
        kv("Final forest memory",  fmt_mb(ft["host_forest_mb"]))
        kv("Final RSS",            fmt_mb(ft["host_rss_mb"]))

        ceil = ft["system_total_mb"] * 0.80
        fixed = ft["host_cache_mb"] + ft["host_base_mb"] + ft["host_file_mb"]
        used_pct = ((fixed + ft["host_forest_mb"]) / ceil * 100) if ceil else 0
        kv("Memory ceiling usage", f"{used_pct:.1f}%  {bar(used_pct)}")

    kv("Snapshots saved", f"{len(load_snapshots(args.database))} in {test_snap_dir}")
    print()
    print(f"    To review trends:  python3 ../ml_capacity.py --trend --database {args.database}")
    print(f"    To clean up:       docker compose down -v")
    print()


if __name__ == "__main__":
    main()
