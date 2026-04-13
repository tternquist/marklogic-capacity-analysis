#!/usr/bin/env python3
"""
MLCA Stress Test — Load documents toward the projected memory ceiling
and validate that the capacity projection holds up.

This is a ONE-TIME test that:
  1. Takes a "before" snapshot with current projections
  2. Loads documents in waves (default 100K per wave)
  3. Samples memory after each wave and checks against the ceiling
  4. Stops at a configurable fraction of the projected limit (default 75%)
  5. Takes an "after" snapshot and compares actual vs projected
  6. Cleans up all test documents

Safety:
  - Stops loading if host forest memory exceeds the safety threshold
  - Monitors RSS and swap after each wave
  - All test documents go into the 'stress-test' collection for clean removal
"""

import argparse
import getpass
import json
import sys
import time

sys.path.insert(0, ".")
from ml_capacity import (
    MarkLogicClient, collect_snapshot, save_snapshot,
    fmt_mb, color, header, sub_header, kv, bar, status_badge,
    BOLD, CYAN, GREEN, RED, YELLOW, DIM, RESET,
    _INDEX_MEMORY_JS,
)

COLLECTION = "stress-test"
URI_PREFIX = "/stress-test/"

# ── Server-side batch insert (same generator as ml_capacity_test.py) ─

BATCH_INSERT_JS = """
declareUpdate();
const words = [
  'cluster','forest','index','fragment','stand','merge','shard',
  'document','query','search','cache','memory','disk','scale',
  'capacity','replication','backup','restore','journal','lock',
  'transaction','database','schema','field','element','attribute',
  'namespace','collection','permission','role','user','host',
  'server','group','partition','replica','failover','index','node'
];
const categories = ['alpha','beta','gamma','delta','epsilon','zeta'];
const statuses   = ['active','inactive','pending','archived','draft'];

function pick(arr) { return arr[Math.floor(Math.random() * arr.length)]; }
function randStr(n) {
  let s = '';
  const c = 'abcdefghijklmnopqrstuvwxyz';
  for (const i = 0; i < n; i++) s += c[Math.floor(Math.random() * c.length)];
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
    id:        'stress-' + String(id).padStart(10, '0'),
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
    for (let h = 0; h < nH; h++)
      doc.history.push({ date: randDate(), action: pick(statuses) });
  }

  xdmp.documentInsert(
    uriPrefix + doc.id + '.json', doc,
    { permissions: perms, collections: [collection] }
  );
  inserted++;
}
inserted;
"""

SAMPLE_XQ = """
let $host := xdmp:hosts()[1]
let $s    := xdmp:host-status($host)
let $db   := xdmp:database("{database}")
let $forests := xdmp:database-forests($db)
let $doc-count := sum(
  for $f in $forests
  return xdmp:forest-counts($f)/*[local-name()='document-count']/data()
)
let $disk-mb := sum(
  for $f in $forests let $fs := xdmp:forest-status($f)
  return sum($fs/*[local-name()='stands']/*[local-name()='stand']/*[local-name()='disk-size']/data())
)
let $stand-count := sum(
  for $f in $forests let $fs := xdmp:forest-status($f)
  return count($fs/*[local-name()='stands']/*[local-name()='stand'])
)
return xdmp:to-json(map:new((
  map:entry("doc-count",            $doc-count),
  map:entry("disk-size-mb",         $disk-mb),
  map:entry("stand-count",          $stand-count),
  map:entry("host-forest-mb",       $s/*:memory-forest-size/data()),
  map:entry("host-cache-mb",        $s/*:memory-cache-size/data()),
  map:entry("host-base-mb",         $s/*:host-size/data()),
  map:entry("host-file-mb",         $s/*:memory-file-size/data()),
  map:entry("rss-mb",               $s/*:memory-process-rss/data()),
  map:entry("swap-mb",              $s/*:memory-process-swap-size/data()),
  map:entry("system-total-mb",      $s/*:memory-system-total/data()),
  map:entry("system-free-mb",       $s/*:memory-system-free/data())
)))
"""

CLEANUP_JS = """
declareUpdate();
let deleted = 0;
const uris = cts.uriMatch(prefix + '*').toArray();
for (let i = 0; i < uris.length; i++) {
  xdmp.documentDelete(uris[i]);
  deleted++;
}
deleted;
"""


def sample(client, database):
    xq = SAMPLE_XQ.replace("{database}", database)
    results = client.eval_xquery(xq)
    return results[0] if results else None


def insert_wave(client, database, wave_size, global_offset):
    result = client.eval_javascript(
        BATCH_INSERT_JS, database=database,
        vars={"count": wave_size, "offset": global_offset,
              "uriPrefix": URI_PREFIX, "collection": COLLECTION},
    )
    return result[0] if result else 0


def cleanup(client, database):
    result = client.eval_javascript(
        CLEANUP_JS, database=database,
        vars={"prefix": URI_PREFIX},
    )
    return result[0] if result else 0


def main():
    parser = argparse.ArgumentParser(
        description="MLCA Stress Test — load toward projected memory ceiling"
    )
    parser.add_argument("--host",       default="localhost")
    parser.add_argument("--port",       type=int, default=8002)
    parser.add_argument("--user",       default="admin")
    parser.add_argument("--password")
    parser.add_argument("--database",   default="Documents")
    parser.add_argument("--auth-type",  choices=["digest", "basic"], default="digest")
    parser.add_argument("--wave-size",  type=int, default=100000,
                        help="Documents per wave (default: 100,000)")
    parser.add_argument("--target-pct", type=int, default=75,
                        help="Stop at this %% of projected limit (default: 75)")
    parser.add_argument("--no-cleanup", action="store_true",
                        help="Leave test documents in place after run")

    args = parser.parse_args()
    if not args.password:
        args.password = getpass.getpass(f"Password for {args.user}@{args.host}: ")

    client = MarkLogicClient(args.host, args.port, args.user, args.password, args.auth_type)
    database = args.database
    target_pct = args.target_pct / 100.0

    print(color("""
    ╔══════════════════════════════════════════════════════╗
    ║     MLCA Stress Test — Memory Ceiling Validation     ║
    ╚══════════════════════════════════════════════════════╝
    """, CYAN))

    # ── Before snapshot ──────────────────────────────────────────────
    header("BEFORE: Capacity Projection")
    before_snap = collect_snapshot(client, database)
    before_path = save_snapshot(before_snap)
    print(f"    {color('Snapshot saved:', DIM)} {before_path}")

    t = before_snap["totals"]
    baseline_docs     = t["documents"]
    host_forest_mb    = t["host_forest_mb"]
    host_cache_mb     = t["host_cache_mb"]
    host_base_mb      = t["host_base_mb"]
    host_file_mb      = t["host_file_mb"]
    system_total_mb   = t["system_total_mb"]
    rss_mb            = t["host_rss_mb"]

    fixed_mb  = host_cache_mb + host_base_mb + host_file_mb
    ceiling   = system_total_mb * 0.80
    headroom  = ceiling - fixed_mb - host_forest_mb

    if baseline_docs > 0 and host_forest_mb > 0:
        forest_bytes_per_doc = (host_forest_mb * 1024 * 1024) / baseline_docs
    else:
        print(f"    {color('ERROR: Need existing documents to establish per-doc baseline', RED)}")
        sys.exit(1)

    projected_additional = int((headroom * 1024 * 1024) / forest_bytes_per_doc)
    projected_total = baseline_docs + projected_additional
    target_docs = int(projected_additional * target_pct)

    kv("Baseline documents",         f"{baseline_docs:,}")
    kv("Forest memory",              fmt_mb(host_forest_mb))
    kv("Fixed components",           fmt_mb(fixed_mb))
    kv("Memory ceiling (80% RAM)",   fmt_mb(ceiling))
    kv("Forest headroom",            fmt_mb(headroom))
    kv("Forest bytes/doc",           f"{forest_bytes_per_doc:,.0f}")
    kv("Projected additional docs",  f"{projected_additional:,}")
    kv("Projected total at ceiling", f"{projected_total:,}")
    print()
    kv("Target",                     f"{target_pct*100:.0f}% of projected limit = "
                                     f"{color(f'{target_docs:,}', BOLD)} additional docs")

    # Safety threshold: stop if forest memory reaches 90% of headroom
    safety_forest_mb = fixed_mb + headroom * 0.90

    # ── Load waves ───────────────────────────────────────────────────
    header("LOADING")

    waves = []
    inserted_total = 0
    wave_num = 0
    stopped_reason = None

    while inserted_total < target_docs:
        wave_num += 1
        remaining = target_docs - inserted_total
        wave_size = min(args.wave_size, remaining)

        t0 = time.time()
        n = insert_wave(client, database, wave_size, inserted_total)
        elapsed = time.time() - t0
        inserted_total += n
        rate = n / elapsed if elapsed > 0 else 0

        # Sample metrics
        m = sample(client, database)
        if not m:
            print(f"    Wave {wave_num}: inserted {n:,} but could not sample metrics")
            continue

        doc_count   = m["doc-count"]
        forest_mb   = m["host-forest-mb"]
        rss         = m["rss-mb"]
        swap        = m["swap-mb"]
        disk_mb     = m["disk-size-mb"]
        stands      = m["stand-count"]
        free_mb     = m["system-free-mb"]

        forest_pct = ((forest_mb + fixed_mb) / ceiling * 100) if ceiling else 0

        wave_data = {
            "wave": wave_num,
            "inserted": inserted_total,
            "doc_count": doc_count,
            "forest_mb": forest_mb,
            "rss_mb": rss,
            "swap_mb": swap,
            "disk_mb": disk_mb,
            "stands": stands,
            "free_mb": free_mb,
            "elapsed": elapsed,
        }
        waves.append(wave_data)

        print(
            f"    Wave {wave_num:>3}  +{n:>7,} ({inserted_total:>10,} total)  "
            f"{elapsed:>5.1f}s  {rate:>7,.0f}/s  "
            f"forest={fmt_mb(forest_mb)}  rss={fmt_mb(rss)}  "
            f"disk={fmt_mb(disk_mb)}  stands={stands}  "
            f"ceiling={forest_pct:.1f}%"
        )

        # Safety checks
        if swap > 0:
            stopped_reason = f"SWAP DETECTED ({fmt_mb(swap)}) — stopping to protect cluster"
            break

        if forest_mb + fixed_mb > safety_forest_mb:
            stopped_reason = (f"Forest memory ({fmt_mb(forest_mb)}) approaching 90% "
                              f"of headroom — stopping for safety")
            break

        if free_mb < system_total_mb * 0.10:
            stopped_reason = (f"System free memory ({fmt_mb(free_mb)}) below 10% — "
                              f"stopping for safety")
            break

    if stopped_reason:
        print()
        print(f"    {color('STOPPED: ' + stopped_reason, RED + BOLD)}")

    # ── After snapshot ───────────────────────────────────────────────
    header("AFTER: Actual vs Projected")
    after_snap = collect_snapshot(client, database)
    after_path = save_snapshot(after_snap)
    print(f"    {color('Snapshot saved:', DIM)} {after_path}")
    print()

    at = after_snap["totals"]
    final_docs      = at["documents"]
    final_forest_mb = at["host_forest_mb"]
    final_rss_mb    = at["host_rss_mb"]
    final_disk_mb   = at["forest_disk_mb"]

    docs_added  = final_docs - baseline_docs
    forest_grew = final_forest_mb - host_forest_mb
    rss_grew    = final_rss_mb - rss_mb

    kv("Documents loaded",  f"{docs_added:,}")
    kv("Total documents",   f"{final_docs:,}")
    kv("Target was",        f"{baseline_docs + target_docs:,} ({target_pct*100:.0f}% of ceiling)")
    print()

    # Actual per-doc costs
    sub_header("Actual vs Projected Per-Document Cost")

    if docs_added > 0:
        actual_forest_bpd = (forest_grew * 1024 * 1024) / docs_added
        actual_disk_bpd   = ((final_disk_mb - before_snap["totals"]["forest_disk_mb"])
                             * 1024 * 1024) / docs_added

        kv("Projected forest bytes/doc", f"{forest_bytes_per_doc:,.0f}")
        kv("Actual forest bytes/doc",    f"{actual_forest_bpd:,.0f}")

        if forest_bytes_per_doc > 0:
            pct_diff = ((actual_forest_bpd - forest_bytes_per_doc) /
                        forest_bytes_per_doc * 100)
            kv("Difference",
               f"{pct_diff:+.1f}%  {status_badge(abs(pct_diff) < 25, 'ACCURATE', 'DIVERGED')}")
        print()
        kv("Actual disk bytes/doc", f"{actual_disk_bpd:,.0f}")

    # Revised projection based on actual observed rate
    sub_header("Revised Capacity Projection")

    if docs_added > 0 and forest_grew > 0:
        actual_mbpd = forest_grew / docs_added  # MB per doc
        remaining_headroom = ceiling - fixed_mb - final_forest_mb
        revised_additional = int((remaining_headroom * 1024 * 1024) /
                                 (actual_mbpd * 1024 * 1024)) if actual_mbpd > 0 else 0
        revised_total = final_docs + revised_additional

        kv("Current forest memory",       fmt_mb(final_forest_mb))
        kv("Remaining headroom",          fmt_mb(remaining_headroom))
        kv("Observed forest MB/doc",       f"{actual_mbpd * 1024 * 1024:,.0f} bytes")
        kv("Revised additional docs",      f"{revised_additional:,}")
        kv("Revised total at ceiling",     f"{revised_total:,}")
        print()

        original_total_proj = projected_total
        kv("Original projection (before)", f"{original_total_proj:,} total docs at ceiling")
        kv("Revised projection (after)",   f"{revised_total:,} total docs at ceiling")

        if original_total_proj > 0:
            accuracy = revised_total / original_total_proj * 100
            kv("Projection accuracy",
               f"{accuracy:.1f}%  {status_badge(abs(accuracy - 100) < 20, 'GOOD', 'REVIEW')}")
    elif forest_grew <= 0:
        print(f"    Forest memory did not grow — merges may have compressed data.")
        print(f"    Re-run with more waves or larger wave size.")

    # ── Wave summary table ───────────────────────────────────────────
    sub_header("Wave Summary")
    print()
    hdr = f"  {'Wave':>5}  {'Docs Added':>12}  {'Total Docs':>12}  {'Forest':>10}  {'RSS':>10}  {'Disk':>10}  {'Stands':>7}  {'%Ceil':>7}"
    print(color(hdr, BOLD))
    print(color("  " + "-" * (len(hdr) - 2), DIM))

    for w in waves:
        f_pct = ((w["forest_mb"] + fixed_mb) / ceiling * 100) if ceiling else 0
        print(
            f"  {w['wave']:>5}  {w['inserted']:>12,}  {w['doc_count']:>12,}  "
            f"{fmt_mb(w['forest_mb']):>10}  {fmt_mb(w['rss_mb']):>10}  "
            f"{fmt_mb(w['disk_mb']):>10}  {w['stands']:>7}  {f_pct:>6.1f}%"
        )

    # ── Cleanup ──────────────────────────────────────────────────────
    if not args.no_cleanup:
        header("CLEANUP")
        print(f"    Removing {inserted_total:,} test documents...")
        # Clean up in batches to avoid transaction timeouts
        total_deleted = 0
        while True:
            deleted = cleanup(client, database)
            total_deleted += deleted
            if deleted == 0:
                break
            print(f"      Deleted {total_deleted:,} so far...")
        kv("Total removed", f"{total_deleted:,}")
    else:
        header("SKIPPED CLEANUP")
        print(f"    {inserted_total:,} test documents remain in collection '{COLLECTION}'")

    print()
    print(color("=" * 62, DIM))
    print(color(f"  Stress test complete — {docs_added:,} documents loaded", DIM))
    print(color("=" * 62, DIM))
    print()


if __name__ == "__main__":
    main()
