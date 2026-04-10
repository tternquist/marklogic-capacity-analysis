#!/usr/bin/env python3
"""
MarkLogic Capacity Scaling Test

Validates the capacity model used by ml_capacity.py by loading large
volumes of randomized documents and observing how key metrics scale:

  - disk-size    : grows linearly with doc count (most reliable signal)
  - inmem-write  : the active in-memory write-stand; resets on each flush
  - host-forest  : all-forest memory; grows in steps at flush boundaries
  - RSS          : total process RSS; should track disk growth over time

Documents are generated server-side (only count+offset are sent as vars)
so batch size is unlimited — use 50K–100K/batch to see stand flush events.

Metrics explained:
  Stand flush  — when the in-memory write stand fills (in-memory-limit KB
                 of list data), it flushes to disk. Each flush creates one
                 new on-disk stand. memory-forest-size jumps at flush.
  Disk size    — accumulates across all on-disk stands. Best linear proxy
                 for document count; unaffected by merge scheduling.
  Regression   — we fit disk-size ~ doc-count to get bytes/doc on disk.
                 Memory scaling is: disk_bytes_per_doc × cache-to-disk-ratio.

Usage:
    python ml_capacity_test.py --host HOST --user USER --password PW
                               [--database DB] [--auth-type basic|digest]
                               [--batches N] [--batch-size N]
                               [--no-cleanup]
"""

import argparse
import getpass
import json
import sys
import time
from urllib.error import HTTPError, URLError

# Re-use the client from ml_capacity
sys.path.insert(0, ".")
from ml_capacity import (
    MarkLogicClient,
    fmt_mb, color, header, sub_header, kv, bar, status_badge,
    BOLD, CYAN, GREEN, RED, YELLOW, DIM, RESET,
)

COLLECTION = "capacity-test"
URI_PREFIX = "/capacity-test/"

# ── Server-side document generation ────────────────────────────────
# Documents are generated entirely on the MarkLogic server using SJS.
# We only send (count, offset, uriPrefix, collection) as vars — no
# payload size limit regardless of batch size.

BATCH_INSERT_JS = """
declareUpdate();

var words = [
  'cluster','forest','index','fragment','stand','merge','shard',
  'document','query','search','cache','memory','disk','scale',
  'capacity','replication','backup','restore','journal','lock',
  'transaction','database','schema','field','element','attribute',
  'namespace','collection','permission','role','user','host',
  'server','group','partition','replica','failover','index','node'
];
var categories = ['alpha','beta','gamma','delta','epsilon','zeta'];
var statuses   = ['active','inactive','pending','archived','draft'];

function pick(arr) { return arr[Math.floor(Math.random() * arr.length)]; }

function randStr(n) {
  var s = '', chars = 'abcdefghijklmnopqrstuvwxyz';
  for (var i = 0; i < n; i++) s += chars[Math.floor(Math.random() * chars.length)];
  return s;
}

function randSentence(min, max) {
  var n = min + Math.floor(Math.random() * (max - min + 1));
  var out = [];
  for (var i = 0; i < n; i++) out.push(pick(words));
  return out.join(' ');
}

function randDate() {
  var y = 2018 + Math.floor(Math.random() * 9);
  var m = 1  + Math.floor(Math.random() * 12);
  var d = 1  + Math.floor(Math.random() * 28);
  return y + '-' + String(m).padStart(2,'0') + '-' + String(d).padStart(2,'0');
}

var inserted = 0;
var perms = [
  xdmp.permission('rest-reader', 'read'),
  xdmp.permission('rest-writer', 'update')
];

for (var i = 0; i < count; i++) {
  var id = offset + i;
  var sizeRoll = Math.random();
  // 25% small, 60% medium, 15% large
  var sizeClass = sizeRoll < 0.25 ? 'small' : sizeRoll < 0.85 ? 'medium' : 'large';

  var doc = {
    id:        'cap-test-' + String(id).padStart(10, '0'),
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
    doc.version = (1 + Math.floor(Math.random()*4)) + '.' +
                  Math.floor(Math.random()*10) + '.' +
                  Math.floor(Math.random()*20);
    doc.attrs   = {};
    var nAttrs  = 3 + Math.floor(Math.random() * 6);
    for (var a = 0; a < nAttrs; a++) doc.attrs[randStr(5)] = randStr(10);
    doc.history = [];
    var nHist   = 2 + Math.floor(Math.random() * 5);
    for (var h = 0; h < nHist; h++) {
      doc.history.push({ date: randDate(), action: pick(statuses) });
    }
  }

  xdmp.documentInsert(
    uriPrefix + doc.id + '.json',
    doc,
    { permissions: perms, collections: [collection] }
  );
  inserted++;
}
inserted;
"""


def insert_batch(client, database, batch_num, batch_size, global_offset):
    """Generate and insert a batch of documents entirely server-side."""
    result = client.eval_javascript(
        BATCH_INSERT_JS,
        database=database,
        vars={
            "count":     batch_size,
            "offset":    global_offset,
            "uriPrefix": URI_PREFIX,
            "collection": COLLECTION,
        },
    )
    return result[0] if result else 0


def eval_javascript(self, javascript, database=None, vars=None):
    """POST to /v1/eval for Server-Side JavaScript."""
    from urllib.parse import urlencode
    from urllib.request import Request, urlopen
    from urllib.error import HTTPError

    path = "/v1/eval"
    if database:
        path += f"?database={database}"

    body_parts = {"javascript": javascript}
    if vars:
        body_parts["vars"] = json.dumps(vars)

    body = urlencode(body_parts).encode()
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
    }

    if self.auth_type == "basic":
        headers["Authorization"] = self._basic_auth_header()

    try:
        req = Request(self.base + path, data=body, headers=headers, method="POST")
        with urlopen(req) as resp:
            return self._parse_eval_response(resp.read().decode())
    except HTTPError as e:
        if e.code != 401 or self.auth_type == "basic":
            raise
        auth_header = e.headers.get("WWW-Authenticate", "")
        if "Digest" not in auth_header:
            raise
        headers["Authorization"] = self._digest_response(auth_header, "POST", path)
        req = Request(self.base + path, data=body, headers=headers, method="POST")
        with urlopen(req) as resp:
            return self._parse_eval_response(resp.read().decode())


# Monkey-patch eval_javascript onto MarkLogicClient
MarkLogicClient.eval_javascript = eval_javascript


# ── Metric sampling ──────────────────────────────────────────────────

SAMPLE_XQ = """
(: Collect metrics at three levels:
   1. Host-level  — RSS, cache alloc, host forest size (all forests)
   2. DB-level    — in-memory write buffer size (from db-status API)
   3. Forest-level — per-forest stand disk size and cached memory pages

   Why three levels?
   - memory-forest-size (host) includes ALL forests and grows in steps as
     in-memory stands pre-allocate then flush (not linear per-doc).
   - in-memory-size (db-status) is the active write buffer; it grows as docs
     are inserted, resets after each flush to disk.
   - disk-size (forest-status/stand) is the most reliable doc-count proxy:
     it grows monotonically and linearly as flushed stands accumulate.
   - memory-size (forest-status/stand) = OS-cached pages of disk stands;
     grows with both doc count and query activity. :)

let $host    := xdmp:hosts()[1]
let $s       := xdmp:host-status($host)
let $db      := xdmp:database("{database}")
let $forests := xdmp:database-forests($db)

(: Fragment counts (from forest-counts, nested under stands-counts) :)
let $frag-active  := sum(
  for $f in $forests
  let $fc := xdmp:forest-counts($f)
  let $sc := $fc/*[local-name()='stands-counts']/*[local-name()='stand-counts']
  return sum($sc/*[local-name()='active-fragment-count']/data())
)
let $frag-deleted := sum(
  for $f in $forests
  let $fc := xdmp:forest-counts($f)
  let $sc := $fc/*[local-name()='stands-counts']/*[local-name()='stand-counts']
  return sum($sc/*[local-name()='deleted-fragment-count']/data())
)
let $doc-count := sum(
  for $f in $forests
  return xdmp:forest-counts($f)/*[local-name()='document-count']/data()
)

(: Forest-status for per-db disk and memory sizes :)
let $disk-size-mb := sum(
  for $f in $forests
  let $fs := xdmp:forest-status($f)
  return sum($fs/*[local-name()='stands']/*[local-name()='stand']/*[local-name()='disk-size']/data())
)
let $cached-mem-mb := sum(
  for $f in $forests
  let $fs := xdmp:forest-status($f)
  return sum($fs/*[local-name()='stands']/*[local-name()='stand']/*[local-name()='memory-size']/data())
)
let $stand-count := sum(
  for $f in $forests
  let $fs := xdmp:forest-status($f)
  return count($fs/*[local-name()='stands']/*[local-name()='stand'])
)

(: In-memory write stand — identified by stand-kind='InMemory'.
   This is the active write buffer that fills up between flushes.
   It is absent (sum=0) immediately after a flush to disk. :)
let $inmem-mb := sum(
  for $f in $forests
  let $fs := xdmp:forest-status($f)
  for $st in $fs/*[local-name()='stands']/*[local-name()='stand']
  where $st/*[local-name()='stand-kind'] = 'InMemory'
  return $st/*[local-name()='memory-size']/data()
)

return xdmp:to-json(map:new((
  map:entry("doc-count",             $doc-count),
  map:entry("active-fragments",      $frag-active),
  map:entry("deleted-fragments",     $frag-deleted),
  map:entry("disk-size-mb",          $disk-size-mb),
  map:entry("cached-mem-mb",         $cached-mem-mb),
  map:entry("inmem-write-mb",        $inmem-mb),
  map:entry("stand-count",           $stand-count),
  map:entry("host-forest-size-mb",   $s/*:memory-forest-size/data()),
  map:entry("memory-cache-size-mb",  $s/*:memory-cache-size/data()),
  map:entry("memory-process-rss-mb", $s/*:memory-process-rss/data()),
  map:entry("host-size-mb",          $s/*:host-size/data()),
  map:entry("memory-file-size-mb",   $s/*:memory-file-size/data())
)))
"""


def sample_metrics(client, database):
    xquery = SAMPLE_XQ.replace("{database}", database)
    results = client.eval_xquery(xquery)
    if not results:
        return None
    return results[0]


# ── Cleanup ─────────────────────────────────────────────────────────

CLEANUP_JS = """
declareUpdate();
var deleted = 0;
cts.uriMatch(prefix + '*').toArray().forEach(function(u) {
  xdmp.documentDelete(u);
  deleted++;
});
deleted;
"""


def cleanup(client, database):
    result = client.eval_javascript(
        CLEANUP_JS,
        database=database,
        vars={"prefix": URI_PREFIX},
    )
    return result[0] if result else 0


# ── Linear regression ────────────────────────────────────────────────

def linear_regression(xs, ys):
    """Return (slope, intercept, r_squared) for a simple OLS fit."""
    n = len(xs)
    if n < 2:
        return None, None, None
    sx  = sum(xs)
    sy  = sum(ys)
    sxx = sum(x * x for x in xs)
    sxy = sum(x * y for x, y in zip(xs, ys))
    denom = n * sxx - sx * sx
    if denom == 0:
        return None, None, None
    slope     = (n * sxy - sx * sy) / denom
    intercept = (sy - slope * sx) / n
    y_mean    = sy / n
    ss_tot    = sum((y - y_mean) ** 2 for y in ys)
    ss_res    = sum((y - (slope * x + intercept)) ** 2 for x, y in zip(xs, ys))
    r_squared = 1 - ss_res / ss_tot if ss_tot > 0 else 1.0
    return slope, intercept, r_squared


# ── Report ───────────────────────────────────────────────────────────

def print_sample_table(samples):
    print()
    hdr = (
        f"  {'Docs':>10}  {'Disk MB':>9}  {'InMem MB':>9}  "
        f"{'HostForest':>11}  {'Stands':>7}  {'Δ disk B/doc':>14}"
    )
    print(color(hdr, BOLD))
    print(color("  " + "-" * (len(hdr) - 2), DIM))

    prev_disk_val = None
    prev_doc_val  = None
    for s in samples:
        docs   = s["doc-count"]
        disk   = s["disk-size-mb"]
        inmem  = s["inmem-write-mb"]
        hforest= s["host-forest-size-mb"]
        stands = s["stand-count"]
        # Marginal disk bytes/doc since previous sample
        if prev_disk_val is not None and docs > prev_doc_val:
            delta_disk = disk - prev_disk_val
            delta_docs = docs - prev_doc_val
            bpd = f"{(delta_disk * 1024 * 1024 / delta_docs):,.0f}" if delta_docs > 0 else "—"
        else:
            bpd = "—  (baseline)"
        prev_disk_val = disk
        prev_doc_val  = docs

        print(
            f"  {docs:>10,}  {disk:>9.1f}  {inmem:>9.1f}  "
            f"{fmt_mb(hforest):>11}  {stands:>7}  {bpd:>14}"
        )


def print_regression(samples):
    # disk-size is the most reliable linear proxy for document count:
    # it grows monotonically as flushed stands accumulate, unaffected
    # by merge scheduling or cache warming.
    xs = [float(s["doc-count"])    for s in samples]
    ys = [float(s["disk-size-mb"]) for s in samples]

    slope, intercept, r2 = linear_regression(xs, ys)
    if slope is None:
        print("    Not enough data points for regression.")
        return None

    bytes_per_doc = slope * 1024 * 1024

    sub_header("Linear Regression  (disk size ~ document count)")
    kv("Slope (MB per doc)",    f"{slope:.6f} MB  =  {bytes_per_doc:,.1f} bytes/doc on disk")
    kv("Intercept",             f"{intercept:.2f} MB  (pre-existing data at 0 test docs)")
    r2_ok = r2 > 0.85
    kv("R²",
       f"{r2:.4f}  {status_badge(r2_ok, 'LINEAR', 'NON-LINEAR')}"
       + ("" if r2_ok else
          color("  — check for merges mid-run or insufficient sample", YELLOW)))

    return slope, intercept, r2, bytes_per_doc


def compare_to_estimate(samples, slope_mb_per_doc, database, client):
    """Compare observed disk slope to ml_capacity.py's memory-based projection."""
    sub_header("Comparison with ml_capacity.py projection")

    last = samples[-1]
    current_disk_mb   = float(last["disk-size-mb"])
    current_docs      = int(last["doc-count"])
    current_rss       = float(last["memory-process-rss-mb"])

    cache  = float(last.get("memory-cache-size-mb", 0))
    base   = float(last.get("host-size-mb", 0))
    fmem   = float(last.get("memory-file-size-mb", 0))
    fixed  = cache + base + fmem
    host_forest = float(last.get("host-forest-size-mb", 0))

    # System total for memory ceiling
    xq = """
    let $s := xdmp:host-status(xdmp:hosts()[1])
    return xdmp:to-json(map:new((
      map:entry("total",    $s/*:memory-system-total/data()),
      map:entry("ml-limit", $s/*:memory-size/data()),
      map:entry("data-dir", $s/*:data-dir-space/data())
    )))
    """
    info = client.eval_xquery(xq)
    sys_total  = float(info[0].get("total",    0)) if info else 0
    ml_limit   = float(info[0].get("ml-limit", 0)) if info else 0
    disk_free  = float(info[0].get("data-dir", 0)) if info else 0
    mem_ceiling = min(ml_limit or sys_total * 0.80, sys_total * 0.80)
    forest_headroom = mem_ceiling - fixed - host_forest

    # Disk runway
    disk_remaining_mb = disk_free
    reg_bytes_per_doc = slope_mb_per_doc * 1024 * 1024

    kv("Fixed memory (cache+base+file)", fmt_mb(fixed))
    kv("Host forest memory (all DBs)",   fmt_mb(host_forest))
    kv("Memory headroom for forests",
       f"{fmt_mb(forest_headroom)}  {status_badge(forest_headroom > 512, 'OK', 'LOW')}")
    kv("Disk free",                      fmt_mb(disk_remaining_mb))
    print()

    # Disk-based estimate: how many more docs until disk runs out?
    if reg_bytes_per_doc > 0:
        disk_docs_remaining = int((disk_remaining_mb * 1024 * 1024) / reg_bytes_per_doc)
        kv("Observed disk bytes/doc (regression)", f"{reg_bytes_per_doc:,.0f} bytes")
        kv("Est. docs until disk full",            f"{disk_docs_remaining:,}")

    # ml_capacity.py memory-forest estimate for comparison
    # It uses forest-memory / doc-count from a snapshot; show the ratio
    ml_cap_forest_mb = host_forest
    if current_docs > 0 and ml_cap_forest_mb > 0:
        snap_bpd_mem = (ml_cap_forest_mb * 1024 * 1024) / current_docs
        mem_docs_remaining = int((forest_headroom * 1024 * 1024) / snap_bpd_mem) if snap_bpd_mem > 0 else 0
        kv("ml_capacity forest bytes/doc (snapshot)", f"{snap_bpd_mem:,.0f} bytes")
        kv("Est. docs until memory ceiling",           f"{mem_docs_remaining:,}")
    print()
    kv("Binding constraint",
       color("DISK" if disk_docs_remaining < mem_docs_remaining else "MEMORY", BOLD)
       if reg_bytes_per_doc > 0 and current_docs > 0 else "insufficient data")


# ── Main ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="MarkLogic capacity scaling test — load docs and measure memory growth"
    )
    parser.add_argument("--host",       default="localhost")
    parser.add_argument("--port",       type=int, default=8002)
    parser.add_argument("--user",       default="admin")
    parser.add_argument("--password",   help="Prompted if not provided")
    parser.add_argument("--database",   default="Documents")
    parser.add_argument("--auth-type",  choices=["digest", "basic"], default="digest")
    parser.add_argument("--batches",    type=int, default=10,
                        help="Number of batches to insert (default: 10)")
    parser.add_argument("--batch-size", type=int, default=50000,
                        help="Documents per batch (default: 50000). "
                             "Needs ~65K docs to trigger one stand flush "
                             "(in-memory-limit=128MB at ~2KB/doc).")
    parser.add_argument("--no-cleanup", action="store_true",
                        help="Leave test documents in place after run")

    args = parser.parse_args()

    if not args.password:
        args.password = getpass.getpass(f"Password for {args.user}@{args.host}: ")

    client   = MarkLogicClient(args.host, args.port, args.user, args.password, args.auth_type)
    total    = args.batches * args.batch_size
    database = args.database

    print(color("""
    ╔══════════════════════════════════════════════════════╗
    ║         MarkLogic Capacity Scaling Test               ║
    ║         Memory Growth Validation                      ║
    ╚══════════════════════════════════════════════════════╝
    """, CYAN))

    header("TEST PLAN")
    kv("Host",       f"{args.host}:{args.port}")
    kv("Database",   database)
    kv("Batches",    args.batches)
    kv("Batch size", f"{args.batch_size:,} docs")
    kv("Total docs", f"{total:,}")
    kv("Cleanup",    "yes" if not args.no_cleanup else color("NO — docs will remain", YELLOW))

    # ── Baseline sample ──────────────────────────────────────────────
    header("BASELINE (before inserts)")
    baseline = sample_metrics(client, database)
    if not baseline:
        print(color("ERROR: Could not sample metrics. Check eval access.", RED))
        sys.exit(1)

    kv("Existing documents",  f"{baseline['doc-count']:,}")
    kv("Forest disk size",    fmt_mb(baseline["disk-size-mb"]))
    kv("In-mem write buffer", fmt_mb(baseline["inmem-write-mb"]))
    kv("Stand count",         baseline["stand-count"])
    kv("Host forest memory",  fmt_mb(baseline["host-forest-size-mb"]))
    kv("RSS",                 fmt_mb(baseline["memory-process-rss-mb"]))

    if baseline["doc-count"] > 0:
        print(f"\n    {color('Note:', YELLOW)} {baseline['doc-count']:,} pre-existing docs detected.")
        print(f"    Regression uses delta from baseline; snapshot uses cumulative forest memory.")

    samples = [baseline]
    doc_offset = baseline["doc-count"]

    # ── Insert batches ───────────────────────────────────────────────
    header("INSERTING DOCUMENTS")

    inserted_so_far = 0
    for batch_num in range(1, args.batches + 1):
        t0 = time.time()
        n  = insert_batch(client, database, batch_num, args.batch_size,
                          global_offset=inserted_so_far)
        elapsed = time.time() - t0

        inserted_so_far += n
        rate = n / elapsed if elapsed > 0 else 0

        # Sample metrics after each batch
        sample = sample_metrics(client, database)
        if sample:
            samples.append(sample)
            disk_mb   = sample["disk-size-mb"]
            inmem_mb  = sample["inmem-write-mb"]
            stands    = sample["stand-count"]
            rss_mb    = sample["memory-process-rss-mb"]
            print(
                f"    Batch {batch_num:>3}/{args.batches}  "
                f"{n:>6,} docs  {elapsed:>5.1f}s  ({rate:>7,.0f}/s)  "
                f"disk={disk_mb:.0f}MB  inmem={inmem_mb:.0f}MB  "
                f"stands={stands}  rss={fmt_mb(rss_mb)}"
            )
        else:
            print(f"    Batch {batch_num:>3}/{args.batches}  inserted {n} docs  (metrics unavailable)")

    # ── Results ──────────────────────────────────────────────────────
    header("SCALING RESULTS")

    print_sample_table(samples)

    regression_result = print_regression(samples)

    if regression_result:
        slope_mb, intercept, r2, bytes_per_doc = regression_result

        compare_to_estimate(samples, slope_mb, database, client)

        sub_header("Validation")
        checks = []

        last = samples[-1]
        current_disk = float(last["disk-size-mb"])
        current_docs = int(last["doc-count"])

        # Check 1: disk size scales linearly with doc count
        checks.append((
            r2 >= 0.85,
            f"Disk size scales linearly with doc count (R²={r2:.4f} ≥ 0.85)",
            f"Disk growth non-linear (R²={r2:.4f} < 0.85) — merges running mid-test?"
        ))

        # Check 2: disk bytes/doc is plausible (100 bytes – 10 MB per doc)
        bpd_ok = 100 < bytes_per_doc < 10 * 1024 * 1024
        checks.append((
            bpd_ok,
            f"Disk bytes/doc is plausible ({bytes_per_doc:,.0f} bytes)",
            f"Disk bytes/doc ({bytes_per_doc:,.0f}) outside expected range 100B–10MB"
        ))

        # Check 3: at least one stand flush occurred (stand-count changed)
        baseline_stands = int(samples[0]["stand-count"])
        final_stands    = int(last["stand-count"])
        stands_changed  = final_stands != baseline_stands
        checks.append((
            stands_changed,
            f"Stand flush observed (stands: {baseline_stands} → {final_stands})",
            f"No stand flush detected (stands stayed at {baseline_stands}) — "
            f"load more docs to exceed in-memory-limit and trigger flush"
        ))

        print()
        all_pass = True
        for ok, pass_msg, fail_msg in checks:
            if ok:
                print(f"    {color('PASS', GREEN)}  {pass_msg}")
            else:
                print(f"    {color('FAIL', RED)}  {fail_msg}")
                all_pass = False

        print()
        if all_pass:
            print(f"    {color('All checks passed.', GREEN + BOLD)} "
                  f"The capacity model is validated for this database.")
            print(f"    Observed: {color(f'{bytes_per_doc:,.0f} bytes', BOLD)} per document "
                  f"in forest memory  (R²={r2:.4f})")
        else:
            print(f"    {color('Some checks failed.', YELLOW + BOLD)} "
                  f"Review results above before relying on capacity projections.")

    # ── Cleanup ──────────────────────────────────────────────────────
    if not args.no_cleanup:
        header("CLEANUP")
        deleted = cleanup(client, database)
        kv("Test documents removed", f"{deleted:,}")
    else:
        header("SKIPPED CLEANUP")
        print(f"    Test documents remain in collection '{COLLECTION}'")
        print(f"    URI pattern: {URI_PREFIX}*")

    print()
    print(color("=" * 62, DIM))
    print(color(f"  Test complete — {inserted_so_far:,} documents inserted", DIM))
    print(color("=" * 62, DIM))
    print()


if __name__ == "__main__":
    main()
