"""Data collection functions for MarkLogic capacity analysis.

Each function takes a `client` parameter (a MarkLogicClient instance)
and returns parsed JSON from the Management REST API or eval endpoints.
"""

from ml_capacity.validation import validate_database_name


def collect_cluster_overview(client):
    return client.get_json("/manage/v2?format=json")


def collect_database_status(client, database):
    return client.get_json(f"/manage/v2/databases/{database}?view=status&format=json")


def collect_database_properties(client, database):
    return client.get_json(f"/manage/v2/databases/{database}/properties?format=json")


def collect_forests(client, database):
    data = client.get_json(
        f"/manage/v2/forests?format=json&database-id={database}&view=status"
    )
    return data


def collect_forest_detail(client, forest_name):
    return client.get_json(
        f"/manage/v2/forests/{forest_name}?view=status&format=json"
    )


def collect_host_status(client):
    hosts_data = client.get_json("/manage/v2/hosts?format=json")
    items = hosts_data.get("host-default-list", {}).get("list-items", {}).get("list-item", [])
    results = []
    for h in items:
        name = h.get("nameref", "unknown")
        detail = client.get_json(f"/manage/v2/hosts/{name}?view=status&format=json")
        results.append(detail)
    return results


def collect_host_memory(client):
    """Use XQuery to get the full host memory breakdown from xdmp:host-status().

    Memory components from the host-status XML:
      memory-process-size     — ML virtual address space (VSZ)
      memory-process-rss      — Physical RAM pages currently resident (RSS)
      memory-process-anon     — Anonymous (heap) allocations, subset of RSS
      memory-process-rss-hwm  — Peak RSS since last startup
      memory-process-swap-size— Pages swapped out (non-zero = memory pressure)
      memory-size             — Configured ML memory limit (group setting)
      memory-cache-size       — List cache + compressed-tree cache allocated
      memory-forest-size      — In-memory stands across all forests on host
      memory-file-size        — OS file cache (mmap) pages held by ML
      host-size               — DISK metric: total on-disk size of all forests on this
                                host (sum of stand disk-size across all forests).
                                Despite appearing in xdmp:host-status() alongside memory
                                metrics it measures storage, not RAM. Correlates with
                                memory pressure in constrained containers (OS page-caches
                                forest files) but is NOT a memory component — do not use
                                in RAM ceiling calculations on production clusters.
      memory-join-size        — Active join workspace
      memory-unclosed-size    — Stands currently being merged (transient)
      memory-registry-size    — Registry memory
      memory-system-pagein-rate  — OS page-in rate (MB/s), non-zero = paging
      memory-system-pageout-rate — OS page-out rate (MB/s)
      memory-system-swapin-rate  — Swap-in rate (non-zero = severe pressure)
      memory-system-swapout-rate — Swap-out rate
    """
    xquery = """
    let $results :=
      for $host in xdmp:hosts()
      let $s := xdmp:host-status($host)
      return xdmp:to-json(map:new((
        map:entry("hostname",                  xdmp:host-name($host)),
        map:entry("cpus",                      $s/*:cpus/data()),
        map:entry("cores",                     $s/*:cores/data()),
        map:entry("memory-system-total-mb",    $s/*:memory-system-total/data()),
        map:entry("memory-system-free-mb",     $s/*:memory-system-free/data()),
        map:entry("memory-system-pagein-rate", $s/*:memory-system-pagein-rate/data()),
        map:entry("memory-system-pageout-rate",$s/*:memory-system-pageout-rate/data()),
        map:entry("memory-system-swapin-rate", $s/*:memory-system-swapin-rate/data()),
        map:entry("memory-system-swapout-rate",$s/*:memory-system-swapout-rate/data()),
        map:entry("memory-process-size-mb",    $s/*:memory-process-size/data()),
        map:entry("memory-process-rss-mb",     $s/*:memory-process-rss/data()),
        map:entry("memory-process-anon-mb",    $s/*:memory-process-anon/data()),
        map:entry("memory-process-rss-hwm-mb", $s/*:memory-process-rss-hwm/data()),
        map:entry("memory-process-swap-mb",    $s/*:memory-process-swap-size/data()),
        map:entry("memory-size-mb",            $s/*:memory-size/data()),
        map:entry("memory-cache-size-mb",      $s/*:memory-cache-size/data()),
        map:entry("memory-forest-size-mb",     $s/*:memory-forest-size/data()),
        map:entry("memory-file-size-mb",       $s/*:memory-file-size/data()),
        map:entry("host-size-mb",              $s/*:host-size/data()),
        map:entry("memory-join-size-mb",       $s/*:memory-join-size/data()),
        map:entry("memory-unclosed-size-mb",   $s/*:memory-unclosed-size/data()),
        map:entry("memory-registry-size-mb",   $s/*:memory-registry-size/data()),
        map:entry("host-large-data-size-mb",   $s/*:host-large-data-size/data()),
        map:entry("log-device-space-mb",       $s/*:log-device-space/data()),
        map:entry("data-dir-space-mb",         $s/*:data-dir-space/data())
      )))
    return xdmp:to-json(json:to-array($results))
    """
    return client.eval_xquery(xquery)


def collect_forest_counts(client, database):
    """Use XQuery to get detailed forest fragment counts.

    xdmp:forest-counts() returns an XML node — child elements live in
    the MarkLogic namespace, so we address them with the xs:QName form
    or declare a default namespace. Using local-name() matching is the
    safest portable approach.
    """
    validate_database_name(database)
    xquery = f"""
    let $db := xdmp:database("{database}")
    let $forests := xdmp:database-forests($db)
    let $results :=
      for $f in $forests
      let $fc := xdmp:forest-counts($f)
      let $fs := xdmp:forest-status($f)
      (: Address children by local-name to avoid namespace binding issues :)
      (: document-count is a direct child; fragment counts are under stands-counts/stand-counts :)
      let $doc-count     := ($fc/*[local-name()="document-count"]/data(), 0)[1]
      let $sc            := $fc/*[local-name()="stands-counts"]/*[local-name()="stand-counts"]
      let $active-count  := sum($sc/*[local-name()="active-fragment-count"]/data())
      let $deleted-count := sum($sc/*[local-name()="deleted-fragment-count"]/data())
      let $nascent-count := sum($sc/*[local-name()="nascent-fragment-count"]/data())
      let $stand-count   := count($fs/*[local-name()="stands"]/*[local-name()="stand"])
      let $disk-mb       := sum($fs/*[local-name()="stands"]/*[local-name()="stand"]/*[local-name()="disk-size"]/data())
      let $mem-mb        := sum($fs/*[local-name()="stands"]/*[local-name()="stand"]/*[local-name()="memory-size"]/data())
      return xdmp:to-json(map:new((
        map:entry("forest-name",          xdmp:forest-name($f)),
        map:entry("document-count",       $doc-count),
        map:entry("active-fragment-count",$active-count),
        map:entry("deleted-fragment-count",$deleted-count),
        map:entry("nascent-fragment-count",$nascent-count),
        map:entry("stand-count",          $stand-count),
        map:entry("disk-size-mb",         $disk-mb),
        map:entry("memory-size-mb",       $mem-mb)
      )))
    return xdmp:to-json(json:to-array($results))
    """
    return client.eval_xquery(xquery, database=database)


_INDEX_MEMORY_JS = """
var db = xdmp.database(dbName);

// 1. Get all index definitions with their IDs
var indexDefs = xdmp.databaseDescribeIndexes(db).toObject();
var allIndexes = [];
Object.keys(indexDefs).forEach(function(type) {
  var items = indexDefs[type];
  if (!Array.isArray(items)) items = [items];
  items.forEach(function(idx) {
    if (idx.indexId) {
      idx.indexType = type;
      allIndexes.push(idx);
    }
  });
});

// 2. Get forest statuses with memoryDetail
var forests = xdmp.databaseForests(db);
var statuses = xdmp.forestStatus(forests, 'memoryDetail').toArray();

// 3. Aggregate per-index memory and disk across all stands
var indexTotals = {};
var standSummaries = [];

statuses.forEach(function(statusNode) {
  var s = statusNode.toObject();
  var stands = s.stands;
  if (!stands) return;
  var standArr = Array.isArray(stands) ? stands : [stands];
  standArr.forEach(function(stand) {
    if (stand.memorySummary) {
      standSummaries.push({
        standPath: stand.path,
        diskSize: stand.diskSize,
        memorySize: stand.memorySize,
        summary: stand.memorySummary
      });
    }
    if (stand.memoryDetail && stand.memoryDetail.memoryRangeIndexes) {
      var indexes = stand.memoryDetail.memoryRangeIndexes.index;
      if (!indexes) return;
      if (!Array.isArray(indexes)) indexes = [indexes];
      indexes.forEach(function(idx) {
        var id = String(idx.indexId);
        if (!indexTotals[id]) indexTotals[id] = { memBytes: 0, diskBytes: 0 };
        indexTotals[id].memBytes += (idx.indexMemoryBytes || 0);
        indexTotals[id].diskBytes += (idx.indexOnDiskBytes || 0);
      });
    }
  });
});

// 4. Join
var report = allIndexes.map(function(def) {
  var totals = indexTotals[String(def.indexId)] || { memBytes: 0, diskBytes: 0 };
  return {
    indexType: def.indexType,
    localname: def.localname || null,
    namespaceURI: def.namespaceURI || null,
    scalarType: def.scalarType || null,
    pathExpression: def.pathExpression || null,
    indexId: def.indexId,
    totalMemoryBytes: totals.memBytes,
    totalOnDiskBytes: totals.diskBytes
  };
});

var result = { indexes: report, standSummaries: standSummaries };
result;
"""
