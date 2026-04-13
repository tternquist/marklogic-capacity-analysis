/**
 * MLCA Disconnected Snapshot Collector
 *
 * Run this Server-Side JavaScript on a MarkLogic cluster that is not directly
 * reachable from the machine running MLCA.  It produces a JSON snapshot in the
 * exact format that MLCA expects, so you can copy the output into the
 * .ml-capacity/ directory and use --trend, --compare, and all other analysis
 * features offline.
 *
 * Usage (Query Console):
 *   1. Open Query Console on the target cluster.
 *   2. Paste this script into a new JavaScript tab.
 *   3. Set the DATABASE variable below to the database you want to analyze.
 *   4. Run the script.  Copy the JSON output.
 *   5. Save the JSON to a file and import it into MLCA:
 *        python ml_capacity.py --import-snapshot snapshot.json
 *
 * Usage (curl / ml-gradle eval):
 *   curl -s --anyauth -u admin:admin \
 *     -X POST http://host:8000/v1/eval \
 *     -H "Content-Type: application/x-www-form-urlencoded" \
 *     -d "javascript=$(cat scripts/collect-snapshot.sjs)" \
 *     | python3 -c "import sys,json; [print(json.dumps(json.loads(p.split('\r\n\r\n',1)[1]),indent=2)) for p in sys.stdin.read().split('--') if 'application/json' in p]"
 *
 * Requirements:
 *   - The user executing this script needs the following privileges:
 *       xdmp:host-status, xdmp:forest-status, xdmp:forest-counts,
 *       xdmp:hosts, xdmp:database-forests, admin-module-read
 *   - For index memory detail (ML 11+): xdmp:database-describe-indexes
 */

// ── Configuration ────────────────────────────────────────────────────
const DATABASE = "Documents";   // ← Change this to your target database
// ─────────────────────────────────────────────────────────────────────

const db = xdmp.database(DATABASE);
const now = new Date();

// ── 1. Cluster overview ──────────────────────────────────────────────
const clusterName = xdmp.clusterName();
const version = xdmp.version();
const hostIds = Array.from(xdmp.hosts());
const dbIds = Array.from(xdmp.databases());
const allForestIds = Array.from(xdmp.forests());

const cluster = {
  name: clusterName,
  version: version,
  hosts: hostIds.length,
  databases: dbIds.length,
  forests: allForestIds.length,
  servers: 0
};

// Count app servers
try {
  let serverCount = 0;
  const groups = Array.from(xdmp.groups());
  for (var g = 0; g < groups.length; g++) {
    serverCount += fn.count(xdmp.groupServers(groups[g]));
  }
  cluster.servers = serverCount;
} catch (e) {
  // Non-critical — leave as 0
}

// ── 2. Host memory ──────────────────────────────────────────────────
// xdmp.hostStatus() returns a JSON ObjectNode in SJS — use .toObject()
// to get a plain JS object with camelCase property names.
const hosts = [];
for (let i = 0; i < hostIds.length; i++) {
  const hostId = hostIds[i];
  const hs = fn.head(xdmp.hostStatus(hostId)).toObject();

  hosts.push({
    "hostname":                  xdmp.hostName(hostId),
    "cpus":                      hs.cpus || 0,
    "cores":                     hs.cores || 0,
    "memory-system-total-mb":    hs.memorySystemTotal || 0,
    "memory-system-free-mb":     hs.memorySystemFree || 0,
    "memory-system-pagein-rate": hs.memorySystemPageinRate || 0,
    "memory-system-pageout-rate":hs.memorySystemPageoutRate || 0,
    "memory-system-swapin-rate": hs.memorySystemSwapinRate || 0,
    "memory-system-swapout-rate":hs.memorySystemSwapoutRate || 0,
    "memory-process-size-mb":    hs.memoryProcessSize || 0,
    "memory-process-rss-mb":     hs.memoryProcessRss || 0,
    "memory-process-anon-mb":    hs.memoryProcessAnon || 0,
    "memory-process-rss-hwm-mb": hs.memoryProcessRssHwm || 0,
    "memory-process-swap-mb":    hs.memoryProcessSwapSize || 0,
    "memory-size-mb":            hs.memorySize || 0,
    "memory-cache-size-mb":      hs.memoryCacheSize || 0,
    "memory-forest-size-mb":     hs.memoryForestSize || 0,
    "memory-file-size-mb":       hs.memoryFileSize || 0,
    "host-size-mb":              hs.hostSize || 0,
    "memory-join-size-mb":       hs.memoryJoinSize || 0,
    "memory-unclosed-size-mb":   hs.memoryUnclosedSize || 0,
    "memory-registry-size-mb":   hs.memoryRegistrySize || 0,
    "host-large-data-size-mb":   hs.hostLargeDataSize || 0,
    "log-device-space-mb":       hs.logDeviceSpace || 0,
    "data-dir-space-mb":         hs.dataDirSpace || 0
  });
}

// ── 3. Database status ───────────────────────────────────────────────
const dbForests = Array.from(xdmp.databaseForests(db));
let totalDataSizeMb = 0;
let totalInMemSizeMb = 0;
let totalLargeDataMb = 0;
let deviceSpaceMb = 0;
let leastRemainingMb = Infinity;
let mergeCount = 0;

// ── 4. Forest details ────────────────────────────────────────────────
// xdmp.forestCounts() and xdmp.forestStatus() also return JSON ObjectNodes.
const forests = [];
for (let fi = 0; fi < dbForests.length; fi++) {
  const fid = dbForests[fi];
  const fcObj = fn.head(xdmp.forestCounts(fid)).toObject();
  const fsObj = fn.head(xdmp.forestStatus(fid)).toObject();

  const docCount = fcObj.documentCount || 0;

  // Fragment counts are under standsCounts[] (camelCase array)
  let activeCount = 0, deletedCount = 0, nascentCount = 0;
  const standsCounts = fcObj.standsCounts || [];
  if (!Array.isArray(standsCounts)) standsCounts = [standsCounts];
  for (let sci = 0; sci < standsCounts.length; sci++) {
    const sc = standsCounts[sci];
    activeCount  += sc.activeFragmentCount  || 0;
    deletedCount += sc.deletedFragmentCount || 0;
    nascentCount += sc.nascentFragmentCount || 0;
  }

  // Stand info from forest-status: stands[] array with diskSize, memorySize
  const stands = fsObj.stands || [];
  if (!Array.isArray(stands)) stands = [stands];
  const standCount = stands.length;
  let diskMb = 0, memMb = 0;
  for (let si = 0; si < stands.length; si++) {
    diskMb += stands[si].diskSize   || 0;
    memMb  += stands[si].memorySize || 0;
  }

  totalDataSizeMb += diskMb;
  totalInMemSizeMb += memMb;

  // Device space from forest status
  const fDeviceSpace = fsObj.deviceSpace || 0;
  if (fDeviceSpace > 0) {
    deviceSpaceMb = Math.max(deviceSpaceMb, fDeviceSpace);
    leastRemainingMb = Math.min(leastRemainingMb, fDeviceSpace);
  }

  // Merge count from merges array
  const merges = fsObj.merges || [];
  if (!Array.isArray(merges)) merges = [merges];
  mergeCount += merges.length;

  forests.push({
    "forest-name":           xdmp.forestName(fid),
    "document-count":        docCount,
    "active-fragment-count": activeCount,
    "deleted-fragment-count":deletedCount,
    "nascent-fragment-count":nascentCount,
    "stand-count":           standCount,
    "disk-size-mb":          diskMb,
    "memory-size-mb":        memMb
  });
}

if (leastRemainingMb === Infinity) leastRemainingMb = 0;

const databaseStatus = {
  "state":              "available",
  "forests_count":      dbForests.length,
  "data_size_mb":       totalDataSizeMb,
  "device_space_mb":    deviceSpaceMb,
  "in_memory_size_mb":  totalInMemSizeMb,
  "large_data_size_mb": totalLargeDataMb,
  "least_remaining_mb": leastRemainingMb,
  "merge_count":        mergeCount,
  "list_cache_ratio":   0
};

// ── 5. Database properties (via admin module) ────────────────────────
const admin = require("/MarkLogic/admin.xqy");
const config = admin.getConfiguration();

const dbProperties = {
  "in_memory_limit":              Number(admin.databaseGetInMemoryLimit(config, db)),
  "in_memory_list_size":          Number(admin.databaseGetInMemoryListSize(config, db)),
  "in_memory_tree_size":          Number(admin.databaseGetInMemoryTreeSize(config, db)),
  "in_memory_range_index_size":   Number(admin.databaseGetInMemoryRangeIndexSize(config, db)),
  "in_memory_reverse_index_size": Number(admin.databaseGetInMemoryReverseIndexSize(config, db)),
  "in_memory_triple_index_size":  Number(admin.databaseGetInMemoryTripleIndexSize(config, db)),
  "preload_mapped_data":          xs.boolean(admin.databaseGetPreloadMappedData(config, db))
};

// ── 6. Index counts ──────────────────────────────────────────────────
const rangeElementIndexes = admin.databaseGetRangeElementIndexes(config, db);
let rangePathIndexes = [];
try { rangePathIndexes = admin.databaseGetRangePathIndexes(config, db); } catch(e) {}
let rangeFieldIndexes = [];
try { rangeFieldIndexes = admin.databaseGetRangeFieldIndexes(config, db); } catch(e) {}

const boolChecks = {
  "word-searches":                     function() { return admin.databaseGetWordSearches(config, db); },
  "fast-phrase-searches":              function() { return admin.databaseGetFastPhraseSearches(config, db); },
  "triple-index":                      function() { return admin.databaseGetTripleIndex(config, db); },
  "fast-case-sensitive-searches":      function() { return admin.databaseGetFastCaseSensitiveSearches(config, db); },
  "fast-diacritic-sensitive-searches": function() { return admin.databaseGetFastDiacriticSensitiveSearches(config, db); },
  "fast-element-word-searches":        function() { return admin.databaseGetFastElementWordSearches(config, db); },
  "fast-element-phrase-searches":      function() { return admin.databaseGetFastElementPhraseSearches(config, db); },
  "uri-lexicon":                       function() { return admin.databaseGetUriLexicon(config, db); },
  "collection-lexicon":                function() { return admin.databaseGetCollectionLexicon(config, db); },
  "trailing-wildcard-searches":        function() { return admin.databaseGetTrailingWildcardSearches(config, db); },
  "three-character-searches":          function() { return admin.databaseGetThreeCharacterSearches(config, db); }
};
let enabledBools = 0;
Object.keys(boolChecks).forEach(function(key) {
  try {
    const val = boolChecks[key]();
    if (val === true || String(val) === "true") enabledBools++;
  } catch(e) { /* function may not exist in older versions */ }
});

const indexCounts = {
  "range_element": fn.count(rangeElementIndexes),
  "range_path":    fn.count(rangePathIndexes),
  "range_field":   fn.count(rangeFieldIndexes),
  "enabled_boolean_indexes": enabledBools
};

// ── 7. Index memory (ML 11+ only) ───────────────────────────────────
let indexMemory = null;
try {
  const indexDefs = xdmp.databaseDescribeIndexes(db).toObject();
  const allIndexes = [];
  Object.keys(indexDefs).forEach(function(type) {
    const items = indexDefs[type];
    if (!Array.isArray(items)) items = [items];
    items.forEach(function(idx) {
      if (idx.indexId) {
        idx.indexType = type;
        allIndexes.push(idx);
      }
    });
  });

  const statuses = Array.from(xdmp.forestStatus(xdmp.databaseForests(db), "memoryDetail"));
  const indexTotals = {};
  const standSummaries = [];

  statuses.forEach(function(statusNode) {
    const sObj = statusNode.toObject();
    const sStands = sObj.stands;
    if (!sStands) return;
    const standList = Array.isArray(sStands) ? sStands : [sStands];
    standList.forEach(function(stand) {
      if (stand.memorySummary) {
        standSummaries.push({
          standPath: stand.path,
          diskSize: stand.diskSize,
          memorySize: stand.memorySize,
          summary: stand.memorySummary
        });
      }
      if (stand.memoryDetail && stand.memoryDetail.memoryRangeIndexes) {
        const indexes = stand.memoryDetail.memoryRangeIndexes.index;
        if (!indexes) return;
        if (!Array.isArray(indexes)) indexes = [indexes];
        indexes.forEach(function(idx) {
          const id = String(idx.indexId);
          if (!indexTotals[id]) indexTotals[id] = { memBytes: 0, diskBytes: 0 };
          indexTotals[id].memBytes  += (idx.indexMemoryBytes || 0);
          indexTotals[id].diskBytes += (idx.indexOnDiskBytes || 0);
        });
      }
    });
  });

  const report = allIndexes.map(function(def) {
    const t = indexTotals[String(def.indexId)] || { memBytes: 0, diskBytes: 0 };
    return {
      indexType:        def.indexType,
      localname:        def.localname || null,
      namespaceURI:     def.namespaceURI || null,
      scalarType:       def.scalarType || null,
      pathExpression:   def.pathExpression || null,
      indexId:          def.indexId,
      totalMemoryBytes: t.memBytes,
      totalOnDiskBytes: t.diskBytes
    };
  });

  indexMemory = { indexes: report, standSummaries: standSummaries };
} catch (e) {
  // ML < 11 or insufficient privileges — index memory will be null
}

// ── 8. Compute totals ────────────────────────────────────────────────
let totalDocs = 0, totalActive = 0, totalDeleted = 0;
let totalForestDisk = 0, totalForestMem = 0;
for (let ti = 0; ti < forests.length; ti++) {
  totalDocs       += forests[ti]["document-count"]        || 0;
  totalActive     += forests[ti]["active-fragment-count"] || 0;
  totalDeleted    += forests[ti]["deleted-fragment-count"]|| 0;
  totalForestDisk += forests[ti]["disk-size-mb"]          || 0;
  totalForestMem  += forests[ti]["memory-size-mb"]        || 0;
}

const hsum = function(key) {
  let total = 0;
  for (let hi = 0; hi < hosts.length; hi++) {
    const v = hosts[hi][key];
    if (v !== undefined && v !== null) total += Number(v);
  }
  return total;
};

const totals = {
  "documents":        totalDocs,
  "active_fragments": totalActive,
  "deleted_fragments":totalDeleted,
  "forest_disk_mb":   totalForestDisk,
  "forest_memory_mb": totalForestMem,
  "host_forest_mb":   hsum("memory-forest-size-mb"),
  "host_cache_mb":    hsum("memory-cache-size-mb"),
  "host_rss_mb":      hsum("memory-process-rss-mb"),
  "host_base_mb":     hsum("host-size-mb"),
  "host_file_mb":     hsum("memory-file-size-mb"),
  "ml_limit_mb":      hsum("memory-size-mb"),
  "system_total_mb":  hsum("memory-system-total-mb"),
  "system_free_mb":   hsum("memory-system-free-mb")
};

// Container fallback: if system_total_mb is 0, use ML configured limit
if (totals.system_total_mb === 0 && totals.ml_limit_mb > 0) {
  totals.system_total_mb = totals.ml_limit_mb;
}

// ── 9. Assemble snapshot ─────────────────────────────────────────────
const snapshot = {
  "version":         1,
  "timestamp":       now.toISOString(),
  "database":        DATABASE,
  "cluster":         cluster,
  "hosts":           hosts,
  "database_status": databaseStatus,
  "forests":         forests,
  "db_properties":   dbProperties,
  "index_counts":    indexCounts,
  "index_memory":    indexMemory,
  "totals":          totals
};

snapshot;
