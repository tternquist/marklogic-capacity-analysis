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
 *       xdmp:hosts, xdmp:database-forests
 *   - For index memory detail (ML 11+): xdmp:database-describe-indexes
 */

// ── Configuration ────────────────────────────────────────────────────
var DATABASE = "Documents";   // ← Change this to your target database
// ─────────────────────────────────────────────────────────────────────

var db = xdmp.database(DATABASE);
var now = new Date();

// ── 1. Cluster overview ──────────────────────────────────────────────
var clusterName = xdmp.clusterName();
var version = xdmp.version();
var hostIds = xdmp.hosts();
var hostCount = fn.count(hostIds);
var dbIds = xdmp.databases();
var dbCount = fn.count(dbIds);
var forestIds = xdmp.forests();
var forestCount = fn.count(forestIds);

var cluster = {
  name: clusterName,
  version: version,
  hosts: hostCount,
  databases: dbCount,
  forests: forestCount,
  servers: 0
};

// Count app servers
try {
  var serverCount = 0;
  var groups = xdmp.groups().toArray();
  for (var g = 0; g < groups.length; g++) {
    var servers = xdmp.groupServers(groups[g]);
    serverCount += fn.count(servers);
  }
  cluster.servers = serverCount;
} catch (e) {
  // Non-critical — leave as 0
}

// ── 2. Host memory ──────────────────────────────────────────────────
var hosts = [];
var hostArr = hostIds.toArray();
for (var i = 0; i < hostArr.length; i++) {
  var hostId = hostArr[i];
  var s = fn.head(xdmp.hostStatus(hostId));
  // Extract values via xpath on the status node
  var getVal = function(node, localName) {
    var seq = node.xpath("//*[local-name()='" + localName + "']/data(.)");
    var arr = seq.toArray();
    return arr.length > 0 ? Number(arr[0]) : 0;
  };

  hosts.push({
    "hostname":                  xdmp.hostName(hostId),
    "cpus":                      getVal(s, "cpus"),
    "cores":                     getVal(s, "cores"),
    "memory-system-total-mb":    getVal(s, "memory-system-total"),
    "memory-system-free-mb":     getVal(s, "memory-system-free"),
    "memory-system-pagein-rate": getVal(s, "memory-system-pagein-rate"),
    "memory-system-pageout-rate":getVal(s, "memory-system-pageout-rate"),
    "memory-system-swapin-rate": getVal(s, "memory-system-swapin-rate"),
    "memory-system-swapout-rate":getVal(s, "memory-system-swapout-rate"),
    "memory-process-size-mb":    getVal(s, "memory-process-size"),
    "memory-process-rss-mb":     getVal(s, "memory-process-rss"),
    "memory-process-anon-mb":    getVal(s, "memory-process-anon"),
    "memory-process-rss-hwm-mb": getVal(s, "memory-process-rss-hwm"),
    "memory-process-swap-mb":    getVal(s, "memory-process-swap-size"),
    "memory-size-mb":            getVal(s, "memory-size"),
    "memory-cache-size-mb":      getVal(s, "memory-cache-size"),
    "memory-forest-size-mb":     getVal(s, "memory-forest-size"),
    "memory-file-size-mb":       getVal(s, "memory-file-size"),
    "host-size-mb":              getVal(s, "host-size"),
    "memory-join-size-mb":       getVal(s, "memory-join-size"),
    "memory-unclosed-size-mb":   getVal(s, "memory-unclosed-size"),
    "memory-registry-size-mb":   getVal(s, "memory-registry-size"),
    "host-large-data-size-mb":   getVal(s, "host-large-data-size"),
    "log-device-space-mb":       getVal(s, "log-device-space"),
    "data-dir-space-mb":         getVal(s, "data-dir-space")
  });
}

// ── 3. Database status (via Management API internal) ─────────────────
var dbForests = xdmp.databaseForests(db).toArray();
var totalDataSizeMb = 0;
var totalInMemSizeMb = 0;
var totalLargeDataMb = 0;
var deviceSpaceMb = 0;
var leastRemainingMb = Infinity;
var mergeCount = 0;

// ── 4. Forest details ────────────────────────────────────────────────
var forests = [];
for (var fi = 0; fi < dbForests.length; fi++) {
  var fid = dbForests[fi];
  var fc = fn.head(xdmp.forestCounts(fid));
  var fs = fn.head(xdmp.forestStatus(fid));

  var getValFromNode = function(node, localName) {
    var seq = node.xpath("//*[local-name()='" + localName + "']/data(.)");
    var arr = seq.toArray();
    return arr.length > 0 ? Number(arr[0]) : 0;
  };

  var docCount = getValFromNode(fc, "document-count");

  // Fragment counts are under stands-counts/stand-counts
  var standCountsNodes = fc.xpath("//*[local-name()='stands-counts']/*[local-name()='stand-counts']");
  var activeCount = 0, deletedCount = 0, nascentCount = 0;
  var scArr = standCountsNodes.toArray();
  for (var sci = 0; sci < scArr.length; sci++) {
    var scNode = scArr[sci];
    var getScVal = function(n, ln) {
      var s2 = n.xpath("*[local-name()='" + ln + "']/data(.)");
      var a2 = s2.toArray();
      return a2.length > 0 ? Number(a2[0]) : 0;
    };
    activeCount  += getScVal(scNode, "active-fragment-count");
    deletedCount += getScVal(scNode, "deleted-fragment-count");
    nascentCount += getScVal(scNode, "nascent-fragment-count");
  }

  // Stand info from forest-status
  var standNodes = fs.xpath("//*[local-name()='stands']/*[local-name()='stand']");
  var standArr2 = standNodes.toArray();
  var standCount = standArr2.length;
  var diskMb = 0, memMb = 0;
  for (var si = 0; si < standArr2.length; si++) {
    var standNode = standArr2[si];
    var getDiskMem = function(n, ln) {
      var s3 = n.xpath("*[local-name()='" + ln + "']/data(.)");
      var a3 = s3.toArray();
      return a3.length > 0 ? Number(a3[0]) : 0;
    };
    diskMb += getDiskMem(standNode, "disk-size");
    memMb  += getDiskMem(standNode, "memory-size");
  }

  totalDataSizeMb += diskMb;
  totalInMemSizeMb += memMb;

  // Device space from forest status
  var fDeviceSpace = getValFromNode(fs, "device-space");
  if (fDeviceSpace > 0) {
    deviceSpaceMb = Math.max(deviceSpaceMb, fDeviceSpace);
    leastRemainingMb = Math.min(leastRemainingMb, fDeviceSpace);
  }

  mergeCount += getValFromNode(fs, "merge-count");

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

var databaseStatus = {
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
var admin = require("/MarkLogic/admin.xqy");
var config = admin.getConfiguration();

var dbProperties = {
  "in_memory_limit":              Number(admin.databaseGetInMemoryLimit(config, db)),
  "in_memory_list_size":          Number(admin.databaseGetInMemoryListSize(config, db)),
  "in_memory_tree_size":          Number(admin.databaseGetInMemoryTreeSize(config, db)),
  "in_memory_range_index_size":   Number(admin.databaseGetInMemoryRangeIndexSize(config, db)),
  "in_memory_reverse_index_size": Number(admin.databaseGetInMemoryReverseIndexSize(config, db)),
  "in_memory_triple_index_size":  Number(admin.databaseGetInMemoryTripleIndexSize(config, db)),
  "preload_mapped_data":          xs.boolean(admin.databaseGetPreloadMappedData(config, db))
};

// ── 6. Index counts ──────────────────────────────────────────────────
var rangeElementIndexes = admin.databaseGetRangeElementIndexes(config, db);
var rangePathIndexes = [];
try { rangePathIndexes = admin.databaseGetRangePathIndexes(config, db); } catch(e) {}
var rangeFieldIndexes = [];
try { rangeFieldIndexes = admin.databaseGetRangeFieldIndexes(config, db); } catch(e) {}

var boolChecks = {
  "word-searches":                   function() { return admin.databaseGetWordSearches(config, db); },
  "fast-phrase-searches":            function() { return admin.databaseGetFastPhraseSearches(config, db); },
  "triple-index":                    function() { return admin.databaseGetTripleIndex(config, db); },
  "fast-case-sensitive-searches":    function() { return admin.databaseGetFastCaseSensitiveSearches(config, db); },
  "fast-diacritic-sensitive-searches": function() { return admin.databaseGetFastDiacriticSensitiveSearches(config, db); },
  "fast-element-word-searches":      function() { return admin.databaseGetFastElementWordSearches(config, db); },
  "fast-element-phrase-searches":    function() { return admin.databaseGetFastElementPhraseSearches(config, db); },
  "uri-lexicon":                     function() { return admin.databaseGetUriLexicon(config, db); },
  "collection-lexicon":              function() { return admin.databaseGetCollectionLexicon(config, db); },
  "trailing-wildcard-searches":      function() { return admin.databaseGetTrailingWildcardSearches(config, db); },
  "three-character-searches":        function() { return admin.databaseGetThreeCharacterSearches(config, db); }
};
var enabledBools = 0;
Object.keys(boolChecks).forEach(function(key) {
  try {
    var val = boolChecks[key]();
    if (val === true || String(val) === "true") enabledBools++;
  } catch(e) { /* function may not exist in older versions */ }
});

var indexCounts = {
  "range_element": fn.count(rangeElementIndexes),
  "range_path":    fn.count(rangePathIndexes),
  "range_field":   fn.count(rangeFieldIndexes),
  "enabled_boolean_indexes": enabledBools
};

// ── 7. Index memory (ML 11+ only) ───────────────────────────────────
var indexMemory = null;
try {
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

  var statuses = xdmp.forestStatus(xdmp.databaseForests(db), "memoryDetail").toArray();
  var indexTotals = {};
  var standSummaries = [];

  statuses.forEach(function(statusNode) {
    var sObj = statusNode.toObject();
    var stands = sObj.stands;
    if (!stands) return;
    var standList = Array.isArray(stands) ? stands : [stands];
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
        var indexes = stand.memoryDetail.memoryRangeIndexes.index;
        if (!indexes) return;
        if (!Array.isArray(indexes)) indexes = [indexes];
        indexes.forEach(function(idx) {
          var id = String(idx.indexId);
          if (!indexTotals[id]) indexTotals[id] = { memBytes: 0, diskBytes: 0 };
          indexTotals[id].memBytes  += (idx.indexMemoryBytes || 0);
          indexTotals[id].diskBytes += (idx.indexOnDiskBytes || 0);
        });
      }
    });
  });

  var report = allIndexes.map(function(def) {
    var totals = indexTotals[String(def.indexId)] || { memBytes: 0, diskBytes: 0 };
    return {
      indexType:        def.indexType,
      localname:        def.localname || null,
      namespaceURI:     def.namespaceURI || null,
      scalarType:       def.scalarType || null,
      pathExpression:   def.pathExpression || null,
      indexId:          def.indexId,
      totalMemoryBytes: totals.memBytes,
      totalOnDiskBytes: totals.diskBytes
    };
  });

  indexMemory = { indexes: report, standSummaries: standSummaries };
} catch (e) {
  // ML < 11 or insufficient privileges — index memory will be null
}

// ── 8. Compute totals ────────────────────────────────────────────────
var totalDocs = 0, totalActive = 0, totalDeleted = 0;
var totalForestDisk = 0, totalForestMem = 0;
for (var ti = 0; ti < forests.length; ti++) {
  totalDocs       += forests[ti]["document-count"]        || 0;
  totalActive     += forests[ti]["active-fragment-count"] || 0;
  totalDeleted    += forests[ti]["deleted-fragment-count"]|| 0;
  totalForestDisk += forests[ti]["disk-size-mb"]          || 0;
  totalForestMem  += forests[ti]["memory-size-mb"]        || 0;
}

var hsum = function(key) {
  var total = 0;
  for (var hi = 0; hi < hosts.length; hi++) {
    var v = hosts[hi][key];
    if (v !== undefined && v !== null) total += Number(v);
  }
  return total;
};

var totals = {
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
var snapshot = {
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
