from ml_capacity.formatting import (
    color, header, sub_header, kv, bar, fmt_mb, status_badge,
    YELLOW, GREEN, RED, CYAN, BOLD, DIM,
)
from ml_capacity.collect import (
    collect_cluster_overview, collect_database_status,
    collect_database_properties, collect_host_memory,
    collect_forest_counts, INDEX_MEMORY_JS,
)


def report_cluster(client):
    """Print a summary of the cluster's name, version, and headline object counts."""
    header("CLUSTER OVERVIEW")
    data = collect_cluster_overview(client)
    cluster = data.get("local-cluster-default", {})
    kv("Cluster name", cluster.get("name", "unknown"))
    kv("MarkLogic version", cluster.get("version", "unknown"))

    relations = cluster.get("relations", {}).get("relation-group", [])
    for rg in relations:
        t = rg.get("typeref", "")
        count = rg.get("relation-count", {}).get("value", 0)
        if t in ("hosts", "databases", "forests", "servers"):
            kv(t.capitalize(), count)


def report_host_memory(client):
    """Print per-host RAM breakdown (system, cache, forest, base, file) with headroom.

    Returns a list of host dicts for downstream use, or ``None`` if eval is disabled.
    """
    header("HOST MEMORY")
    try:
        results = collect_host_memory(client)
        if not results:
            print("    Could not retrieve host memory (eval may be disabled)")
            return None

        host_data = results[0] if isinstance(results[0], list) else [results[0]]
        all_hosts = []

        for host in host_data:
            name    = host.get("hostname", "unknown")
            cpus    = host.get("cpus", "?")
            cores   = host.get("cores", "?")

            # System-level
            total   = float(host.get("memory-system-total-mb", 0) or 0)
            free    = float(host.get("memory-system-free-mb",  0) or 0)
            used    = total - free

            # ML process breakdown
            rss      = float(host.get("memory-process-rss-mb",     0) or 0)
            rss_hwm  = float(host.get("memory-process-rss-hwm-mb", 0) or 0)
            virt     = float(host.get("memory-process-size-mb",    0) or 0)
            anon     = float(host.get("memory-process-anon-mb",    0) or 0)
            swap     = float(host.get("memory-process-swap-mb",    0) or 0)

            # ML memory components (these add up to explain RSS)
            cache    = float(host.get("memory-cache-size-mb",    0) or 0)
            forest   = float(host.get("memory-forest-size-mb",   0) or 0)
            filecache= float(host.get("memory-file-size-mb",     0) or 0)
            base     = float(host.get("host-size-mb",            0) or 0)
            join     = float(host.get("memory-join-size-mb",     0) or 0)
            unclosed = float(host.get("memory-unclosed-size-mb", 0) or 0)
            largedata= float(host.get("host-large-data-size-mb", 0) or 0)

            # ML configured memory limit
            ml_limit = float(host.get("memory-size-mb", 0) or 0)

            # Paging / swap pressure
            pagein   = float(host.get("memory-system-pagein-rate",  0) or 0)
            pageout  = float(host.get("memory-system-pageout-rate", 0) or 0)
            swapin   = float(host.get("memory-system-swapin-rate",  0) or 0)
            swapout  = float(host.get("memory-system-swapout-rate", 0) or 0)

            # Data dir space
            data_dir = float(host.get("data-dir-space-mb", 0) or 0)

            sub_header(f"Host: {name}")
            kv("CPUs / Cores", f"{cpus} / {cores}")

            print()
            print(f"    {color('System Memory', BOLD)}")
            kv("  Total RAM", fmt_mb(total))
            kv("  Free RAM",  fmt_mb(free))
            sys_pct = (used / total * 100) if total else 0
            kv("  Used RAM",  f"{fmt_mb(used)}  {bar(sys_pct)}")
            kv("  Data dir free", fmt_mb(data_dir))

            print()
            print(f"    {color('MarkLogic Process', BOLD)}")
            kv("  Virtual size (VSZ)", fmt_mb(virt))
            rss_pct = (rss / total * 100) if total else 0
            kv("  Resident size (RSS)", f"{fmt_mb(rss)}  {bar(rss_pct)}")
            kv("  RSS peak (HWM)",     fmt_mb(rss_hwm))
            kv("  Anonymous/heap",     fmt_mb(anon))
            kv("  Swap in use",        f"{fmt_mb(swap)}  {status_badge(swap == 0, 'OK', 'SWAPPING')}")
            if ml_limit:
                kv("  Configured ML limit", fmt_mb(ml_limit))

            print()
            print(f"    {color('ML Memory Components  (explain RSS)', BOLD)}")
            kv("  Cache alloc (list+tree)", fmt_mb(cache))
            kv("  Forest in-memory stands", fmt_mb(forest))
            kv("  File cache (mmap)",       fmt_mb(filecache))
            kv("  Base ML overhead",        fmt_mb(base))
            if join:
                kv("  Join workspace",      fmt_mb(join))
            if unclosed:
                kv("  Unclosed stands",     fmt_mb(unclosed))
            if largedata:
                kv("  Large binary cache",  fmt_mb(largedata))
            accounted = cache + forest + filecache + base + join + unclosed + largedata
            kv("  Accounted total",
               f"{fmt_mb(accounted)}  {color('(vs RSS ' + fmt_mb(rss) + ')', DIM)}")

            print()
            print(f"    {color('Paging / Swap Pressure', BOLD)}")
            pagein_ok  = pagein  < 100
            pageout_ok = pageout < 100
            kv("  Page-in rate",   f"{pagein:.1f} MB/s  {status_badge(pagein_ok,  'OK', 'HIGH')}")
            kv("  Page-out rate",  f"{pageout:.1f} MB/s  {status_badge(pageout_ok, 'OK', 'HIGH')}")
            kv("  Swap-in rate",   f"{swapin:.3f} MB/s  {status_badge(swapin == 0,  'OK', 'SWAPPING')}")
            kv("  Swap-out rate",  f"{swapout:.3f} MB/s  {status_badge(swapout == 0, 'OK', 'SWAPPING')}")

            all_hosts.append(host)

        return all_hosts
    except Exception as e:
        print(f"    Could not retrieve host memory: {e}")
        print("    (Ensure ML_ALLOW_EVAL=true for full capacity analysis)")
        return None


def report_database_stats(client, database):
    """Print document/fragment counts and disk/memory totals for one database."""
    header(f"DATABASE: {database}")
    data = collect_database_status(client, database)
    status = data.get("database-status", {})
    props = status.get("status-properties", {})

    kv("State", props.get("state", {}).get("value", "unknown"))
    kv("Forests", props.get("forests-count", {}).get("value", 0))

    data_size = props.get("data-size", {}).get("value", 0)
    device_space = props.get("device-space", {}).get("value", "0")
    device_space = float(device_space)
    in_mem = props.get("in-memory-size", {}).get("value", 0)
    large_data = props.get("large-data-size", {}).get("value", 0)

    sub_header("Storage")
    kv("Data on disk", fmt_mb(data_size))
    kv("Large binary data", fmt_mb(large_data))
    kv("In-memory size", fmt_mb(in_mem))
    kv("Device space (total)", fmt_mb(device_space))

    remaining = props.get("least-remaining-space-forest", {}).get("value", 0)
    remaining = float(remaining)
    if device_space > 0:
        used_pct = ((device_space - remaining) / device_space) * 100
        kv("Disk used", bar(used_pct))
        kv("Remaining (least forest)", fmt_mb(remaining))

        # Disk runway estimation
        if data_size and float(data_size) > 0:
            ratio = remaining / float(data_size)
            kv("Disk runway", f"~{ratio:.1f}x current data can still fit")

    sub_header("Cache Performance")
    cache = props.get("cache-properties", {})
    list_ratio = cache.get("list-cache-ratio", {}).get("value", 0)
    kv("List cache hit ratio", f"{list_ratio}%  {status_badge(list_ratio > 80, 'GOOD', 'LOW')}")

    triple_ratio = cache.get("triple-value-cache-ratio", {}).get("value", 0)
    kv("Triple value cache ratio", f"{triple_ratio}%  {status_badge(triple_ratio > 80, 'GOOD', 'LOW')}")

    sub_header("Activity")
    kv("Active merges", props.get("merge-count", {}).get("value", 0))
    kv("Active reindexes", props.get("reindex-count", {}).get("value", 0))
    kv("Active backups", props.get("backup-count", {}).get("value", 0))

    return data_size, device_space, remaining


def report_forest_health(client, database):
    """Print per-forest stand counts, state, and merge/reindex status for one database."""
    header(f"FOREST HEALTH: {database}")

    try:
        results = collect_forest_counts(client, database)
        if not results:
            print("    Could not retrieve forest counts (eval may be disabled)")
            return []

        forests = results[0] if isinstance(results[0], list) else [results[0]]
        forest_data = []

        for f in forests:
            name = f.get("forest-name", "unknown")
            docs = f.get("document-count", 0) or 0
            active = f.get("active-fragment-count", 0) or 0
            deleted = f.get("deleted-fragment-count", 0) or 0
            nascent = f.get("nascent-fragment-count", 0) or 0
            stands = f.get("stand-count", 0) or 0
            disk = f.get("disk-size-mb", 0) or 0
            memory = f.get("memory-size-mb", 0) or 0

            sub_header(f"Forest: {name}")
            kv("Documents", f"{docs:,}")
            kv("Active fragments", f"{active:,}")
            kv("Deleted fragments", f"{deleted:,}")

            total_frags = active + deleted
            frag_pct = (deleted / total_frags * 100) if total_frags > 0 else 0

            if frag_pct >= 50:
                frag_badge = color("[CRITICAL]", RED + BOLD)
            elif frag_pct >= 25:
                frag_badge = color("[HIGH]", RED)
            elif frag_pct >= 10:
                frag_badge = color("[MODERATE]", YELLOW)
            else:
                frag_badge = color("[OK]", GREEN)
            kv("Fragmentation", f"{frag_pct:.1f}%  ({deleted:,} deleted / {total_frags:,} total)  {frag_badge}")

            if frag_pct >= 25:
                # Estimate wasted space
                if total_frags > 0 and disk > 0:
                    wasted_mb = disk * (deleted / total_frags)
                    print(f"      {color('!', RED)} ~{fmt_mb(wasted_mb)} of disk occupied by deleted fragments")
                if frag_pct >= 50:
                    print(f"      {color('!', RED)} {color('Merge recommended to reclaim space and improve projection accuracy', BOLD)}")
                    print(f"      {color('  Run:', DIM)} xdmp:merge(xdmp:database-forests(xdmp:database(\"{name}\")))")
                    print(f"      {color('  Or via Admin UI: Database > Merge > Merge Now', DIM)}")
                else:
                    print(f"      {color('!', YELLOW)} High deleted fragment ratio inflates disk and memory metrics")
                    print(f"      {color('  Consider forcing a merge if projections seem off', DIM)}")

            kv("Stands", f"{stands} / 64 max  {bar(stands / 64 * 100, 50, 75)}")
            kv("Disk size", fmt_mb(disk))
            kv("In-memory size", fmt_mb(memory))

            MAX_FRAGMENTS = 96_000_000
            frag_pct_of_max = (active / MAX_FRAGMENTS) * 100
            kv("Fragment capacity", f"{active:,} / {MAX_FRAGMENTS:,}  {bar(frag_pct_of_max)}")

            forest_data.append(f)

        return forest_data

    except Exception as e:
        print(f"    Could not retrieve forest details: {e}")
        return []


def report_index_config(client, database):
    """Print the configured index set for a database (range, word, path, etc.)."""
    header(f"INDEX CONFIGURATION: {database}")
    props = collect_database_properties(client, database)

    # Boolean indexes
    bool_indexes = [
        ("word-searches", "Word searches"),
        ("word-positions", "Word positions"),
        ("fast-phrase-searches", "Fast phrase searches"),
        ("fast-reverse-searches", "Fast reverse searches"),
        ("triple-index", "Triple index"),
        ("triple-positions", "Triple positions"),
        ("fast-case-sensitive-searches", "Fast case-sensitive"),
        ("fast-diacritic-sensitive-searches", "Fast diacritic-sensitive"),
        ("fast-element-word-searches", "Fast element word"),
        ("element-word-positions", "Element word positions"),
        ("fast-element-phrase-searches", "Fast element phrase"),
        ("uri-lexicon", "URI lexicon"),
        ("collection-lexicon", "Collection lexicon"),
        ("trailing-wildcard-searches", "Trailing wildcard"),
        ("three-character-searches", "Three-character searches"),
        ("two-character-searches", "Two-character searches"),
        ("one-character-searches", "One-character searches"),
        ("field-value-searches", "Field value searches"),
    ]

    enabled = []
    disabled = []
    for key, label in bool_indexes:
        val = props.get(key, False)
        if val is True or val == "true":
            enabled.append(label)
        else:
            disabled.append(label)

    sub_header("Enabled Indexes")
    for idx in enabled:
        print(f"      {color('+', GREEN)} {idx}")

    sub_header("Disabled Indexes")
    for idx in disabled:
        print(f"      {color('-', DIM)} {idx}")

    # Range indexes
    range_indexes = props.get("range-element-index", [])
    range_path = props.get("range-path-index", [])
    range_field = props.get("range-field-index", [])

    sub_header("Range Indexes")
    total_range = len(range_indexes) + len(range_path) + len(range_field)
    kv("Element range indexes", len(range_indexes))
    kv("Path range indexes", len(range_path))
    kv("Field range indexes", len(range_field))
    for ri in range_indexes:
        ln = ri.get("localname", "?")
        st = ri.get("scalar-type", "?")
        print(f"      {color('>', CYAN)} {ln} ({st})")
    for ri in range_path:
        pe = ri.get("path-expression", "?")
        st = ri.get("scalar-type", "?")
        print(f"      {color('>', CYAN)} {pe} ({st})")

    # In-memory settings (critical for capacity)
    sub_header("In-Memory Settings")
    kv("in-memory-limit", f"{props.get('in-memory-limit', 'N/A'):,} KB")
    kv("in-memory-list-size", f"{props.get('in-memory-list-size', 'N/A')} MB")
    kv("in-memory-tree-size", f"{props.get('in-memory-tree-size', 'N/A')} MB")
    kv("in-memory-range-index-size", f"{props.get('in-memory-range-index-size', 'N/A')} MB")
    kv("in-memory-reverse-index-size", f"{props.get('in-memory-reverse-index-size', 'N/A')} MB")
    kv("in-memory-triple-index-size", f"{props.get('in-memory-triple-index-size', 'N/A')} MB")

    return props, total_range, len(enabled)


def report_index_memory(client, database):
    """Print per-index memory usage for the given database with range-index detail."""
    """Per-index memory and disk usage using xdmp.forestStatus('memoryDetail').

    Requires MarkLogic 11+ and ML_ALLOW_EVAL=true.
    Uses xdmp.databaseDescribeIndexes() for index definitions and
    xdmp.forestStatus(forests, 'memoryDetail') for per-stand, per-index
    memory and on-disk byte counts. Aggregates across all stands.

    Also shows the per-stand memorySummary breakdown (list, tree, range
    indexes, triple index, timestamps, etc.) which explains where
    forest memory is being consumed.
    """
    header(f"INDEX MEMORY USAGE: {database}")

    # Check preload-mapped-data setting
    try:
        db_config = client.get_json(
            f"/manage/v2/databases/{database}/properties?format=json"
        )
        preload = db_config.get("preload-mapped-data", False)
    except Exception:
        preload = None

    if preload is False:
        print()
        print(f"    {color('!', YELLOW)} {color('preload-mapped-data is DISABLED', YELLOW + BOLD)}")
        print(f"    {color('  Range index memory numbers below may underreport.', DIM)}")
        print(f"    {color('  Without preload, index data is only loaded into memory on', DIM)}")
        print(f"    {color('  first query — not on forest open. Enable it for accurate', DIM)}")
        print(f"    {color('  memory reporting:', DIM)}")
        print(f"    {color('  Admin UI: Database > Settings > preload mapped data = true', DIM)}")
        print()

    try:
        results = client.eval_javascript(INDEX_MEMORY_JS, database=database,
                                         vars={"dbName": database})
        if not results:
            print("    Could not retrieve index memory data")
            return

        data = results[0]
        indexes = data.get("indexes", [])
        stand_summaries = data.get("standSummaries", [])

        # -- Per-stand memory summary ------------------------------------
        sub_header("Stand Memory Breakdown")

        # Sum across stands for the aggregate
        agg = {}
        for ss in stand_summaries:
            summary = ss.get("summary", {})
            for k, v in summary.items():
                agg[k] = agg.get(k, 0) + (v or 0)

        total_stand_mem = sum(ss.get("memorySize", 0) or 0 for ss in stand_summaries)
        total_stand_disk = sum(ss.get("diskSize", 0) or 0 for ss in stand_summaries)

        kv("Stands",            len(stand_summaries))
        kv("Total disk",        fmt_mb(total_stand_disk))
        kv("Total memory",      fmt_mb(total_stand_mem))
        print()

        # Show component breakdown (sorted by size descending)
        component_labels = {
            "rangeIndexesBytes":            "Range indexes",
            "timestampsFileBytes":          "Timestamps",
            "uniqueKeyIndexBytes":          "Unique key index",
            "uriKeyIndexBytes":             "URI key index",
            "linkKeysFileBytes":            "Link keys",
            "ordinalsFileBytes":            "Ordinals",
            "uniqKeysFileBytes":            "Unique keys",
            "uriKeysFileBytes":             "URI keys",
            "tripleIndexBytes":             "Triple index",
            "qualitiesFileBytes":           "Qualities",
            "lengthsFileBytes":             "Lengths",
            "listFileBytes":                "List index",
            "treeFileBytes":                "Tree index",
            "frequenciesFileBytes":         "Frequencies",
            "reverseIndexBytes":            "Reverse index",
            "geoSpatialRegionIndexesBytes": "Geospatial region",
            "binaryKeysFileBytes":          "Binary keys",
            "linkKeyIndexBytes":            "Link key index",
            "stopKeySetFileBytes":          "Stop key set",
        }

        sorted_components = sorted(agg.items(), key=lambda x: x[1], reverse=True)
        total_bytes = sum(v for _, v in sorted_components) or 1

        for key, bytes_val in sorted_components:
            if bytes_val == 0:
                continue
            label = component_labels.get(key, key)
            pct = bytes_val / total_bytes * 100
            mb = bytes_val / (1024 * 1024)
            kv(label, f"{mb:>8.2f} MB  ({pct:>5.1f}%)")

        # -- Per-index memory usage --------------------------------------
        sub_header("Per-Index Memory & Disk")

        # Group by index type
        by_type = {}
        for idx in indexes:
            t = idx.get("indexType", "unknown")
            if t not in by_type:
                by_type[t] = []
            by_type[t].append(idx)

        type_labels = {
            "uriLexicon":          "URI Lexicon",
            "collectionLexicon":   "Collection Lexicon",
            "rangeElementIndex":   "Element Range Index",
            "rangePathIndex":      "Path Range Index",
            "rangeFieldIndex":     "Field Range Index",
            "geospatialPathIndex": "Geospatial Path Index",
        }

        grand_mem = 0
        grand_disk = 0

        for idx_type, items in by_type.items():
            type_label = type_labels.get(idx_type, idx_type)
            print()
            print(f"    {color(type_label, BOLD)}")

            for idx in sorted(items, key=lambda x: x.get("totalMemoryBytes", 0), reverse=True):
                mem_b  = idx.get("totalMemoryBytes", 0) or 0
                disk_b = idx.get("totalOnDiskBytes", 0) or 0
                grand_mem  += mem_b
                grand_disk += disk_b

                name = idx.get("localname") or idx_type
                ns   = idx.get("namespaceURI", "")
                st   = idx.get("scalarType", "")

                name_str = name
                if st:
                    name_str += f" ({st})"
                if ns:
                    name_str += f"  {color(ns, DIM)}"

                mem_str  = fmt_mb(mem_b / (1024 * 1024))  if mem_b  else "n/a"
                disk_str = fmt_mb(disk_b / (1024 * 1024)) if disk_b else "n/a"

                print(f"      {color('>', CYAN)} {name_str}")
                print(f"        Memory: {mem_str:>12}    Disk: {disk_str:>12}")

        print()
        kv("Per-index cached memory", fmt_mb(grand_mem / (1024 * 1024)))
        kv("Per-index on-disk",       fmt_mb(grand_disk / (1024 * 1024)))

        # The per-index memory from memoryDetail only shows data loaded into
        # the list cache — not total resident range index pages. The stand-level
        # aggregate is the real memory picture.
        total_range_bytes = sum(
            ss.get("summary", {}).get("rangeIndexesBytes", 0)
            for ss in stand_summaries
        )
        if total_range_bytes > 0:
            print()
            kv("Total range index memory (all indexes, stand-level)",
               color(fmt_mb(total_range_bytes / (1024 * 1024)), BOLD))
            print(f"      {color('Note:', DIM)} Per-index values above reflect only cache-warmed data.")
            print(f"      {color('The stand-level total includes all resident range index pages.', DIM)}")
            print(f"      {color('Use --index-impact for measured per-index costs.', DIM)}")

    except Exception as e:
        print(f"    Could not retrieve index memory usage: {e}")
        print(f"    (Requires ML 11+ and ML_ALLOW_EVAL=true)")


def report_capacity_estimate(database, db_props, forest_data, host_data,
                             remaining_disk_mb=0):
    """Project headroom: derive per-doc disk/memory, then report docs-until-ceiling.

    Inputs come from :mod:`ml_capacity.collect` (pre-collected to avoid refetch);
    output is printed to stdout alongside a human-readable projection summary.
    """
    header(f"CAPACITY ESTIMATE: {database}")

    if not forest_data:
        print("    Insufficient data for capacity estimation (no forest metrics)")
        return

    # Gather current totals
    total_docs = sum(f.get("document-count", 0) or 0 for f in forest_data)
    total_active = sum(f.get("active-fragment-count", 0) or 0 for f in forest_data)
    total_deleted = sum(f.get("deleted-fragment-count", 0) or 0 for f in forest_data)
    total_memory = sum(f.get("memory-size-mb", 0) or 0 for f in forest_data)
    total_disk = sum(f.get("disk-size-mb", 0) or 0 for f in forest_data)
    num_forests = len(forest_data)
    total_frags = total_active + total_deleted
    frag_pct = (total_deleted / total_frags * 100) if total_frags > 0 else 0

    # In-memory settings from db properties.
    # Real ML defaults (confirmed against Management API): limit=32768 KB,
    # list=64 MB, tree=16 MB, range=2 MB, reverse=2 MB, triple=16 MB.
    # These settings directly determine how much RAM an in-memory stand
    # occupies before it is flushed to disk (validated: sum ≈ observed peak).
    in_mem_limit_kb    = db_props.get("in-memory-limit", 32768)           # flush trigger (KB)
    in_mem_list_mb     = db_props.get("in-memory-list-size", 64)          # word/element lists
    in_mem_tree_mb     = db_props.get("in-memory-tree-size", 16)          # compressed trees
    in_mem_range_mb    = db_props.get("in-memory-range-index-size", 2)    # range index entries
    in_mem_reverse_mb  = db_props.get("in-memory-reverse-index-size", 2)  # reverse index
    in_mem_triple_mb   = db_props.get("in-memory-triple-index-size", 16)  # triple index

    # Sum of per-forest in-memory components = peak RAM consumed by one
    # active in-memory stand.  Each forest can hold one in-memory stand at
    # a time; when it hits in-memory-limit the stand is flushed to disk.
    # This memory must be reserved as *ingestion headroom* — it is not
    # visible in a snapshot taken at rest (no active load).
    in_mem_per_forest = (in_mem_list_mb + in_mem_tree_mb
                         + in_mem_range_mb + in_mem_reverse_mb
                         + in_mem_triple_mb)
    in_mem_total_all  = in_mem_per_forest * num_forests

    sub_header("Current State")
    kv("Total documents", f"{total_docs:,}")
    kv("Total active fragments", f"{total_active:,}")
    kv("Forests", num_forests)
    kv("Total forest memory", fmt_mb(total_memory))
    kv("Total forest disk", fmt_mb(total_disk))

    if total_docs > 0:
        avg_mem_per_doc = (total_memory * 1024 * 1024) / total_docs  # bytes
        avg_disk_per_doc = (total_disk * 1024 * 1024) / total_docs if total_disk > 0 else 0
        kv("Avg memory per document", f"{avg_mem_per_doc:.0f} bytes")
        kv("Avg disk per document", f"{avg_disk_per_doc:.0f} bytes" if avg_disk_per_doc > 0 else "N/A")

    sub_header("Fragment Limits")
    MAX_FRAGS_PER_FOREST = 96_000_000
    max_fragments_total = MAX_FRAGS_PER_FOREST * num_forests
    frags_remaining = max_fragments_total - total_active
    frags_pct_used = (total_active / max_fragments_total) * 100

    kv("Max fragments (all forests)", f"{max_fragments_total:,}")
    kv("Current active fragments", f"{total_active:,}")
    kv("Fragments remaining", f"{frags_remaining:,}")
    kv("Fragment utilization", bar(frags_pct_used))

    # Estimate docs-per-fragment ratio
    if total_active > 0:
        doc_frag_ratio = total_docs / total_active
        docs_remaining_by_frags = int(frags_remaining * doc_frag_ratio)
        kv("Doc/fragment ratio", f"{doc_frag_ratio:.2f}")
        kv("Est. documents until fragment limit", f"{docs_remaining_by_frags:,}")

    sub_header("In-Memory Stand Limits")
    kv("in-memory-limit (flush trigger)",
       f"{in_mem_limit_kb:,} KB — stand flushed to disk when list reaches this size")
    kv("Per-forest stand budget",
       f"{in_mem_per_forest} MB  "
       f"(list:{in_mem_list_mb} + tree:{in_mem_tree_mb} + range:{in_mem_range_mb}"
       f" + reverse:{in_mem_reverse_mb} + triple:{in_mem_triple_mb})")
    kv("Ingestion reserve (all forests)",
       f"{color(fmt_mb(in_mem_total_all), YELLOW)}  "
       f"({num_forests} forest(s) \u00d7 {in_mem_per_forest} MB)  "
       f"— RAM that must remain free during bulk loads")

    # Memory-based capacity using the detailed component breakdown
    if host_data:
        def hsum(key):
            return sum(float(v) for h in host_data if (v := h.get(key)) is not None)

        total_sys        = hsum("memory-system-total-mb")
        free_sys         = hsum("memory-system-free-mb")
        rss              = hsum("memory-process-rss-mb")
        rss_hwm          = hsum("memory-process-rss-hwm-mb")
        swap             = hsum("memory-process-swap-mb")
        cache_alloc      = hsum("memory-cache-size-mb")
        forest_mem       = hsum("memory-forest-size-mb")
        file_cache       = hsum("memory-file-size-mb")
        base_overhead    = hsum("host-size-mb")
        join_mem         = hsum("memory-join-size-mb")
        unclosed_mem     = hsum("memory-unclosed-size-mb")
        ml_limit         = hsum("memory-size-mb")
        swapin           = hsum("memory-system-swapin-rate")

        sub_header("Memory Capacity Analysis")

        # -- Current breakdown -------------------------------------------
        print()
        print(f"    {color('Current ML memory breakdown', BOLD)}")
        kv("  Cache alloc (list+tree)",    fmt_mb(cache_alloc),  indent=6)
        kv("  Forest in-memory stands",    fmt_mb(forest_mem),   indent=6)
        kv("  File cache (mmap)",          fmt_mb(file_cache),   indent=6)
        kv("  Base ML overhead",           fmt_mb(base_overhead), indent=6)
        if join_mem:
            kv("  Join workspace",         fmt_mb(join_mem),     indent=6)
        if unclosed_mem:
            kv("  Unclosed stands",        fmt_mb(unclosed_mem), indent=6)
        kv("  Total RSS (actual)",         fmt_mb(rss),          indent=6)
        kv("  RSS peak (HWM)",             fmt_mb(rss_hwm),      indent=6)

        # -- Forest memory is the piece that grows with documents --------
        # cache_alloc is configured upfront (list cache + tree cache)
        # forest_mem grows with in-memory stand activity
        # NOTE: base_overhead = host-size = total forest DISK on host (not RAM).
        # It correlates with memory pressure in containers (OS page-caches forest files)
        # but should not be treated as a fixed RAM component on production clusters.
        # So headroom for more data = headroom in forest_mem + any unallocated cache
        print()
        print(f"    {color('Headroom analysis', BOLD)}")

        # MarkLogic's configured limit is ml_limit (group memory-size setting)
        # RSS should stay under min(ml_limit, system_ram * 0.80)
        # When system RAM is unavailable (containers, just-restarted), fall
        # back to ml_limit as the ceiling.
        if total_sys > 0:
            ram_cap = total_sys * 0.80
            safe_cap = min(ml_limit, ram_cap) if ml_limit else ram_cap
        elif ml_limit > 0:
            safe_cap = ml_limit  # container or just-restarted — use ML limit
        else:
            safe_cap = 0
        headroom = safe_cap - rss
        rss_pct  = (rss / safe_cap * 100) if safe_cap else 0

        # Ingestion headroom = how much RSS slack remains after reserving
        # one in-memory stand per forest.  A snapshot taken at rest will not
        # show this memory as consumed, but it will be occupied the moment a
        # bulk load begins.  If ingestion_headroom < 0, a large load will
        # push RSS over the ceiling before the data even hits disk.
        ingestion_headroom = headroom - in_mem_total_all

        kv("  Configured ML limit",        fmt_mb(ml_limit) if ml_limit else "not set", indent=6)
        kv("  Safe capacity (80% RAM)",     fmt_mb(total_sys * 0.80), indent=6)
        kv("  Effective ceiling",           fmt_mb(safe_cap), indent=6)
        kv("  Current RSS vs ceiling",
           f"{fmt_mb(rss)}  {bar(rss_pct)}", indent=6)
        kv("  Headroom (at rest)",
           f"{fmt_mb(headroom)}  {status_badge(headroom > 1024, 'OK', 'LOW')}",
           indent=6)
        kv("  In-memory stand reserve",
           f"{color(fmt_mb(in_mem_total_all), YELLOW)}  "
           f"({num_forests} forest(s) \u00d7 {in_mem_per_forest} MB  "
           f"— occupied during bulk load)", indent=6)
        kv("  Headroom during bulk load",
           f"{fmt_mb(ingestion_headroom)}  "
           f"{status_badge(ingestion_headroom > 512, 'OK', 'LOW')}",
           indent=6)

        if ingestion_headroom < 0:
            print(f"      {color('WARNING: In-memory stand reserve exceeds available headroom!', RED)}")
            print(f"      {color('Bulk loads may push RSS over ceiling. Reduce in-memory-* settings', RED)}")
            print(f"      {color('or add RAM before running large ingestion jobs.', RED)}")
        elif ingestion_headroom < 512:
            print(f"      {color('Caution: Limited headroom during bulk loads. Monitor RSS closely.', YELLOW)}")

        if swap > 0:
            print(f"      {color('WARNING: ML is using swap (' + fmt_mb(swap) + ') — memory is over-committed!', RED)}")
        if swapin > 0:
            print(f"      {color('WARNING: System swap-in active — OS is paging memory!', RED)}")

        # -- Doc capacity estimate ---------------------------------------
        # Disk-based projection is the primary model — disk size grows linearly
        # and predictably with document count, unlike forest memory which
        # fluctuates with merge activity, cached stand pages, and query patterns.
        #
        # Forest memory (host-level memory-forest-size) is shown as a secondary
        # indicator but NOT used for the headline projection, because it includes
        # cached on-disk stand pages that get compressed by merges and don't
        # scale linearly with doc count.
        print()
        print(f"    {color('Document capacity projection', BOLD)}")
        fixed_mem = cache_alloc + base_overhead + file_cache
        forest_headroom = safe_cap - fixed_mem - forest_mem

        # During ingestion one in-memory stand per forest is held in RAM.
        # Subtract that from forest_headroom to get the headroom available
        # for *new* data while a load is in progress.
        forest_headroom_loading = forest_headroom - in_mem_total_all

        kv("  Fixed components (cache+base+file)", fmt_mb(fixed_mem),         indent=6)
        kv("  Current forest memory",              fmt_mb(forest_mem),        indent=6)
        kv("  Remaining for forest growth (at rest)",
           f"{fmt_mb(forest_headroom)}  {status_badge(forest_headroom > 512, 'OK', 'LOW')}",
           indent=6)
        kv("  Remaining for forest growth (during load)",
           f"{fmt_mb(forest_headroom_loading)}  "
           f"{color('(' + fmt_mb(in_mem_total_all) + ' reserved for in-memory stands)', DIM)}  "
           f"{status_badge(forest_headroom_loading > 256, 'OK', 'LOW')}",
           indent=6)

        # -- Fragmentation warning ---------------------------------------
        if frag_pct >= 25:
            print()
            if frag_pct >= 50:
                print(f"    {color('!' * 3, RED)} {color(f'FRAGMENTATION: {frag_pct:.0f}% deleted fragments', RED + BOLD)}")
                print(f"    {color('!' * 3, RED)} {color('Disk and memory metrics are significantly inflated.', RED)}")
                print(f"    {color('!' * 3, RED)} {color('Capacity projections below are UNRELIABLE until a merge completes.', RED + BOLD)}")
                print(f"    {color('!' * 3, RED)} {color('Force a merge, then re-run this report for accurate projections.', RED)}")
            else:
                print(f"    {color('!', YELLOW)} {color(f'Fragmentation: {frag_pct:.0f}% deleted fragments', YELLOW + BOLD)}")
                print(f"    {color('  Disk metrics may be inflated. Consider a merge for more accurate projections.', DIM)}")

        # Primary: disk-based projection
        if total_docs > 0 and total_disk > 0:
            disk_bytes_per_doc = (total_disk * 1024 * 1024) / total_docs

            # If fragmentation is high, estimate the "clean" bytes/doc.
            # Deleted fragments don't occupy space proportional to their count —
            # they share stands with active fragments, and document sizes vary.
            # Use a conservative model: assume deleted fragments occupy at most
            # half the per-fragment average. This avoids over-estimating the space
            # reclaimed by merging (validated: raw active/total ratio overpredicts
            # reclamation by ~40% in testing).
            if frag_pct >= 25 and total_frags > 0:
                avg_bytes_per_frag = (total_disk * 1024 * 1024) / total_frags
                estimated_waste = total_deleted * avg_bytes_per_frag * 0.5
                estimated_clean_disk = max(total_disk - estimated_waste / (1024 * 1024),
                                           total_disk * 0.5)  # floor: at least 50% remains
                clean_bytes_per_doc = (estimated_clean_disk * 1024 * 1024) / total_docs
            else:
                clean_bytes_per_doc = None

            print()
            print(f"    {color('Disk capacity (primary — most reliable)', BOLD)}")
            kv("  Current disk size",  fmt_mb(total_disk), indent=6)
            kv("  Disk bytes/doc",     f"{disk_bytes_per_doc:,.0f}", indent=6)

            if clean_bytes_per_doc is not None:
                kv("  Est. bytes/doc after merge",
                   f"{color(f'{clean_bytes_per_doc:,.0f}', BOLD)}  "
                   f"{color(f'({frag_pct:.0f}% fragmentation, ~{estimated_clean_disk:.0f} MB estimated post-merge)', DIM)}",
                   indent=6)

            if remaining_disk_mb > 0:
                # Use clean estimate if fragmentation is high
                proj_bpd = clean_bytes_per_doc if clean_bytes_per_doc else disk_bytes_per_doc
                disk_docs_remaining = int((remaining_disk_mb * 1024 * 1024) / proj_bpd)
                disk_total = total_docs + disk_docs_remaining
                kv("  Disk remaining",     fmt_mb(remaining_disk_mb), indent=6)
                kv("  Est. additional documents until disk full",
                   f"{color(f'{disk_docs_remaining:,}', BOLD)}"
                   + (f"  {color('(adjusted for fragmentation)', DIM)}" if clean_bytes_per_doc else ""),
                   indent=6)
                kv("  Est. total documents at disk full",
                   f"{disk_total:,}", indent=6)

        # Secondary: forest memory indicator (shown for context, not for projection)
        if total_docs > 0 and forest_mem > 0:
            forest_bytes_per_doc = (forest_mem * 1024 * 1024) / total_docs
            print()
            print(f"    {color('Memory runway (use --trend for growth-rate projection)', BOLD)}")
            kv("  Current forest memory",              fmt_mb(forest_mem),            indent=6)
            kv("  Forest headroom (at rest)",          fmt_mb(forest_headroom),        indent=6)
            kv("  Forest headroom (during load)",      fmt_mb(forest_headroom_loading),indent=6)
            kv("  In-memory stand reserve",
               f"{fmt_mb(in_mem_total_all)}  (tunable via in-memory-* DB settings)",  indent=6)
            kv("  Forest memory/doc (snapshot)",       f"{forest_bytes_per_doc:,.0f} bytes", indent=6)
            print()
            print(f"      {color('Memory is the binding constraint in most deployments.', YELLOW)}")
            print(f"      {color('Forest memory fluctuates with merges — point-in-time snapshots', DIM)}")
            print(f"      {color('can over- or under-estimate. For reliable memory runway:', DIM)}")
            print(f"      {color('take snapshots over time, then run: --trend', BOLD)}")

        elif total_docs == 0:
            kv("  No documents loaded yet", "load docs to establish per-doc baseline", indent=6)

    sub_header("Stand-Based Limits")
    MAX_STANDS = 64
    for f in forest_data:
        name = f.get("forest-name", "?")
        stands = f.get("stand-count", 0) or 0
        remaining_stands = MAX_STANDS - stands
        # Each stand flush happens at in-memory-limit fragments
        # So remaining ingestion bursts before hitting stand limit:
        remaining_flushes = remaining_stands
        docs_per_flush = in_mem_limit_kb  # roughly 1 fragment per KB, conservative
        kv(f"Forest '{name}' stands remaining",
           f"{remaining_stands} / {MAX_STANDS}  {status_badge(remaining_stands > 10, 'OK', 'MERGE NEEDED')}")
        if remaining_stands < 10:
            print(f"      {color('  Merges must keep up with ingestion to avoid stand exhaustion', YELLOW)}")

    # Final summary
    sub_header("Scaling Recommendations")

    issues = []
    if frags_pct_used > 70:
        issues.append(("Add forests", f"Fragment utilization at {frags_pct_used:.0f}% - add forests to increase fragment capacity"))

    for f in forest_data:
        stands = f.get("stand-count", 0) or 0
        if stands > 48:
            issues.append(("Merge pressure", f"Forest '{f.get('forest-name')}' has {stands}/64 stands"))
        deleted = f.get("deleted-fragment-count", 0) or 0
        active = f.get("active-fragment-count", 0) or 0
        if active > 0 and deleted > 0:
            f_ratio = deleted / (active + deleted)
            f_name = f.get("forest-name", "?")
            if f_ratio > 0.50:
                issues.append(("Force merge needed",
                    f"Forest '{f_name}' at {f_ratio*100:.0f}% deleted fragments — "
                    f"projections unreliable, run: xdmp:merge(xdmp:database-forests(xdmp:database(\"{f_name}\")))"))
            elif f_ratio > 0.25:
                issues.append(("High fragmentation",
                    f"Forest '{f_name}' at {f_ratio*100:.0f}% deleted fragments — "
                    f"consider forcing a merge to reclaim space"))

    if host_data:
        total_sys = sum(float(v) for h in host_data if (v := h.get("memory-system-total-mb")) is not None)
        ml_rss = sum(float(v) for h in host_data if (v := h.get("memory-process-rss-mb")) is not None)
        if total_sys > 0 and ml_rss / total_sys > 0.7:
            issues.append(("Memory pressure", f"ML process using {ml_rss/total_sys*100:.0f}% of system memory"))

    if not issues:
        print(f"    {color('No immediate scaling concerns detected.', GREEN)}")
        print(f"    Cluster has capacity for significant additional document loading.")
    else:
        for title, detail in issues:
            print(f"    {color('!', RED)} {color(title, BOLD)}: {detail}")
