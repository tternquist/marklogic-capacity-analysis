from ml_capacity.formatting import color, sub_header, GREEN, RED, BOLD, DIM, YELLOW


def extract_config_fingerprint(snap):
    """Extract the configuration fields that should remain constant.

    If any of these change between snapshots, trend analysis is unreliable
    because the baseline shifted — e.g. adding RAM, changing cache sizes,
    adding range indexes, or adding hosts all change what "growth" means.
    """
    cluster = snap.get("cluster", {})
    db_props = snap.get("db_properties", {})
    idx_counts = snap.get("index_counts", {})
    db_status = snap.get("database_status", {})
    hosts = snap.get("hosts", [])

    # Per-host config (sorted by hostname for stable comparison)
    host_configs = []
    for h in sorted(hosts, key=lambda x: x.get("hostname", "")):
        host_configs.append({
            "hostname":       h.get("hostname"),
            "system_ram_mb":  h.get("memory-system-total-mb"),
            "ml_limit_mb":    h.get("memory-size-mb"),
            "cache_alloc_mb": h.get("memory-cache-size-mb"),
            "cores":          h.get("cores"),
        })

    return {
        "ml_version":       cluster.get("version"),
        "host_count":       cluster.get("hosts", 0),
        "forest_count":     db_status.get("forests_count", 0),
        "hosts":            host_configs,
        "in_memory_limit":            db_props.get("in_memory_limit"),
        "in_memory_list_size":        db_props.get("in_memory_list_size"),
        "in_memory_tree_size":        db_props.get("in_memory_tree_size"),
        "in_memory_range_index_size": db_props.get("in_memory_range_index_size"),
        "in_memory_reverse_index_size": db_props.get("in_memory_reverse_index_size"),
        "in_memory_triple_index_size":  db_props.get("in_memory_triple_index_size"),
        "preload_mapped_data":        db_props.get("preload_mapped_data"),
        "range_element_indexes": idx_counts.get("range_element", 0),
        "range_path_indexes":    idx_counts.get("range_path", 0),
        "range_field_indexes":   idx_counts.get("range_field", 0),
        "enabled_boolean_indexes": idx_counts.get("enabled_boolean_indexes", 0),
    }


# Fields reported by the Management API in MB that can wobble +/-1
# due to internal rounding (e.g. memory-cache-size reported as 5121
# one call and 5120 the next despite no config change).
_FUZZY_MB_FIELDS = frozenset({
    "system_ram_mb", "ml_limit_mb", "cache_alloc_mb",
})


def _values_match(a, b, field_name=""):
    """Compare two config values, with tolerance for known noisy MB fields.

    Only fields in _FUZZY_MB_FIELDS get tolerance (0.5% or 2 MB).
    All other fields (counts, strings, versions) use exact equality
    so that e.g. an index count changing from 3 to 5 is always caught.

    None/0 values are treated as "unknown" and always match — this
    handles container environments where some OS metrics aren't available
    in early snapshots but get populated later via fallback.
    """
    if a == b:
        return True
    # Treat None or 0 as "unknown" — don't flag drift for missing data.
    # Trade-off: won't detect removal of ALL range indexes (count→0), but
    # that's extremely rare and the alternative (false alarms in containers
    # where OS metrics start as 0/None) is worse.
    if a is None or b is None or a == 0 or b == 0:
        return True
    if field_name in _FUZZY_MB_FIELDS and isinstance(a, (int, float)) and isinstance(b, (int, float)):
        abs_tol = max(2, abs(a) * 0.005)  # 0.5% or 2 MB, whichever is larger
        return abs(a - b) <= abs_tol
    return False


def check_config_drift(snaps):
    """Compare configuration fingerprints across snapshots.

    Returns a list of (field, old_value, new_value, snap_index) tuples
    for any configuration changes detected. An empty list means all
    snapshots share the same configuration — safe for trending.
    """
    if len(snaps) < 2:
        return []

    baseline = extract_config_fingerprint(snaps[0])
    drift = []

    for i, snap in enumerate(snaps[1:], start=1):
        current = extract_config_fingerprint(snap)

        # Compare all top-level keys except 'hosts' (handled separately)
        for key in baseline:
            if key == "hosts":
                continue
            if not _values_match(baseline[key], current.get(key), key):
                drift.append((key, baseline[key], current.get(key), i))

        # Compare per-host config
        if len(baseline.get("hosts", [])) != len(current.get("hosts", [])):
            drift.append(("host_configs",
                          f"{len(baseline.get('hosts', []))} host(s)",
                          f"{len(current.get('hosts', []))} host(s)", i))
        else:
            for bh, ch in zip(baseline.get("hosts", []), current.get("hosts", [])):
                for hk in bh:
                    if not _values_match(bh[hk], ch.get(hk), hk):
                        host = bh.get("hostname", "?")
                        drift.append((f"host[{host}].{hk}", bh[hk], ch.get(hk), i))

    return drift


def report_config_drift(snaps):
    """Check and report configuration drift across snapshots.

    Returns True if the configuration is stable (safe for trending),
    False if drift was detected.
    """
    drift = check_config_drift(snaps)

    sub_header("Configuration Stability Check")

    if not drift:
        print(f"    {color('STABLE', GREEN)} — cluster config, cache sizes, index settings,")
        print(f"    and system resources are consistent across all {len(snaps)} snapshots.")
        return True

    # Deduplicate: only show the first occurrence of each field change
    seen = set()
    unique_drift = []
    for field, old_val, new_val, snap_idx in drift:
        key = (field, str(old_val), str(new_val))
        if key not in seen:
            seen.add(key)
            unique_drift.append((field, old_val, new_val, snap_idx))

    print(f"    {color('DRIFT DETECTED', RED + BOLD)} — configuration changed between snapshots.")
    print(f"    Trend projections may be unreliable across these changes.")
    print()

    field_descriptions = {
        "ml_version":                "MarkLogic version",
        "host_count":                "Number of hosts in cluster",
        "forest_count":              "Number of forests on database",
        "host_configs":              "Host topology",
        "in_memory_limit":              "In-memory stand flush threshold (KB)",
        "in_memory_list_size":          "In-memory list size (MB)",
        "in_memory_tree_size":          "In-memory tree size (MB)",
        "in_memory_range_index_size":   "In-memory range index size (MB)",
        "in_memory_reverse_index_size": "In-memory reverse index size (MB)",
        "in_memory_triple_index_size":  "In-memory triple index size (MB)",
        "preload_mapped_data":       "Preload mapped data (range indexes loaded on forest open)",
        "range_element_indexes":     "Element range index count",
        "range_path_indexes":        "Path range index count",
        "range_field_indexes":       "Field range index count",
        "enabled_boolean_indexes":   "Enabled boolean index count",
    }

    for field, old_val, new_val, snap_idx in unique_drift:
        label = field_descriptions.get(field, field)
        snap_ts = snaps[snap_idx].get("timestamp", "?")[:19].replace("T", " ")
        print(f"    {color('!', RED)} {color(label, BOLD)}")
        print(f"        {old_val} -> {new_val}  {color(f'(snapshot #{snap_idx}, {snap_ts})', DIM)}")

    print()
    print(f"    {color('Tip:', YELLOW)} For accurate trends, compare snapshots with the same")
    print(f"    configuration. Use --compare N to diff specific snapshots.")
    return False
