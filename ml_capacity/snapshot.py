import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

from ml_capacity.formatting import color, header, sub_header, kv, fmt_mb, BOLD, DIM
from ml_capacity.collect import (
    collect_cluster_overview, collect_database_status,
    collect_database_properties, collect_host_memory,
    collect_forest_counts, _INDEX_MEMORY_JS,
)

log = logging.getLogger("mlca")

SNAPSHOT_DIR = Path(__file__).parent.parent / ".ml-capacity"


def collect_snapshot(client, database):
    """Gather all capacity metrics into a single JSON-serializable dict.

    This is the canonical snapshot format — everything the report sections
    display, collected once, so we can both print and persist from the
    same data.
    """
    snap = {
        "version": 1,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "database": database,
    }

    # Cluster
    cluster_raw = collect_cluster_overview(client)
    cluster = cluster_raw.get("local-cluster-default", {})
    snap["cluster"] = {
        "name": cluster.get("name"),
        "version": cluster.get("version"),
    }
    relations = cluster.get("relations", {}).get("relation-group", [])
    for rg in relations:
        t = rg.get("typeref", "")
        if t in ("hosts", "databases", "forests", "servers"):
            snap["cluster"][t] = rg.get("relation-count", {}).get("value", 0)

    # Host memory (via eval)
    try:
        host_results = collect_host_memory(client)
        if host_results:
            raw = host_results[0] if isinstance(host_results[0], list) else [host_results[0]]
            snap["hosts"] = raw
        else:
            snap["hosts"] = []
    except Exception as e:
        log.warning("Host memory collection failed (requires ML_ALLOW_EVAL=true): %s", e)
        snap["hosts"] = []

    # Database status
    db_raw = collect_database_status(client, database)
    props = db_raw.get("database-status", {}).get("status-properties", {})
    snap["database_status"] = {
        "state":           props.get("state", {}).get("value"),
        "forests_count":   props.get("forests-count", {}).get("value", 0),
        "data_size_mb":    props.get("data-size", {}).get("value", 0),
        "device_space_mb": float(props.get("device-space", {}).get("value", "0")),
        "in_memory_size_mb": props.get("in-memory-size", {}).get("value", 0),
        "large_data_size_mb": props.get("large-data-size", {}).get("value", 0),
        "least_remaining_mb": float(props.get("least-remaining-space-forest", {}).get("value", 0)),
        "merge_count":     props.get("merge-count", {}).get("value", 0),
        "list_cache_ratio": props.get("cache-properties", {}).get("list-cache-ratio", {}).get("value", 0),
    }

    # Forest health (via eval)
    try:
        fc_results = collect_forest_counts(client, database)
        if fc_results:
            forests = fc_results[0] if isinstance(fc_results[0], list) else [fc_results[0]]
            snap["forests"] = forests
        else:
            snap["forests"] = []
    except Exception as e:
        log.warning("Forest counts collection failed: %s", e)
        snap["forests"] = []

    # Database properties (for index config)
    db_props = collect_database_properties(client, database)
    snap["db_properties"] = {
        "in_memory_limit":            db_props.get("in-memory-limit", 32768),
        "in_memory_list_size":        db_props.get("in-memory-list-size", 64),
        "in_memory_tree_size":        db_props.get("in-memory-tree-size", 16),
        "in_memory_range_index_size": db_props.get("in-memory-range-index-size", 2),
        "in_memory_reverse_index_size": db_props.get("in-memory-reverse-index-size", 2),
        "in_memory_triple_index_size":  db_props.get("in-memory-triple-index-size", 16),
        "preload_mapped_data":        db_props.get("preload-mapped-data", False),
    }

    # Count indexes
    range_el = db_props.get("range-element-index", [])
    range_path = db_props.get("range-path-index", [])
    range_field = db_props.get("range-field-index", [])
    enabled_bools = sum(1 for k in [
        "word-searches", "fast-phrase-searches", "triple-index",
        "fast-case-sensitive-searches", "fast-diacritic-sensitive-searches",
        "fast-element-word-searches", "fast-element-phrase-searches",
        "uri-lexicon", "collection-lexicon", "trailing-wildcard-searches",
        "three-character-searches", "field-value-searches",
    ] if db_props.get(k) is True or db_props.get(k) == "true")
    snap["index_counts"] = {
        "range_element": len(range_el),
        "range_path":    len(range_path),
        "range_field":   len(range_field),
        "enabled_boolean_indexes": enabled_bools,
    }

    # Index memory (via eval — ML 11+)
    try:
        idx_results = client.eval_javascript(_INDEX_MEMORY_JS, database=database,
                                             vars={"dbName": database})
        if idx_results:
            snap["index_memory"] = idx_results[0]
        else:
            snap["index_memory"] = None
    except Exception as e:
        log.warning("Index memory collection failed (requires ML 11+): %s", e)
        snap["index_memory"] = None

    # Derived totals for easy trending
    total_docs    = sum(f.get("document-count", 0) or 0 for f in snap["forests"])
    total_active  = sum(f.get("active-fragment-count", 0) or 0 for f in snap["forests"])
    total_deleted = sum(f.get("deleted-fragment-count", 0) or 0 for f in snap["forests"])
    total_disk    = sum(f.get("disk-size-mb", 0) or 0 for f in snap["forests"])
    total_mem     = sum(f.get("memory-size-mb", 0) or 0 for f in snap["forests"])

    def hsum(key):
        return sum(float(v) for h in snap["hosts"] if (v := h.get(key)) is not None)

    snap["totals"] = {
        "documents":        total_docs,
        "active_fragments": total_active,
        "deleted_fragments": total_deleted,
        "forest_disk_mb":   total_disk,
        "forest_memory_mb": total_mem,
        "host_forest_mb":   hsum("memory-forest-size-mb"),
        "host_cache_mb":    hsum("memory-cache-size-mb"),
        "host_rss_mb":      hsum("memory-process-rss-mb"),
        "host_base_mb":     hsum("host-size-mb"),
        "host_file_mb":     hsum("memory-file-size-mb"),
        "ml_limit_mb":      hsum("memory-size-mb"),
        "system_total_mb":  hsum("memory-system-total-mb"),
        "system_free_mb":   hsum("memory-system-free-mb"),
    }

    # In containers, system_total_mb may be 0 (cgroup doesn't expose it).
    # Fall back to the ML configured limit (memory-size) as the ceiling.
    if snap["totals"]["system_total_mb"] == 0 and snap["totals"]["ml_limit_mb"] > 0:
        snap["totals"]["system_total_mb"] = snap["totals"]["ml_limit_mb"]

    return snap


def save_snapshot(snap):
    """Save a snapshot to .ml-capacity/ as a timestamped JSON file."""
    SNAPSHOT_DIR.mkdir(exist_ok=True)
    ts = snap["timestamp"].replace(":", "").replace("-", "")[:15]
    db = snap["database"]
    filename = f"{ts}_{db}.json"
    path = SNAPSHOT_DIR / filename
    with open(path, "w") as f:
        json.dump(snap, f, indent=2, default=str)
    return path


def prune_snapshots(retention_days):
    """Delete snapshot files older than retention_days. 0 means keep all."""
    if retention_days <= 0 or not SNAPSHOT_DIR.exists():
        return 0
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    removed = 0
    for p in sorted(SNAPSHOT_DIR.glob("*.json")):
        try:
            # Parse timestamp from filename: YYYYMMDDTHHMMSS_DB.json
            ts_str = p.stem.split("_")[0]
            ts = datetime.strptime(ts_str, "%Y%m%dT%H%M%S").replace(
                tzinfo=timezone.utc)
            if ts < cutoff:
                p.unlink()
                removed += 1
        except (ValueError, IndexError, OSError):
            continue
    return removed


def load_snapshots(database=None):
    """Load all snapshots, optionally filtered by database. Returns sorted by timestamp."""
    if not SNAPSHOT_DIR.exists():
        return []
    snaps = []
    for p in sorted(SNAPSHOT_DIR.glob("*.json")):
        try:
            with open(p) as f:
                s = json.load(f)
            if database and s.get("database") != database:
                continue
            s["_file"] = p.name
            snaps.append(s)
        except (json.JSONDecodeError, KeyError):
            continue
    return snaps


def import_snapshots(file_paths):
    """Import snapshot JSON files from disconnected environments.

    Validates the snapshot structure, then saves each file into the
    .ml-capacity/ directory using the standard naming convention.
    Returns the number of successfully imported snapshots.
    """
    from ml_capacity.validation import validate_database_name
    from ml_capacity.formatting import YELLOW, GREEN

    REQUIRED_KEYS = {"version", "timestamp", "database", "hosts", "forests", "totals"}
    imported = 0

    for fpath in file_paths:
        p = Path(fpath)
        if not p.exists():
            print(f"    {color('SKIP', YELLOW)}: file not found: {fpath}")
            continue
        try:
            with open(p) as f:
                snap = json.load(f)
        except json.JSONDecodeError as e:
            print(f"    {color('SKIP', YELLOW)}: invalid JSON in {p.name}: {e}")
            continue

        missing = REQUIRED_KEYS - set(snap.keys())
        if missing:
            print(f"    {color('SKIP', YELLOW)}: {p.name} missing required keys: {', '.join(sorted(missing))}")
            continue

        if snap.get("version", 0) != 1:
            print(f"    {color('SKIP', YELLOW)}: {p.name} has unsupported version {snap.get('version')}")
            continue

        db = snap.get("database", "")
        try:
            validate_database_name(db)
        except ValueError:
            print(f"    {color('SKIP', YELLOW)}: {p.name} has invalid database name '{db}'")
            continue

        saved_path = save_snapshot(snap)
        print(f"    {color('OK', GREEN)}: {p.name} → {saved_path.name}")
        imported += 1

    return imported


def list_snapshots(database=None):
    """Print a table of saved snapshots."""
    snaps = load_snapshots(database)
    if not snaps:
        print("    No snapshots found.")
        if database:
            print(f"    (filtered to database '{database}')")
        return

    print()
    hdr = f"  {'#':>4}  {'Timestamp':24}  {'Database':16}  {'Documents':>12}  {'Forest Disk':>12}  {'RSS':>10}"
    print(color(hdr, BOLD))
    print(color("  " + "-" * (len(hdr) - 2), DIM))

    for i, s in enumerate(snaps):
        t = s.get("totals", {})
        ts = s.get("timestamp", "?")[:19].replace("T", " ")
        db = s.get("database", "?")
        docs = t.get("documents", 0)
        disk = t.get("forest_disk_mb", 0)
        rss  = t.get("host_rss_mb", 0)
        print(
            f"  {i:>4}  {ts:24}  {db:16}  {docs:>12,}  "
            f"{fmt_mb(disk):>12}  {fmt_mb(rss):>10}"
        )
    print()
    return snaps
