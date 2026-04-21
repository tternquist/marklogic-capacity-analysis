import json
from datetime import datetime, timezone
from urllib.request import Request, urlopen

from ml_capacity.types import Snapshot


def snapshot_to_prometheus(snap: Snapshot) -> str:
    """Convert a snapshot to Prometheus text exposition format.

    Returns a string of Prometheus-format metrics. Works for both
    one-shot (--format prometheus) and service (/metrics endpoint).
    """
    lines = []

    def gauge(name, help_text, value, labels=None):
        if value is None:
            return
        lines.append(f"# HELP {name} {help_text}")
        lines.append(f"# TYPE {name} gauge")
        label_str = ""
        if labels:
            pairs = ",".join(f'{k}="{v}"' for k, v in labels.items())
            label_str = f"{{{pairs}}}"
        lines.append(f"{name}{label_str} {value}")

    db = snap.get("database", "unknown")
    t = snap.get("totals", {})
    db_labels = {"database": db}

    # -- Document & fragment metrics ------------------------------------
    gauge("mlca_documents_total",
          "Total document count",
          t.get("documents"), db_labels)
    gauge("mlca_fragments_active",
          "Active fragment count",
          t.get("active_fragments"), db_labels)
    gauge("mlca_fragments_deleted",
          "Deleted fragment count (fragmentation indicator)",
          t.get("deleted_fragments"), db_labels)

    active = t.get("active_fragments", 0)
    deleted = t.get("deleted_fragments", 0)
    total_frags = active + deleted
    if total_frags > 0:
        gauge("mlca_fragmentation_ratio",
              "Ratio of deleted to total fragments",
              round(deleted / total_frags, 4), db_labels)

    # -- Forest storage -------------------------------------------------
    gauge("mlca_forest_disk_mb",
          "Total on-disk size of all forests in MB",
          t.get("forest_disk_mb"), db_labels)
    gauge("mlca_forest_memory_mb",
          "Forest in-memory stand data in MB (per-db sum)",
          t.get("forest_memory_mb"), db_labels)

    # -- Host memory breakdown ------------------------------------------
    for host in snap.get("hosts", []):
        hostname = host.get("hostname", "unknown")
        h_labels = {"host": hostname}

        gauge("mlca_host_rss_mb",
              "MarkLogic process resident set size in MB",
              host.get("memory-process-rss-mb"), h_labels)
        gauge("mlca_host_rss_hwm_mb",
              "Peak RSS since last restart in MB",
              host.get("memory-process-rss-hwm-mb"), h_labels)
        gauge("mlca_host_cache_mb",
              "Allocated list + compressed tree cache in MB (fixed)",
              host.get("memory-cache-size-mb"), h_labels)
        gauge("mlca_host_forest_mb",
              "Forest in-memory stand memory at host level in MB",
              host.get("memory-forest-size-mb"), h_labels)
        gauge("mlca_host_base_mb",
              "Total forest disk on host in MB (host-size; correlates with RAM in containers)",
              host.get("host-size-mb"), h_labels)
        gauge("mlca_host_file_mb",
              "OS file cache (mmap) pages in MB",
              host.get("memory-file-size-mb"), h_labels)
        gauge("mlca_host_swap_mb",
              "ML process swap usage in MB (non-zero = memory pressure)",
              host.get("memory-process-swap-mb"), h_labels)
        gauge("mlca_system_total_mb",
              "Total system RAM in MB",
              host.get("memory-system-total-mb"), h_labels)
        gauge("mlca_system_free_mb",
              "Free system RAM in MB",
              host.get("memory-system-free-mb"), h_labels)
        gauge("mlca_ml_limit_mb",
              "Configured MarkLogic memory limit in MB",
              host.get("memory-size-mb"), h_labels)
        gauge("mlca_host_pagein_rate",
              "System page-in rate in MB/s",
              host.get("memory-system-pagein-rate"), h_labels)
        gauge("mlca_host_pageout_rate",
              "System page-out rate in MB/s",
              host.get("memory-system-pageout-rate"), h_labels)
        gauge("mlca_host_swapin_rate",
              "System swap-in rate in MB/s (non-zero = severe pressure)",
              host.get("memory-system-swapin-rate"), h_labels)

    # -- Memory capacity (computed) -------------------------------------
    sys_total = t.get("system_total_mb", 0)
    cache_mb = t.get("host_cache_mb", 0)
    base_mb = t.get("host_base_mb", 0)
    file_mb = t.get("host_file_mb", 0)
    forest_mb = t.get("host_forest_mb", 0)
    fixed = cache_mb + base_mb + file_mb

    if sys_total > 0:
        ceiling = sys_total * 0.80
        headroom = ceiling - fixed - forest_mb
        gauge("mlca_memory_ceiling_mb",
              "Memory ceiling (80% of system RAM) in MB",
              round(ceiling, 1), db_labels)
        gauge("mlca_memory_fixed_mb",
              "Fixed memory components (cache + base + file) in MB",
              round(fixed, 1), db_labels)
        gauge("mlca_memory_headroom_mb",
              "Remaining memory headroom for forest growth in MB",
              round(headroom, 1), db_labels)
        if ceiling > 0:
            gauge("mlca_memory_utilization_ratio",
                  "Memory utilization ratio (fixed + forest) / ceiling",
                  round((fixed + forest_mb) / ceiling, 4), db_labels)

    # -- Disk capacity --------------------------------------------------
    db_status = snap.get("database_status", {})
    remaining = db_status.get("least_remaining_mb", 0)
    if remaining:
        gauge("mlca_disk_remaining_mb",
              "Least remaining disk space on any forest in MB",
              float(remaining), db_labels)

    docs = t.get("documents") or 0
    disk = t.get("forest_disk_mb") or 0
    if docs > 0 and disk > 0:
        gauge("mlca_disk_bytes_per_doc",
              "Disk bytes per document",
              round(disk * 1024 * 1024 / docs, 0), db_labels)

    # -- Per-index memory (ML 11+) --------------------------------------
    idx_mem = snap.get("index_memory") or {}
    for idx in idx_mem.get("indexes", []):
        mem = idx.get("totalMemoryBytes", 0) or 0
        disk_b = idx.get("totalOnDiskBytes", 0) or 0
        name = idx.get("localname") or idx.get("pathExpression") or idx.get("indexType", "unknown")
        stype = idx.get("scalarType") or ""
        idx_label = f"{name}({stype})" if stype else name
        i_labels = {"database": db, "index": idx_label}
        gauge("mlca_index_memory_bytes",
              "Per-index memory in bytes",
              mem, i_labels)
        gauge("mlca_index_disk_bytes",
              "Per-index on-disk size in bytes",
              disk_b, i_labels)

    # -- Stand memory components ----------------------------------------
    stand_summaries = idx_mem.get("standSummaries", [])
    if stand_summaries:
        agg = {}
        for ss in stand_summaries:
            for k, v in ss.get("summary", {}).items():
                agg[k] = agg.get(k, 0) + (v or 0)
        component_metrics = {
            "rangeIndexesBytes":    ("mlca_stand_range_indexes_bytes", "Range index data in bytes"),
            "tripleIndexBytes":     ("mlca_stand_triple_index_bytes", "Triple index data in bytes"),
            "timestampsFileBytes":  ("mlca_stand_timestamps_bytes", "Timestamp data in bytes"),
            "listFileBytes":        ("mlca_stand_list_bytes", "List index data in bytes"),
            "treeFileBytes":        ("mlca_stand_tree_bytes", "Tree index data in bytes"),
        }
        for key, (metric_name, help_text) in component_metrics.items():
            if agg.get(key, 0) > 0:
                gauge(metric_name, help_text, agg[key], db_labels)

    lines.append("")  # trailing newline
    return "\n".join(lines)


def parse_interval(s):
    """Parse interval string like '5m', '15m', '1h', '30s' to seconds.

    Raises ValueError for empty strings, unknown suffixes, or missing numbers.
    """
    s = s.strip().lower()
    if not s:
        raise ValueError("Empty interval string")
    if s.endswith("s"):
        return int(s[:-1])
    if s.endswith("m"):
        return int(s[:-1]) * 60
    if s.endswith("h"):
        return int(s[:-1]) * 3600
    try:
        return int(s)
    except ValueError:
        raise ValueError(f"Invalid interval '{s}': use a number with optional s/m/h suffix")


def push_otlp(snap, endpoint):
    """Push metrics to an OpenTelemetry Collector via OTLP HTTP JSON.

    Uses stdlib only — no opentelemetry-sdk required. Sends a minimal
    OTLP JSON payload with gauge data points.
    """
    t = snap.get("totals", {})
    db = snap.get("database", "unknown")
    now_ns = int(datetime.now(timezone.utc).timestamp() * 1e9)

    def dp(name, value, unit=""):
        return {
            "name": name,
            "unit": unit,
            "gauge": {
                "dataPoints": [{
                    "asDouble": float(value) if value else 0,
                    "timeUnixNano": str(now_ns),
                    "attributes": [
                        {"key": "database", "value": {"stringValue": db}}
                    ],
                }]
            }
        }

    metrics = [
        dp("mlca.documents.total", t.get("documents", 0)),
        dp("mlca.memory.forest", t.get("host_forest_mb", 0), "MB"),
        dp("mlca.memory.rss", t.get("host_rss_mb", 0), "MB"),
        dp("mlca.memory.headroom", t.get("system_total_mb", 0) * 0.8 -
           t.get("host_cache_mb", 0) - t.get("host_base_mb", 0) -
           t.get("host_file_mb", 0) - t.get("host_forest_mb", 0), "MB"),
        dp("mlca.disk.used", t.get("forest_disk_mb", 0), "MB"),
        dp("mlca.fragments.active", t.get("active_fragments", 0)),
        dp("mlca.fragments.deleted", t.get("deleted_fragments", 0)),
    ]

    payload = {
        "resourceMetrics": [{
            "resource": {
                "attributes": [
                    {"key": "service.name", "value": {"stringValue": "mlca"}},
                ]
            },
            "scopeMetrics": [{
                "scope": {"name": "mlca"},
                "metrics": metrics,
            }]
        }]
    }

    body = json.dumps(payload).encode()
    url = endpoint.rstrip("/")
    if not url.endswith("/v1/metrics"):
        url += "/v1/metrics"
    req = Request(url, data=body, method="POST",
                  headers={"Content-Type": "application/json"})
    with urlopen(req, timeout=10) as resp:
        return resp.status
