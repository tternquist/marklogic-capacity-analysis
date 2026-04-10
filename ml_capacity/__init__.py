"""MarkLogic Capacity Analyzer — package root.

Re-exports public API for backward compatibility with code that does:
    import ml_capacity as mc
    mc.snapshot_to_prometheus(snap)
"""

from ml_capacity.formatting import (
    YELLOW, GREEN, RED, CYAN, BOLD, DIM, RESET, BAR_WIDTH,
    color, header, sub_header, kv, bar, fmt_mb, status_badge,
)
from ml_capacity.client import MarkLogicClient, HTTP_TIMEOUT
from ml_capacity.validation import validate_database_name, _SAFE_DB_NAME
from ml_capacity.collect import (
    collect_cluster_overview, collect_database_status,
    collect_database_properties, collect_forests, collect_forest_detail,
    collect_host_status, collect_host_memory, collect_forest_counts,
    _INDEX_MEMORY_JS,
)
from ml_capacity.snapshot import (
    SNAPSHOT_DIR, collect_snapshot, save_snapshot, prune_snapshots,
    load_snapshots, list_snapshots,
)
from ml_capacity.config_drift import (
    extract_config_fingerprint, _values_match, _FUZZY_MB_FIELDS,
    check_config_drift, report_config_drift,
)
from ml_capacity.index_analysis import (
    _index_key, _index_label, diff_index_memory,
    report_index_impact, wait_for_reindex,
)
from ml_capacity.report import (
    report_cluster, report_host_memory, report_database_stats,
    report_forest_health, report_index_config, report_index_memory,
    report_capacity_estimate,
)
from ml_capacity.trend import report_trend, report_compare
from ml_capacity.prometheus import snapshot_to_prometheus, push_otlp, parse_interval
from ml_capacity.service import run_service
from ml_capacity.main import main

import logging
log = logging.getLogger("mlca")
