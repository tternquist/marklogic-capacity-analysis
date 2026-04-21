"""Typed shape of the canonical MLCA snapshot dict.

The snapshot is the central data contract — it flows from
:func:`ml_capacity.snapshot.collect_snapshot` into the reporter, Prometheus
exporter, trend analyzer, and HTTP/JSON API. This module documents the keys
with ``TypedDict`` so editors can autocomplete and type checkers can catch
typos.

These are structural hints only; they are *not* enforced at runtime. All keys
are treated as optional (``total=False``) because historic snapshot files may
omit newer fields, and partial snapshots appear on eval-disabled clusters.

Key naming note: outer-layer keys in ``ClusterInfo``, ``HostEntry``, and
``ForestEntry`` use hyphens because they mirror the shape returned by the
MarkLogic Management REST API. Derived ``Totals`` fields use underscores.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, TypedDict


class ClusterInfo(TypedDict, total=False):
    name: Optional[str]
    version: Optional[str]
    hosts: int
    databases: int
    forests: int
    servers: int


class HostEntry(TypedDict, total=False):
    hostname: str
    cpus: int
    cores: int
    # Memory keys from xdmp.hostStatus() — hyphenated, MB unless noted.
    memoryminussystemminustotalminusmb: float  # "memory-system-total-mb"
    # (Most keys are accessed via .get() with the literal string; see
    # :func:`ml_capacity.snapshot.collect_snapshot`. This struct documents the
    # shape at a coarse level without exhaustively listing every hyphenated key.)


class DatabaseStatus(TypedDict, total=False):
    state: Optional[str]
    forests_count: int
    data_size_mb: float
    device_space_mb: float
    in_memory_size_mb: float
    large_data_size_mb: float
    least_remaining_mb: float
    merge_count: int
    list_cache_ratio: float


class ForestEntry(TypedDict, total=False):
    """Per-forest counts as returned by the forest-counts eval payload."""
    # Keys are hyphenated from MarkLogic's forest-status API.


class DbProperties(TypedDict, total=False):
    in_memory_limit: int
    in_memory_list_size: int
    in_memory_tree_size: int
    in_memory_range_index_size: int
    in_memory_reverse_index_size: int
    in_memory_triple_index_size: int
    preload_mapped_data: bool


class IndexCounts(TypedDict, total=False):
    range_element: int
    range_path: int
    range_field: int
    enabled_boolean_indexes: int


class Totals(TypedDict, total=False):
    documents: int
    active_fragments: int
    deleted_fragments: int
    forest_disk_mb: float
    forest_memory_mb: float
    host_forest_mb: float
    host_cache_mb: float
    host_rss_mb: float
    host_base_mb: float
    host_file_mb: float
    ml_limit_mb: float
    system_total_mb: float
    system_free_mb: float


class Snapshot(TypedDict, total=False):
    """Canonical MLCA snapshot dict produced by :func:`collect_snapshot`.

    Consumed by the reporter, Prometheus/OTLP exporters, trend analyzer, and
    HTTP JSON API. Persisted as ``{timestamp}_{database}.json`` files in
    ``.ml-capacity/``.
    """
    version: int
    timestamp: str
    database: str
    cluster: ClusterInfo
    hosts: List[HostEntry]
    database_status: DatabaseStatus
    forests: List[ForestEntry]
    db_properties: DbProperties
    index_counts: IndexCounts
    index_memory: Optional[Dict[str, Any]]  # {"indexes": [...], "standSummaries": [...]}
    totals: Totals
    # Added by load_snapshots() to record source file; absent on fresh snapshots.
    _file: str
