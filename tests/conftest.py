"""Shared fixtures for MLCA unit and API tests."""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest


def _make_snapshot(database="Documents", docs=100000, disk_mb=500, mem_mb=200,
                   active_frags=100000, deleted_frags=5000, timestamp=None,
                   hosts=None, index_memory=None, db_properties=None,
                   index_counts=None, cluster=None, database_status=None):
    """Build a realistic snapshot dict for testing."""
    if timestamp is None:
        timestamp = "2026-04-10T10:00:00+00:00"

    if hosts is None:
        hosts = [{
            "hostname": "ml-host-1",
            "memory-process-rss-mb": 3200,
            "memory-process-rss-hwm-mb": 3400,
            "memory-cache-size-mb": 1024,
            "memory-forest-size-mb": mem_mb,
            "host-size-mb": disk_mb,
            "memory-file-size-mb": 150,
            "memory-process-swap-mb": 0,
            "memory-system-total-mb": 8192,
            "memory-system-free-mb": 2048,
            "memory-size-mb": 6144,
            "memory-system-pagein-rate": 0,
            "memory-system-pageout-rate": 0,
            "memory-system-swapin-rate": 0,
            "cores": 4,
        }]

    if db_properties is None:
        db_properties = {
            "in_memory_limit": 32768,
            "in_memory_list_size": 64,
            "in_memory_tree_size": 16,
            "in_memory_range_index_size": 2,
            "in_memory_reverse_index_size": 2,
            "in_memory_triple_index_size": 16,
            "preload_mapped_data": False,
        }

    if index_counts is None:
        index_counts = {
            "range_element": 3,
            "range_path": 1,
            "range_field": 0,
            "enabled_boolean_indexes": 5,
        }

    if cluster is None:
        cluster = {
            "name": "test-cluster",
            "version": "12.0-1",
            "hosts": 1,
            "databases": 4,
            "forests": 3,
        }

    if database_status is None:
        database_status = {
            "state": "open",
            "forests_count": 3,
            "data_size_mb": disk_mb,
            "device_space_mb": 50000.0,
            "in_memory_size_mb": mem_mb,
            "large_data_size_mb": 0,
            "least_remaining_mb": 45000.0,
            "merge_count": 0,
            "list_cache_ratio": 95.0,
        }

    return {
        "version": 1,
        "timestamp": timestamp,
        "database": database,
        "cluster": cluster,
        "hosts": hosts,
        "database_status": database_status,
        "forests": [
            {
                "forest-name": "Documents-1",
                "document-count": docs,
                "active-fragment-count": active_frags,
                "deleted-fragment-count": deleted_frags,
                "nascent-fragment-count": 0,
                "stand-count": 4,
                "disk-size-mb": disk_mb,
                "memory-size-mb": mem_mb,
            }
        ],
        "db_properties": db_properties,
        "index_counts": index_counts,
        "index_memory": index_memory,
        "totals": {
            "documents": docs,
            "active_fragments": active_frags,
            "deleted_fragments": deleted_frags,
            "forest_disk_mb": disk_mb,
            "forest_memory_mb": mem_mb,
            "host_rss_mb": 3200,
            "host_cache_mb": 1024,
            "host_forest_mb": mem_mb,
            "host_base_mb": disk_mb,
            "host_file_mb": 150,
            "system_total_mb": 8192,
        },
    }


@pytest.fixture
def sample_snapshot():
    """A single realistic snapshot."""
    return _make_snapshot()


@pytest.fixture
def snapshot_pair():
    """Two snapshots with growth between them for trend/diff testing."""
    old = _make_snapshot(
        docs=100000, disk_mb=500, mem_mb=200,
        active_frags=100000, deleted_frags=5000,
        timestamp="2026-04-08T10:00:00+00:00",
        index_memory={
            "indexes": [
                {"indexType": "range", "localname": "severity", "scalarType": "int",
                 "totalMemoryBytes": 1048576, "totalOnDiskBytes": 524288},
                {"indexType": "range", "localname": "metric", "scalarType": "string",
                 "totalMemoryBytes": 2097152, "totalOnDiskBytes": 1048576},
            ],
            "standSummaries": [],
        },
    )
    new = _make_snapshot(
        docs=200000, disk_mb=1000, mem_mb=400,
        active_frags=200000, deleted_frags=8000,
        timestamp="2026-04-10T10:00:00+00:00",
        index_memory={
            "indexes": [
                {"indexType": "range", "localname": "severity", "scalarType": "int",
                 "totalMemoryBytes": 2097152, "totalOnDiskBytes": 1048576},
                {"indexType": "range", "localname": "metric", "scalarType": "string",
                 "totalMemoryBytes": 4194304, "totalOnDiskBytes": 2097152},
                {"indexType": "range", "localname": "value", "scalarType": "double",
                 "totalMemoryBytes": 1048576, "totalOnDiskBytes": 524288},
            ],
            "standSummaries": [],
        },
    )
    return old, new


@pytest.fixture
def tmp_snapshot_dir(tmp_path):
    """Create a temp snapshot dir with some snapshot files.

    Timestamps are relative to now so that prune/retention tests are
    deterministic regardless of when they run: two snapshots older than
    one day and one recent snapshot.
    """
    import ml_capacity.snapshot as snap_mod
    original_dir = snap_mod.SNAPSHOT_DIR
    snap_mod.SNAPSHOT_DIR = tmp_path

    now = datetime.now(timezone.utc)
    for offset, docs in [
        (timedelta(days=3), 100000),
        (timedelta(days=2), 150000),
        (timedelta(minutes=10), 200000),
    ]:
        t = now - offset
        snap = _make_snapshot(docs=docs, timestamp=t.isoformat())
        fname = f"{t.strftime('%Y%m%dT%H%M%S')}_Documents.json"
        with open(tmp_path / fname, "w") as f:
            json.dump(snap, f)

    yield tmp_path

    snap_mod.SNAPSHOT_DIR = original_dir
