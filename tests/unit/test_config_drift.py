"""Unit tests for config fingerprinting, drift detection, and index diffing."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import ml_capacity as mc
from tests.conftest import _make_snapshot


class TestExtractConfigFingerprint:
    def test_basic_fields(self, sample_snapshot):
        fp = mc.extract_config_fingerprint(sample_snapshot)
        assert fp["ml_version"] == "12.0-1"
        assert fp["host_count"] == 1
        assert fp["forest_count"] == 3
        assert fp["in_memory_list_size"] == 64
        assert fp["range_element_indexes"] == 3

    def test_host_config_sorted(self):
        snap = _make_snapshot(hosts=[
            {"hostname": "z-host", "memory-system-total-mb": 8192,
             "memory-size-mb": 6144, "memory-cache-size-mb": 1024, "cores": 4},
            {"hostname": "a-host", "memory-system-total-mb": 4096,
             "memory-size-mb": 3072, "memory-cache-size-mb": 512, "cores": 2},
        ])
        fp = mc.extract_config_fingerprint(snap)
        assert fp["hosts"][0]["hostname"] == "a-host"
        assert fp["hosts"][1]["hostname"] == "z-host"

    def test_empty_hosts(self):
        snap = _make_snapshot(hosts=[])
        fp = mc.extract_config_fingerprint(snap)
        assert fp["hosts"] == []


class TestValuesMatch:
    def test_exact_equal(self):
        assert mc._values_match(42, 42) is True

    def test_none_always_matches(self):
        assert mc._values_match(None, 42) is True
        assert mc._values_match(42, None) is True

    def test_zero_always_matches(self):
        assert mc._values_match(0, 42) is True

    def test_different_values_no_tolerance(self):
        assert mc._values_match(3, 5, "range_element_indexes") is False

    def test_fuzzy_mb_within_tolerance(self):
        assert mc._values_match(5120, 5121, "cache_alloc_mb") is True

    def test_fuzzy_mb_outside_tolerance(self):
        assert mc._values_match(5120, 5200, "cache_alloc_mb") is False

    def test_non_fuzzy_field_exact(self):
        assert mc._values_match(3, 4, "forest_count") is False


class TestCheckConfigDrift:
    def test_no_drift(self):
        snaps = [_make_snapshot(), _make_snapshot()]
        assert mc.check_config_drift(snaps) == []

    def test_single_snapshot(self):
        assert mc.check_config_drift([_make_snapshot()]) == []

    def test_empty_list(self):
        assert mc.check_config_drift([]) == []

    def test_version_drift(self):
        snap1 = _make_snapshot(cluster={"name": "c", "version": "11.0-1", "hosts": 1})
        snap2 = _make_snapshot(cluster={"name": "c", "version": "12.0-1", "hosts": 1})
        drift = mc.check_config_drift([snap1, snap2])
        fields = [d[0] for d in drift]
        assert "ml_version" in fields

    def test_host_count_drift(self):
        snap1 = _make_snapshot(cluster={"name": "c", "version": "12.0-1", "hosts": 1})
        snap2 = _make_snapshot(
            cluster={"name": "c", "version": "12.0-1", "hosts": 2},
            hosts=[
                {"hostname": "h1", "memory-system-total-mb": 8192,
                 "memory-size-mb": 6144, "memory-cache-size-mb": 1024, "cores": 4},
                {"hostname": "h2", "memory-system-total-mb": 8192,
                 "memory-size-mb": 6144, "memory-cache-size-mb": 1024, "cores": 4},
            ],
        )
        drift = mc.check_config_drift([snap1, snap2])
        fields = [d[0] for d in drift]
        assert any("host" in f for f in fields)

    def test_index_count_drift(self):
        snap1 = _make_snapshot(index_counts={
            "range_element": 3, "range_path": 1, "range_field": 0,
            "enabled_boolean_indexes": 5,
        })
        snap2 = _make_snapshot(index_counts={
            "range_element": 5, "range_path": 1, "range_field": 0,
            "enabled_boolean_indexes": 5,
        })
        drift = mc.check_config_drift([snap1, snap2])
        fields = [d[0] for d in drift]
        assert "range_element_indexes" in fields

    def test_fuzzy_cache_tolerance(self):
        snap1 = _make_snapshot(hosts=[{
            "hostname": "h1", "memory-system-total-mb": 8192,
            "memory-size-mb": 6144, "memory-cache-size-mb": 5120, "cores": 4,
        }])
        snap2 = _make_snapshot(hosts=[{
            "hostname": "h1", "memory-system-total-mb": 8192,
            "memory-size-mb": 6144, "memory-cache-size-mb": 5121, "cores": 4,
        }])
        drift = mc.check_config_drift([snap1, snap2])
        assert drift == []  # within tolerance


class TestDiffIndexMemory:
    def test_basic_diff(self, snapshot_pair):
        old, new = snapshot_pair
        diff = mc.diff_index_memory(old, new)
        assert len(diff["added"]) == 1  # "value" index added
        assert len(diff["removed"]) == 0
        assert len(diff["changed"]) == 2  # severity and metric changed

    def test_summary_totals(self, snapshot_pair):
        old, new = snapshot_pair
        diff = mc.diff_index_memory(old, new)
        s = diff["summary"]
        assert s["doc_delta"] == 100000
        assert s["total_mem_delta"] > 0
        assert s["total_disk_delta"] > 0

    def test_removed_index(self):
        old = _make_snapshot(index_memory={
            "indexes": [
                {"indexType": "range", "localname": "foo", "scalarType": "string",
                 "totalMemoryBytes": 100, "totalOnDiskBytes": 50},
            ],
            "standSummaries": [],
        })
        new = _make_snapshot(index_memory={
            "indexes": [],
            "standSummaries": [],
        })
        diff = mc.diff_index_memory(old, new)
        assert len(diff["removed"]) == 1
        assert len(diff["added"]) == 0

    def test_no_index_memory(self):
        old = _make_snapshot(index_memory=None)
        new = _make_snapshot(index_memory=None)
        diff = mc.diff_index_memory(old, new)
        assert diff["added"] == []
        assert diff["removed"] == []
        assert diff["changed"] == []

    def test_unchanged_indexes(self):
        idx = {"indexes": [
            {"indexType": "range", "localname": "x", "scalarType": "int",
             "totalMemoryBytes": 1000, "totalOnDiskBytes": 500},
        ], "standSummaries": []}
        old = _make_snapshot(index_memory=idx)
        new = _make_snapshot(index_memory=idx)
        diff = mc.diff_index_memory(old, new)
        assert diff["changed"] == []
