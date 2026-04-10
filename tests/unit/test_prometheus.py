"""Unit tests for Prometheus metric formatting."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import ml_capacity as mc


class TestSnapshotToPrometheus:
    def test_basic_output(self, sample_snapshot):
        output = mc.snapshot_to_prometheus(sample_snapshot)
        assert "mlca_documents_total" in output
        assert "mlca_forest_disk_mb" in output
        assert "mlca_forest_memory_mb" in output

    def test_database_label(self, sample_snapshot):
        output = mc.snapshot_to_prometheus(sample_snapshot)
        assert 'database="Documents"' in output

    def test_host_metrics(self, sample_snapshot):
        output = mc.snapshot_to_prometheus(sample_snapshot)
        assert "mlca_host_rss_mb" in output
        assert 'host="ml-host-1"' in output

    def test_gauge_format(self, sample_snapshot):
        output = mc.snapshot_to_prometheus(sample_snapshot)
        lines = output.strip().split("\n")
        # Every metric should have HELP, TYPE, and value lines
        help_lines = [l for l in lines if l.startswith("# HELP")]
        type_lines = [l for l in lines if l.startswith("# TYPE")]
        assert len(help_lines) > 0
        assert len(type_lines) == len(help_lines)
        for tl in type_lines:
            assert "gauge" in tl

    def test_fragmentation_ratio(self, sample_snapshot):
        output = mc.snapshot_to_prometheus(sample_snapshot)
        assert "mlca_fragmentation_ratio" in output

    def test_memory_ceiling(self, sample_snapshot):
        output = mc.snapshot_to_prometheus(sample_snapshot)
        assert "mlca_memory_ceiling_mb" in output
        assert "mlca_memory_headroom_mb" in output
        assert "mlca_memory_utilization_ratio" in output

    def test_disk_metrics(self, sample_snapshot):
        output = mc.snapshot_to_prometheus(sample_snapshot)
        assert "mlca_disk_remaining_mb" in output
        assert "mlca_disk_bytes_per_doc" in output

    def test_empty_hosts(self, sample_snapshot):
        sample_snapshot["hosts"] = []
        output = mc.snapshot_to_prometheus(sample_snapshot)
        assert "mlca_host_rss_mb" not in output
        assert "mlca_documents_total" in output  # non-host metrics still present

    def test_no_index_memory(self, sample_snapshot):
        sample_snapshot["index_memory"] = None
        output = mc.snapshot_to_prometheus(sample_snapshot)
        assert "mlca_index_memory_bytes" not in output
        assert "mlca_documents_total" in output

    def test_with_index_memory(self, snapshot_pair):
        _, new_snap = snapshot_pair
        output = mc.snapshot_to_prometheus(new_snap)
        assert "mlca_index_memory_bytes" in output
        assert 'index="severity(int)"' in output

    def test_none_values_skipped(self, sample_snapshot):
        sample_snapshot["totals"]["documents"] = None
        sample_snapshot["totals"]["forest_disk_mb"] = None
        output = mc.snapshot_to_prometheus(sample_snapshot)
        assert "mlca_documents_total" not in output

    def test_zero_fragments(self):
        from tests.conftest import _make_snapshot
        snap = _make_snapshot(active_frags=0, deleted_frags=0)
        output = mc.snapshot_to_prometheus(snap)
        assert "mlca_fragmentation_ratio" not in output

    def test_zero_system_total(self):
        from tests.conftest import _make_snapshot
        snap = _make_snapshot(hosts=[{
            "hostname": "h1",
            "memory-system-total-mb": 0,
        }])
        snap["totals"]["system_total_mb"] = 0
        output = mc.snapshot_to_prometheus(snap)
        assert "mlca_memory_ceiling_mb" not in output
