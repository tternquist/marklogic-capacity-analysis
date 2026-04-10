"""Unit tests for capacity estimation logic.

The report_capacity_estimate() function contains the core capacity math:
  - memory ceiling calculation
  - headroom analysis
  - in-memory stand reservation
  - disk-based document projection
  - fragmentation-adjusted projection

These tests validate the math by capturing printed output, since the function
communicates results via print statements.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import ml_capacity as mc
from tests.conftest import _make_snapshot


def _build_host_data(system_total=8192, rss=3200, cache=1024, forest=200,
                     file_cache=150, base=500, ml_limit=6144, swap=0,
                     swapin=0):
    """Build a host_data list for report_capacity_estimate."""
    return [{
        "hostname": "ml-host-1",
        "memory-system-total-mb": system_total,
        "memory-system-free-mb": system_total - rss,
        "memory-process-rss-mb": rss,
        "memory-process-rss-hwm-mb": rss + 100,
        "memory-cache-size-mb": cache,
        "memory-forest-size-mb": forest,
        "memory-file-size-mb": file_cache,
        "host-size-mb": base,
        "memory-size-mb": ml_limit,
        "memory-process-swap-mb": swap,
        "memory-system-swapin-rate": swapin,
        "memory-join-size-mb": 0,
        "memory-unclosed-size-mb": 0,
        "host-large-data-size-mb": 0,
    }]


def _build_db_props(**overrides):
    """Build database properties dict."""
    props = {
        "in-memory-limit": 32768,
        "in-memory-list-size": 64,
        "in-memory-tree-size": 16,
        "in-memory-range-index-size": 2,
        "in-memory-reverse-index-size": 2,
        "in-memory-triple-index-size": 16,
    }
    props.update(overrides)
    return props


def _build_forest_data(docs=100000, active=100000, deleted=5000,
                       disk_mb=500, mem_mb=200, stands=4, num_forests=1):
    """Build a forest_data list."""
    forests = []
    for i in range(num_forests):
        forests.append({
            "forest-name": f"Documents-{i+1}",
            "document-count": docs // num_forests,
            "active-fragment-count": active // num_forests,
            "deleted-fragment-count": deleted // num_forests,
            "nascent-fragment-count": 0,
            "stand-count": stands,
            "disk-size-mb": disk_mb // num_forests,
            "memory-size-mb": mem_mb // num_forests,
        })
    return forests


class TestCapacityEstimateOutput:
    """Test that report_capacity_estimate produces expected output sections."""

    def test_runs_without_error(self, capsys):
        db_props = _build_db_props()
        forest_data = _build_forest_data()
        host_data = _build_host_data()
        mc.report_capacity_estimate("Documents", db_props, forest_data,
                                    host_data, remaining_disk_mb=45000)
        out = capsys.readouterr().out
        assert "CAPACITY ESTIMATE" in out

    def test_shows_current_state(self, capsys):
        forest_data = _build_forest_data(docs=100000)
        mc.report_capacity_estimate("Documents", _build_db_props(),
                                    forest_data, _build_host_data())
        out = capsys.readouterr().out
        assert "100,000" in out  # total docs
        assert "Current State" in out

    def test_no_forest_data(self, capsys):
        mc.report_capacity_estimate("Documents", _build_db_props(),
                                    [], _build_host_data())
        out = capsys.readouterr().out
        assert "Insufficient data" in out

    def test_memory_capacity_section(self, capsys):
        forest_data = _build_forest_data()
        host_data = _build_host_data()
        mc.report_capacity_estimate("Documents", _build_db_props(),
                                    forest_data, host_data)
        out = capsys.readouterr().out
        assert "Memory Capacity Analysis" in out
        assert "ceiling" in out.lower()

    def test_disk_projection_section(self, capsys):
        forest_data = _build_forest_data(docs=100000, disk_mb=500)
        host_data = _build_host_data()
        mc.report_capacity_estimate("Documents", _build_db_props(),
                                    forest_data, host_data,
                                    remaining_disk_mb=45000)
        out = capsys.readouterr().out
        assert "Disk capacity" in out
        assert "bytes/doc" in out.lower()

    def test_no_host_data(self, capsys):
        """Without host data, memory capacity section is skipped."""
        forest_data = _build_forest_data()
        mc.report_capacity_estimate("Documents", _build_db_props(),
                                    forest_data, None)
        out = capsys.readouterr().out
        assert "Current State" in out
        # No memory capacity analysis without host data
        assert "Memory Capacity Analysis" not in out

    def test_zero_docs(self, capsys):
        forest_data = _build_forest_data(docs=0, active=0, deleted=0,
                                         disk_mb=0, mem_mb=0)
        host_data = _build_host_data()
        mc.report_capacity_estimate("Documents", _build_db_props(),
                                    forest_data, host_data)
        out = capsys.readouterr().out
        assert "No documents loaded yet" in out


class TestMemoryCeiling:
    """Test the memory ceiling / headroom math."""

    def test_ceiling_is_80_percent_of_ram(self, capsys):
        # 8192 MB RAM -> ceiling = 6553.6 MB
        host_data = _build_host_data(system_total=8192, ml_limit=6144)
        forest_data = _build_forest_data()
        mc.report_capacity_estimate("Documents", _build_db_props(),
                                    forest_data, host_data)
        out = capsys.readouterr().out
        # 80% of 8192 = 6553.6 MB, but effective ceiling = min(ml_limit, ram_cap)
        # ml_limit=6144 < 6553.6, so effective = 6144
        assert "6.00 GB" in out  # 6144 MB = 6 GB

    def test_ml_limit_caps_ceiling(self, capsys):
        # When ML limit < 80% RAM, ceiling = ML limit
        host_data = _build_host_data(system_total=16384, ml_limit=4096)
        forest_data = _build_forest_data()
        mc.report_capacity_estimate("Documents", _build_db_props(),
                                    forest_data, host_data)
        out = capsys.readouterr().out
        assert "Effective ceiling" in out

    def test_swap_warning(self, capsys):
        host_data = _build_host_data(swap=100)
        forest_data = _build_forest_data()
        mc.report_capacity_estimate("Documents", _build_db_props(),
                                    forest_data, host_data)
        out = capsys.readouterr().out
        assert "swap" in out.lower()


class TestInMemoryStandReservation:
    """Test in-memory stand budget calculations."""

    def test_single_forest(self, capsys):
        # Default: list=64 + tree=16 + range=2 + reverse=2 + triple=16 = 100 MB per forest
        forest_data = _build_forest_data(num_forests=1)
        host_data = _build_host_data()
        mc.report_capacity_estimate("Documents", _build_db_props(),
                                    forest_data, host_data)
        out = capsys.readouterr().out
        assert "100" in out  # 100 MB per forest
        assert "Ingestion reserve" in out

    def test_multiple_forests(self, capsys):
        # 3 forests * 100 MB = 300 MB total
        forest_data = _build_forest_data(num_forests=3)
        host_data = _build_host_data()
        mc.report_capacity_estimate("Documents", _build_db_props(),
                                    forest_data, host_data)
        out = capsys.readouterr().out
        assert "3 forest(s)" in out


class TestDiskProjection:
    """Test disk-based document capacity projection."""

    def test_bytes_per_doc(self, capsys):
        # 500 MB disk, 100k docs -> 5242.88 bytes/doc
        forest_data = _build_forest_data(docs=100000, disk_mb=500)
        host_data = _build_host_data()
        mc.report_capacity_estimate("Documents", _build_db_props(),
                                    forest_data, host_data,
                                    remaining_disk_mb=45000)
        out = capsys.readouterr().out
        assert "5,243" in out  # bytes/doc

    def test_high_fragmentation_adjusted(self, capsys):
        # With >25% fragmentation, bytes/doc should be adjusted
        forest_data = _build_forest_data(docs=100000, active=100000,
                                         deleted=50000, disk_mb=500)
        host_data = _build_host_data()
        mc.report_capacity_estimate("Documents", _build_db_props(),
                                    forest_data, host_data,
                                    remaining_disk_mb=45000)
        out = capsys.readouterr().out
        assert "after merge" in out.lower()
        assert "fragmentation" in out.lower()

    def test_critical_fragmentation_warning(self, capsys):
        # >50% fragmentation should show critical warning
        forest_data = _build_forest_data(docs=100000, active=100000,
                                         deleted=110000, disk_mb=500)
        host_data = _build_host_data()
        mc.report_capacity_estimate("Documents", _build_db_props(),
                                    forest_data, host_data)
        out = capsys.readouterr().out
        assert "FRAGMENTATION" in out
        assert "UNRELIABLE" in out


class TestScalingRecommendations:
    """Test the scaling recommendations section."""

    def test_no_issues(self, capsys):
        forest_data = _build_forest_data(stands=4)
        host_data = _build_host_data(rss=2000, system_total=16384)
        mc.report_capacity_estimate("Documents", _build_db_props(),
                                    forest_data, host_data)
        out = capsys.readouterr().out
        assert "No immediate scaling concerns" in out

    def test_high_stand_count_warning(self, capsys):
        forest_data = _build_forest_data(stands=50)
        host_data = _build_host_data()
        mc.report_capacity_estimate("Documents", _build_db_props(),
                                    forest_data, host_data)
        out = capsys.readouterr().out
        assert "Merge pressure" in out

    def test_high_memory_pressure_warning(self, capsys):
        # RSS at 75% of system total
        host_data = _build_host_data(system_total=8192, rss=6144)
        forest_data = _build_forest_data()
        mc.report_capacity_estimate("Documents", _build_db_props(),
                                    forest_data, host_data)
        out = capsys.readouterr().out
        assert "Memory pressure" in out
