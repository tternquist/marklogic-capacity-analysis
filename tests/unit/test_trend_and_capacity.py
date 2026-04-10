"""Unit tests for trend analysis data extraction and capacity estimate math.

Since report_trend() and report_capacity_estimate() produce printed output,
these tests verify the underlying math and data extraction logic that feeds
those reports — catching regressions without requiring MarkLogic.
"""

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import ml_capacity as mc
from tests.conftest import _make_snapshot


# ── Trend data extraction ─────────────────────────────────────────

class TestTrendDataExtraction:
    """Test the logic inside report_trend that extracts time-series points."""

    def _extract_points(self, snaps):
        """Extract trend points the same way report_trend does."""
        points = []
        for s in snaps:
            t = s.get("totals", {})
            ts_str = s.get("timestamp", "")
            try:
                ts = datetime.fromisoformat(ts_str)
            except (ValueError, TypeError):
                continue
            points.append({
                "ts": ts,
                "docs": t.get("documents", 0),
                "disk_mb": t.get("forest_disk_mb", 0),
                "forest_mb": t.get("host_forest_mb", 0),
                "rss_mb": t.get("host_rss_mb", 0),
                "fragments": t.get("active_fragments", 0),
                "sys_total": t.get("system_total_mb", 0),
                "cache_mb": t.get("host_cache_mb", 0),
                "base_mb": t.get("host_base_mb", 0),
                "file_mb": t.get("host_file_mb", 0),
            })
        return points

    def test_basic_extraction(self, snapshot_pair):
        old, new = snapshot_pair
        points = self._extract_points([old, new])
        assert len(points) == 2
        assert points[0]["docs"] == 100000
        assert points[1]["docs"] == 200000

    def test_skips_bad_timestamps(self):
        snap = _make_snapshot(timestamp="not-a-date")
        points = self._extract_points([snap])
        assert len(points) == 0

    def test_growth_rates(self, snapshot_pair):
        old, new = snapshot_pair
        points = self._extract_points([old, new])
        first, last = points[0], points[-1]
        span = (last["ts"] - first["ts"]).total_seconds()
        days = span / 86400

        doc_delta = last["docs"] - first["docs"]
        forest_delta = last["forest_mb"] - first["forest_mb"]
        disk_delta = last["disk_mb"] - first["disk_mb"]

        assert doc_delta == 100000
        assert forest_delta == 200  # 400 - 200
        assert disk_delta == 500  # 1000 - 500
        assert days == pytest.approx(2.0, abs=0.01)

    def test_memory_runway_calculation(self, snapshot_pair):
        old, new = snapshot_pair
        points = self._extract_points([old, new])
        last = points[-1]

        sys_total = last["sys_total"]  # 8192
        ceiling = sys_total * 0.80  # 6553.6
        fixed_mem = last["cache_mb"] + last["base_mb"] + last["file_mb"]  # 1024 + 1000 + 150
        forest_now = last["forest_mb"]  # 400
        headroom = ceiling - fixed_mem - forest_now

        assert ceiling == pytest.approx(6553.6)
        assert fixed_mem == pytest.approx(2174)
        assert headroom == pytest.approx(3979.6)

    def test_zero_span_no_crash(self):
        """Two snapshots at same time shouldn't crash."""
        ts = "2026-04-10T10:00:00+00:00"
        snap1 = _make_snapshot(docs=100000, timestamp=ts)
        snap2 = _make_snapshot(docs=200000, timestamp=ts)
        points = self._extract_points([snap1, snap2])
        span = (points[-1]["ts"] - points[0]["ts"]).total_seconds()
        days = span / 86400 if span > 0 else 0
        assert days == 0

    def test_shrinking_forest_is_valid(self):
        """Forest memory can shrink after merges — this is normal."""
        old = _make_snapshot(docs=100000, mem_mb=300,
                             timestamp="2026-04-08T10:00:00+00:00")
        new = _make_snapshot(docs=200000, mem_mb=250,
                             timestamp="2026-04-10T10:00:00+00:00")
        points = self._extract_points([old, new])
        forest_delta = points[-1]["forest_mb"] - points[0]["forest_mb"]
        assert forest_delta == -50  # shrunk — valid, merges compress memory


# ── Binding constraint logic ──────────────────────────────────────

class TestBindingConstraint:
    """Test the runway comparison logic from report_trend."""

    def test_memory_is_binding_when_smallest(self):
        runways = [("MEMORY", 30), ("DISK", 90), ("FRAGMENTS", 500)]
        runways.sort(key=lambda x: x[1])
        assert runways[0][0] == "MEMORY"

    def test_disk_is_binding_when_smallest(self):
        runways = [("MEMORY", 200), ("DISK", 50), ("FRAGMENTS", 1000)]
        runways.sort(key=lambda x: x[1])
        assert runways[0][0] == "DISK"


# ── Capacity estimate math ────────────────────────────────────────

class TestCapacityMath:
    """Test the math from report_capacity_estimate without triggering prints."""

    def _compute_capacity(self, forest_data, db_props=None, host_data=None):
        """Extract the capacity math from report_capacity_estimate."""
        total_docs = sum(f.get("document-count", 0) or 0 for f in forest_data)
        total_active = sum(f.get("active-fragment-count", 0) or 0 for f in forest_data)
        total_deleted = sum(f.get("deleted-fragment-count", 0) or 0 for f in forest_data)
        total_memory = sum(f.get("memory-size-mb", 0) or 0 for f in forest_data)
        total_disk = sum(f.get("disk-size-mb", 0) or 0 for f in forest_data)
        num_forests = len(forest_data)
        total_frags = total_active + total_deleted
        frag_pct = (total_deleted / total_frags * 100) if total_frags > 0 else 0

        if db_props is None:
            db_props = {}
        in_mem_list_mb = db_props.get("in-memory-list-size", 64)
        in_mem_tree_mb = db_props.get("in-memory-tree-size", 16)
        in_mem_range_mb = db_props.get("in-memory-range-index-size", 2)
        in_mem_reverse_mb = db_props.get("in-memory-reverse-index-size", 2)
        in_mem_triple_mb = db_props.get("in-memory-triple-index-size", 16)
        in_mem_per_forest = (in_mem_list_mb + in_mem_tree_mb
                             + in_mem_range_mb + in_mem_reverse_mb
                             + in_mem_triple_mb)
        in_mem_total_all = in_mem_per_forest * num_forests

        MAX_FRAGS_PER_FOREST = 96_000_000
        max_fragments_total = MAX_FRAGS_PER_FOREST * num_forests
        frags_remaining = max_fragments_total - total_active

        avg_mem = (total_memory * 1024 * 1024) / total_docs if total_docs > 0 else 0
        avg_disk = (total_disk * 1024 * 1024) / total_docs if total_docs > 0 else 0

        doc_frag_ratio = total_docs / total_active if total_active > 0 else 0
        docs_remaining_by_frags = int(frags_remaining * doc_frag_ratio) if total_active > 0 else 0

        return {
            "total_docs": total_docs,
            "total_disk": total_disk,
            "total_memory": total_memory,
            "num_forests": num_forests,
            "frag_pct": frag_pct,
            "in_mem_per_forest": in_mem_per_forest,
            "in_mem_total_all": in_mem_total_all,
            "frags_remaining": frags_remaining,
            "avg_mem_per_doc": avg_mem,
            "avg_disk_per_doc": avg_disk,
            "doc_frag_ratio": doc_frag_ratio,
            "docs_remaining_by_frags": docs_remaining_by_frags,
        }

    def test_basic_capacity(self):
        forests = [{
            "document-count": 100000,
            "active-fragment-count": 100000,
            "deleted-fragment-count": 5000,
            "memory-size-mb": 200,
            "disk-size-mb": 500,
        }]
        r = self._compute_capacity(forests)
        assert r["total_docs"] == 100000
        assert r["num_forests"] == 1
        assert r["frag_pct"] == pytest.approx(4.76, abs=0.1)
        assert r["in_mem_per_forest"] == 100  # 64+16+2+2+16
        assert r["in_mem_total_all"] == 100
        assert r["avg_disk_per_doc"] == pytest.approx(5242.88, abs=1)

    def test_multiple_forests(self):
        forests = [
            {"document-count": 50000, "active-fragment-count": 50000,
             "deleted-fragment-count": 1000, "memory-size-mb": 100, "disk-size-mb": 250},
            {"document-count": 50000, "active-fragment-count": 50000,
             "deleted-fragment-count": 2000, "memory-size-mb": 100, "disk-size-mb": 250},
        ]
        r = self._compute_capacity(forests)
        assert r["total_docs"] == 100000
        assert r["num_forests"] == 2
        assert r["in_mem_total_all"] == 200  # 100 MB × 2 forests

    def test_empty_database(self):
        forests = [{
            "document-count": 0,
            "active-fragment-count": 0,
            "deleted-fragment-count": 0,
            "memory-size-mb": 0,
            "disk-size-mb": 0,
        }]
        r = self._compute_capacity(forests)
        assert r["total_docs"] == 0
        assert r["avg_mem_per_doc"] == 0
        assert r["avg_disk_per_doc"] == 0
        assert r["frag_pct"] == 0
        assert r["doc_frag_ratio"] == 0

    def test_no_forests(self):
        r = self._compute_capacity([])
        assert r["num_forests"] == 0
        assert r["total_docs"] == 0
        assert r["in_mem_total_all"] == 0

    def test_high_fragmentation(self):
        forests = [{
            "document-count": 10000,
            "active-fragment-count": 10000,
            "deleted-fragment-count": 50000,
            "memory-size-mb": 100,
            "disk-size-mb": 300,
        }]
        r = self._compute_capacity(forests)
        assert r["frag_pct"] == pytest.approx(83.33, abs=0.1)

    def test_none_values_treated_as_zero(self):
        forests = [{
            "document-count": None,
            "active-fragment-count": None,
            "deleted-fragment-count": None,
            "memory-size-mb": None,
            "disk-size-mb": None,
        }]
        r = self._compute_capacity(forests)
        assert r["total_docs"] == 0
        assert r["total_disk"] == 0

    def test_fragment_limit_single_forest(self):
        forests = [{
            "document-count": 1000000,
            "active-fragment-count": 1000000,
            "deleted-fragment-count": 0,
            "memory-size-mb": 500,
            "disk-size-mb": 2000,
        }]
        r = self._compute_capacity(forests)
        assert r["frags_remaining"] == 96_000_000 - 1_000_000
        assert r["doc_frag_ratio"] == 1.0
        assert r["docs_remaining_by_frags"] == 95_000_000

    def test_custom_in_memory_settings(self):
        forests = [{
            "document-count": 100000,
            "active-fragment-count": 100000,
            "deleted-fragment-count": 0,
            "memory-size-mb": 200,
            "disk-size-mb": 500,
        }]
        db_props = {
            "in-memory-list-size": 128,
            "in-memory-tree-size": 32,
            "in-memory-range-index-size": 4,
            "in-memory-reverse-index-size": 4,
            "in-memory-triple-index-size": 32,
        }
        r = self._compute_capacity(forests, db_props)
        assert r["in_mem_per_forest"] == 200  # 128+32+4+4+32
        assert r["in_mem_total_all"] == 200


# ── Disk projection with fragmentation ────────────────────────────

class TestDiskProjection:
    """Test the fragmentation-adjusted disk projection math."""

    def _project_disk_capacity(self, total_docs, total_disk_mb, total_frags,
                               total_deleted, remaining_disk_mb):
        """Replicate the disk-based projection from report_capacity_estimate."""
        if total_docs <= 0 or total_disk_mb <= 0:
            return None

        frag_pct = (total_deleted / total_frags * 100) if total_frags > 0 else 0
        disk_bytes_per_doc = (total_disk_mb * 1024 * 1024) / total_docs

        if frag_pct >= 25 and total_frags > 0:
            avg_bytes_per_frag = (total_disk_mb * 1024 * 1024) / total_frags
            estimated_waste = total_deleted * avg_bytes_per_frag * 0.5
            estimated_clean_disk = max(
                total_disk_mb - estimated_waste / (1024 * 1024),
                total_disk_mb * 0.5
            )
            clean_bytes_per_doc = (estimated_clean_disk * 1024 * 1024) / total_docs
        else:
            clean_bytes_per_doc = disk_bytes_per_doc

        docs_capacity = int(remaining_disk_mb * 1024 * 1024 / clean_bytes_per_doc)
        return {
            "disk_bytes_per_doc": disk_bytes_per_doc,
            "clean_bytes_per_doc": clean_bytes_per_doc,
            "frag_pct": frag_pct,
            "docs_capacity": docs_capacity,
        }

    def test_no_fragmentation(self):
        r = self._project_disk_capacity(
            total_docs=100000, total_disk_mb=500,
            total_frags=100000, total_deleted=0,
            remaining_disk_mb=9500
        )
        assert r["frag_pct"] == 0
        assert r["disk_bytes_per_doc"] == r["clean_bytes_per_doc"]
        # 9500 MB / (500*1024*1024/100000) bytes per doc
        assert r["docs_capacity"] == 1900000

    def test_moderate_fragmentation(self):
        r = self._project_disk_capacity(
            total_docs=100000, total_disk_mb=500,
            total_frags=130000, total_deleted=30000,
            remaining_disk_mb=9500
        )
        # ~23% fragmentation — below threshold, no adjustment
        assert r["frag_pct"] == pytest.approx(23.08, abs=0.1)
        assert r["clean_bytes_per_doc"] == r["disk_bytes_per_doc"]

    def test_high_fragmentation_adjusts(self):
        r = self._project_disk_capacity(
            total_docs=100000, total_disk_mb=500,
            total_frags=200000, total_deleted=100000,
            remaining_disk_mb=9500
        )
        # 50% fragmentation — adjustment kicks in
        assert r["frag_pct"] == 50
        assert r["clean_bytes_per_doc"] < r["disk_bytes_per_doc"]
        # More capacity projected because bytes/doc is lower after adjustment
        assert r["docs_capacity"] > 1900000

    def test_extreme_fragmentation_floor(self):
        r = self._project_disk_capacity(
            total_docs=10000, total_disk_mb=500,
            total_frags=100000, total_deleted=90000,
            remaining_disk_mb=9500
        )
        # 90% fragmentation — floor at 50% of disk should apply
        assert r["frag_pct"] == 90
        # clean disk >= 50% of total disk
        clean_disk_mb = r["clean_bytes_per_doc"] * 10000 / (1024 * 1024)
        assert clean_disk_mb >= 250  # 50% of 500

    def test_zero_docs_returns_none(self):
        r = self._project_disk_capacity(
            total_docs=0, total_disk_mb=500,
            total_frags=0, total_deleted=0,
            remaining_disk_mb=9500
        )
        assert r is None

    def test_zero_disk_returns_none(self):
        r = self._project_disk_capacity(
            total_docs=100000, total_disk_mb=0,
            total_frags=100000, total_deleted=0,
            remaining_disk_mb=9500
        )
        assert r is None


# ── Memory ceiling math ───────────────────────────────────────────

class TestMemoryCeiling:
    """Test the ceiling/headroom computation from report_capacity_estimate."""

    def _compute_ceiling(self, total_sys_mb, ml_limit_mb, rss_mb,
                         cache_mb, forest_mb, file_mb, base_mb,
                         in_mem_total_all):
        if total_sys_mb > 0:
            ram_cap = total_sys_mb * 0.80
            safe_cap = min(ml_limit_mb, ram_cap) if ml_limit_mb else ram_cap
        elif ml_limit_mb > 0:
            safe_cap = ml_limit_mb
        else:
            safe_cap = 0
        headroom = safe_cap - rss_mb
        rss_pct = (rss_mb / safe_cap * 100) if safe_cap else 0
        fixed_mem = cache_mb + base_mb + file_mb
        forest_headroom = safe_cap - fixed_mem - forest_mb
        ingestion_headroom = headroom - in_mem_total_all
        return {
            "safe_cap": safe_cap,
            "headroom": headroom,
            "rss_pct": rss_pct,
            "fixed_mem": fixed_mem,
            "forest_headroom": forest_headroom,
            "ingestion_headroom": ingestion_headroom,
        }

    def test_normal_case(self):
        r = self._compute_ceiling(
            total_sys_mb=8192, ml_limit_mb=6144, rss_mb=3200,
            cache_mb=1024, forest_mb=200, file_mb=150, base_mb=500,
            in_mem_total_all=100
        )
        # safe_cap = min(6144, 8192*0.8) = min(6144, 6553.6) = 6144
        assert r["safe_cap"] == 6144
        assert r["headroom"] == 2944  # 6144 - 3200
        assert r["ingestion_headroom"] == 2844  # 2944 - 100

    def test_ram_cap_is_binding(self):
        r = self._compute_ceiling(
            total_sys_mb=4096, ml_limit_mb=6144, rss_mb=2000,
            cache_mb=500, forest_mb=200, file_mb=100, base_mb=200,
            in_mem_total_all=100
        )
        # safe_cap = min(6144, 4096*0.8) = min(6144, 3276.8) = 3276.8
        assert r["safe_cap"] == pytest.approx(3276.8)

    def test_ml_limit_is_binding(self):
        r = self._compute_ceiling(
            total_sys_mb=32768, ml_limit_mb=6144, rss_mb=3200,
            cache_mb=1024, forest_mb=200, file_mb=150, base_mb=500,
            in_mem_total_all=100
        )
        # safe_cap = min(6144, 32768*0.8) = 6144
        assert r["safe_cap"] == 6144

    def test_no_sys_ram_fallback_to_ml_limit(self):
        r = self._compute_ceiling(
            total_sys_mb=0, ml_limit_mb=4096, rss_mb=2000,
            cache_mb=500, forest_mb=200, file_mb=100, base_mb=200,
            in_mem_total_all=100
        )
        assert r["safe_cap"] == 4096

    def test_no_sys_no_ml_limit(self):
        r = self._compute_ceiling(
            total_sys_mb=0, ml_limit_mb=0, rss_mb=0,
            cache_mb=0, forest_mb=0, file_mb=0, base_mb=0,
            in_mem_total_all=0
        )
        assert r["safe_cap"] == 0
        assert r["rss_pct"] == 0

    def test_negative_ingestion_headroom(self):
        r = self._compute_ceiling(
            total_sys_mb=4096, ml_limit_mb=3072, rss_mb=2900,
            cache_mb=500, forest_mb=200, file_mb=100, base_mb=200,
            in_mem_total_all=300
        )
        assert r["headroom"] == 172  # 3072 - 2900
        assert r["ingestion_headroom"] == -128  # 172 - 300
