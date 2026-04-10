"""Unit tests for snapshot management: save, load, prune, parse_interval."""

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import ml_capacity as mc
import ml_capacity.snapshot as snap_mod


class TestParseInterval:
    def test_seconds(self):
        assert mc.parse_interval("30s") == 30

    def test_minutes(self):
        assert mc.parse_interval("15m") == 900

    def test_hours(self):
        assert mc.parse_interval("1h") == 3600

    def test_bare_number(self):
        assert mc.parse_interval("120") == 120

    def test_whitespace(self):
        assert mc.parse_interval("  5m  ") == 300

    def test_case_insensitive(self):
        assert mc.parse_interval("5M") == 300

    def test_invalid_suffix_raises(self):
        with pytest.raises(ValueError):
            mc.parse_interval("10x")

    def test_empty_string_raises(self):
        with pytest.raises(ValueError):
            mc.parse_interval("")

    def test_only_suffix_raises(self):
        with pytest.raises(ValueError):
            mc.parse_interval("m")

    def test_large_value(self):
        assert mc.parse_interval("24h") == 86400

    def test_one_second(self):
        assert mc.parse_interval("1s") == 1

    def test_zero(self):
        assert mc.parse_interval("0") == 0


class TestValidateDatabaseName:
    def test_valid_simple(self):
        mc.validate_database_name("Documents")

    def test_valid_with_hyphens(self):
        mc.validate_database_name("my-database")

    def test_valid_with_underscores(self):
        mc.validate_database_name("my_database_123")

    def test_rejects_quotes(self):
        with pytest.raises(ValueError):
            mc.validate_database_name('foo"bar')

    def test_rejects_semicolons(self):
        with pytest.raises(ValueError):
            mc.validate_database_name("foo;bar")

    def test_rejects_spaces(self):
        with pytest.raises(ValueError):
            mc.validate_database_name("my database")

    def test_rejects_xquery_injection(self):
        with pytest.raises(ValueError):
            mc.validate_database_name('") ; xdmp:restart() ; xdmp:database("')

    def test_rejects_empty(self):
        with pytest.raises(ValueError):
            mc.validate_database_name("")

    def test_rejects_parens(self):
        with pytest.raises(ValueError):
            mc.validate_database_name("db()")


class TestSaveSnapshot:
    def test_save_creates_file(self, tmp_path, sample_snapshot):
        original = snap_mod.SNAPSHOT_DIR
        snap_mod.SNAPSHOT_DIR = tmp_path
        try:
            path = mc.save_snapshot(sample_snapshot)
            assert path.exists()
            assert path.suffix == ".json"
            assert "Documents" in path.name

            with open(path) as f:
                saved = json.load(f)
            assert saved["database"] == "Documents"
            assert saved["totals"]["documents"] == 100000
        finally:
            snap_mod.SNAPSHOT_DIR = original

    def test_save_creates_directory(self, tmp_path, sample_snapshot):
        snap_dir = tmp_path / "new_dir"
        original = snap_mod.SNAPSHOT_DIR
        snap_mod.SNAPSHOT_DIR = snap_dir
        try:
            path = mc.save_snapshot(sample_snapshot)
            assert snap_dir.exists()
            assert path.exists()
        finally:
            snap_mod.SNAPSHOT_DIR = original


class TestLoadSnapshots:
    def test_load_all(self, tmp_snapshot_dir):
        snaps = mc.load_snapshots()
        assert len(snaps) == 3

    def test_load_filtered(self, tmp_snapshot_dir):
        snaps = mc.load_snapshots(database="Documents")
        assert len(snaps) == 3

    def test_load_filtered_no_match(self, tmp_snapshot_dir):
        snaps = mc.load_snapshots(database="NonExistent")
        assert len(snaps) == 0

    def test_load_sorted_by_timestamp(self, tmp_snapshot_dir):
        snaps = mc.load_snapshots()
        docs = [s["totals"]["documents"] for s in snaps]
        assert docs == [100000, 150000, 200000]

    def test_load_includes_filename(self, tmp_snapshot_dir):
        snaps = mc.load_snapshots()
        assert all("_file" in s for s in snaps)

    def test_load_empty_dir(self, tmp_path):
        original = snap_mod.SNAPSHOT_DIR
        snap_mod.SNAPSHOT_DIR = tmp_path
        try:
            snaps = mc.load_snapshots()
            assert snaps == []
        finally:
            snap_mod.SNAPSHOT_DIR = original

    def test_load_nonexistent_dir(self, tmp_path):
        original = snap_mod.SNAPSHOT_DIR
        snap_mod.SNAPSHOT_DIR = tmp_path / "does_not_exist"
        try:
            snaps = mc.load_snapshots()
            assert snaps == []
        finally:
            snap_mod.SNAPSHOT_DIR = original

    def test_load_skips_malformed_json(self, tmp_snapshot_dir):
        # Add a malformed file
        with open(tmp_snapshot_dir / "20260411T100000_Documents.json", "w") as f:
            f.write("{invalid json")
        snaps = mc.load_snapshots()
        assert len(snaps) == 3  # malformed file skipped


class TestPruneSnapshots:
    def test_prune_old(self, tmp_snapshot_dir):
        # Snapshots are from April 8-10. Pruning with 1 day retention should remove 2.
        removed = mc.prune_snapshots(retention_days=1)
        assert removed == 2
        remaining = mc.load_snapshots()
        assert len(remaining) == 1

    def test_prune_zero_keeps_all(self, tmp_snapshot_dir):
        removed = mc.prune_snapshots(retention_days=0)
        assert removed == 0
        assert len(mc.load_snapshots()) == 3

    def test_prune_large_retention_removes_none(self, tmp_snapshot_dir):
        removed = mc.prune_snapshots(retention_days=365)
        assert removed == 0
        assert len(mc.load_snapshots()) == 3

    def test_prune_nonexistent_dir(self, tmp_path):
        original = snap_mod.SNAPSHOT_DIR
        snap_mod.SNAPSHOT_DIR = tmp_path / "nope"
        try:
            removed = mc.prune_snapshots(retention_days=1)
            assert removed == 0
        finally:
            snap_mod.SNAPSHOT_DIR = original


class TestFmtMb:
    def test_none(self):
        assert mc.fmt_mb(None) == "N/A"

    def test_mb(self):
        assert "MB" in mc.fmt_mb(500)

    def test_gb(self):
        result = mc.fmt_mb(2048)
        assert "GB" in result

    def test_tb(self):
        result = mc.fmt_mb(1024 * 1024 * 2)
        assert "TB" in result

    def test_string_input(self):
        result = mc.fmt_mb("1024")
        assert "GB" in result
