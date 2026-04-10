"""Unit tests for formatting helpers: bar(), status_badge(), color()."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import ml_capacity as mc


class TestBar:
    def test_zero_percent(self):
        result = mc.bar(0)
        assert "0.0%" in result
        assert mc.GREEN in result

    def test_hundred_percent(self):
        result = mc.bar(100)
        assert "100.0%" in result
        assert mc.RED in result

    def test_below_warn_threshold(self):
        result = mc.bar(50)
        assert mc.GREEN in result

    def test_at_warn_threshold(self):
        result = mc.bar(70)
        assert mc.YELLOW in result

    def test_between_warn_and_crit(self):
        result = mc.bar(85)
        assert mc.YELLOW in result

    def test_at_crit_threshold(self):
        result = mc.bar(90)
        assert mc.RED in result

    def test_above_crit_threshold(self):
        result = mc.bar(99)
        assert mc.RED in result

    def test_custom_thresholds(self):
        # With custom thresholds: warn=50, crit=75
        result = mc.bar(60, warn_threshold=50, crit_threshold=75)
        assert mc.YELLOW in result

        result = mc.bar(80, warn_threshold=50, crit_threshold=75)
        assert mc.RED in result

        result = mc.bar(40, warn_threshold=50, crit_threshold=75)
        assert mc.GREEN in result

    def test_over_100_clamps(self):
        result = mc.bar(150)
        # Should not crash, filled should clamp to BAR_WIDTH
        assert "150.0%" in result

    def test_negative_clamps(self):
        result = mc.bar(-10)
        # Should not crash, filled should clamp to 0
        assert "-10.0%" in result


class TestStatusBadge:
    def test_ok_true(self):
        result = mc.status_badge(True)
        assert "[OK]" in result
        assert mc.GREEN in result

    def test_ok_false(self):
        result = mc.status_badge(False)
        assert "[WARNING]" in result
        assert mc.RED in result

    def test_custom_text(self):
        result = mc.status_badge(True, ok_text="GOOD", bad_text="BAD")
        assert "[GOOD]" in result

        result = mc.status_badge(False, ok_text="GOOD", bad_text="BAD")
        assert "[BAD]" in result


class TestColor:
    def test_wraps_text(self):
        result = mc.color("hello", mc.RED)
        assert result.startswith(mc.RED)
        assert result.endswith(mc.RESET)
        assert "hello" in result

    def test_empty_text(self):
        result = mc.color("", mc.GREEN)
        assert result == f"{mc.GREEN}{mc.RESET}"
