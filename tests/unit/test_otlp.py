"""Unit tests for OTLP payload construction."""

import json
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import ml_capacity as mc
from tests.conftest import _make_snapshot


class TestPushOtlp:
    def _capture_otlp_payload(self, snap, endpoint="http://collector:4318"):
        """Call push_otlp with a mocked urlopen and return the parsed payload."""
        captured = {}

        def mock_urlopen(req, timeout=None):
            captured["url"] = req.full_url
            captured["method"] = req.method
            captured["headers"] = dict(req.headers)
            captured["body"] = json.loads(req.data.decode())
            resp = MagicMock()
            resp.status = 200
            resp.__enter__ = lambda s: resp
            resp.__exit__ = MagicMock(return_value=False)
            return resp

        # push_otlp re-imports urlopen from urllib.request locally
        with patch("urllib.request.urlopen", mock_urlopen):
            mc.push_otlp(snap, endpoint)

        return captured

    def test_payload_structure(self, sample_snapshot):
        captured = self._capture_otlp_payload(sample_snapshot)
        body = captured["body"]
        assert "resourceMetrics" in body
        assert len(body["resourceMetrics"]) == 1
        rm = body["resourceMetrics"][0]
        assert "resource" in rm
        assert "scopeMetrics" in rm
        # Check service name
        attrs = rm["resource"]["attributes"]
        service_name = next(a for a in attrs if a["key"] == "service.name")
        assert service_name["value"]["stringValue"] == "mlca"

    def test_metrics_present(self, sample_snapshot):
        captured = self._capture_otlp_payload(sample_snapshot)
        metrics = captured["body"]["resourceMetrics"][0]["scopeMetrics"][0]["metrics"]
        metric_names = [m["name"] for m in metrics]
        assert "mlca.documents.total" in metric_names
        assert "mlca.memory.forest" in metric_names
        assert "mlca.memory.rss" in metric_names
        assert "mlca.disk.used" in metric_names
        assert "mlca.fragments.active" in metric_names
        assert "mlca.fragments.deleted" in metric_names
        assert "mlca.memory.headroom" in metric_names

    def test_database_label(self, sample_snapshot):
        captured = self._capture_otlp_payload(sample_snapshot)
        metrics = captured["body"]["resourceMetrics"][0]["scopeMetrics"][0]["metrics"]
        for m in metrics:
            dp = m["gauge"]["dataPoints"][0]
            db_attr = next(a for a in dp["attributes"] if a["key"] == "database")
            assert db_attr["value"]["stringValue"] == "Documents"

    def test_endpoint_url_appends_path(self):
        snap = _make_snapshot()
        captured = self._capture_otlp_payload(snap, "http://collector:4318")
        assert captured["url"] == "http://collector:4318/v1/metrics"

    def test_endpoint_url_no_double_path(self):
        snap = _make_snapshot()
        captured = self._capture_otlp_payload(snap, "http://collector:4318/v1/metrics")
        assert captured["url"] == "http://collector:4318/v1/metrics"

    def test_content_type(self, sample_snapshot):
        captured = self._capture_otlp_payload(sample_snapshot)
        assert captured["headers"]["Content-type"] == "application/json"

    def test_method_is_post(self, sample_snapshot):
        captured = self._capture_otlp_payload(sample_snapshot)
        assert captured["method"] == "POST"

    def test_metric_values_match_snapshot(self, sample_snapshot):
        captured = self._capture_otlp_payload(sample_snapshot)
        metrics = captured["body"]["resourceMetrics"][0]["scopeMetrics"][0]["metrics"]
        docs_metric = next(m for m in metrics if m["name"] == "mlca.documents.total")
        assert docs_metric["gauge"]["dataPoints"][0]["asDouble"] == 100000.0

    def test_timestamp_is_nanoseconds(self, sample_snapshot):
        captured = self._capture_otlp_payload(sample_snapshot)
        metrics = captured["body"]["resourceMetrics"][0]["scopeMetrics"][0]["metrics"]
        dp = metrics[0]["gauge"]["dataPoints"][0]
        ts = int(dp["timeUnixNano"])
        # Should be a reasonable nanosecond timestamp (year 2020+)
        assert ts > 1_577_836_800_000_000_000  # 2020-01-01 in ns
