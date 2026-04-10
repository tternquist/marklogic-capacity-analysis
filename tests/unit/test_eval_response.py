"""Unit tests for _parse_eval_response multipart parsing."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import ml_capacity as mc


@pytest.fixture
def client():
    """A MarkLogicClient instance (no real connection needed for parsing)."""
    return mc.MarkLogicClient("localhost", 8002, "admin", "admin")


class TestParseEvalResponse:
    def test_single_json_value(self, client):
        text = (
            '--boundary\r\n'
            'Content-Type: application/json\r\n'
            '\r\n'
            '{"key": "value"}\r\n'
            '--boundary--'
        )
        result = client._parse_eval_response(text)
        assert len(result) == 1
        assert result[0] == {"key": "value"}

    def test_multiple_json_values(self, client):
        text = (
            '--boundary\r\n'
            'Content-Type: application/json\r\n'
            '\r\n'
            '42\r\n'
            '--boundary\r\n'
            'Content-Type: application/json\r\n'
            '\r\n'
            '"hello"\r\n'
            '--boundary--'
        )
        result = client._parse_eval_response(text)
        assert len(result) == 2
        assert result[0] == 42
        assert result[1] == "hello"

    def test_json_array(self, client):
        text = (
            '--boundary\r\n'
            'Content-Type: application/json\r\n'
            '\r\n'
            '[{"a":1},{"b":2}]\r\n'
            '--boundary--'
        )
        result = client._parse_eval_response(text)
        assert len(result) == 1
        assert result[0] == [{"a": 1}, {"b": 2}]

    def test_non_json_body_returned_as_string(self, client):
        text = (
            '--boundary\r\n'
            'Content-Type: text/plain\r\n'
            '\r\n'
            'some plain text\r\n'
            '--boundary--'
        )
        result = client._parse_eval_response(text)
        assert len(result) == 1
        assert result[0] == "some plain text"

    def test_empty_response(self, client):
        result = client._parse_eval_response("")
        assert result == []

    def test_boundary_only(self, client):
        result = client._parse_eval_response("----")
        assert result == []

    def test_lf_only_line_endings(self, client):
        """MarkLogic may return LF-only instead of CRLF."""
        text = (
            '--boundary\n'
            'Content-Type: application/json\n'
            '\n'
            '{"lf": true}\n'
            '--boundary--'
        )
        result = client._parse_eval_response(text)
        assert len(result) == 1
        assert result[0] == {"lf": True}

    def test_xquery_forest_counts_format(self, client):
        """Simulate the multipart response from collect_forest_counts XQuery."""
        text = (
            '--ML_BOUNDARY\r\n'
            'Content-Type: application/json\r\n'
            'X-Primitive: json\r\n'
            '\r\n'
            '[{"forest-name":"Documents-1","document-count":100000,'
            '"active-fragment-count":100000,"deleted-fragment-count":5000,'
            '"stand-count":4,"disk-size-mb":500,"memory-size-mb":200}]\r\n'
            '--ML_BOUNDARY--'
        )
        result = client._parse_eval_response(text)
        assert len(result) == 1
        assert isinstance(result[0], list)
        assert result[0][0]["forest-name"] == "Documents-1"
        assert result[0][0]["document-count"] == 100000

    def test_nested_json(self, client):
        text = (
            '--b\r\n'
            'Content-Type: application/json\r\n'
            '\r\n'
            '{"indexes":[{"localname":"severity","totalMemoryBytes":1024}]}\r\n'
            '--b--'
        )
        result = client._parse_eval_response(text)
        assert result[0]["indexes"][0]["localname"] == "severity"
