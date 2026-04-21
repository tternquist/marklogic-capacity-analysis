"""Unit tests for index helper functions: _index_key(), _index_label()."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from ml_capacity.index_analysis import _index_key, _index_label


class TestIndexKey:
    def test_range_element_index(self):
        idx = {
            "indexType": "range",
            "localname": "severity",
            "namespaceURI": "",
            "scalarType": "int",
        }
        key = _index_key(idx)
        assert key == ("range", "severity", "", "int")

    def test_path_index(self):
        idx = {
            "indexType": "range",
            "pathExpression": "/doc/timestamp",
            "namespaceURI": "",
            "scalarType": "dateTime",
        }
        key = _index_key(idx)
        assert key == ("range", "/doc/timestamp", "", "dateTime")

    def test_localname_preferred_over_path(self):
        idx = {
            "indexType": "range",
            "localname": "myField",
            "pathExpression": "/some/path",
            "scalarType": "string",
        }
        key = _index_key(idx)
        # localname should be picked first
        assert key[1] == "myField"

    def test_with_namespace(self):
        idx = {
            "indexType": "range",
            "localname": "id",
            "namespaceURI": "http://example.com/ns",
            "scalarType": "string",
        }
        key = _index_key(idx)
        assert key[2] == "http://example.com/ns"

    def test_empty_index(self):
        key = _index_key({})
        assert key == ("", "", "", "")

    def test_different_indexes_produce_different_keys(self):
        idx1 = {"indexType": "range", "localname": "a", "scalarType": "int"}
        idx2 = {"indexType": "range", "localname": "b", "scalarType": "int"}
        assert _index_key(idx1) != _index_key(idx2)

    def test_same_name_different_type(self):
        idx1 = {"indexType": "range", "localname": "x", "scalarType": "int"}
        idx2 = {"indexType": "range", "localname": "x", "scalarType": "string"}
        assert _index_key(idx1) != _index_key(idx2)


class TestIndexLabel:
    def test_basic_label(self):
        idx = {"localname": "severity", "scalarType": "int"}
        assert _index_label(idx) == "severity (int)"

    def test_path_expression(self):
        idx = {"pathExpression": "/doc/ts", "scalarType": "dateTime"}
        assert _index_label(idx) == "/doc/ts (dateTime)"

    def test_with_namespace(self):
        idx = {
            "localname": "id",
            "scalarType": "string",
            "namespaceURI": "http://example.com/my-namespace",
        }
        label = _index_label(idx)
        assert "id" in label
        assert "(string)" in label
        assert "ns:my-namespace" in label

    def test_no_scalar_type(self):
        idx = {"localname": "name"}
        label = _index_label(idx)
        assert label == "name"
        assert "(" not in label

    def test_fallback_to_index_type(self):
        idx = {"indexType": "uriLexicon"}
        label = _index_label(idx)
        assert label == "uriLexicon"

    def test_empty_index(self):
        label = _index_label({})
        assert label == "?"
