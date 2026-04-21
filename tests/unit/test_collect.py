"""Unit tests for ml_capacity.collect data collection functions.

Each function delegates to a MarkLogicClient method; we verify the
request path, XQuery/JS payload, and response shape using a stub client.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import ml_capacity as mc


class StubClient:
    """Capture requests and return canned responses per path or call."""

    def __init__(self, json_responses=None, eval_responses=None):
        self.json_responses = json_responses or {}
        self.eval_responses = eval_responses or []
        self.get_json_calls = []
        self.eval_xquery_calls = []
        self.eval_javascript_calls = []

    def get_json(self, path):
        self.get_json_calls.append(path)
        if path in self.json_responses:
            resp = self.json_responses[path]
            return resp() if callable(resp) else resp
        # Support prefix matching: pick the first registered path the call starts with.
        for key, resp in self.json_responses.items():
            if path.startswith(key):
                return resp() if callable(resp) else resp
        raise KeyError(f"No stubbed response for {path}")

    def eval_xquery(self, xquery, database=None):
        self.eval_xquery_calls.append((xquery, database))
        return self.eval_responses.pop(0) if self.eval_responses else []

    def eval_javascript(self, javascript, database=None, vars=None):
        self.eval_javascript_calls.append((javascript, database, vars))
        return self.eval_responses.pop(0) if self.eval_responses else []


class TestCollectClusterOverview:
    def test_hits_management_root(self):
        client = StubClient(json_responses={
            "/manage/v2?format=json": {"local-cluster-default": {"name": "c1"}}
        })
        result = mc.collect_cluster_overview(client)
        assert result == {"local-cluster-default": {"name": "c1"}}
        assert client.get_json_calls == ["/manage/v2?format=json"]


class TestCollectDatabaseStatus:
    def test_includes_status_view(self):
        client = StubClient(json_responses={
            "/manage/v2/databases/Documents?view=status&format=json":
                {"database-status": {"status-properties": {}}}
        })
        result = mc.collect_database_status(client, "Documents")
        assert "database-status" in result
        assert client.get_json_calls[0].endswith("view=status&format=json")
        assert "Documents" in client.get_json_calls[0]

    def test_uses_provided_database_name(self):
        client = StubClient(json_responses={
            "/manage/v2/databases/App-Services?view=status&format=json": {}
        })
        mc.collect_database_status(client, "App-Services")
        assert "App-Services" in client.get_json_calls[0]


class TestCollectDatabaseProperties:
    def test_hits_properties_endpoint(self):
        client = StubClient(json_responses={
            "/manage/v2/databases/Documents/properties?format=json":
                {"database-name": "Documents"}
        })
        result = mc.collect_database_properties(client, "Documents")
        assert result == {"database-name": "Documents"}
        assert client.get_json_calls == [
            "/manage/v2/databases/Documents/properties?format=json"
        ]


class TestCollectForests:
    def test_filters_by_database_id(self):
        client = StubClient(json_responses={
            "/manage/v2/forests?format=json&database-id=Documents&view=status":
                {"forest-default-list": {"list-items": {"list-item": []}}}
        })
        result = mc.collect_forests(client, "Documents")
        assert "forest-default-list" in result
        path = client.get_json_calls[0]
        assert "database-id=Documents" in path
        assert "view=status" in path


class TestCollectForestDetail:
    def test_includes_forest_name_and_status_view(self):
        client = StubClient(json_responses={
            "/manage/v2/forests/Documents-1?view=status&format=json":
                {"forest-status": {"forest-name": "Documents-1"}}
        })
        result = mc.collect_forest_detail(client, "Documents-1")
        assert result["forest-status"]["forest-name"] == "Documents-1"


class TestCollectHostStatus:
    def test_fans_out_to_each_host(self):
        client = StubClient(json_responses={
            "/manage/v2/hosts?format=json": {
                "host-default-list": {
                    "list-items": {
                        "list-item": [
                            {"nameref": "host-a"},
                            {"nameref": "host-b"},
                        ]
                    }
                }
            },
            "/manage/v2/hosts/host-a?view=status&format=json": {"host": "a"},
            "/manage/v2/hosts/host-b?view=status&format=json": {"host": "b"},
        })
        results = mc.collect_host_status(client)
        assert results == [{"host": "a"}, {"host": "b"}]
        # One listing call + one detail call per host
        assert len(client.get_json_calls) == 3
        assert client.get_json_calls[0] == "/manage/v2/hosts?format=json"

    def test_empty_host_list(self):
        client = StubClient(json_responses={
            "/manage/v2/hosts?format=json": {"host-default-list": {"list-items": {"list-item": []}}}
        })
        assert mc.collect_host_status(client) == []

    def test_missing_list_items_returns_empty(self):
        """If ML returns an unexpected structure, collector yields []."""
        client = StubClient(json_responses={
            "/manage/v2/hosts?format=json": {}
        })
        assert mc.collect_host_status(client) == []


class TestCollectHostMemory:
    def test_posts_xquery_to_eval(self):
        payload = [[{"hostname": "ml-host-1", "memory-process-rss-mb": 3200}]]
        client = StubClient(eval_responses=[payload])
        result = mc.collect_host_memory(client)
        assert result == payload
        assert len(client.eval_xquery_calls) == 1
        xquery, database = client.eval_xquery_calls[0]
        # No per-database scoping for host memory
        assert database is None
        # Verify it's querying host-status fields we care about
        assert "xdmp:host-status" in xquery
        assert "memory-process-rss" in xquery
        assert "memory-forest-size" in xquery


class TestCollectForestCounts:
    def test_validates_database_name(self):
        """Injection-unsafe names are rejected before any server call."""
        client = StubClient()
        with pytest.raises(ValueError):
            mc.collect_forest_counts(client, 'Docs"; xdmp:shutdown()')
        assert client.eval_xquery_calls == []

    def test_passes_database_into_xquery_and_eval_scope(self):
        client = StubClient(eval_responses=[[[{"forest-name": "F1",
                                               "document-count": 5}]]])
        result = mc.collect_forest_counts(client, "Documents")
        assert result[0][0]["forest-name"] == "F1"
        xquery, database = client.eval_xquery_calls[0]
        # Database passed as eval scope AND interpolated into the XQuery body
        assert database == "Documents"
        assert 'xdmp:database("Documents")' in xquery
        # Sanity-check the aggregations the report consumes
        assert "document-count" in xquery
        assert "stands-counts" in xquery

    def test_accepts_valid_names_with_hyphens(self):
        client = StubClient(eval_responses=[[[]]])
        mc.collect_forest_counts(client, "App-Services")
        assert client.eval_xquery_calls[0][1] == "App-Services"


class TestIndexMemoryJsPayload:
    """INDEX_MEMORY_JS is consumed by report_index_memory; verify its shape."""

    def test_references_expected_apis(self):
        js = mc.INDEX_MEMORY_JS
        assert "xdmp.databaseDescribeIndexes" in js
        assert "xdmp.forestStatus" in js
        assert "memoryDetail" in js
        # Returns the aggregated shape report_index_memory expects
        assert "indexes" in js
        assert "standSummaries" in js
