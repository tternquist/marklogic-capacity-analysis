"""Unit tests for ml_capacity.report sections.

These tests drive each report_* function with a stub MarkLogicClient and
assert on the rendered text via capsys. Coverage focuses on: request
routing, parsing of Management API shapes, threshold badges, and graceful
handling of missing / eval-disabled responses.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import ml_capacity as mc


class StubClient:
    """Minimal client satisfying the subset of methods report_* calls."""

    def __init__(self, json_responses=None, eval_xquery_results=None,
                 eval_js_results=None, raise_on_eval=None):
        self.json_responses = json_responses or {}
        self._xquery_queue = list(eval_xquery_results or [])
        self._js_queue = list(eval_js_results or [])
        self.raise_on_eval = raise_on_eval

    def get_json(self, path):
        if path in self.json_responses:
            return self.json_responses[path]
        for key, resp in self.json_responses.items():
            if path.startswith(key):
                return resp
        raise KeyError(f"No stubbed response for {path}")

    def eval_xquery(self, xquery, database=None):
        if self.raise_on_eval:
            raise self.raise_on_eval
        return self._xquery_queue.pop(0) if self._xquery_queue else []

    def eval_javascript(self, javascript, database=None, vars=None):
        if self.raise_on_eval:
            raise self.raise_on_eval
        return self._js_queue.pop(0) if self._js_queue else []


# ── report_cluster ────────────────────────────────────────────────────

class TestReportCluster:
    def test_prints_cluster_name_version_and_counts(self, capsys):
        client = StubClient(json_responses={
            "/manage/v2?format=json": {
                "local-cluster-default": {
                    "name": "prod-cluster",
                    "version": "12.0-1",
                    "relations": {
                        "relation-group": [
                            {"typeref": "hosts", "relation-count": {"value": 3}},
                            {"typeref": "databases", "relation-count": {"value": 9}},
                            {"typeref": "forests", "relation-count": {"value": 27}},
                            {"typeref": "servers", "relation-count": {"value": 5}},
                            # An ignored type should not be emitted
                            {"typeref": "groups", "relation-count": {"value": 1}},
                        ]
                    },
                }
            }
        })
        mc.report_cluster(client)
        out = capsys.readouterr().out
        assert "CLUSTER OVERVIEW" in out
        assert "prod-cluster" in out
        assert "12.0-1" in out
        assert "Hosts" in out and "3" in out
        assert "Databases" in out and "9" in out
        assert "Forests" in out and "27" in out
        assert "Servers" in out and "5" in out
        assert "Groups" not in out  # filtered by report

    def test_missing_fields_render_unknown(self, capsys):
        client = StubClient(json_responses={"/manage/v2?format=json": {}})
        mc.report_cluster(client)
        out = capsys.readouterr().out
        assert "unknown" in out


# ── report_host_memory ────────────────────────────────────────────────

def _host_row(**overrides):
    base = {
        "hostname": "ml-host-1",
        "cpus": 2, "cores": 8,
        "memory-system-total-mb": 8192,
        "memory-system-free-mb": 2048,
        "memory-process-rss-mb": 3200,
        "memory-process-rss-hwm-mb": 3400,
        "memory-process-size-mb": 4000,
        "memory-process-anon-mb": 1800,
        "memory-process-swap-mb": 0,
        "memory-cache-size-mb": 1024,
        "memory-forest-size-mb": 200,
        "memory-file-size-mb": 150,
        "host-size-mb": 500,
        "memory-join-size-mb": 0,
        "memory-unclosed-size-mb": 0,
        "host-large-data-size-mb": 0,
        "memory-size-mb": 6144,
        "memory-system-pagein-rate": 0,
        "memory-system-pageout-rate": 0,
        "memory-system-swapin-rate": 0,
        "memory-system-swapout-rate": 0,
        "data-dir-space-mb": 50000,
    }
    base.update(overrides)
    return base


class TestReportHostMemory:
    def test_returns_hosts_and_prints_sections(self, capsys):
        client = StubClient(eval_xquery_results=[[[_host_row()]]])
        result = mc.report_host_memory(client)
        out = capsys.readouterr().out
        assert result and result[0]["hostname"] == "ml-host-1"
        assert "HOST MEMORY" in out
        assert "ml-host-1" in out
        assert "System Memory" in out
        assert "MarkLogic Process" in out
        assert "ML Memory Components" in out
        assert "Paging / Swap Pressure" in out

    def test_swap_flags_pressure(self, capsys):
        client = StubClient(eval_xquery_results=[
            [[_host_row(**{"memory-process-swap-mb": 512})]]
        ])
        mc.report_host_memory(client)
        out = capsys.readouterr().out
        assert "SWAPPING" in out

    def test_empty_results_returns_none(self, capsys):
        client = StubClient(eval_xquery_results=[[]])
        assert mc.report_host_memory(client) is None
        assert "Could not retrieve host memory" in capsys.readouterr().out

    def test_eval_error_caught(self, capsys):
        client = StubClient(raise_on_eval=RuntimeError("eval disabled"))
        assert mc.report_host_memory(client) is None
        out = capsys.readouterr().out
        assert "Could not retrieve host memory" in out
        assert "ML_ALLOW_EVAL=true" in out

    def test_accepts_single_host_dict_wrapped(self, capsys):
        """Collector sometimes returns [dict] rather than [[dict]]; both work."""
        client = StubClient(eval_xquery_results=[[_host_row()]])
        result = mc.report_host_memory(client)
        assert len(result) == 1


# ── report_database_stats ─────────────────────────────────────────────

def _db_status_payload(data_size=500, device_space=100000, in_memory=200,
                       large_data=0, remaining=45000, list_ratio=95,
                       triple_ratio=82, merges=0, reindex=0, backups=0,
                       forests=3, state="open"):
    return {
        "database-status": {
            "status-properties": {
                "state": {"value": state},
                "forests-count": {"value": forests},
                "data-size": {"value": data_size},
                "device-space": {"value": str(device_space)},
                "in-memory-size": {"value": in_memory},
                "large-data-size": {"value": large_data},
                "least-remaining-space-forest": {"value": str(remaining)},
                "merge-count": {"value": merges},
                "reindex-count": {"value": reindex},
                "backup-count": {"value": backups},
                "cache-properties": {
                    "list-cache-ratio": {"value": list_ratio},
                    "triple-value-cache-ratio": {"value": triple_ratio},
                },
            }
        }
    }


class TestReportDatabaseStats:
    def test_returns_disk_metrics_and_prints_sections(self, capsys):
        client = StubClient(json_responses={
            "/manage/v2/databases/Documents?view=status&format=json":
                _db_status_payload()
        })
        data_size, device_space, remaining = mc.report_database_stats(
            client, "Documents")
        assert data_size == 500
        assert device_space == 100000.0
        assert remaining == 45000.0
        out = capsys.readouterr().out
        assert "DATABASE: Documents" in out
        assert "Storage" in out
        assert "Cache Performance" in out
        assert "Activity" in out

    def test_low_list_cache_ratio_flagged(self, capsys):
        client = StubClient(json_responses={
            "/manage/v2/databases/Documents?view=status&format=json":
                _db_status_payload(list_ratio=50, triple_ratio=40)
        })
        mc.report_database_stats(client, "Documents")
        out = capsys.readouterr().out
        # Both ratios below the 80% threshold → LOW badge
        assert "LOW" in out

    def test_high_cache_ratios_marked_good(self, capsys):
        client = StubClient(json_responses={
            "/manage/v2/databases/Documents?view=status&format=json":
                _db_status_payload(list_ratio=97, triple_ratio=95)
        })
        mc.report_database_stats(client, "Documents")
        out = capsys.readouterr().out
        assert "GOOD" in out

    def test_disk_runway_printed_when_data_present(self, capsys):
        client = StubClient(json_responses={
            "/manage/v2/databases/Documents?view=status&format=json":
                _db_status_payload(data_size=1000, remaining=5000)
        })
        mc.report_database_stats(client, "Documents")
        out = capsys.readouterr().out
        assert "Disk runway" in out
        # 5000 / 1000 = 5.0x
        assert "5.0x" in out


# ── report_forest_health ──────────────────────────────────────────────

def _forest(name="Documents-1", docs=100000, active=100000, deleted=5000,
            nascent=0, stands=4, disk=500, mem=200):
    return {
        "forest-name": name,
        "document-count": docs,
        "active-fragment-count": active,
        "deleted-fragment-count": deleted,
        "nascent-fragment-count": nascent,
        "stand-count": stands,
        "disk-size-mb": disk,
        "memory-size-mb": mem,
    }


class TestReportForestHealth:
    def test_healthy_forest_shows_ok_badge(self, capsys):
        client = StubClient(eval_xquery_results=[[[_forest(active=100000, deleted=2000)]]])
        result = mc.report_forest_health(client, "Documents")
        assert result[0]["forest-name"] == "Documents-1"
        out = capsys.readouterr().out
        assert "FOREST HEALTH: Documents" in out
        assert "[OK]" in out

    def test_critical_fragmentation_triggers_merge_recommendation(self, capsys):
        # 60k deleted / 100k total = 60% → CRITICAL
        client = StubClient(eval_xquery_results=[
            [[_forest(active=40000, deleted=60000, disk=1000)]]
        ])
        mc.report_forest_health(client, "Documents")
        out = capsys.readouterr().out
        assert "CRITICAL" in out
        assert "Merge recommended" in out
        assert "xdmp:merge" in out

    def test_moderate_fragmentation_flagged(self, capsys):
        # 15% deleted fragments → MODERATE badge
        client = StubClient(eval_xquery_results=[
            [[_forest(active=85000, deleted=15000)]]
        ])
        mc.report_forest_health(client, "Documents")
        out = capsys.readouterr().out
        assert "MODERATE" in out

    def test_empty_results_returns_empty_list(self, capsys):
        client = StubClient(eval_xquery_results=[[]])
        assert mc.report_forest_health(client, "Documents") == []
        assert "Could not retrieve forest counts" in capsys.readouterr().out

    def test_eval_error_returns_empty(self, capsys):
        client = StubClient(raise_on_eval=RuntimeError("no eval"))
        assert mc.report_forest_health(client, "Documents") == []
        assert "Could not retrieve forest details" in capsys.readouterr().out


# ── report_index_config ───────────────────────────────────────────────

def _db_properties(**overrides):
    base = {
        "word-searches": True,
        "word-positions": False,
        "fast-phrase-searches": True,
        "triple-index": False,
        "uri-lexicon": True,
        "collection-lexicon": True,
        "trailing-wildcard-searches": False,
        "range-element-index": [
            {"localname": "severity", "scalar-type": "int"},
            {"localname": "metric",   "scalar-type": "string"},
        ],
        "range-path-index": [
            {"path-expression": "/doc/score", "scalar-type": "double"},
        ],
        "range-field-index": [],
        "in-memory-limit": 32768,
        "in-memory-list-size": 64,
        "in-memory-tree-size": 16,
        "in-memory-range-index-size": 2,
        "in-memory-reverse-index-size": 2,
        "in-memory-triple-index-size": 16,
    }
    base.update(overrides)
    return base


class TestReportIndexConfig:
    def test_returns_props_range_and_enabled_count(self, capsys):
        client = StubClient(json_responses={
            "/manage/v2/databases/Documents/properties?format=json": _db_properties()
        })
        props, total_range, enabled_count = mc.report_index_config(client, "Documents")
        assert total_range == 3  # 2 element + 1 path + 0 field
        # Four boolean indexes enabled above (word-searches, fast-phrase-searches,
        # uri-lexicon, collection-lexicon)
        assert enabled_count == 4
        assert props.get("in-memory-limit") == 32768

    def test_prints_range_index_detail_and_in_memory_settings(self, capsys):
        client = StubClient(json_responses={
            "/manage/v2/databases/Documents/properties?format=json": _db_properties()
        })
        mc.report_index_config(client, "Documents")
        out = capsys.readouterr().out
        assert "INDEX CONFIGURATION: Documents" in out
        assert "severity (int)" in out
        assert "/doc/score (double)" in out
        assert "In-Memory Settings" in out
        assert "32,768 KB" in out


# ── report_index_memory ───────────────────────────────────────────────

class TestReportIndexMemory:
    def test_preload_disabled_warning(self, capsys):
        client = StubClient(
            json_responses={
                "/manage/v2/databases/Documents/properties?format=json":
                    {"preload-mapped-data": False}
            },
            eval_js_results=[[{"indexes": [], "standSummaries": []}]],
        )
        mc.report_index_memory(client, "Documents")
        out = capsys.readouterr().out
        assert "preload-mapped-data is DISABLED" in out

    def test_renders_per_index_breakdown(self, capsys):
        client = StubClient(
            json_responses={
                "/manage/v2/databases/Documents/properties?format=json":
                    {"preload-mapped-data": True}
            },
            eval_js_results=[[{
                "indexes": [
                    {"indexType": "rangeElementIndex",
                     "localname": "severity",
                     "scalarType": "int",
                     "totalMemoryBytes": 5 * 1024 * 1024,
                     "totalOnDiskBytes": 2 * 1024 * 1024},
                ],
                "standSummaries": [
                    {"standPath": "/var/forest/Stand-1",
                     "diskSize": 1000, "memorySize": 200,
                     "summary": {
                         "rangeIndexesBytes": 5 * 1024 * 1024,
                         "listFileBytes": 3 * 1024 * 1024,
                         "treeFileBytes": 1 * 1024 * 1024,
                     }},
                ],
            }]],
        )
        mc.report_index_memory(client, "Documents")
        out = capsys.readouterr().out
        assert "INDEX MEMORY USAGE: Documents" in out
        assert "Element Range Index" in out
        assert "severity (int)" in out
        assert "Range indexes" in out  # component label
        # preload was True -> no warning
        assert "DISABLED" not in out

    def test_eval_failure_falls_through_gracefully(self, capsys):
        client = StubClient(
            json_responses={
                "/manage/v2/databases/Documents/properties?format=json":
                    {"preload-mapped-data": True}
            },
            raise_on_eval=RuntimeError("no eval"),
        )
        mc.report_index_memory(client, "Documents")
        out = capsys.readouterr().out
        assert "Could not retrieve index memory" in out
        assert "ML 11+" in out

    def test_empty_js_results(self, capsys):
        client = StubClient(
            json_responses={
                "/manage/v2/databases/Documents/properties?format=json":
                    {"preload-mapped-data": True}
            },
            eval_js_results=[[]],
        )
        mc.report_index_memory(client, "Documents")
        out = capsys.readouterr().out
        assert "Could not retrieve index memory data" in out
