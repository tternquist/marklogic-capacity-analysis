"""Unit tests for ml_capacity.main CLI entry point.

Focus on dispatch paths that don't require a live MarkLogic — those paths
exit before constructing the client. Server-dependent paths are exercised
by mocking collect_snapshot + report_* so we verify routing, not math.
"""

import json
import sys
from pathlib import Path
from urllib.error import HTTPError, URLError

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import ml_capacity as mc
# ml_capacity/__init__.py re-exports `main` as a function, which ends up
# shadowing the submodule attribute. Reach the module object via sys.modules.
import ml_capacity.main  # ensure loaded
main_mod = sys.modules["ml_capacity.main"]


@pytest.fixture
def base_argv():
    """Minimal argv — callers append their own flags."""
    return ["mlca", "--password", "secret", "--database", "Documents"]


@pytest.fixture
def stub_snapshot():
    """A minimal snapshot dict the non-text output paths expect."""
    return {
        "version": 1,
        "timestamp": "2026-04-10T10:00:00+00:00",
        "database": "Documents",
        "cluster": {"name": "c", "version": "12.0-1"},
        "hosts": [],
        "database_status": {},
        "forests": [],
        "db_properties": {},
        "index_counts": {},
        "index_memory": None,
        "totals": {
            "documents": 0, "active_fragments": 0, "deleted_fragments": 0,
            "forest_disk_mb": 0, "forest_memory_mb": 0, "host_rss_mb": 0,
        },
    }


# ── Early-exit paths that do NOT require a MarkLogic connection ───────

class TestInvalidDatabaseName:
    def test_rejects_dangerous_name(self, monkeypatch, capsys):
        monkeypatch.setattr(sys, "argv",
                            ["mlca", "--database", 'evil"; xdmp:shutdown()'])
        with pytest.raises(SystemExit) as exc:
            main_mod.main()
        assert exc.value.code == 1
        out = capsys.readouterr().out
        assert "ERROR" in out


class TestSnapshotsFlag:
    def test_lists_and_exits_without_client(self, monkeypatch, capsys, tmp_path):
        """--snapshots must not prompt for password or open a connection."""
        monkeypatch.setattr(main_mod, "SNAPSHOT_DIR", tmp_path)

        called = {"client": False, "getpass": False}
        def fail_client(*a, **kw):
            called["client"] = True
            raise AssertionError("MarkLogicClient should not be constructed")
        def fail_getpass(*a, **kw):
            called["getpass"] = True
            raise AssertionError("getpass should not be called")

        monkeypatch.setattr(main_mod, "MarkLogicClient", fail_client)
        monkeypatch.setattr(main_mod.getpass, "getpass", fail_getpass)
        monkeypatch.setattr(sys, "argv",
                            ["mlca", "--database", "Documents", "--snapshots"])

        with pytest.raises(SystemExit) as exc:
            main_mod.main()
        assert exc.value.code == 0
        assert not called["client"]
        assert not called["getpass"]
        out = capsys.readouterr().out
        assert "SAVED SNAPSHOTS: Documents" in out


class TestImportSnapshot:
    def test_imports_valid_file_and_exits(self, monkeypatch, tmp_path, capsys):
        # Prepare a valid snapshot file for import
        snap = {
            "version": 1,
            "timestamp": "2026-04-10T10:00:00+00:00",
            "database": "Documents",
            "hosts": [], "forests": [],
            "totals": {"documents": 42},
        }
        src = tmp_path / "snap.json"
        src.write_text(json.dumps(snap))

        import ml_capacity.snapshot as snap_mod
        monkeypatch.setattr(snap_mod, "SNAPSHOT_DIR", tmp_path / "store")
        (tmp_path / "store").mkdir()

        monkeypatch.setattr(main_mod, "MarkLogicClient",
                            lambda *a, **kw: pytest.fail("no client"))
        monkeypatch.setattr(sys, "argv", [
            "mlca", "--database", "Documents",
            "--import-snapshot", str(src),
        ])

        with pytest.raises(SystemExit) as exc:
            main_mod.main()
        assert exc.value.code == 0
        out = capsys.readouterr().out
        assert "Import complete" in out

    def test_invalid_file_exits_nonzero(self, monkeypatch, tmp_path, capsys):
        missing = tmp_path / "does-not-exist.json"
        monkeypatch.setattr(main_mod, "MarkLogicClient",
                            lambda *a, **kw: pytest.fail("no client"))
        monkeypatch.setattr(sys, "argv", [
            "mlca", "--database", "Documents",
            "--import-snapshot", str(missing),
        ])
        with pytest.raises(SystemExit) as exc:
            main_mod.main()
        assert exc.value.code == 1


# ── Server-dependent dispatch paths, with collect/report mocked ───────

class TestFormatJson:
    def test_emits_json_and_exits(self, monkeypatch, capsys, base_argv, stub_snapshot):
        monkeypatch.setattr(main_mod, "collect_snapshot", lambda c, db: stub_snapshot)
        saved = []
        monkeypatch.setattr(main_mod, "save_snapshot", lambda s: saved.append(s))
        monkeypatch.setattr(main_mod, "MarkLogicClient",
                            lambda *a, **kw: object())
        monkeypatch.setattr(sys, "argv", base_argv + ["--format", "json"])

        with pytest.raises(SystemExit) as exc:
            main_mod.main()
        assert exc.value.code == 0
        out = capsys.readouterr().out
        payload = json.loads(out)
        assert payload["database"] == "Documents"
        assert saved == [stub_snapshot]

    def test_no_snapshot_flag_skips_save(self, monkeypatch, capsys,
                                         base_argv, stub_snapshot):
        monkeypatch.setattr(main_mod, "collect_snapshot", lambda c, db: stub_snapshot)
        def should_not_save(s):
            pytest.fail("save_snapshot should not run with --no-snapshot")
        monkeypatch.setattr(main_mod, "save_snapshot", should_not_save)
        monkeypatch.setattr(main_mod, "MarkLogicClient", lambda *a, **kw: object())
        monkeypatch.setattr(sys, "argv",
                            base_argv + ["--format", "json", "--no-snapshot"])

        with pytest.raises(SystemExit) as exc:
            main_mod.main()
        assert exc.value.code == 0


class TestFormatPrometheus:
    def test_emits_prometheus_and_exits(self, monkeypatch, capsys, base_argv,
                                        stub_snapshot):
        monkeypatch.setattr(main_mod, "collect_snapshot", lambda c, db: stub_snapshot)
        monkeypatch.setattr(main_mod, "save_snapshot", lambda s: None)
        monkeypatch.setattr(main_mod, "MarkLogicClient", lambda *a, **kw: object())
        monkeypatch.setattr(main_mod, "snapshot_to_prometheus",
                            lambda s: "mlca_documents 0\n")
        monkeypatch.setattr(sys, "argv", base_argv + ["--format", "prometheus"])

        with pytest.raises(SystemExit) as exc:
            main_mod.main()
        assert exc.value.code == 0
        assert "mlca_documents 0" in capsys.readouterr().out


class TestSnapshotOnly:
    def test_collects_and_saves_then_exits(self, monkeypatch, base_argv,
                                           stub_snapshot):
        calls = {"collect": 0, "save": 0, "report": 0}
        monkeypatch.setattr(main_mod, "collect_snapshot",
                            lambda c, db: (calls.__setitem__("collect", calls["collect"] + 1), stub_snapshot)[1])
        monkeypatch.setattr(main_mod, "save_snapshot",
                            lambda s: (calls.__setitem__("save", calls["save"] + 1), Path("/tmp/snap.json"))[1])
        monkeypatch.setattr(main_mod, "prune_snapshots", lambda n: 0)
        monkeypatch.setattr(main_mod, "MarkLogicClient", lambda *a, **kw: object())
        # Any report_ call should NOT happen
        monkeypatch.setattr(main_mod, "report_cluster",
                            lambda c: pytest.fail("report should not run"))
        monkeypatch.setattr(sys, "argv", base_argv + ["--snapshot-only"])

        with pytest.raises(SystemExit) as exc:
            main_mod.main()
        assert exc.value.code == 0
        assert calls["collect"] == 1
        assert calls["save"] == 1


class TestFullReportDispatch:
    def test_invokes_all_report_sections_in_order(self, monkeypatch, base_argv,
                                                  stub_snapshot):
        order = []
        def rec(name, *ret):
            def _fn(*a, **kw):
                order.append(name)
                return ret[0] if ret else None
            return _fn

        monkeypatch.setattr(main_mod, "collect_snapshot", lambda c, db: stub_snapshot)
        monkeypatch.setattr(main_mod, "save_snapshot", lambda s: Path("/tmp/x.json"))
        monkeypatch.setattr(main_mod, "prune_snapshots", lambda n: 0)
        monkeypatch.setattr(main_mod, "MarkLogicClient", lambda *a, **kw: object())

        monkeypatch.setattr(main_mod, "report_cluster", rec("cluster"))
        monkeypatch.setattr(main_mod, "report_host_memory", rec("host", []))
        monkeypatch.setattr(main_mod, "report_database_stats",
                            rec("dbstats", (0, 0, 0)))
        monkeypatch.setattr(main_mod, "report_forest_health", rec("forest", []))
        monkeypatch.setattr(main_mod, "report_index_config",
                            rec("idxcfg", ({}, 0, 0)))
        monkeypatch.setattr(main_mod, "report_index_memory", rec("idxmem"))
        monkeypatch.setattr(main_mod, "report_capacity_estimate", rec("capacity"))

        monkeypatch.setattr(sys, "argv", base_argv)
        main_mod.main()  # no SystemExit on success path

        assert order == ["cluster", "host", "dbstats", "forest",
                         "idxcfg", "idxmem", "capacity"]


class TestTrendMode:
    def test_trend_dispatches_to_report_trend_and_exits(self, monkeypatch,
                                                        base_argv, stub_snapshot):
        called = []
        monkeypatch.setattr(main_mod, "collect_snapshot", lambda c, db: stub_snapshot)
        monkeypatch.setattr(main_mod, "save_snapshot", lambda s: Path("/tmp/x.json"))
        monkeypatch.setattr(main_mod, "prune_snapshots", lambda n: 0)
        monkeypatch.setattr(main_mod, "MarkLogicClient", lambda *a, **kw: object())
        monkeypatch.setattr(main_mod, "report_trend",
                            lambda db: called.append(db))
        # Full-report fns must not run
        monkeypatch.setattr(main_mod, "report_cluster",
                            lambda c: pytest.fail("no full report"))
        monkeypatch.setattr(sys, "argv", base_argv + ["--trend"])

        with pytest.raises(SystemExit) as exc:
            main_mod.main()
        assert exc.value.code == 0
        assert called == ["Documents"]


# ── Error paths ───────────────────────────────────────────────────────

class TestErrorHandling:
    def _run(self, monkeypatch, base_argv, exc):
        def raising(*a, **kw):
            raise exc
        monkeypatch.setattr(main_mod, "collect_snapshot", raising)
        monkeypatch.setattr(main_mod, "MarkLogicClient", lambda *a, **kw: object())
        monkeypatch.setattr(sys, "argv", base_argv)
        with pytest.raises(SystemExit) as ex:
            main_mod.main()
        return ex.value.code

    def test_401_prints_auth_hint(self, monkeypatch, base_argv, capsys):
        import io
        err = HTTPError("http://x", 401, "Unauthorized", {},
                        io.BytesIO(b"auth failed"))
        code = self._run(monkeypatch, base_argv, err)
        out = capsys.readouterr().out
        assert code == 1
        assert "HTTP 401" in out
        assert "username/password" in out

    def test_404_prints_database_hint(self, monkeypatch, base_argv, capsys):
        import io
        err = HTTPError("http://x", 404, "Not Found", {},
                        io.BytesIO(b"not found"))
        code = self._run(monkeypatch, base_argv, err)
        out = capsys.readouterr().out
        assert code == 1
        assert "not found" in out.lower()

    def test_url_error_prints_connection_hint(self, monkeypatch, base_argv, capsys):
        err = URLError("connection refused")
        code = self._run(monkeypatch, base_argv, err)
        out = capsys.readouterr().out
        assert code == 1
        assert "Cannot connect" in out


# ── Password prompting ────────────────────────────────────────────────

class TestPasswordPrompt:
    def test_prompts_when_password_missing(self, monkeypatch, stub_snapshot):
        prompted = []
        monkeypatch.setattr(main_mod.getpass, "getpass",
                            lambda p: (prompted.append(p), "typed-pw")[1])
        monkeypatch.setattr(main_mod, "MarkLogicClient",
                            lambda host, port, user, pw, auth: (prompted.append(pw), object())[1])
        monkeypatch.setattr(main_mod, "collect_snapshot", lambda c, db: stub_snapshot)
        monkeypatch.setattr(main_mod, "save_snapshot", lambda s: Path("/tmp/x.json"))
        monkeypatch.setattr(sys, "argv", [
            "mlca", "--database", "Documents",
            "--format", "json",  # quick exit path
        ])

        with pytest.raises(SystemExit):
            main_mod.main()
        # Prompt was issued, password was passed to client
        assert any("Password for" in p for p in prompted if isinstance(p, str))
        assert "typed-pw" in prompted
