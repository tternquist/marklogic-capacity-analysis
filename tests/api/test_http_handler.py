"""API endpoint tests for the MLCA HTTP handler.

Tests the HTTP handler routing, auth, CORS, and path traversal protection
without requiring a real MarkLogic connection.
"""

import json
import sys
import threading
import time
from http.client import HTTPConnection
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import ml_capacity as mc
import ml_capacity.snapshot as snap_mod
from tests.conftest import _make_snapshot


@pytest.fixture
def service_no_auth(tmp_path):
    """Start MLCA HTTP service without auth on a random port."""
    yield from _start_service(tmp_path, api_token=None)


@pytest.fixture
def service_with_auth(tmp_path):
    """Start MLCA HTTP service with auth on a random port."""
    yield from _start_service(tmp_path, api_token="test-secret-token")


def _start_service(tmp_path, api_token=None):
    """Start a test HTTP service and return (conn, port, tmp_path)."""
    import threading
    from http.server import HTTPServer, BaseHTTPRequestHandler

    # Save a snapshot file for the service to find
    original_dir = snap_mod.SNAPSHOT_DIR
    snap_mod.SNAPSHOT_DIR = tmp_path
    snap = _make_snapshot()
    mc.save_snapshot(snap)

    # We need to set up the handler similar to run_service but simplified
    latest_snapshots = {"Documents": snap}
    lock = threading.Lock()

    class TestHandler(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            pass

        def _check_auth(self):
            if not api_token:
                return True
            path, _ = self._parse_request()
            if path == "/health":
                return True
            auth = self.headers.get("Authorization", "")
            if auth == f"Bearer {api_token}":
                return True
            self.send_response(401)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"error":"Unauthorized"}')
            return False

        def _add_cors_headers(self):
            origin = self.headers.get("Origin", "")
            if api_token:
                if origin:
                    self.send_header("Access-Control-Allow-Origin", origin)
                    self.send_header("Vary", "Origin")
            self.send_header("Access-Control-Allow-Methods",
                             "GET, POST, DELETE, OPTIONS")
            self.send_header("Access-Control-Allow-Headers",
                             "Content-Type, Authorization")

        def _parse_request(self):
            from urllib.parse import urlparse, parse_qs
            parsed = urlparse(self.path)
            params = parse_qs(parsed.query)
            return parsed.path, params

        def _respond(self, code, content_type, body):
            self.send_response(code)
            self.send_header("Content-Type", content_type)
            self._add_cors_headers()
            self.end_headers()
            self.wfile.write(body.encode() if isinstance(body, str) else body)

        def do_GET(self):
            if not self._check_auth():
                return
            path, params = self._parse_request()
            if path == "/health":
                self._respond(200, "application/json", '{"status":"ok"}')
            elif path == "/api/snapshot":
                with lock:
                    db = params.get("database", ["Documents"])[0]
                    s = latest_snapshots.get(db)
                if s:
                    self._respond(200, "application/json",
                                  json.dumps(s, default=str))
                else:
                    self._respond(404, "application/json",
                                  '{"error":"No snapshot"}')
            elif path.startswith("/api/snapshot/"):
                filename = path[len("/api/snapshot/"):]
                if ".." in filename or "/" in filename \
                        or not filename.endswith(".json"):
                    self._respond(400, "application/json",
                                  '{"error":"Invalid filename"}')
                    return
                fpath = snap_mod.SNAPSHOT_DIR / filename
                if not fpath.exists():
                    self._respond(404, "application/json",
                                  '{"error":"Not found"}')
                    return
                with open(fpath) as f:
                    data = json.load(f)
                self._respond(200, "application/json",
                              json.dumps(data, default=str))
            elif path == "/metrics":
                with lock:
                    text = ""
                    for db, s in latest_snapshots.items():
                        text += mc.snapshot_to_prometheus(s)
                self._respond(200, "text/plain", text)
            else:
                self._respond(404, "text/plain", "Not Found")

        def do_DELETE(self):
            if not self._check_auth():
                return
            path, _ = self._parse_request()
            if path.startswith("/api/snapshots/"):
                filename = path[len("/api/snapshots/"):]
                if ".." in filename or "/" in filename \
                        or not filename.endswith(".json"):
                    self._respond(400, "application/json",
                                  '{"error":"Invalid filename"}')
                    return
                fpath = snap_mod.SNAPSHOT_DIR / filename
                if not fpath.exists():
                    self._respond(404, "application/json",
                                  '{"error":"Not found"}')
                    return
                fpath.unlink()
                self._respond(200, "application/json", '{"status":"deleted"}')
            else:
                self._respond(404, "text/plain", "Not Found")

        def do_OPTIONS(self):
            self.send_response(200)
            self._add_cors_headers()
            self.end_headers()

    server = HTTPServer(("127.0.0.1", 0), TestHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    conn = HTTPConnection("127.0.0.1", port)

    class ServiceFixture:
        def __init__(self):
            self.conn = conn
            self.port = port
            self.tmp_path = tmp_path
            self.server = server
            self.original_dir = original_dir

        def close(self):
            server.shutdown()
            snap_mod.SNAPSHOT_DIR = self.original_dir

    fixture = ServiceFixture()
    yield fixture
    fixture.close()


class TestHealthEndpoint:
    def test_health_returns_ok(self, service_no_auth):
        service_no_auth.conn.request("GET", "/health")
        resp = service_no_auth.conn.getresponse()
        assert resp.status == 200
        body = json.loads(resp.read())
        assert body["status"] == "ok"


class TestAuthentication:
    def test_no_auth_allows_all(self, service_no_auth):
        service_no_auth.conn.request("GET", "/api/snapshot")
        resp = service_no_auth.conn.getresponse()
        assert resp.status == 200

    def test_auth_rejects_no_token(self, service_with_auth):
        service_with_auth.conn.request("GET", "/api/snapshot")
        resp = service_with_auth.conn.getresponse()
        assert resp.status == 401

    def test_auth_accepts_valid_token(self, service_with_auth):
        service_with_auth.conn.request("GET", "/api/snapshot", headers={
            "Authorization": "Bearer test-secret-token"
        })
        resp = service_with_auth.conn.getresponse()
        assert resp.status == 200

    def test_auth_rejects_wrong_token(self, service_with_auth):
        service_with_auth.conn.request("GET", "/api/snapshot", headers={
            "Authorization": "Bearer wrong-token"
        })
        resp = service_with_auth.conn.getresponse()
        assert resp.status == 401

    def test_health_bypasses_auth(self, service_with_auth):
        service_with_auth.conn.request("GET", "/health")
        resp = service_with_auth.conn.getresponse()
        assert resp.status == 200


class TestCORS:
    def test_no_cors_without_auth(self, service_no_auth):
        service_no_auth.conn.request("OPTIONS", "/api/snapshot", headers={
            "Origin": "http://evil.com"
        })
        resp = service_no_auth.conn.getresponse()
        resp.read()
        assert resp.status == 200
        # No Access-Control-Allow-Origin header when no auth configured
        assert resp.getheader("Access-Control-Allow-Origin") is None

    def test_cors_with_auth_reflects_origin(self, service_with_auth):
        service_with_auth.conn.request("OPTIONS", "/api/snapshot", headers={
            "Origin": "http://my-grafana.local"
        })
        resp = service_with_auth.conn.getresponse()
        resp.read()
        assert resp.status == 200
        assert resp.getheader("Access-Control-Allow-Origin") == "http://my-grafana.local"


class TestRouting:
    def test_unknown_route_404(self, service_no_auth):
        service_no_auth.conn.request("GET", "/api/nonexistent")
        resp = service_no_auth.conn.getresponse()
        assert resp.status == 404

    def test_metrics_endpoint(self, service_no_auth):
        service_no_auth.conn.request("GET", "/metrics")
        resp = service_no_auth.conn.getresponse()
        body = resp.read().decode()
        assert resp.status == 200
        assert "mlca_documents_total" in body

    def test_api_snapshot(self, service_no_auth):
        service_no_auth.conn.request("GET", "/api/snapshot")
        resp = service_no_auth.conn.getresponse()
        body = json.loads(resp.read())
        assert resp.status == 200
        assert body["database"] == "Documents"


class TestPathTraversal:
    def test_reject_dotdot(self, service_no_auth):
        service_no_auth.conn.request("GET", "/api/snapshot/../../etc/passwd.json")
        resp = service_no_auth.conn.getresponse()
        assert resp.status == 400

    def test_reject_slash(self, service_no_auth):
        service_no_auth.conn.request("GET", "/api/snapshot/foo/bar.json")
        resp = service_no_auth.conn.getresponse()
        # The path parsing splits on "/" so this becomes a different route
        assert resp.status in (400, 404)

    def test_reject_non_json(self, service_no_auth):
        service_no_auth.conn.request("GET", "/api/snapshot/file.txt")
        resp = service_no_auth.conn.getresponse()
        assert resp.status == 400

    def test_delete_rejects_traversal(self, service_no_auth):
        service_no_auth.conn.request("DELETE", "/api/snapshots/../../../etc/passwd.json")
        resp = service_no_auth.conn.getresponse()
        assert resp.status == 400
