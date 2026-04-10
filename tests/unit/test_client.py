"""Unit tests for MarkLogicClient methods that don't require a live connection."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import ml_capacity as mc


class TestBasicAuthHeader:
    def test_encodes_credentials(self):
        client = mc.MarkLogicClient("localhost", 8002, "admin", "secret", "basic")
        header = client._basic_auth_header()
        assert header.startswith("Basic ")
        # Decode and verify
        import base64
        decoded = base64.b64decode(header.split(" ", 1)[1]).decode()
        assert decoded == "admin:secret"

    def test_special_characters_in_password(self):
        client = mc.MarkLogicClient("localhost", 8002, "user", "p@ss:w0rd!", "basic")
        header = client._basic_auth_header()
        import base64
        decoded = base64.b64decode(header.split(" ", 1)[1]).decode()
        assert decoded == "user:p@ss:w0rd!"


class TestDigestResponse:
    def test_produces_digest_header(self):
        client = mc.MarkLogicClient("localhost", 8002, "admin", "admin", "digest")
        www_auth = (
            'Digest realm="public", '
            'nonce="abc123def456", '
            'qop="auth", '
            'opaque="opaque789"'
        )
        result = client._digest_response(www_auth, "GET", "/manage/v2")
        assert result.startswith("Digest ")
        assert 'username="admin"' in result
        assert 'realm="public"' in result
        assert 'nonce="abc123def456"' in result
        assert 'uri="/manage/v2"' in result
        assert 'qop=auth' in result
        assert 'opaque="opaque789"' in result
        assert "response=" in result

    def test_without_opaque(self):
        client = mc.MarkLogicClient("localhost", 8002, "admin", "admin", "digest")
        www_auth = 'Digest realm="public", nonce="abc123", qop="auth"'
        result = client._digest_response(www_auth, "GET", "/v1/eval")
        assert result.startswith("Digest ")
        assert 'uri="/v1/eval"' in result
        assert "opaque=" not in result

    def test_different_methods(self):
        client = mc.MarkLogicClient("localhost", 8002, "admin", "admin", "digest")
        www_auth = 'Digest realm="public", nonce="abc123", qop="auth"'
        get_result = client._digest_response(www_auth, "GET", "/path")
        post_result = client._digest_response(www_auth, "POST", "/path")
        # Different methods should produce different response hashes
        # (the HA2 component differs)
        assert get_result != post_result


class TestParseEvalResponse:
    def test_json_body(self):
        client = mc.MarkLogicClient("localhost", 8002, "admin", "admin")
        text = (
            '--boundary\r\n'
            'Content-Type: application/json\r\n'
            '\r\n'
            '{"key": "value"}\r\n'
            '--boundary--'
        )
        results = client._parse_eval_response(text)
        assert len(results) >= 1
        assert results[0] == {"key": "value"}

    def test_multiple_results(self):
        client = mc.MarkLogicClient("localhost", 8002, "admin", "admin")
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
        results = client._parse_eval_response(text)
        assert 42 in results
        assert "hello" in results

    def test_non_json_body(self):
        client = mc.MarkLogicClient("localhost", 8002, "admin", "admin")
        text = (
            '--boundary\r\n'
            'Content-Type: text/plain\r\n'
            '\r\n'
            'plain text result\r\n'
            '--boundary--'
        )
        results = client._parse_eval_response(text)
        assert any("plain text result" in str(r) for r in results)

    def test_empty_response(self):
        client = mc.MarkLogicClient("localhost", 8002, "admin", "admin")
        results = client._parse_eval_response("")
        assert results == []

    def test_array_result(self):
        client = mc.MarkLogicClient("localhost", 8002, "admin", "admin")
        text = (
            '--boundary\r\n'
            'Content-Type: application/json\r\n'
            '\r\n'
            '[{"name":"forest-1"},{"name":"forest-2"}]\r\n'
            '--boundary--'
        )
        results = client._parse_eval_response(text)
        assert len(results) >= 1
        assert isinstance(results[0], list)
        assert len(results[0]) == 2

    def test_newline_separator(self):
        """Some responses use \\n\\n instead of \\r\\n\\r\\n."""
        client = mc.MarkLogicClient("localhost", 8002, "admin", "admin")
        text = (
            '--boundary\n'
            'Content-Type: application/json\n'
            '\n'
            '{"result": true}\n'
            '--boundary--'
        )
        results = client._parse_eval_response(text)
        assert len(results) >= 1
        assert results[0] == {"result": True}


class TestClientInit:
    def test_base_url(self):
        client = mc.MarkLogicClient("myhost", 9000, "user", "pass")
        assert client.base == "http://myhost:9000"

    def test_default_auth_type(self):
        client = mc.MarkLogicClient("localhost", 8002, "admin", "admin")
        assert client.auth_type == "digest"

    def test_basic_auth_type(self):
        client = mc.MarkLogicClient("localhost", 8002, "admin", "admin", "basic")
        assert client.auth_type == "basic"
