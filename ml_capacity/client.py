"""HTTP client for MarkLogic Management API (no external deps)."""

import json
from base64 import b64encode
from urllib.error import HTTPError
from urllib.request import Request, urlopen

HTTP_TIMEOUT = 30  # seconds — applied to all urlopen() calls


class MarkLogicClient:
    def __init__(self, host, port, user, password, auth_type="digest"):
        self.base = f"http://{host}:{port}"
        self.user = user
        self.password = password
        self.auth_type = auth_type

    def _basic_auth_header(self):
        creds = b64encode(f"{self.user}:{self.password}".encode()).decode()
        return f"Basic {creds}"

    def get_json(self, path):
        url = f"{self.base}{path}"
        headers = {"Accept": "application/json"}

        if self.auth_type == "basic":
            headers["Authorization"] = self._basic_auth_header()
            req = Request(url, headers=headers)
            with urlopen(req, timeout=HTTP_TIMEOUT) as resp:
                return json.loads(resp.read())

        # Digest auth
        try:
            req = Request(url, headers=headers)
            with urlopen(req, timeout=HTTP_TIMEOUT) as resp:
                return json.loads(resp.read())
        except HTTPError as e:
            if e.code != 401:
                raise
            auth_header = e.headers.get("WWW-Authenticate", "")
            if "Digest" not in auth_header:
                raise
            digest_resp = self._digest_response(auth_header, "GET", path)
            headers["Authorization"] = digest_resp
            req = Request(url, headers=headers)
            with urlopen(req, timeout=HTTP_TIMEOUT) as resp:
                return json.loads(resp.read())

    def _digest_response(self, www_auth, method, uri):
        import hashlib
        import re
        import time

        fields = {}
        for m in re.finditer(r'(\w+)=["\']?([^"\',$]+)["\']?', www_auth):
            fields[m.group(1)] = m.group(2)

        realm = fields.get("realm", "")
        nonce = fields.get("nonce", "")
        qop = fields.get("qop", "auth")
        opaque = fields.get("opaque", "")

        nc = "00000001"
        cnonce = hashlib.md5(str(time.time()).encode()).hexdigest()[:16]

        ha1 = hashlib.md5(f"{self.user}:{realm}:{self.password}".encode()).hexdigest()
        ha2 = hashlib.md5(f"{method}:{uri}".encode()).hexdigest()

        if qop:
            response = hashlib.md5(
                f"{ha1}:{nonce}:{nc}:{cnonce}:{qop}:{ha2}".encode()
            ).hexdigest()
        else:
            response = hashlib.md5(f"{ha1}:{nonce}:{ha2}".encode()).hexdigest()

        parts = [
            f'username="{self.user}"',
            f'realm="{realm}"',
            f'nonce="{nonce}"',
            f'uri="{uri}"',
            f'response="{response}"',
        ]
        if qop:
            parts += [f"qop={qop}", f"nc={nc}", f'cnonce="{cnonce}"']
        if opaque:
            parts.append(f'opaque="{opaque}"')

        return "Digest " + ", ".join(parts)

    def eval_xquery(self, xquery, database=None):
        """POST to /v1/eval for XQuery evaluation."""
        from urllib.parse import urlencode

        path = "/v1/eval"
        if database:
            path += f"?database={database}"

        url = f"{self.base}{path}"
        body = urlencode({"xquery": xquery}).encode()
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        }

        if self.auth_type == "basic":
            headers["Authorization"] = self._basic_auth_header()

        # First attempt (may need digest challenge)
        try:
            req = Request(url, data=body, headers=headers, method="POST")
            with urlopen(req, timeout=HTTP_TIMEOUT) as resp:
                return self._parse_eval_response(resp.read().decode())
        except HTTPError as e:
            if e.code != 401 or self.auth_type == "basic":
                raise
            auth_header = e.headers.get("WWW-Authenticate", "")
            if "Digest" not in auth_header:
                raise
            headers["Authorization"] = self._digest_response(
                auth_header, "POST", path
            )
            req = Request(url, data=body, headers=headers, method="POST")
            with urlopen(req, timeout=HTTP_TIMEOUT) as resp:
                return self._parse_eval_response(resp.read().decode())

    def eval_javascript(self, javascript, database=None, vars=None):
        """POST to /v1/eval for Server-Side JavaScript evaluation."""
        from urllib.parse import urlencode

        path = "/v1/eval"
        if database:
            path += f"?database={database}"

        body_parts = {"javascript": javascript}
        if vars:
            body_parts["vars"] = json.dumps(vars)

        body = urlencode(body_parts).encode()
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        }

        if self.auth_type == "basic":
            headers["Authorization"] = self._basic_auth_header()

        try:
            req = Request(self.base + path, data=body, headers=headers, method="POST")
            with urlopen(req, timeout=HTTP_TIMEOUT) as resp:
                return self._parse_eval_response(resp.read().decode())
        except HTTPError as e:
            if e.code != 401 or self.auth_type == "basic":
                raise
            auth_header = e.headers.get("WWW-Authenticate", "")
            if "Digest" not in auth_header:
                raise
            headers["Authorization"] = self._digest_response(
                auth_header, "POST", path
            )
            req = Request(self.base + path, data=body, headers=headers, method="POST")
            with urlopen(req, timeout=HTTP_TIMEOUT) as resp:
                return self._parse_eval_response(resp.read().decode())

    def put_json(self, path, data):
        """PUT JSON to a Management API endpoint."""
        url = f"{self.base}{path}"
        body = json.dumps(data).encode()
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        if self.auth_type == "basic":
            headers["Authorization"] = self._basic_auth_header()

        try:
            req = Request(url, data=body, headers=headers, method="PUT")
            with urlopen(req, timeout=HTTP_TIMEOUT) as resp:
                return resp.status
        except HTTPError as e:
            if e.code != 401 or self.auth_type == "basic":
                raise
            auth_header = e.headers.get("WWW-Authenticate", "")
            if "Digest" not in auth_header:
                raise
            headers["Authorization"] = self._digest_response(
                auth_header, "PUT", path
            )
            req = Request(url, data=body, headers=headers, method="PUT")
            with urlopen(req, timeout=HTTP_TIMEOUT) as resp:
                return resp.status

    def post_json(self, path, data=None):
        """POST JSON to a Management API endpoint; return HTTP status."""
        url = f"{self.base}{path}"
        body = json.dumps(data).encode() if data is not None else b""
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        if self.auth_type == "basic":
            headers["Authorization"] = self._basic_auth_header()

        try:
            req = Request(url, data=body, headers=headers, method="POST")
            with urlopen(req, timeout=HTTP_TIMEOUT) as resp:
                return resp.status
        except HTTPError as e:
            if e.code not in (401,) or self.auth_type == "basic":
                raise
            auth_header = e.headers.get("WWW-Authenticate", "")
            if "Digest" not in auth_header:
                raise
            headers["Authorization"] = self._digest_response(
                auth_header, "POST", path
            )
            req = Request(url, data=body, headers=headers, method="POST")
            with urlopen(req, timeout=HTTP_TIMEOUT) as resp:
                return resp.status

    def delete_resource(self, path):
        """DELETE a Management API resource; return HTTP status."""
        url = f"{self.base}{path}"
        headers = {"Accept": "application/json"}

        if self.auth_type == "basic":
            headers["Authorization"] = self._basic_auth_header()

        try:
            req = Request(url, headers=headers, method="DELETE")
            with urlopen(req, timeout=HTTP_TIMEOUT) as resp:
                return resp.status
        except HTTPError as e:
            if e.code != 401 or self.auth_type == "basic":
                raise
            auth_header = e.headers.get("WWW-Authenticate", "")
            if "Digest" not in auth_header:
                raise
            headers["Authorization"] = self._digest_response(
                auth_header, "DELETE", path
            )
            req = Request(url, headers=headers, method="DELETE")
            with urlopen(req, timeout=HTTP_TIMEOUT) as resp:
                return resp.status

    def _parse_eval_response(self, text):
        """Parse multipart/mixed eval response into JSON values."""
        results = []
        for part in text.split("--"):
            part = part.strip()
            if not part or part == "" or part.startswith("X-"):
                continue
            # Find the JSON body after headers
            if "\r\n\r\n" in part:
                body = part.split("\r\n\r\n", 1)[1]
            elif "\n\n" in part:
                body = part.split("\n\n", 1)[1]
            else:
                continue
            body = body.strip()
            if body and body != "--":
                try:
                    results.append(json.loads(body))
                except json.JSONDecodeError:
                    results.append(body)
        return results
