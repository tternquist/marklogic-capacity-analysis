#!/usr/bin/env python3
"""
MarkLogic Capacity Analyzer

A CLI tool that connects to a MarkLogic cluster via the Management API
and provides capacity analysis including:
  - Cluster overview (version, hosts, databases)
  - Database statistics (document counts, sizes, cache ratios)
  - Forest health (stands, fragmentation, merge status)
  - Disk utilization and runway
  - Index configuration summary
  - Per-index memory usage (ML 11+)
  - Document capacity estimation (how many more docs before memory limits)
  - Snapshot persistence and trend analysis over time

Snapshots:
  Every run saves a snapshot to .ml-capacity/ (next to this script).
  Use --trend to see growth curves, --compare <id> to diff vs a past snapshot.
  Use --snapshot-only to save without printing the full report.

Usage:
    python ml_capacity.py [--host HOST] [--port PORT] [--user USER]
                          [--password PASSWORD] [--database DATABASE]
                          [--auth-type digest|basic]
                          [--trend] [--compare ID] [--snapshot-only]
                          [--no-snapshot]
"""

import argparse
import getpass
import json
import math
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from base64 import b64encode


# ── Build info ──────────────────────────────────────────────────────

_BUILD = os.environ.get("MLCA_BUILD", "dev")[:12]


# ── Formatting helpers ──────────────────────────────────────────────

YELLOW = "\033[33m"
GREEN = "\033[32m"
RED = "\033[31m"
CYAN = "\033[36m"
BOLD = "\033[1m"
DIM = "\033[2m"
RESET = "\033[0m"

BAR_WIDTH = 30


def color(text, c):
    return f"{c}{text}{RESET}"


def header(title):
    width = 62
    print()
    print(color("=" * width, DIM))
    print(color(f"  {title}", BOLD + CYAN))
    print(color("=" * width, DIM))


def sub_header(title):
    print()
    print(color(f"  --- {title} ---", BOLD))


def kv(key, value, indent=4):
    pad = " " * indent
    print(f"{pad}{color(key + ':', DIM):.<48s} {value}")


def bar(pct, warn_threshold=70, crit_threshold=90):
    filled = int(round(pct / 100 * BAR_WIDTH))
    filled = max(0, min(BAR_WIDTH, filled))
    empty = BAR_WIDTH - filled
    if pct >= crit_threshold:
        c = RED
    elif pct >= warn_threshold:
        c = YELLOW
    else:
        c = GREEN
    return f"{c}{'█' * filled}{'░' * empty}{RESET} {pct:.1f}%"


def fmt_mb(mb):
    if mb is None:
        return "N/A"
    mb = float(mb)
    if mb >= 1024 * 1024:
        return f"{mb / (1024 * 1024):.2f} TB"
    if mb >= 1024:
        return f"{mb / 1024:.2f} GB"
    return f"{mb:.1f} MB"


def status_badge(ok, ok_text="OK", bad_text="WARNING"):
    if ok:
        return color(f"[{ok_text}]", GREEN)
    return color(f"[{bad_text}]", RED)


# ── HTTP client (no external deps) ─────────────────────────────────

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
            with urlopen(req) as resp:
                return json.loads(resp.read())

        # Digest auth
        try:
            req = Request(url, headers=headers)
            with urlopen(req) as resp:
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
            with urlopen(req) as resp:
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
            with urlopen(req) as resp:
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
            with urlopen(req) as resp:
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
            with urlopen(req) as resp:
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
            with urlopen(req) as resp:
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
            with urlopen(req) as resp:
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
            with urlopen(req) as resp:
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


# ── Data collection ─────────────────────────────────────────────────

def collect_cluster_overview(client):
    return client.get_json("/manage/v2?format=json")


def collect_database_status(client, database):
    return client.get_json(f"/manage/v2/databases/{database}?view=status&format=json")


def collect_database_properties(client, database):
    return client.get_json(f"/manage/v2/databases/{database}/properties?format=json")


def collect_forests(client, database):
    data = client.get_json(
        f"/manage/v2/forests?format=json&database-id={database}&view=status"
    )
    return data


def collect_forest_detail(client, forest_name):
    return client.get_json(
        f"/manage/v2/forests/{forest_name}?view=status&format=json"
    )


def collect_host_status(client):
    hosts_data = client.get_json("/manage/v2/hosts?format=json")
    items = hosts_data.get("host-default-list", {}).get("list-items", {}).get("list-item", [])
    results = []
    for h in items:
        name = h.get("nameref", "unknown")
        detail = client.get_json(f"/manage/v2/hosts/{name}?view=status&format=json")
        results.append(detail)
    return results


def collect_host_memory(client):
    """Use XQuery to get the full host memory breakdown from xdmp:host-status().

    Memory components from the host-status XML:
      memory-process-size     — ML virtual address space (VSZ)
      memory-process-rss      — Physical RAM pages currently resident (RSS)
      memory-process-anon     — Anonymous (heap) allocations, subset of RSS
      memory-process-rss-hwm  — Peak RSS since last startup
      memory-process-swap-size— Pages swapped out (non-zero = memory pressure)
      memory-size             — Configured ML memory limit (group setting)
      memory-cache-size       — List cache + compressed-tree cache allocated
      memory-forest-size      — In-memory stands across all forests on host
      memory-file-size        — OS file cache (mmap) pages held by ML
      host-size               — Base ML overhead (threads, code, bookkeeping)
      memory-join-size        — Active join workspace
      memory-unclosed-size    — Stands currently being merged (transient)
      memory-registry-size    — Registry memory
      memory-system-pagein-rate  — OS page-in rate (MB/s), non-zero = paging
      memory-system-pageout-rate — OS page-out rate (MB/s)
      memory-system-swapin-rate  — Swap-in rate (non-zero = severe pressure)
      memory-system-swapout-rate — Swap-out rate
    """
    xquery = """
    let $results :=
      for $host in xdmp:hosts()
      let $s := xdmp:host-status($host)
      return xdmp:to-json(map:new((
        map:entry("hostname",                  xdmp:host-name($host)),
        map:entry("cpus",                      $s/*:cpus/data()),
        map:entry("cores",                     $s/*:cores/data()),
        map:entry("memory-system-total-mb",    $s/*:memory-system-total/data()),
        map:entry("memory-system-free-mb",     $s/*:memory-system-free/data()),
        map:entry("memory-system-pagein-rate", $s/*:memory-system-pagein-rate/data()),
        map:entry("memory-system-pageout-rate",$s/*:memory-system-pageout-rate/data()),
        map:entry("memory-system-swapin-rate", $s/*:memory-system-swapin-rate/data()),
        map:entry("memory-system-swapout-rate",$s/*:memory-system-swapout-rate/data()),
        map:entry("memory-process-size-mb",    $s/*:memory-process-size/data()),
        map:entry("memory-process-rss-mb",     $s/*:memory-process-rss/data()),
        map:entry("memory-process-anon-mb",    $s/*:memory-process-anon/data()),
        map:entry("memory-process-rss-hwm-mb", $s/*:memory-process-rss-hwm/data()),
        map:entry("memory-process-swap-mb",    $s/*:memory-process-swap-size/data()),
        map:entry("memory-size-mb",            $s/*:memory-size/data()),
        map:entry("memory-cache-size-mb",      $s/*:memory-cache-size/data()),
        map:entry("memory-forest-size-mb",     $s/*:memory-forest-size/data()),
        map:entry("memory-file-size-mb",       $s/*:memory-file-size/data()),
        map:entry("host-size-mb",              $s/*:host-size/data()),
        map:entry("memory-join-size-mb",       $s/*:memory-join-size/data()),
        map:entry("memory-unclosed-size-mb",   $s/*:memory-unclosed-size/data()),
        map:entry("memory-registry-size-mb",   $s/*:memory-registry-size/data()),
        map:entry("host-large-data-size-mb",   $s/*:host-large-data-size/data()),
        map:entry("log-device-space-mb",       $s/*:log-device-space/data()),
        map:entry("data-dir-space-mb",         $s/*:data-dir-space/data())
      )))
    return xdmp:to-json(json:to-array($results))
    """
    return client.eval_xquery(xquery)


def collect_forest_counts(client, database):
    """Use XQuery to get detailed forest fragment counts.

    xdmp:forest-counts() returns an XML node — child elements live in
    the MarkLogic namespace, so we address them with the xs:QName form
    or declare a default namespace. Using local-name() matching is the
    safest portable approach.
    """
    xquery = f"""
    let $db := xdmp:database("{database}")
    let $forests := xdmp:database-forests($db)
    let $results :=
      for $f in $forests
      let $fc := xdmp:forest-counts($f)
      let $fs := xdmp:forest-status($f)
      (: Address children by local-name to avoid namespace binding issues :)
      (: document-count is a direct child; fragment counts are under stands-counts/stand-counts :)
      let $doc-count     := ($fc/*[local-name()="document-count"]/data(), 0)[1]
      let $sc            := $fc/*[local-name()="stands-counts"]/*[local-name()="stand-counts"]
      let $active-count  := sum($sc/*[local-name()="active-fragment-count"]/data())
      let $deleted-count := sum($sc/*[local-name()="deleted-fragment-count"]/data())
      let $nascent-count := sum($sc/*[local-name()="nascent-fragment-count"]/data())
      let $stand-count   := count($fs/*[local-name()="stands"]/*[local-name()="stand"])
      let $disk-mb       := sum($fs/*[local-name()="stands"]/*[local-name()="stand"]/*[local-name()="disk-size"]/data())
      let $mem-mb        := sum($fs/*[local-name()="stands"]/*[local-name()="stand"]/*[local-name()="memory-size"]/data())
      return xdmp:to-json(map:new((
        map:entry("forest-name",          xdmp:forest-name($f)),
        map:entry("document-count",       $doc-count),
        map:entry("active-fragment-count",$active-count),
        map:entry("deleted-fragment-count",$deleted-count),
        map:entry("nascent-fragment-count",$nascent-count),
        map:entry("stand-count",          $stand-count),
        map:entry("disk-size-mb",         $disk-mb),
        map:entry("memory-size-mb",       $mem-mb)
      )))
    return xdmp:to-json(json:to-array($results))
    """
    return client.eval_xquery(xquery, database=database)


# ── Report sections ─────────────────────────────────────────────────

def report_cluster(client):
    header("CLUSTER OVERVIEW")
    data = collect_cluster_overview(client)
    cluster = data.get("local-cluster-default", {})
    kv("Cluster name", cluster.get("name", "unknown"))
    kv("MarkLogic version", cluster.get("version", "unknown"))

    relations = cluster.get("relations", {}).get("relation-group", [])
    for rg in relations:
        t = rg.get("typeref", "")
        count = rg.get("relation-count", {}).get("value", 0)
        if t in ("hosts", "databases", "forests", "servers"):
            kv(t.capitalize(), count)


def report_host_memory(client):
    header("HOST MEMORY")
    try:
        results = collect_host_memory(client)
        if not results:
            print("    Could not retrieve host memory (eval may be disabled)")
            return None

        host_data = results[0] if isinstance(results[0], list) else [results[0]]
        all_hosts = []

        for host in host_data:
            name    = host.get("hostname", "unknown")
            cpus    = host.get("cpus", "?")
            cores   = host.get("cores", "?")

            # System-level
            total   = float(host.get("memory-system-total-mb", 0) or 0)
            free    = float(host.get("memory-system-free-mb",  0) or 0)
            used    = total - free

            # ML process breakdown
            rss      = float(host.get("memory-process-rss-mb",     0) or 0)
            rss_hwm  = float(host.get("memory-process-rss-hwm-mb", 0) or 0)
            virt     = float(host.get("memory-process-size-mb",    0) or 0)
            anon     = float(host.get("memory-process-anon-mb",    0) or 0)
            swap     = float(host.get("memory-process-swap-mb",    0) or 0)

            # ML memory components (these add up to explain RSS)
            cache    = float(host.get("memory-cache-size-mb",    0) or 0)
            forest   = float(host.get("memory-forest-size-mb",   0) or 0)
            filecache= float(host.get("memory-file-size-mb",     0) or 0)
            base     = float(host.get("host-size-mb",            0) or 0)
            join     = float(host.get("memory-join-size-mb",     0) or 0)
            unclosed = float(host.get("memory-unclosed-size-mb", 0) or 0)
            largedata= float(host.get("host-large-data-size-mb", 0) or 0)

            # ML configured memory limit
            ml_limit = float(host.get("memory-size-mb", 0) or 0)

            # Paging / swap pressure
            pagein   = float(host.get("memory-system-pagein-rate",  0) or 0)
            pageout  = float(host.get("memory-system-pageout-rate", 0) or 0)
            swapin   = float(host.get("memory-system-swapin-rate",  0) or 0)
            swapout  = float(host.get("memory-system-swapout-rate", 0) or 0)

            # Data dir space
            data_dir = float(host.get("data-dir-space-mb", 0) or 0)

            sub_header(f"Host: {name}")
            kv("CPUs / Cores", f"{cpus} / {cores}")

            print()
            print(f"    {color('System Memory', BOLD)}")
            kv("  Total RAM", fmt_mb(total))
            kv("  Free RAM",  fmt_mb(free))
            sys_pct = (used / total * 100) if total else 0
            kv("  Used RAM",  f"{fmt_mb(used)}  {bar(sys_pct)}")
            kv("  Data dir free", fmt_mb(data_dir))

            print()
            print(f"    {color('MarkLogic Process', BOLD)}")
            kv("  Virtual size (VSZ)", fmt_mb(virt))
            rss_pct = (rss / total * 100) if total else 0
            kv("  Resident size (RSS)", f"{fmt_mb(rss)}  {bar(rss_pct)}")
            kv("  RSS peak (HWM)",     fmt_mb(rss_hwm))
            kv("  Anonymous/heap",     fmt_mb(anon))
            kv("  Swap in use",        f"{fmt_mb(swap)}  {status_badge(swap == 0, 'OK', 'SWAPPING')}")
            if ml_limit:
                kv("  Configured ML limit", fmt_mb(ml_limit))

            print()
            print(f"    {color('ML Memory Components  (explain RSS)', BOLD)}")
            kv("  Cache alloc (list+tree)", fmt_mb(cache))
            kv("  Forest in-memory stands", fmt_mb(forest))
            kv("  File cache (mmap)",       fmt_mb(filecache))
            kv("  Base ML overhead",        fmt_mb(base))
            if join:
                kv("  Join workspace",      fmt_mb(join))
            if unclosed:
                kv("  Unclosed stands",     fmt_mb(unclosed))
            if largedata:
                kv("  Large binary cache",  fmt_mb(largedata))
            accounted = cache + forest + filecache + base + join + unclosed + largedata
            kv("  Accounted total",
               f"{fmt_mb(accounted)}  {color('(vs RSS ' + fmt_mb(rss) + ')', DIM)}")

            print()
            print(f"    {color('Paging / Swap Pressure', BOLD)}")
            pagein_ok  = pagein  < 100
            pageout_ok = pageout < 100
            kv("  Page-in rate",   f"{pagein:.1f} MB/s  {status_badge(pagein_ok,  'OK', 'HIGH')}")
            kv("  Page-out rate",  f"{pageout:.1f} MB/s  {status_badge(pageout_ok, 'OK', 'HIGH')}")
            kv("  Swap-in rate",   f"{swapin:.3f} MB/s  {status_badge(swapin == 0,  'OK', 'SWAPPING')}")
            kv("  Swap-out rate",  f"{swapout:.3f} MB/s  {status_badge(swapout == 0, 'OK', 'SWAPPING')}")

            all_hosts.append(host)

        return all_hosts
    except Exception as e:
        print(f"    Could not retrieve host memory: {e}")
        print("    (Ensure ML_ALLOW_EVAL=true for full capacity analysis)")
        return None


def report_database_stats(client, database):
    header(f"DATABASE: {database}")
    data = collect_database_status(client, database)
    status = data.get("database-status", {})
    props = status.get("status-properties", {})

    kv("State", props.get("state", {}).get("value", "unknown"))
    kv("Forests", props.get("forests-count", {}).get("value", 0))

    data_size = props.get("data-size", {}).get("value", 0)
    device_space = props.get("device-space", {}).get("value", "0")
    device_space = float(device_space)
    in_mem = props.get("in-memory-size", {}).get("value", 0)
    large_data = props.get("large-data-size", {}).get("value", 0)

    sub_header("Storage")
    kv("Data on disk", fmt_mb(data_size))
    kv("Large binary data", fmt_mb(large_data))
    kv("In-memory size", fmt_mb(in_mem))
    kv("Device space (total)", fmt_mb(device_space))

    remaining = props.get("least-remaining-space-forest", {}).get("value", 0)
    remaining = float(remaining)
    if device_space > 0:
        used_pct = ((device_space - remaining) / device_space) * 100
        kv("Disk used", bar(used_pct))
        kv("Remaining (least forest)", fmt_mb(remaining))

        # Disk runway estimation
        if data_size and float(data_size) > 0:
            ratio = remaining / float(data_size)
            kv("Disk runway", f"~{ratio:.1f}x current data can still fit")

    sub_header("Cache Performance")
    cache = props.get("cache-properties", {})
    list_ratio = cache.get("list-cache-ratio", {}).get("value", 0)
    kv("List cache hit ratio", f"{list_ratio}%  {status_badge(list_ratio > 80, 'GOOD', 'LOW')}")

    triple_ratio = cache.get("triple-value-cache-ratio", {}).get("value", 0)
    kv("Triple value cache ratio", f"{triple_ratio}%  {status_badge(triple_ratio > 80, 'GOOD', 'LOW')}")

    sub_header("Activity")
    kv("Active merges", props.get("merge-count", {}).get("value", 0))
    kv("Active reindexes", props.get("reindex-count", {}).get("value", 0))
    kv("Active backups", props.get("backup-count", {}).get("value", 0))

    return data_size, device_space, remaining


def report_forest_health(client, database):
    header(f"FOREST HEALTH: {database}")

    try:
        results = collect_forest_counts(client, database)
        if not results:
            print("    Could not retrieve forest counts (eval may be disabled)")
            return []

        forests = results[0] if isinstance(results[0], list) else [results[0]]
        forest_data = []

        for f in forests:
            name = f.get("forest-name", "unknown")
            docs = f.get("document-count", 0) or 0
            active = f.get("active-fragment-count", 0) or 0
            deleted = f.get("deleted-fragment-count", 0) or 0
            nascent = f.get("nascent-fragment-count", 0) or 0
            stands = f.get("stand-count", 0) or 0
            disk = f.get("disk-size-mb", 0) or 0
            memory = f.get("memory-size-mb", 0) or 0

            sub_header(f"Forest: {name}")
            kv("Documents", f"{docs:,}")
            kv("Active fragments", f"{active:,}")
            kv("Deleted fragments", f"{deleted:,}")

            total_frags = active + deleted
            frag_pct = (deleted / total_frags * 100) if total_frags > 0 else 0

            if frag_pct >= 50:
                frag_badge = color("[CRITICAL]", RED + BOLD)
            elif frag_pct >= 25:
                frag_badge = color("[HIGH]", RED)
            elif frag_pct >= 10:
                frag_badge = color("[MODERATE]", YELLOW)
            else:
                frag_badge = color("[OK]", GREEN)
            kv("Fragmentation", f"{frag_pct:.1f}%  ({deleted:,} deleted / {total_frags:,} total)  {frag_badge}")

            if frag_pct >= 25:
                # Estimate wasted space
                if total_frags > 0 and disk > 0:
                    wasted_mb = disk * (deleted / total_frags)
                    print(f"      {color('!', RED)} ~{fmt_mb(wasted_mb)} of disk occupied by deleted fragments")
                if frag_pct >= 50:
                    print(f"      {color('!', RED)} {color('Merge recommended to reclaim space and improve projection accuracy', BOLD)}")
                    print(f"      {color('  Run:', DIM)} xdmp:merge(xdmp:database-forests(xdmp:database(\"{name}\")))")
                    print(f"      {color('  Or via Admin UI: Database > Merge > Merge Now', DIM)}")
                else:
                    print(f"      {color('!', YELLOW)} High deleted fragment ratio inflates disk and memory metrics")
                    print(f"      {color('  Consider forcing a merge if projections seem off', DIM)}")

            kv("Stands", f"{stands} / 64 max  {bar(stands / 64 * 100, 50, 75)}")
            kv("Disk size", fmt_mb(disk))
            kv("In-memory size", fmt_mb(memory))

            MAX_FRAGMENTS = 96_000_000
            frag_pct_of_max = (active / MAX_FRAGMENTS) * 100
            kv("Fragment capacity", f"{active:,} / {MAX_FRAGMENTS:,}  {bar(frag_pct_of_max)}")

            forest_data.append(f)

        return forest_data

    except Exception as e:
        print(f"    Could not retrieve forest details: {e}")
        return []


def report_index_config(client, database):
    header(f"INDEX CONFIGURATION: {database}")
    props = collect_database_properties(client, database)

    # Boolean indexes
    bool_indexes = [
        ("word-searches", "Word searches"),
        ("word-positions", "Word positions"),
        ("fast-phrase-searches", "Fast phrase searches"),
        ("fast-reverse-searches", "Fast reverse searches"),
        ("triple-index", "Triple index"),
        ("triple-positions", "Triple positions"),
        ("fast-case-sensitive-searches", "Fast case-sensitive"),
        ("fast-diacritic-sensitive-searches", "Fast diacritic-sensitive"),
        ("fast-element-word-searches", "Fast element word"),
        ("element-word-positions", "Element word positions"),
        ("fast-element-phrase-searches", "Fast element phrase"),
        ("uri-lexicon", "URI lexicon"),
        ("collection-lexicon", "Collection lexicon"),
        ("trailing-wildcard-searches", "Trailing wildcard"),
        ("three-character-searches", "Three-character searches"),
        ("two-character-searches", "Two-character searches"),
        ("one-character-searches", "One-character searches"),
        ("field-value-searches", "Field value searches"),
    ]

    enabled = []
    disabled = []
    for key, label in bool_indexes:
        val = props.get(key, False)
        if val is True or val == "true":
            enabled.append(label)
        else:
            disabled.append(label)

    sub_header("Enabled Indexes")
    for idx in enabled:
        print(f"      {color('+', GREEN)} {idx}")

    sub_header("Disabled Indexes")
    for idx in disabled:
        print(f"      {color('-', DIM)} {idx}")

    # Range indexes
    range_indexes = props.get("range-element-index", [])
    range_path = props.get("range-path-index", [])
    range_field = props.get("range-field-index", [])

    sub_header("Range Indexes")
    total_range = len(range_indexes) + len(range_path) + len(range_field)
    kv("Element range indexes", len(range_indexes))
    kv("Path range indexes", len(range_path))
    kv("Field range indexes", len(range_field))
    for ri in range_indexes:
        ln = ri.get("localname", "?")
        st = ri.get("scalar-type", "?")
        print(f"      {color('>', CYAN)} {ln} ({st})")
    for ri in range_path:
        pe = ri.get("path-expression", "?")
        st = ri.get("scalar-type", "?")
        print(f"      {color('>', CYAN)} {pe} ({st})")

    # In-memory settings (critical for capacity)
    sub_header("In-Memory Settings")
    kv("in-memory-limit", f"{props.get('in-memory-limit', 'N/A'):,} KB")
    kv("in-memory-list-size", f"{props.get('in-memory-list-size', 'N/A')} MB")
    kv("in-memory-tree-size", f"{props.get('in-memory-tree-size', 'N/A')} MB")
    kv("in-memory-range-index-size", f"{props.get('in-memory-range-index-size', 'N/A')} MB")
    kv("in-memory-reverse-index-size", f"{props.get('in-memory-reverse-index-size', 'N/A')} MB")
    kv("in-memory-triple-index-size", f"{props.get('in-memory-triple-index-size', 'N/A')} MB")

    return props, total_range, len(enabled)


def report_index_memory(client, database):
    """Per-index memory and disk usage using xdmp.forestStatus('memoryDetail').

    Requires MarkLogic 11+ and ML_ALLOW_EVAL=true.
    Uses xdmp.databaseDescribeIndexes() for index definitions and
    xdmp.forestStatus(forests, 'memoryDetail') for per-stand, per-index
    memory and on-disk byte counts. Aggregates across all stands.

    Also shows the per-stand memorySummary breakdown (list, tree, range
    indexes, triple index, timestamps, etc.) which explains where
    forest memory is being consumed.
    """
    header(f"INDEX MEMORY USAGE: {database}")

    # Check preload-mapped-data setting
    try:
        db_config = client.get_json(
            f"/manage/v2/databases/{database}/properties?format=json"
        )
        preload = db_config.get("preload-mapped-data", False)
    except Exception:
        preload = None

    if preload is False:
        print()
        print(f"    {color('!', YELLOW)} {color('preload-mapped-data is DISABLED', YELLOW + BOLD)}")
        print(f"    {color('  Range index memory numbers below may underreport.', DIM)}")
        print(f"    {color('  Without preload, index data is only loaded into memory on', DIM)}")
        print(f"    {color('  first query — not on forest open. Enable it for accurate', DIM)}")
        print(f"    {color('  memory reporting:', DIM)}")
        print(f"    {color('  Admin UI: Database > Settings > preload mapped data = true', DIM)}")
        print()

    try:
        results = client.eval_javascript(_INDEX_MEMORY_JS, database=database,
                                         vars={"dbName": database})
        if not results:
            print("    Could not retrieve index memory data")
            return

        data = results[0]
        indexes = data.get("indexes", [])
        stand_summaries = data.get("standSummaries", [])

        # ── Per-stand memory summary ────────────────────────────────
        sub_header("Stand Memory Breakdown")

        # Sum across stands for the aggregate
        agg = {}
        for ss in stand_summaries:
            summary = ss.get("summary", {})
            for k, v in summary.items():
                agg[k] = agg.get(k, 0) + (v or 0)

        total_stand_mem = sum(ss.get("memorySize", 0) or 0 for ss in stand_summaries)
        total_stand_disk = sum(ss.get("diskSize", 0) or 0 for ss in stand_summaries)

        kv("Stands",            len(stand_summaries))
        kv("Total disk",        fmt_mb(total_stand_disk))
        kv("Total memory",      fmt_mb(total_stand_mem))
        print()

        # Show component breakdown (sorted by size descending)
        component_labels = {
            "rangeIndexesBytes":            "Range indexes",
            "timestampsFileBytes":          "Timestamps",
            "uniqueKeyIndexBytes":          "Unique key index",
            "uriKeyIndexBytes":             "URI key index",
            "linkKeysFileBytes":            "Link keys",
            "ordinalsFileBytes":            "Ordinals",
            "uniqKeysFileBytes":            "Unique keys",
            "uriKeysFileBytes":             "URI keys",
            "tripleIndexBytes":             "Triple index",
            "qualitiesFileBytes":           "Qualities",
            "lengthsFileBytes":             "Lengths",
            "listFileBytes":                "List index",
            "treeFileBytes":                "Tree index",
            "frequenciesFileBytes":         "Frequencies",
            "reverseIndexBytes":            "Reverse index",
            "geoSpatialRegionIndexesBytes": "Geospatial region",
            "binaryKeysFileBytes":          "Binary keys",
            "linkKeyIndexBytes":            "Link key index",
            "stopKeySetFileBytes":          "Stop key set",
        }

        sorted_components = sorted(agg.items(), key=lambda x: x[1], reverse=True)
        total_bytes = sum(v for _, v in sorted_components) or 1

        for key, bytes_val in sorted_components:
            if bytes_val == 0:
                continue
            label = component_labels.get(key, key)
            pct = bytes_val / total_bytes * 100
            mb = bytes_val / (1024 * 1024)
            kv(label, f"{mb:>8.2f} MB  ({pct:>5.1f}%)")

        # ── Per-index memory usage ──────────────────────────────────
        sub_header("Per-Index Memory & Disk")

        # Group by index type
        by_type = {}
        for idx in indexes:
            t = idx.get("indexType", "unknown")
            if t not in by_type:
                by_type[t] = []
            by_type[t].append(idx)

        type_labels = {
            "uriLexicon":          "URI Lexicon",
            "collectionLexicon":   "Collection Lexicon",
            "rangeElementIndex":   "Element Range Index",
            "rangePathIndex":      "Path Range Index",
            "rangeFieldIndex":     "Field Range Index",
            "geospatialPathIndex": "Geospatial Path Index",
        }

        grand_mem = 0
        grand_disk = 0

        for idx_type, items in by_type.items():
            type_label = type_labels.get(idx_type, idx_type)
            print()
            print(f"    {color(type_label, BOLD)}")

            for idx in sorted(items, key=lambda x: x.get("totalMemoryBytes", 0), reverse=True):
                mem_b  = idx.get("totalMemoryBytes", 0) or 0
                disk_b = idx.get("totalOnDiskBytes", 0) or 0
                grand_mem  += mem_b
                grand_disk += disk_b

                name = idx.get("localname") or idx_type
                ns   = idx.get("namespaceURI", "")
                st   = idx.get("scalarType", "")

                name_str = name
                if st:
                    name_str += f" ({st})"
                if ns:
                    name_str += f"  {color(ns, DIM)}"

                mem_str  = fmt_mb(mem_b / (1024 * 1024))  if mem_b  else "n/a"
                disk_str = fmt_mb(disk_b / (1024 * 1024)) if disk_b else "n/a"

                print(f"      {color('>', CYAN)} {name_str}")
                print(f"        Memory: {mem_str:>12}    Disk: {disk_str:>12}")

        print()
        kv("Per-index cached memory", fmt_mb(grand_mem / (1024 * 1024)))
        kv("Per-index on-disk",       fmt_mb(grand_disk / (1024 * 1024)))

        # The per-index memory from memoryDetail only shows data loaded into
        # the list cache — not total resident range index pages. The stand-level
        # aggregate is the real memory picture.
        total_range_bytes = sum(
            ss.get("summary", {}).get("rangeIndexesBytes", 0)
            for ss in stand_summaries
        )
        if total_range_bytes > 0:
            print()
            kv("Total range index memory (all indexes, stand-level)",
               color(fmt_mb(total_range_bytes / (1024 * 1024)), BOLD))
            print(f"      {color('Note:', DIM)} Per-index values above reflect only cache-warmed data.")
            print(f"      {color('The stand-level total includes all resident range index pages.', DIM)}")
            print(f"      {color('Use --index-impact for measured per-index costs.', DIM)}")

    except Exception as e:
        print(f"    Could not retrieve index memory usage: {e}")
        print(f"    (Requires ML 11+ and ML_ALLOW_EVAL=true)")


_INDEX_MEMORY_JS = """
var db = xdmp.database(dbName);

// 1. Get all index definitions with their IDs
var indexDefs = xdmp.databaseDescribeIndexes(db).toObject();
var allIndexes = [];
Object.keys(indexDefs).forEach(function(type) {
  var items = indexDefs[type];
  if (!Array.isArray(items)) items = [items];
  items.forEach(function(idx) {
    if (idx.indexId) {
      idx.indexType = type;
      allIndexes.push(idx);
    }
  });
});

// 2. Get forest statuses with memoryDetail
var forests = xdmp.databaseForests(db);
var statuses = xdmp.forestStatus(forests, 'memoryDetail').toArray();

// 3. Aggregate per-index memory and disk across all stands
var indexTotals = {};
var standSummaries = [];

statuses.forEach(function(statusNode) {
  var s = statusNode.toObject();
  var stands = s.stands;
  if (!stands) return;
  var standArr = Array.isArray(stands) ? stands : [stands];
  standArr.forEach(function(stand) {
    if (stand.memorySummary) {
      standSummaries.push({
        standPath: stand.path,
        diskSize: stand.diskSize,
        memorySize: stand.memorySize,
        summary: stand.memorySummary
      });
    }
    if (stand.memoryDetail && stand.memoryDetail.memoryRangeIndexes) {
      var indexes = stand.memoryDetail.memoryRangeIndexes.index;
      if (!indexes) return;
      if (!Array.isArray(indexes)) indexes = [indexes];
      indexes.forEach(function(idx) {
        var id = String(idx.indexId);
        if (!indexTotals[id]) indexTotals[id] = { memBytes: 0, diskBytes: 0 };
        indexTotals[id].memBytes += (idx.indexMemoryBytes || 0);
        indexTotals[id].diskBytes += (idx.indexOnDiskBytes || 0);
      });
    }
  });
});

// 4. Join
var report = allIndexes.map(function(def) {
  var totals = indexTotals[String(def.indexId)] || { memBytes: 0, diskBytes: 0 };
  return {
    indexType: def.indexType,
    localname: def.localname || null,
    namespaceURI: def.namespaceURI || null,
    scalarType: def.scalarType || null,
    pathExpression: def.pathExpression || null,
    indexId: def.indexId,
    totalMemoryBytes: totals.memBytes,
    totalOnDiskBytes: totals.diskBytes
  };
});

var result = { indexes: report, standSummaries: standSummaries };
result;
"""


# ── Index impact assessment ────────────────────────────────────────

def _index_key(idx):
    """Build a composite key for matching an index across snapshots.

    Uses the semantic identity (type + name + namespace + scalar type),
    not indexId, because ML assigns new IDs when indexes are recreated.
    """
    return (
        idx.get("indexType", ""),
        idx.get("localname") or idx.get("pathExpression") or "",
        idx.get("namespaceURI") or "",
        idx.get("scalarType") or "",
    )


def _index_label(idx):
    """Human-readable label for an index."""
    name = idx.get("localname") or idx.get("pathExpression") or idx.get("indexType", "?")
    parts = [name]
    if idx.get("scalarType"):
        parts.append(f"({idx['scalarType']})")
    if idx.get("namespaceURI"):
        parts.append(f"ns:{idx['namespaceURI'].split('/')[-1]}")
    return " ".join(parts)


def diff_index_memory(old_snap, new_snap):
    """Compare per-index memory between two snapshots.

    Returns dict with:
      added:   list of index dicts present in new but not old
      removed: list of index dicts present in old but not new
      changed: list of { index, old_mem, new_mem, old_disk, new_disk, delta_mem, delta_disk }
      summary: { total_mem_delta, total_disk_delta, doc_delta }
    """
    old_im = old_snap.get("index_memory") or {}
    new_im = new_snap.get("index_memory") or {}
    old_indexes = {_index_key(i): i for i in old_im.get("indexes", [])}
    new_indexes = {_index_key(i): i for i in new_im.get("indexes", [])}

    old_keys = set(old_indexes.keys())
    new_keys = set(new_indexes.keys())

    added = [new_indexes[k] for k in (new_keys - old_keys)]
    removed = [old_indexes[k] for k in (old_keys - new_keys)]

    changed = []
    for k in old_keys & new_keys:
        oi = old_indexes[k]
        ni = new_indexes[k]
        om = oi.get("totalMemoryBytes", 0) or 0
        nm = ni.get("totalMemoryBytes", 0) or 0
        od = oi.get("totalOnDiskBytes", 0) or 0
        nd = ni.get("totalOnDiskBytes", 0) or 0
        if om != nm or od != nd:
            changed.append({
                "index": ni,
                "old_mem": om, "new_mem": nm,
                "old_disk": od, "new_disk": nd,
                "delta_mem": nm - om, "delta_disk": nd - od,
            })

    old_total_mem = sum((i.get("totalMemoryBytes", 0) or 0) for i in old_im.get("indexes", []))
    new_total_mem = sum((i.get("totalMemoryBytes", 0) or 0) for i in new_im.get("indexes", []))
    old_total_disk = sum((i.get("totalOnDiskBytes", 0) or 0) for i in old_im.get("indexes", []))
    new_total_disk = sum((i.get("totalOnDiskBytes", 0) or 0) for i in new_im.get("indexes", []))

    old_docs = old_snap.get("totals", {}).get("documents", 0)
    new_docs = new_snap.get("totals", {}).get("documents", 0)

    return {
        "added": added,
        "removed": removed,
        "changed": changed,
        "summary": {
            "total_mem_delta": new_total_mem - old_total_mem,
            "total_disk_delta": new_total_disk - old_total_disk,
            "old_total_mem": old_total_mem,
            "new_total_mem": new_total_mem,
            "old_total_disk": old_total_disk,
            "new_total_disk": new_total_disk,
            "old_docs": old_docs,
            "new_docs": new_docs,
            "doc_delta": new_docs - old_docs,
        },
    }


def report_index_impact(old_snap, new_snap, project_docs=None):
    """Report the memory/disk impact of index changes between two snapshots."""
    header("INDEX IMPACT ASSESSMENT")

    old_im = old_snap.get("index_memory")
    new_im = new_snap.get("index_memory")
    if not old_im or not new_im:
        print("    Both snapshots must have index memory data (requires ML 11+ and eval).")
        return

    old_ts = old_snap.get("timestamp", "?")[:19].replace("T", " ")
    new_ts = new_snap.get("timestamp", "?")[:19].replace("T", " ")
    kv("Before snapshot", old_ts)
    kv("After snapshot", new_ts)

    diff = diff_index_memory(old_snap, new_snap)
    s = diff["summary"]
    new_docs = s["new_docs"]
    doc_delta = s["doc_delta"]

    kv("Documents", f"{s['old_docs']:,} → {s['new_docs']:,}  ({doc_delta:+,})")

    # ── Added indexes ──────────────────────────────────────────────
    if diff["added"]:
        sub_header(f"New Indexes Added ({len(diff['added'])})")
        for idx in diff["added"]:
            label = _index_label(idx)
            mem = idx.get("totalMemoryBytes", 0) or 0
            disk = idx.get("totalOnDiskBytes", 0) or 0
            mem_mb = mem / (1024 * 1024)
            disk_mb = disk / (1024 * 1024)

            print(f"\n    {color('+', GREEN)} {color(label, BOLD)}")
            kv("  Memory", fmt_mb(mem_mb), indent=6)
            kv("  Disk",   fmt_mb(disk_mb), indent=6)

            if new_docs > 0:
                mem_per_doc = mem / new_docs
                disk_per_doc = disk / new_docs
                kv("  Memory/doc", f"{mem_per_doc:,.0f} bytes", indent=6)
                kv("  Disk/doc",   f"{disk_per_doc:,.0f} bytes", indent=6)

                if project_docs:
                    proj_mem = mem_per_doc * project_docs / (1024 * 1024)
                    proj_disk = disk_per_doc * project_docs / (1024 * 1024)
                    kv(f"  At {project_docs:,} docs",
                       f"memory: {fmt_mb(proj_mem)},  disk: {fmt_mb(proj_disk)}",
                       indent=6)

    # ── Removed indexes ────────────────────────────────────────────
    if diff["removed"]:
        sub_header(f"Indexes Removed ({len(diff['removed'])})")
        for idx in diff["removed"]:
            label = _index_label(idx)
            mem = idx.get("totalMemoryBytes", 0) or 0
            disk = idx.get("totalOnDiskBytes", 0) or 0

            print(f"\n    {color('-', RED)} {color(label, BOLD)}")
            kv("  Memory reclaimed", fmt_mb(mem / (1024 * 1024)), indent=6)
            kv("  Disk reclaimed",   fmt_mb(disk / (1024 * 1024)), indent=6)

    # ── Changed indexes ────────────────────────────────────────────
    if diff["changed"]:
        sub_header(f"Index Size Changes ({len(diff['changed'])})")
        for c in sorted(diff["changed"], key=lambda x: abs(x["delta_mem"]), reverse=True):
            label = _index_label(c["index"])
            dm = c["delta_mem"]
            dd = c["delta_disk"]
            dm_mb = dm / (1024 * 1024)
            dd_mb = dd / (1024 * 1024)

            c_color = GREEN if dm <= 0 else YELLOW
            print(f"\n    {color('~', c_color)} {label}")
            kv("  Memory",  f"{fmt_mb(c['old_mem']/(1024*1024))} → {fmt_mb(c['new_mem']/(1024*1024))}  "
                            f"({'+' if dm >= 0 else ''}{fmt_mb(dm_mb)})", indent=6)
            kv("  Disk",    f"{fmt_mb(c['old_disk']/(1024*1024))} → {fmt_mb(c['new_disk']/(1024*1024))}  "
                            f"({'+' if dd >= 0 else ''}{fmt_mb(dd_mb)})", indent=6)

    # ── Summary ────────────────────────────────────────────────────
    sub_header("Total Index Memory Impact")
    kv("Total index memory",
       f"{fmt_mb(s['old_total_mem']/(1024*1024))} → {fmt_mb(s['new_total_mem']/(1024*1024))}  "
       f"({'+' if s['total_mem_delta'] >= 0 else ''}{fmt_mb(s['total_mem_delta']/(1024*1024))})")
    kv("Total index disk",
       f"{fmt_mb(s['old_total_disk']/(1024*1024))} → {fmt_mb(s['new_total_disk']/(1024*1024))}  "
       f"({'+' if s['total_disk_delta'] >= 0 else ''}{fmt_mb(s['total_disk_delta']/(1024*1024))})")

    # ── Projection ─────────────────────────────────────────────────
    if project_docs and new_docs > 0:
        sub_header(f"Projection at {project_docs:,} Documents")
        all_indexes = new_snap.get("index_memory", {}).get("indexes", [])
        total_proj_mem = 0
        total_proj_disk = 0

        for idx in sorted(all_indexes, key=lambda x: x.get("totalMemoryBytes", 0) or 0, reverse=True):
            mem = idx.get("totalMemoryBytes", 0) or 0
            disk = idx.get("totalOnDiskBytes", 0) or 0
            if mem == 0 and disk == 0:
                continue
            mem_per_doc = mem / new_docs
            disk_per_doc = disk / new_docs
            proj_mem = mem_per_doc * project_docs
            proj_disk = disk_per_doc * project_docs
            total_proj_mem += proj_mem
            total_proj_disk += proj_disk

            label = _index_label(idx)
            kv(label,
               f"mem: {fmt_mb(proj_mem/(1024*1024)):>10}  "
               f"disk: {fmt_mb(proj_disk/(1024*1024)):>10}")

        print()
        kv("TOTAL projected index memory", color(fmt_mb(total_proj_mem/(1024*1024)), BOLD))
        kv("TOTAL projected index disk",   color(fmt_mb(total_proj_disk/(1024*1024)), BOLD))


def wait_for_reindex(client, database, timeout=300, poll_interval=5):
    """Poll until reindexing is complete on a database.

    Checks reindex-count and merge-count from the Management API status
    endpoint. Returns True if reindexing completed, False if timed out.
    """
    import time
    start = time.time()
    while time.time() - start < timeout:
        try:
            data = client.get_json(
                f"/manage/v2/databases/{database}?view=status&format=json"
            )
            props = data.get("database-status", {}).get("status-properties", {})
            reindex = props.get("reindex-count", {}).get("value", 0)
            if reindex == 0:
                return True
        except Exception:
            pass
        time.sleep(poll_interval)
    return False


def report_capacity_estimate(database, db_props, forest_data, host_data,
                             remaining_disk_mb=0):
    header(f"CAPACITY ESTIMATE: {database}")

    if not forest_data:
        print("    Insufficient data for capacity estimation (no forest metrics)")
        return

    # Gather current totals
    total_docs = sum(f.get("document-count", 0) or 0 for f in forest_data)
    total_active = sum(f.get("active-fragment-count", 0) or 0 for f in forest_data)
    total_deleted = sum(f.get("deleted-fragment-count", 0) or 0 for f in forest_data)
    total_memory = sum(f.get("memory-size-mb", 0) or 0 for f in forest_data)
    total_disk = sum(f.get("disk-size-mb", 0) or 0 for f in forest_data)
    num_forests = len(forest_data)
    total_frags = total_active + total_deleted
    frag_pct = (total_deleted / total_frags * 100) if total_frags > 0 else 0

    # In-memory settings from db properties
    in_mem_limit_kb = db_props.get("in-memory-limit", 131072)  # default 128 MB in KB
    in_mem_list_mb = db_props.get("in-memory-list-size", 256)
    in_mem_tree_mb = db_props.get("in-memory-tree-size", 64)
    in_mem_range_mb = db_props.get("in-memory-range-index-size", 8)
    in_mem_triple_mb = db_props.get("in-memory-triple-index-size", 21)

    # Total in-memory budget per forest for the in-memory stand
    # The in-memory-limit controls max fragments in the in-memory stand before flush
    # The other settings control memory allocated for structures
    in_mem_total_per_forest = in_mem_list_mb + in_mem_tree_mb + in_mem_range_mb + in_mem_triple_mb
    in_mem_total_all = in_mem_total_per_forest * num_forests

    sub_header("Current State")
    kv("Total documents", f"{total_docs:,}")
    kv("Total active fragments", f"{total_active:,}")
    kv("Forests", num_forests)
    kv("Total forest memory", fmt_mb(total_memory))
    kv("Total forest disk", fmt_mb(total_disk))

    if total_docs > 0:
        avg_mem_per_doc = (total_memory * 1024 * 1024) / total_docs  # bytes
        avg_disk_per_doc = (total_disk * 1024 * 1024) / total_docs if total_disk > 0 else 0
        kv("Avg memory per document", f"{avg_mem_per_doc:.0f} bytes")
        kv("Avg disk per document", f"{avg_disk_per_doc:.0f} bytes" if avg_disk_per_doc > 0 else "N/A")

    sub_header("Fragment Limits")
    MAX_FRAGS_PER_FOREST = 96_000_000
    max_fragments_total = MAX_FRAGS_PER_FOREST * num_forests
    frags_remaining = max_fragments_total - total_active
    frags_pct_used = (total_active / max_fragments_total) * 100

    kv("Max fragments (all forests)", f"{max_fragments_total:,}")
    kv("Current active fragments", f"{total_active:,}")
    kv("Fragments remaining", f"{frags_remaining:,}")
    kv("Fragment utilization", bar(frags_pct_used))

    # Estimate docs-per-fragment ratio
    if total_active > 0:
        doc_frag_ratio = total_docs / total_active
        docs_remaining_by_frags = int(frags_remaining * doc_frag_ratio)
        kv("Doc/fragment ratio", f"{doc_frag_ratio:.2f}")
        kv("Est. documents until fragment limit", f"{docs_remaining_by_frags:,}")

    sub_header("In-Memory Stand Limits")
    kv("in-memory-limit (flush threshold)", f"{in_mem_limit_kb:,} KB ({in_mem_limit_kb // 1024} MB)")
    kv("Per-forest memory budget", f"{in_mem_total_per_forest} MB (list:{in_mem_list_mb} + tree:{in_mem_tree_mb} + range:{in_mem_range_mb} + triple:{in_mem_triple_mb})")
    kv("Total in-memory budget", f"{in_mem_total_all} MB across {num_forests} forest(s)")

    # Memory-based capacity using the detailed component breakdown
    if host_data:
        def hsum(key):
            return sum(float(v) for h in host_data if (v := h.get(key)) is not None)

        total_sys        = hsum("memory-system-total-mb")
        free_sys         = hsum("memory-system-free-mb")
        rss              = hsum("memory-process-rss-mb")
        rss_hwm          = hsum("memory-process-rss-hwm-mb")
        swap             = hsum("memory-process-swap-mb")
        cache_alloc      = hsum("memory-cache-size-mb")
        forest_mem       = hsum("memory-forest-size-mb")
        file_cache       = hsum("memory-file-size-mb")
        base_overhead    = hsum("host-size-mb")
        join_mem         = hsum("memory-join-size-mb")
        unclosed_mem     = hsum("memory-unclosed-size-mb")
        ml_limit         = hsum("memory-size-mb")
        swapin           = hsum("memory-system-swapin-rate")

        sub_header("Memory Capacity Analysis")

        # ── Current breakdown ──────────────────────────────────────────
        print()
        print(f"    {color('Current ML memory breakdown', BOLD)}")
        kv("  Cache alloc (list+tree)",    fmt_mb(cache_alloc),  indent=6)
        kv("  Forest in-memory stands",    fmt_mb(forest_mem),   indent=6)
        kv("  File cache (mmap)",          fmt_mb(file_cache),   indent=6)
        kv("  Base ML overhead",           fmt_mb(base_overhead), indent=6)
        if join_mem:
            kv("  Join workspace",         fmt_mb(join_mem),     indent=6)
        if unclosed_mem:
            kv("  Unclosed stands",        fmt_mb(unclosed_mem), indent=6)
        kv("  Total RSS (actual)",         fmt_mb(rss),          indent=6)
        kv("  RSS peak (HWM)",             fmt_mb(rss_hwm),      indent=6)

        # ── Forest memory is the piece that grows with documents ───────
        # cache_alloc is configured upfront (list cache + tree cache)
        # forest_mem grows with in-memory stand activity
        # base_overhead is fixed
        # So headroom for more data = headroom in forest_mem + any unallocated cache
        print()
        print(f"    {color('Headroom analysis', BOLD)}")

        # MarkLogic's configured limit is ml_limit (group memory-size setting)
        # RSS should stay under min(ml_limit, system_ram * 0.80)
        # When system RAM is unavailable (containers, just-restarted), fall
        # back to ml_limit as the ceiling.
        if total_sys > 0:
            ram_cap = total_sys * 0.80
            safe_cap = min(ml_limit, ram_cap) if ml_limit else ram_cap
        elif ml_limit > 0:
            safe_cap = ml_limit  # container or just-restarted — use ML limit
        else:
            safe_cap = 0
        headroom = safe_cap - rss
        rss_pct  = (rss / safe_cap * 100) if safe_cap else 0

        kv("  Configured ML limit",        fmt_mb(ml_limit) if ml_limit else "not set", indent=6)
        kv("  Safe capacity (80% RAM)",     fmt_mb(total_sys * 0.80), indent=6)
        kv("  Effective ceiling",           fmt_mb(safe_cap), indent=6)
        kv("  Current RSS vs ceiling",
           f"{fmt_mb(rss)}  {bar(rss_pct)}", indent=6)
        kv("  Headroom remaining",
           f"{fmt_mb(headroom)}  {status_badge(headroom > 1024, 'OK', 'LOW')}",
           indent=6)

        if swap > 0:
            print(f"      {color('WARNING: ML is using swap (' + fmt_mb(swap) + ') — memory is over-committed!', RED)}")
        if swapin > 0:
            print(f"      {color('WARNING: System swap-in active — OS is paging memory!', RED)}")

        # ── Doc capacity estimate ──────────────────────────────────────
        # Disk-based projection is the primary model — disk size grows linearly
        # and predictably with document count, unlike forest memory which
        # fluctuates with merge activity, cached stand pages, and query patterns.
        #
        # Forest memory (host-level memory-forest-size) is shown as a secondary
        # indicator but NOT used for the headline projection, because it includes
        # cached on-disk stand pages that get compressed by merges and don't
        # scale linearly with doc count.
        print()
        print(f"    {color('Document capacity projection', BOLD)}")
        fixed_mem = cache_alloc + base_overhead + file_cache
        forest_headroom = safe_cap - fixed_mem - forest_mem

        kv("  Fixed components (cache+base+file)", fmt_mb(fixed_mem),         indent=6)
        kv("  Current forest memory",              fmt_mb(forest_mem),        indent=6)
        kv("  Remaining for forest growth",
           f"{fmt_mb(forest_headroom)}  {status_badge(forest_headroom > 512, 'OK', 'LOW')}",
           indent=6)

        # ── Fragmentation warning ──────────────────────────────────────
        if frag_pct >= 25:
            print()
            if frag_pct >= 50:
                print(f"    {color('!' * 3, RED)} {color(f'FRAGMENTATION: {frag_pct:.0f}% deleted fragments', RED + BOLD)}")
                print(f"    {color('!' * 3, RED)} {color('Disk and memory metrics are significantly inflated.', RED)}")
                print(f"    {color('!' * 3, RED)} {color('Capacity projections below are UNRELIABLE until a merge completes.', RED + BOLD)}")
                print(f"    {color('!' * 3, RED)} {color('Force a merge, then re-run this report for accurate projections.', RED)}")
            else:
                print(f"    {color('!', YELLOW)} {color(f'Fragmentation: {frag_pct:.0f}% deleted fragments', YELLOW + BOLD)}")
                print(f"    {color('  Disk metrics may be inflated. Consider a merge for more accurate projections.', DIM)}")

        # Primary: disk-based projection
        if total_docs > 0 and total_disk > 0:
            disk_bytes_per_doc = (total_disk * 1024 * 1024) / total_docs

            # If fragmentation is high, estimate the "clean" bytes/doc.
            # Deleted fragments don't occupy space proportional to their count —
            # they share stands with active fragments, and document sizes vary.
            # Use a conservative model: assume deleted fragments occupy at most
            # half the per-fragment average. This avoids over-estimating the space
            # reclaimed by merging (validated: raw active/total ratio overpredicts
            # reclamation by ~40% in testing).
            if frag_pct >= 25 and total_frags > 0:
                avg_bytes_per_frag = (total_disk * 1024 * 1024) / total_frags
                estimated_waste = total_deleted * avg_bytes_per_frag * 0.5
                estimated_clean_disk = max(total_disk - estimated_waste / (1024 * 1024),
                                           total_disk * 0.5)  # floor: at least 50% remains
                clean_bytes_per_doc = (estimated_clean_disk * 1024 * 1024) / total_docs
            else:
                clean_bytes_per_doc = None

            print()
            print(f"    {color('Disk capacity (primary — most reliable)', BOLD)}")
            kv("  Current disk size",  fmt_mb(total_disk), indent=6)
            kv("  Disk bytes/doc",     f"{disk_bytes_per_doc:,.0f}", indent=6)

            if clean_bytes_per_doc is not None:
                kv("  Est. bytes/doc after merge",
                   f"{color(f'{clean_bytes_per_doc:,.0f}', BOLD)}  "
                   f"{color(f'({frag_pct:.0f}% fragmentation, ~{estimated_clean_disk:.0f} MB estimated post-merge)', DIM)}",
                   indent=6)

            if remaining_disk_mb > 0:
                # Use clean estimate if fragmentation is high
                proj_bpd = clean_bytes_per_doc if clean_bytes_per_doc else disk_bytes_per_doc
                disk_docs_remaining = int((remaining_disk_mb * 1024 * 1024) / proj_bpd)
                disk_total = total_docs + disk_docs_remaining
                kv("  Disk remaining",     fmt_mb(remaining_disk_mb), indent=6)
                kv("  Est. additional documents until disk full",
                   f"{color(f'{disk_docs_remaining:,}', BOLD)}"
                   + (f"  {color('(adjusted for fragmentation)', DIM)}" if clean_bytes_per_doc else ""),
                   indent=6)
                kv("  Est. total documents at disk full",
                   f"{disk_total:,}", indent=6)

        # Secondary: forest memory indicator (shown for context, not for projection)
        if total_docs > 0 and forest_mem > 0:
            forest_bytes_per_doc = (forest_mem * 1024 * 1024) / total_docs
            print()
            print(f"    {color('Memory runway (use --trend for growth-rate projection)', BOLD)}")
            kv("  Current forest memory",         fmt_mb(forest_mem), indent=6)
            kv("  Forest headroom",               fmt_mb(forest_headroom), indent=6)
            kv("  Forest memory/doc (snapshot)",   f"{forest_bytes_per_doc:,.0f} bytes", indent=6)
            print()
            print(f"      {color('Memory is the binding constraint in most deployments.', YELLOW)}")
            print(f"      {color('Forest memory fluctuates with merges — point-in-time snapshots', DIM)}")
            print(f"      {color('can over- or under-estimate. For reliable memory runway:', DIM)}")
            print(f"      {color('take snapshots over time, then run: --trend', BOLD)}")

        elif total_docs == 0:
            kv("  No documents loaded yet", "load docs to establish per-doc baseline", indent=6)

    sub_header("Stand-Based Limits")
    MAX_STANDS = 64
    for f in forest_data:
        name = f.get("forest-name", "?")
        stands = f.get("stand-count", 0) or 0
        remaining_stands = MAX_STANDS - stands
        # Each stand flush happens at in-memory-limit fragments
        # So remaining ingestion bursts before hitting stand limit:
        remaining_flushes = remaining_stands
        docs_per_flush = in_mem_limit_kb  # roughly 1 fragment per KB, conservative
        kv(f"Forest '{name}' stands remaining",
           f"{remaining_stands} / {MAX_STANDS}  {status_badge(remaining_stands > 10, 'OK', 'MERGE NEEDED')}")
        if remaining_stands < 10:
            print(f"      {color('  Merges must keep up with ingestion to avoid stand exhaustion', YELLOW)}")

    # Final summary
    sub_header("Scaling Recommendations")

    issues = []
    if frags_pct_used > 70:
        issues.append(("Add forests", f"Fragment utilization at {frags_pct_used:.0f}% - add forests to increase fragment capacity"))

    for f in forest_data:
        stands = f.get("stand-count", 0) or 0
        if stands > 48:
            issues.append(("Merge pressure", f"Forest '{f.get('forest-name')}' has {stands}/64 stands"))
        deleted = f.get("deleted-fragment-count", 0) or 0
        active = f.get("active-fragment-count", 0) or 0
        if active > 0 and deleted > 0:
            f_ratio = deleted / (active + deleted)
            f_name = f.get("forest-name", "?")
            if f_ratio > 0.50:
                issues.append(("Force merge needed",
                    f"Forest '{f_name}' at {f_ratio*100:.0f}% deleted fragments — "
                    f"projections unreliable, run: xdmp:merge(xdmp:database-forests(xdmp:database(\"{f_name}\")))"))
            elif f_ratio > 0.25:
                issues.append(("High fragmentation",
                    f"Forest '{f_name}' at {f_ratio*100:.0f}% deleted fragments — "
                    f"consider forcing a merge to reclaim space"))

    if host_data:
        total_sys = sum(float(v) for h in host_data if (v := h.get("memory-system-total-mb")) is not None)
        ml_rss = sum(float(v) for h in host_data if (v := h.get("memory-process-rss-mb")) is not None)
        if total_sys > 0 and ml_rss / total_sys > 0.7:
            issues.append(("Memory pressure", f"ML process using {ml_rss/total_sys*100:.0f}% of system memory"))

    if not issues:
        print(f"    {color('No immediate scaling concerns detected.', GREEN)}")
        print(f"    Cluster has capacity for significant additional document loading.")
    else:
        for title, detail in issues:
            print(f"    {color('!', RED)} {color(title, BOLD)}: {detail}")


# ── Snapshots ──────────────────────────────────────────────────────

SNAPSHOT_DIR = Path(__file__).parent / ".ml-capacity"


def collect_snapshot(client, database):
    """Gather all capacity metrics into a single JSON-serializable dict.

    This is the canonical snapshot format — everything the report sections
    display, collected once, so we can both print and persist from the
    same data.
    """
    snap = {
        "version": 1,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "database": database,
    }

    # Cluster
    cluster_raw = collect_cluster_overview(client)
    cluster = cluster_raw.get("local-cluster-default", {})
    snap["cluster"] = {
        "name": cluster.get("name"),
        "version": cluster.get("version"),
    }
    relations = cluster.get("relations", {}).get("relation-group", [])
    for rg in relations:
        t = rg.get("typeref", "")
        if t in ("hosts", "databases", "forests", "servers"):
            snap["cluster"][t] = rg.get("relation-count", {}).get("value", 0)

    # Host memory (via eval)
    try:
        host_results = collect_host_memory(client)
        if host_results:
            raw = host_results[0] if isinstance(host_results[0], list) else [host_results[0]]
            snap["hosts"] = raw
        else:
            snap["hosts"] = []
    except Exception:
        snap["hosts"] = []

    # Database status
    db_raw = collect_database_status(client, database)
    props = db_raw.get("database-status", {}).get("status-properties", {})
    snap["database_status"] = {
        "state":           props.get("state", {}).get("value"),
        "forests_count":   props.get("forests-count", {}).get("value", 0),
        "data_size_mb":    props.get("data-size", {}).get("value", 0),
        "device_space_mb": float(props.get("device-space", {}).get("value", "0")),
        "in_memory_size_mb": props.get("in-memory-size", {}).get("value", 0),
        "large_data_size_mb": props.get("large-data-size", {}).get("value", 0),
        "least_remaining_mb": float(props.get("least-remaining-space-forest", {}).get("value", 0)),
        "merge_count":     props.get("merge-count", {}).get("value", 0),
        "list_cache_ratio": props.get("cache-properties", {}).get("list-cache-ratio", {}).get("value", 0),
    }

    # Forest health (via eval)
    try:
        fc_results = collect_forest_counts(client, database)
        if fc_results:
            forests = fc_results[0] if isinstance(fc_results[0], list) else [fc_results[0]]
            snap["forests"] = forests
        else:
            snap["forests"] = []
    except Exception:
        snap["forests"] = []

    # Database properties (for index config)
    db_props = collect_database_properties(client, database)
    snap["db_properties"] = {
        "in_memory_limit":       db_props.get("in-memory-limit", 131072),
        "in_memory_list_size":   db_props.get("in-memory-list-size", 256),
        "in_memory_tree_size":   db_props.get("in-memory-tree-size", 64),
        "in_memory_range_index_size": db_props.get("in-memory-range-index-size", 8),
        "in_memory_triple_index_size": db_props.get("in-memory-triple-index-size", 21),
        "preload_mapped_data":   db_props.get("preload-mapped-data", False),
    }

    # Count indexes
    range_el = db_props.get("range-element-index", [])
    range_path = db_props.get("range-path-index", [])
    range_field = db_props.get("range-field-index", [])
    enabled_bools = sum(1 for k in [
        "word-searches", "fast-phrase-searches", "triple-index",
        "fast-case-sensitive-searches", "fast-diacritic-sensitive-searches",
        "fast-element-word-searches", "fast-element-phrase-searches",
        "uri-lexicon", "collection-lexicon", "trailing-wildcard-searches",
        "three-character-searches", "field-value-searches",
    ] if db_props.get(k) is True or db_props.get(k) == "true")
    snap["index_counts"] = {
        "range_element": len(range_el),
        "range_path":    len(range_path),
        "range_field":   len(range_field),
        "enabled_boolean_indexes": enabled_bools,
    }

    # Index memory (via eval — ML 11+)
    try:
        idx_results = client.eval_javascript(_INDEX_MEMORY_JS, database=database,
                                             vars={"dbName": database})
        if idx_results:
            snap["index_memory"] = idx_results[0]
        else:
            snap["index_memory"] = None
    except Exception:
        snap["index_memory"] = None

    # Derived totals for easy trending
    total_docs    = sum(f.get("document-count", 0) or 0 for f in snap["forests"])
    total_active  = sum(f.get("active-fragment-count", 0) or 0 for f in snap["forests"])
    total_deleted = sum(f.get("deleted-fragment-count", 0) or 0 for f in snap["forests"])
    total_disk    = sum(f.get("disk-size-mb", 0) or 0 for f in snap["forests"])
    total_mem     = sum(f.get("memory-size-mb", 0) or 0 for f in snap["forests"])

    def hsum(key):
        return sum(float(v) for h in snap["hosts"] if (v := h.get(key)) is not None)

    snap["totals"] = {
        "documents":        total_docs,
        "active_fragments": total_active,
        "deleted_fragments": total_deleted,
        "forest_disk_mb":   total_disk,
        "forest_memory_mb": total_mem,
        "host_forest_mb":   hsum("memory-forest-size-mb"),
        "host_cache_mb":    hsum("memory-cache-size-mb"),
        "host_rss_mb":      hsum("memory-process-rss-mb"),
        "host_base_mb":     hsum("host-size-mb"),
        "host_file_mb":     hsum("memory-file-size-mb"),
        "ml_limit_mb":      hsum("memory-size-mb"),
        "system_total_mb":  hsum("memory-system-total-mb"),
        "system_free_mb":   hsum("memory-system-free-mb"),
    }

    # In containers, system_total_mb may be 0 (cgroup doesn't expose it).
    # Fall back to the ML configured limit (memory-size) as the ceiling.
    if snap["totals"]["system_total_mb"] == 0 and snap["totals"]["ml_limit_mb"] > 0:
        snap["totals"]["system_total_mb"] = snap["totals"]["ml_limit_mb"]

    return snap


def save_snapshot(snap):
    """Save a snapshot to .ml-capacity/ as a timestamped JSON file."""
    SNAPSHOT_DIR.mkdir(exist_ok=True)
    ts = snap["timestamp"].replace(":", "").replace("-", "")[:15]
    db = snap["database"]
    filename = f"{ts}_{db}.json"
    path = SNAPSHOT_DIR / filename
    with open(path, "w") as f:
        json.dump(snap, f, indent=2, default=str)
    return path


def prune_snapshots(retention_days):
    """Delete snapshot files older than retention_days. 0 means keep all."""
    if retention_days <= 0 or not SNAPSHOT_DIR.exists():
        return 0
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    removed = 0
    for p in sorted(SNAPSHOT_DIR.glob("*.json")):
        try:
            # Parse timestamp from filename: YYYYMMDDTHHMMSS_DB.json
            ts_str = p.stem.split("_")[0]
            ts = datetime.strptime(ts_str, "%Y%m%dT%H%M%S").replace(
                tzinfo=timezone.utc)
            if ts < cutoff:
                p.unlink()
                removed += 1
        except (ValueError, IndexError, OSError):
            continue
    return removed


def load_snapshots(database=None):
    """Load all snapshots, optionally filtered by database. Returns sorted by timestamp."""
    if not SNAPSHOT_DIR.exists():
        return []
    snaps = []
    for p in sorted(SNAPSHOT_DIR.glob("*.json")):
        try:
            with open(p) as f:
                s = json.load(f)
            if database and s.get("database") != database:
                continue
            s["_file"] = p.name
            snaps.append(s)
        except (json.JSONDecodeError, KeyError):
            continue
    return snaps


def list_snapshots(database=None):
    """Print a table of saved snapshots."""
    snaps = load_snapshots(database)
    if not snaps:
        print("    No snapshots found.")
        if database:
            print(f"    (filtered to database '{database}')")
        return

    print()
    hdr = f"  {'#':>4}  {'Timestamp':24}  {'Database':16}  {'Documents':>12}  {'Forest Disk':>12}  {'RSS':>10}"
    print(color(hdr, BOLD))
    print(color("  " + "-" * (len(hdr) - 2), DIM))

    for i, s in enumerate(snaps):
        t = s.get("totals", {})
        ts = s.get("timestamp", "?")[:19].replace("T", " ")
        db = s.get("database", "?")
        docs = t.get("documents", 0)
        disk = t.get("forest_disk_mb", 0)
        rss  = t.get("host_rss_mb", 0)
        print(
            f"  {i:>4}  {ts:24}  {db:16}  {docs:>12,}  "
            f"{fmt_mb(disk):>12}  {fmt_mb(rss):>10}"
        )
    print()
    return snaps


def extract_config_fingerprint(snap):
    """Extract the configuration fields that should remain constant.

    If any of these change between snapshots, trend analysis is unreliable
    because the baseline shifted — e.g. adding RAM, changing cache sizes,
    adding range indexes, or adding hosts all change what "growth" means.
    """
    cluster = snap.get("cluster", {})
    db_props = snap.get("db_properties", {})
    idx_counts = snap.get("index_counts", {})
    db_status = snap.get("database_status", {})
    hosts = snap.get("hosts", [])

    # Per-host config (sorted by hostname for stable comparison)
    host_configs = []
    for h in sorted(hosts, key=lambda x: x.get("hostname", "")):
        host_configs.append({
            "hostname":       h.get("hostname"),
            "system_ram_mb":  h.get("memory-system-total-mb"),
            "ml_limit_mb":    h.get("memory-size-mb"),
            "cache_alloc_mb": h.get("memory-cache-size-mb"),
            "cores":          h.get("cores"),
        })

    return {
        "ml_version":       cluster.get("version"),
        "host_count":       cluster.get("hosts", 0),
        "forest_count":     db_status.get("forests_count", 0),
        "hosts":            host_configs,
        "in_memory_limit":       db_props.get("in_memory_limit"),
        "in_memory_list_size":   db_props.get("in_memory_list_size"),
        "in_memory_tree_size":   db_props.get("in_memory_tree_size"),
        "in_memory_range_index_size": db_props.get("in_memory_range_index_size"),
        "in_memory_triple_index_size": db_props.get("in_memory_triple_index_size"),
        "preload_mapped_data":  db_props.get("preload_mapped_data"),
        "range_element_indexes": idx_counts.get("range_element", 0),
        "range_path_indexes":    idx_counts.get("range_path", 0),
        "range_field_indexes":   idx_counts.get("range_field", 0),
        "enabled_boolean_indexes": idx_counts.get("enabled_boolean_indexes", 0),
    }


# Fields reported by the Management API in MB that can wobble +/-1
# due to internal rounding (e.g. memory-cache-size reported as 5121
# one call and 5120 the next despite no config change).
_FUZZY_MB_FIELDS = frozenset({
    "system_ram_mb", "ml_limit_mb", "cache_alloc_mb",
})


def _values_match(a, b, field_name=""):
    """Compare two config values, with tolerance for known noisy MB fields.

    Only fields in _FUZZY_MB_FIELDS get tolerance (0.5% or 2 MB).
    All other fields (counts, strings, versions) use exact equality
    so that e.g. an index count changing from 3 to 5 is always caught.

    None/0 values are treated as "unknown" and always match — this
    handles container environments where some OS metrics aren't available
    in early snapshots but get populated later via fallback.
    """
    if a == b:
        return True
    # Treat None or 0 as "unknown" — don't flag drift for missing data.
    # Trade-off: won't detect removal of ALL range indexes (count→0), but
    # that's extremely rare and the alternative (false alarms in containers
    # where OS metrics start as 0/None) is worse.
    if a is None or b is None or a == 0 or b == 0:
        return True
    if field_name in _FUZZY_MB_FIELDS and isinstance(a, (int, float)) and isinstance(b, (int, float)):
        abs_tol = max(2, abs(a) * 0.005)  # 0.5% or 2 MB, whichever is larger
        return abs(a - b) <= abs_tol
    return False


def check_config_drift(snaps):
    """Compare configuration fingerprints across snapshots.

    Returns a list of (field, old_value, new_value, snap_index) tuples
    for any configuration changes detected. An empty list means all
    snapshots share the same configuration — safe for trending.
    """
    if len(snaps) < 2:
        return []

    baseline = extract_config_fingerprint(snaps[0])
    drift = []

    for i, snap in enumerate(snaps[1:], start=1):
        current = extract_config_fingerprint(snap)

        # Compare all top-level keys except 'hosts' (handled separately)
        for key in baseline:
            if key == "hosts":
                continue
            if not _values_match(baseline[key], current.get(key), key):
                drift.append((key, baseline[key], current.get(key), i))

        # Compare per-host config
        if len(baseline.get("hosts", [])) != len(current.get("hosts", [])):
            drift.append(("host_configs",
                          f"{len(baseline.get('hosts', []))} host(s)",
                          f"{len(current.get('hosts', []))} host(s)", i))
        else:
            for bh, ch in zip(baseline.get("hosts", []), current.get("hosts", [])):
                for hk in bh:
                    if not _values_match(bh[hk], ch.get(hk), hk):
                        host = bh.get("hostname", "?")
                        drift.append((f"host[{host}].{hk}", bh[hk], ch.get(hk), i))

    return drift


def report_config_drift(snaps):
    """Check and report configuration drift across snapshots.

    Returns True if the configuration is stable (safe for trending),
    False if drift was detected.
    """
    drift = check_config_drift(snaps)

    sub_header("Configuration Stability Check")

    if not drift:
        print(f"    {color('STABLE', GREEN)} — cluster config, cache sizes, index settings,")
        print(f"    and system resources are consistent across all {len(snaps)} snapshots.")
        return True

    # Deduplicate: only show the first occurrence of each field change
    seen = set()
    unique_drift = []
    for field, old_val, new_val, snap_idx in drift:
        key = (field, str(old_val), str(new_val))
        if key not in seen:
            seen.add(key)
            unique_drift.append((field, old_val, new_val, snap_idx))

    print(f"    {color('DRIFT DETECTED', RED + BOLD)} — configuration changed between snapshots.")
    print(f"    Trend projections may be unreliable across these changes.")
    print()

    field_descriptions = {
        "ml_version":                "MarkLogic version",
        "host_count":                "Number of hosts in cluster",
        "forest_count":              "Number of forests on database",
        "host_configs":              "Host topology",
        "in_memory_limit":           "In-memory stand flush threshold (KB)",
        "in_memory_list_size":       "In-memory list size (MB)",
        "in_memory_tree_size":       "In-memory tree size (MB)",
        "in_memory_range_index_size":"In-memory range index size (MB)",
        "in_memory_triple_index_size":"In-memory triple index size (MB)",
        "preload_mapped_data":       "Preload mapped data (range indexes loaded on forest open)",
        "range_element_indexes":     "Element range index count",
        "range_path_indexes":        "Path range index count",
        "range_field_indexes":       "Field range index count",
        "enabled_boolean_indexes":   "Enabled boolean index count",
    }

    for field, old_val, new_val, snap_idx in unique_drift:
        label = field_descriptions.get(field, field)
        snap_ts = snaps[snap_idx].get("timestamp", "?")[:19].replace("T", " ")
        print(f"    {color('!', RED)} {color(label, BOLD)}")
        print(f"        {old_val} -> {new_val}  {color(f'(snapshot #{snap_idx}, {snap_ts})', DIM)}")

    print()
    print(f"    {color('Tip:', YELLOW)} For accurate trends, compare snapshots with the same")
    print(f"    configuration. Use --compare N to diff specific snapshots.")
    return False


def report_trend(database):
    """Show growth curves from saved snapshots for a database."""
    header(f"TREND ANALYSIS: {database}")

    snaps = load_snapshots(database)
    if len(snaps) < 2:
        print(f"    Need at least 2 snapshots to show trends (found {len(snaps)}).")
        print(f"    Run the analyzer multiple times to build history.")
        return

    sub_header("Snapshot History")
    list_snapshots(database)

    # ── Configuration drift check ────────────────────────────────
    config_stable = report_config_drift(snaps)

    # Extract time series
    points = []
    for s in snaps:
        t = s.get("totals", {})
        ts_str = s.get("timestamp", "")
        try:
            ts = datetime.fromisoformat(ts_str)
        except (ValueError, TypeError):
            continue
        points.append({
            "ts": ts,
            "docs":       t.get("documents", 0),
            "disk_mb":    t.get("forest_disk_mb", 0),
            "forest_mb":  t.get("host_forest_mb", 0),
            "rss_mb":     t.get("host_rss_mb", 0),
            "fragments":  t.get("active_fragments", 0),
            "sys_total":  t.get("system_total_mb", 0),
            "cache_mb":   t.get("host_cache_mb", 0),
            "base_mb":    t.get("host_base_mb", 0),
            "file_mb":    t.get("host_file_mb", 0),
        })

    if len(points) < 2:
        print("    Insufficient valid data points.")
        return

    first = points[0]
    last  = points[-1]
    span  = (last["ts"] - first["ts"]).total_seconds()
    days  = span / 86400 if span > 0 else 0

    # ── Memory runway (primary) ──────────────────────────────────
    # Memory is the binding constraint in the vast majority of ML
    # deployments. Disk is easy to add; memory is not. Lead with it.

    sys_total   = last["sys_total"]
    cache_mb    = last["cache_mb"]
    base_mb     = last["base_mb"]
    file_mb     = last["file_mb"]
    forest_now  = last["forest_mb"]
    fixed_mem   = cache_mb + base_mb + file_mb
    ceiling     = sys_total * 0.80
    headroom    = ceiling - fixed_mem - forest_now

    forest_delta = last["forest_mb"] - first["forest_mb"]
    doc_delta    = last["docs"]      - first["docs"]
    disk_delta   = last["disk_mb"]   - first["disk_mb"]

    sub_header("Memory Runway (primary — most deployments are memory-bound)")
    kv("System RAM",                fmt_mb(sys_total))
    kv("Fixed memory (cache+base)", fmt_mb(fixed_mem))
    kv("Current forest memory",     fmt_mb(forest_now))
    mem_used_pct = ((fixed_mem + forest_now) / ceiling * 100) if ceiling else 0
    kv("Memory ceiling (80% RAM)",  f"{fmt_mb(ceiling)}  {bar(mem_used_pct)}")
    kv("Forest headroom",          f"{fmt_mb(headroom)}  {status_badge(headroom > 1024, 'OK', 'LOW')}")

    if days > 0 and forest_delta > 0 and headroom > 0:
        daily_forest = forest_delta / days
        days_until_mem = headroom / daily_forest
        print()
        kv("Forest memory growth",  f"{fmt_mb(first['forest_mb'])} → {fmt_mb(forest_now)}  "
                                    f"{color('+' + fmt_mb(forest_delta), YELLOW)}")
        kv("Growth rate",           f"{color(fmt_mb(daily_forest) + '/day', BOLD)}")
        kv("Observed period",       f"{days:.1f} days ({len(points)} snapshots)")
        if doc_delta > 0:
            forest_bytes_per_doc = (forest_delta * 1024 * 1024) / doc_delta
            kv("Marginal cost/doc", f"{forest_bytes_per_doc:,.0f} bytes forest memory per doc")
            docs_until_ceil = int((headroom * 1024 * 1024) / forest_bytes_per_doc)
            kv("Est. docs until ceiling", f"{color(f'{docs_until_ceil:,}', BOLD)}")
        print()
        print(f"    {color('>>> ', RED)}{color(f'MEMORY RUNWAY: ~{days_until_mem:.0f} days at current growth rate', BOLD + RED)}")
        print(f"    {color('>>> ', RED)}{color(f'Action needed before: forest memory reaches {fmt_mb(ceiling - fixed_mem)} '
              f'(currently {fmt_mb(forest_now)})', DIM)}")
    elif days > 0 and forest_delta <= 0:
        print()
        print(f"    {color('Forest memory stable or shrinking over this period.', GREEN)}")
        print(f"    No immediate memory pressure detected.")
    elif days == 0:
        print()
        print(f"    {color('Snapshots are too close together for rate calculation.', YELLOW)}")
        print(f"    Take snapshots hours or days apart for meaningful trends.")

    # ── Growth summary ─────────────────────────────────────────────
    sub_header("Growth Summary")
    kv("Time span", f"{days:.1f} days ({len(points)} snapshots)")

    metrics = [
        ("Documents",     "docs",      False),
        ("Forest memory", "forest_mb", True),
        ("Forest disk",   "disk_mb",   True),
        ("RSS",           "rss_mb",    True),
        ("Fragments",     "fragments", False),
    ]

    for label, key, is_mb in metrics:
        v_first = first[key]
        v_last  = last[key]
        delta   = v_last - v_first

        if is_mb:
            first_s = fmt_mb(v_first)
            last_s  = fmt_mb(v_last)
            delta_s = f"+{fmt_mb(delta)}" if delta >= 0 else f"-{fmt_mb(-delta)}"
        else:
            first_s = f"{v_first:,}"
            last_s  = f"{v_last:,}"
            delta_s = f"+{delta:,}" if delta >= 0 else f"{delta:,}"

        rate_s = ""
        if days > 0 and delta != 0:
            daily = delta / days
            if is_mb:
                rate_s = f"  ({fmt_mb(daily)}/day)"
            else:
                rate_s = f"  ({daily:,.0f}/day)"

        kv(label, f"{first_s} → {last_s}  {color(delta_s, GREEN if delta >= 0 else RED)}{rate_s}")

    # ── Other runway projections ───────────────────────────────────
    if days > 0:
        sub_header("Other Resource Runways")

        # Disk runway
        remaining_disk = 0
        for s in reversed(snaps):
            rd = s.get("database_status", {}).get("least_remaining_mb", 0)
            if rd:
                remaining_disk = float(rd)
                break

        days_until_disk = None
        if disk_delta > 0 and remaining_disk > 0:
            daily_disk = disk_delta / days
            days_until_disk = remaining_disk / daily_disk
            kv("Disk free",                fmt_mb(remaining_disk))
            kv("Disk growth rate",          f"{fmt_mb(daily_disk)}/day")
            kv("Days until disk full",      f"{days_until_disk:,.0f} days")
        elif disk_delta <= 0:
            kv("Disk", color("stable or shrinking", GREEN))

        # Fragment runway
        days_until_frag = None
        MAX_FRAGS = 96_000_000 * max(1, len(snaps[-1].get("forests", [{}])))
        frag_delta = last["fragments"] - first["fragments"]
        frag_remaining = MAX_FRAGS - last["fragments"]
        if frag_delta > 0 and frag_remaining > 0:
            daily_frags = frag_delta / days
            days_until_frag = frag_remaining / daily_frags
            kv("Fragment headroom",         f"{frag_remaining:,}")
            kv("Fragment growth rate",      f"{daily_frags:,.0f}/day")
            kv("Days until fragment limit", f"{days_until_frag:,.0f} days")

        # ── Binding constraint summary ─────────────────────────────
        sub_header("Binding Constraint")
        days_until_mem = None
        if forest_delta > 0 and headroom > 0:
            days_until_mem = headroom / (forest_delta / days)

        runways = []
        if days_until_mem is not None:
            runways.append(("MEMORY", days_until_mem))
        if days_until_disk is not None:
            runways.append(("DISK", days_until_disk))
        if days_until_frag is not None:
            runways.append(("FRAGMENTS", days_until_frag))

        if runways:
            runways.sort(key=lambda x: x[1])
            for name, d in runways:
                is_binding = (name == runways[0][0])
                marker = color(">>>", RED) if is_binding else "   "
                c = RED + BOLD if is_binding else DIM
                label = f"{name:12} ~{d:,.0f} days"
                if is_binding:
                    label += "  <-- BINDING CONSTRAINT"
                print(f"    {marker} {color(label, c)}")
        else:
            print(f"    {color('No growth detected — all resources stable.', GREEN)}")


def report_compare(database, compare_idx):
    """Compare current snapshot to a past snapshot by index number."""
    header(f"COMPARISON: {database}")

    snaps = load_snapshots(database)
    if not snaps:
        print("    No snapshots found.")
        return

    if compare_idx < 0 or compare_idx >= len(snaps):
        print(f"    Snapshot #{compare_idx} not found. Valid range: 0–{len(snaps)-1}")
        return

    old = snaps[compare_idx]
    # Current state = most recent snapshot (or we could collect fresh, but
    # the caller should have just saved one)
    new = snaps[-1]

    if old["_file"] == new["_file"]:
        print("    Comparing snapshot to itself — nothing to show.")
        print("    Run the analyzer again to create a new snapshot, then compare.")
        return

    old_ts = old.get("timestamp", "?")[:19].replace("T", " ")
    new_ts = new.get("timestamp", "?")[:19].replace("T", " ")

    kv("Old snapshot", f"#{compare_idx}  {old_ts}  ({old.get('_file', '?')})")
    kv("New snapshot", f"#{len(snaps)-1}  {new_ts}  ({new.get('_file', '?')})")

    # ── Configuration drift check between the two snapshots ──────
    report_config_drift([old, new])

    old_t = old.get("totals", {})
    new_t = new.get("totals", {})

    # Time elapsed
    try:
        t_old = datetime.fromisoformat(old["timestamp"])
        t_new = datetime.fromisoformat(new["timestamp"])
        days = (t_new - t_old).total_seconds() / 86400
        kv("Time elapsed", f"{days:.1f} days")
    except (ValueError, TypeError):
        days = 0

    sub_header("Metric Deltas")

    def delta_line(label, old_val, new_val, is_mb=False):
        d = new_val - old_val
        if is_mb:
            old_s = fmt_mb(old_val)
            new_s = fmt_mb(new_val)
            d_s   = f"+{fmt_mb(d)}" if d >= 0 else f"-{fmt_mb(-d)}"
        else:
            old_s = f"{old_val:,}"
            new_s = f"{new_val:,}"
            d_s   = f"+{d:,}" if d >= 0 else f"{d:,}"

        pct = (d / old_val * 100) if old_val else 0
        pct_s = f"({pct:+.1f}%)" if old_val else ""
        c = GREEN if d >= 0 else RED
        kv(label, f"{old_s} → {new_s}  {color(d_s, c)}  {color(pct_s, DIM)}")

    delta_line("Documents",      old_t.get("documents", 0),       new_t.get("documents", 0))
    delta_line("Active frags",   old_t.get("active_fragments", 0),new_t.get("active_fragments", 0))
    delta_line("Deleted frags",  old_t.get("deleted_fragments",0),new_t.get("deleted_fragments",0))
    delta_line("Forest disk",    old_t.get("forest_disk_mb", 0),  new_t.get("forest_disk_mb", 0), True)
    delta_line("Forest memory",  old_t.get("forest_memory_mb",0), new_t.get("forest_memory_mb",0),True)
    delta_line("Host forest",    old_t.get("host_forest_mb", 0),  new_t.get("host_forest_mb", 0), True)
    delta_line("Host RSS",       old_t.get("host_rss_mb", 0),     new_t.get("host_rss_mb", 0),    True)
    delta_line("System free",    old_t.get("system_free_mb", 0),  new_t.get("system_free_mb", 0), True)

    # Per-doc marginal cost
    doc_delta = new_t.get("documents", 0) - old_t.get("documents", 0)
    if doc_delta > 0:
        sub_header("Marginal Cost Per Document")
        disk_delta   = new_t.get("forest_disk_mb", 0) - old_t.get("forest_disk_mb", 0)
        forest_delta = new_t.get("host_forest_mb", 0) - old_t.get("host_forest_mb", 0)

        if disk_delta > 0:
            kv("Disk bytes/doc",   f"{disk_delta * 1024 * 1024 / doc_delta:,.0f} bytes")
        if forest_delta > 0:
            kv("Forest mem bytes/doc", f"{forest_delta * 1024 * 1024 / doc_delta:,.0f} bytes")

    # Index count changes
    old_ic = old.get("index_counts", {})
    new_ic = new.get("index_counts", {})
    if old_ic != new_ic:
        sub_header("Index Configuration Changes")
        for k in set(list(old_ic.keys()) + list(new_ic.keys())):
            ov = old_ic.get(k, 0)
            nv = new_ic.get(k, 0)
            if ov != nv:
                kv(k, f"{ov} → {nv}")

    # Per-index memory diff (if both snapshots have index_memory data)
    if old.get("index_memory") and new.get("index_memory"):
        diff = diff_index_memory(old, new)
        if diff["added"] or diff["removed"] or diff["changed"]:
            sub_header("Per-Index Memory Changes")
            for idx in diff["added"]:
                mem = idx.get("totalMemoryBytes", 0) or 0
                disk = idx.get("totalOnDiskBytes", 0) or 0
                kv(f"  + {_index_label(idx)}",
                   f"mem: {fmt_mb(mem/(1024*1024))}  disk: {fmt_mb(disk/(1024*1024))}")
            for idx in diff["removed"]:
                mem = idx.get("totalMemoryBytes", 0) or 0
                disk = idx.get("totalOnDiskBytes", 0) or 0
                kv(f"  - {_index_label(idx)}",
                   f"mem: -{fmt_mb(mem/(1024*1024))}  disk: -{fmt_mb(disk/(1024*1024))}")
            for c in diff["changed"]:
                dm = c["delta_mem"]
                dd = c["delta_disk"]
                if abs(dm) > 1024 or abs(dd) > 1024:  # only show >1KB changes
                    kv(f"  ~ {_index_label(c['index'])}",
                       f"mem: {'+' if dm >= 0 else ''}{fmt_mb(dm/(1024*1024))}  "
                       f"disk: {'+' if dd >= 0 else ''}{fmt_mb(dd/(1024*1024))}")
            s = diff["summary"]
            print()
            kv("Net index memory",
               f"{'+' if s['total_mem_delta'] >= 0 else ''}{fmt_mb(s['total_mem_delta']/(1024*1024))}")
            print(f"\n    {color('For detailed analysis: --index-impact', DIM)}")


# ── Prometheus format ───────────────────────────────────────────────

def snapshot_to_prometheus(snap):
    """Convert a snapshot to Prometheus text exposition format.

    Returns a string of Prometheus-format metrics. Works for both
    one-shot (--format prometheus) and service (/metrics endpoint).
    """
    lines = []

    def gauge(name, help_text, value, labels=None):
        if value is None:
            return
        lines.append(f"# HELP {name} {help_text}")
        lines.append(f"# TYPE {name} gauge")
        label_str = ""
        if labels:
            pairs = ",".join(f'{k}="{v}"' for k, v in labels.items())
            label_str = f"{{{pairs}}}"
        lines.append(f"{name}{label_str} {value}")

    db = snap.get("database", "unknown")
    t = snap.get("totals", {})
    db_labels = {"database": db}

    # ── Document & fragment metrics ────────────────────────────────
    gauge("mlca_documents_total",
          "Total document count",
          t.get("documents"), db_labels)
    gauge("mlca_fragments_active",
          "Active fragment count",
          t.get("active_fragments"), db_labels)
    gauge("mlca_fragments_deleted",
          "Deleted fragment count (fragmentation indicator)",
          t.get("deleted_fragments"), db_labels)

    active = t.get("active_fragments", 0)
    deleted = t.get("deleted_fragments", 0)
    total_frags = active + deleted
    if total_frags > 0:
        gauge("mlca_fragmentation_ratio",
              "Ratio of deleted to total fragments",
              round(deleted / total_frags, 4), db_labels)

    # ── Forest storage ─────────────────────────────────────────────
    gauge("mlca_forest_disk_mb",
          "Total on-disk size of all forests in MB",
          t.get("forest_disk_mb"), db_labels)
    gauge("mlca_forest_memory_mb",
          "Forest in-memory stand data in MB (per-db sum)",
          t.get("forest_memory_mb"), db_labels)

    # ── Host memory breakdown ──────────────────────────────────────
    for host in snap.get("hosts", []):
        hostname = host.get("hostname", "unknown")
        h_labels = {"host": hostname}

        gauge("mlca_host_rss_mb",
              "MarkLogic process resident set size in MB",
              host.get("memory-process-rss-mb"), h_labels)
        gauge("mlca_host_rss_hwm_mb",
              "Peak RSS since last restart in MB",
              host.get("memory-process-rss-hwm-mb"), h_labels)
        gauge("mlca_host_cache_mb",
              "Allocated list + compressed tree cache in MB (fixed)",
              host.get("memory-cache-size-mb"), h_labels)
        gauge("mlca_host_forest_mb",
              "Forest in-memory stand memory at host level in MB",
              host.get("memory-forest-size-mb"), h_labels)
        gauge("mlca_host_base_mb",
              "Base ML overhead in MB (threads, code — fixed)",
              host.get("host-size-mb"), h_labels)
        gauge("mlca_host_file_mb",
              "OS file cache (mmap) pages in MB",
              host.get("memory-file-size-mb"), h_labels)
        gauge("mlca_host_swap_mb",
              "ML process swap usage in MB (non-zero = memory pressure)",
              host.get("memory-process-swap-mb"), h_labels)
        gauge("mlca_system_total_mb",
              "Total system RAM in MB",
              host.get("memory-system-total-mb"), h_labels)
        gauge("mlca_system_free_mb",
              "Free system RAM in MB",
              host.get("memory-system-free-mb"), h_labels)
        gauge("mlca_ml_limit_mb",
              "Configured MarkLogic memory limit in MB",
              host.get("memory-size-mb"), h_labels)
        gauge("mlca_host_pagein_rate",
              "System page-in rate in MB/s",
              host.get("memory-system-pagein-rate"), h_labels)
        gauge("mlca_host_pageout_rate",
              "System page-out rate in MB/s",
              host.get("memory-system-pageout-rate"), h_labels)
        gauge("mlca_host_swapin_rate",
              "System swap-in rate in MB/s (non-zero = severe pressure)",
              host.get("memory-system-swapin-rate"), h_labels)

    # ── Memory capacity (computed) ─────────────────────────────────
    sys_total = t.get("system_total_mb", 0)
    cache_mb = t.get("host_cache_mb", 0)
    base_mb = t.get("host_base_mb", 0)
    file_mb = t.get("host_file_mb", 0)
    forest_mb = t.get("host_forest_mb", 0)
    fixed = cache_mb + base_mb + file_mb

    if sys_total > 0:
        ceiling = sys_total * 0.80
        headroom = ceiling - fixed - forest_mb
        gauge("mlca_memory_ceiling_mb",
              "Memory ceiling (80% of system RAM) in MB",
              round(ceiling, 1), db_labels)
        gauge("mlca_memory_fixed_mb",
              "Fixed memory components (cache + base + file) in MB",
              round(fixed, 1), db_labels)
        gauge("mlca_memory_headroom_mb",
              "Remaining memory headroom for forest growth in MB",
              round(headroom, 1), db_labels)
        if ceiling > 0:
            gauge("mlca_memory_utilization_ratio",
                  "Memory utilization ratio (fixed + forest) / ceiling",
                  round((fixed + forest_mb) / ceiling, 4), db_labels)

    # ── Disk capacity ──────────────────────────────────────────────
    db_status = snap.get("database_status", {})
    remaining = db_status.get("least_remaining_mb", 0)
    if remaining:
        gauge("mlca_disk_remaining_mb",
              "Least remaining disk space on any forest in MB",
              float(remaining), db_labels)

    docs = t.get("documents", 0)
    disk = t.get("forest_disk_mb", 0)
    if docs > 0 and disk > 0:
        gauge("mlca_disk_bytes_per_doc",
              "Disk bytes per document",
              round(disk * 1024 * 1024 / docs, 0), db_labels)

    # ── Per-index memory (ML 11+) ──────────────────────────────────
    idx_mem = snap.get("index_memory") or {}
    for idx in idx_mem.get("indexes", []):
        mem = idx.get("totalMemoryBytes", 0) or 0
        disk_b = idx.get("totalOnDiskBytes", 0) or 0
        name = idx.get("localname") or idx.get("pathExpression") or idx.get("indexType", "unknown")
        stype = idx.get("scalarType") or ""
        idx_label = f"{name}({stype})" if stype else name
        i_labels = {"database": db, "index": idx_label}
        gauge("mlca_index_memory_bytes",
              "Per-index memory in bytes",
              mem, i_labels)
        gauge("mlca_index_disk_bytes",
              "Per-index on-disk size in bytes",
              disk_b, i_labels)

    # ── Stand memory components ────────────────────────────────────
    stand_summaries = idx_mem.get("standSummaries", [])
    if stand_summaries:
        agg = {}
        for ss in stand_summaries:
            for k, v in ss.get("summary", {}).items():
                agg[k] = agg.get(k, 0) + (v or 0)
        component_metrics = {
            "rangeIndexesBytes":    ("mlca_stand_range_indexes_bytes", "Range index data in bytes"),
            "tripleIndexBytes":     ("mlca_stand_triple_index_bytes", "Triple index data in bytes"),
            "timestampsFileBytes":  ("mlca_stand_timestamps_bytes", "Timestamp data in bytes"),
            "listFileBytes":        ("mlca_stand_list_bytes", "List index data in bytes"),
            "treeFileBytes":        ("mlca_stand_tree_bytes", "Tree index data in bytes"),
        }
        for key, (metric_name, help_text) in component_metrics.items():
            if agg.get(key, 0) > 0:
                gauge(metric_name, help_text, agg[key], db_labels)

    lines.append("")  # trailing newline
    return "\n".join(lines)


# ── Service mode ───────────────────────────────────────────────────

def parse_interval(s):
    """Parse interval string like '5m', '15m', '1h', '30s' to seconds."""
    s = s.strip().lower()
    if s.endswith("s"):
        return int(s[:-1])
    if s.endswith("m"):
        return int(s[:-1]) * 60
    if s.endswith("h"):
        return int(s[:-1]) * 3600
    return int(s)  # bare number = seconds


def run_service(client, databases, interval_sec, port, otlp_endpoint=None,
                retention_days=30):
    """Run MLCA as a persistent service with HTTP endpoints.

    Collects snapshots on schedule, serves /metrics, /api/*, and web UI.
    """
    import threading
    from http.server import HTTPServer, BaseHTTPRequestHandler

    # Shared state
    latest_snapshots = {}  # database -> latest snap
    lock = threading.Lock()

    def collect_all():
        """Collect snapshots for all monitored databases."""
        for db in databases:
            try:
                snap = collect_snapshot(client, db)
                save_snapshot(snap)
                with lock:
                    latest_snapshots[db] = snap
                print(f"  [{datetime.now(timezone.utc).strftime('%H:%M:%S')}] "
                      f"Collected {db}: {snap['totals']['documents']:,} docs, "
                      f"forest={fmt_mb(snap['totals']['host_forest_mb'])}")
            except Exception as e:
                print(f"  [{datetime.now(timezone.utc).strftime('%H:%M:%S')}] "
                      f"Error collecting {db}: {e}")

    def schedule_loop():
        """Run collection on a repeating interval."""
        while True:
            collect_all()
            # Prune old snapshots
            removed = prune_snapshots(retention_days)
            if removed:
                print(f"  [{datetime.now(timezone.utc).strftime('%H:%M:%S')}] "
                      f"Pruned {removed} snapshot(s) older than "
                      f"{retention_days} days")
            # Push to OTLP if configured
            if otlp_endpoint:
                with lock:
                    for db, snap in latest_snapshots.items():
                        try:
                            push_otlp(snap, otlp_endpoint)
                        except Exception as e:
                            print(f"  OTLP push error: {e}")
            threading.Event().wait(interval_sec)

    class MLCAHandler(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            pass  # suppress default access logs

        def _parse_request(self):
            """Parse URL into path and query params."""
            from urllib.parse import urlparse, parse_qs
            parsed = urlparse(self.path)
            params = parse_qs(parsed.query)
            return parsed.path, params

        def _get_db_filter(self, params):
            """Extract optional database filter from query params."""
            vals = params.get("database", [])
            return vals[0] if vals else None

        def do_GET(self):
            path, params = self._parse_request()
            db_filter = self._get_db_filter(params)

            if path == "/metrics":
                self._serve_metrics()
            elif path == "/api/snapshot":
                self._serve_json_snapshot(db_filter)
            elif path == "/api/snapshots":
                self._serve_json_snapshots_list(db_filter)
            elif path == "/api/trend":
                self._serve_json_trend(db_filter)
            elif path == "/api/databases":
                self._serve_json_databases()
            elif path.startswith("/api/snapshot/"):
                filename = path[len("/api/snapshot/"):]
                self._serve_json_snapshot_file(filename)
            elif path == "/":
                self._serve_ui()
            elif path == "/health":
                self._respond(200, "application/json", '{"status":"ok"}')
            elif path == "/api/info":
                self._respond(200, "application/json",
                              json.dumps({"build": _BUILD}))
            else:
                self._respond(404, "text/plain", "Not Found")

        def do_POST(self):
            path, params = self._parse_request()
            if path == "/api/snapshot":
                self._handle_take_snapshot()
            else:
                self._respond(404, "text/plain", "Not Found")

        def do_DELETE(self):
            path, params = self._parse_request()
            if path.startswith("/api/snapshots/"):
                filename = path[len("/api/snapshots/"):]
                self._handle_delete_snapshot(filename)
            else:
                self._respond(404, "text/plain", "Not Found")

        def do_OPTIONS(self):
            """Handle CORS preflight for DELETE/POST."""
            self.send_response(200)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods",
                             "GET, POST, DELETE, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type")
            self.end_headers()

        def _serve_metrics(self):
            with lock:
                all_metrics = []
                for db, snap in latest_snapshots.items():
                    all_metrics.append(snapshot_to_prometheus(snap))
            body = "\n".join(all_metrics) if all_metrics else \
                "# No data collected yet\n"
            self._respond(200,
                          "text/plain; version=0.0.4; charset=utf-8", body)

        def _serve_json_snapshot(self, db_filter=None):
            with lock:
                if db_filter and db_filter in latest_snapshots:
                    snap = latest_snapshots[db_filter]
                elif not db_filter and latest_snapshots:
                    snap = list(latest_snapshots.values())[0]
                elif db_filter:
                    # Collect on-demand for a database we aren't
                    # actively monitoring
                    try:
                        snap = collect_snapshot(client, db_filter)
                        latest_snapshots[db_filter] = snap
                    except Exception as e:
                        self._respond(500, "application/json",
                                      json.dumps({"error": str(e)}))
                        return
                else:
                    self._respond(503, "application/json",
                                  '{"error":"No data collected yet"}')
                    return
                self._respond(200, "application/json",
                              json.dumps(snap, indent=2, default=str))

        def _serve_json_snapshots_list(self, db_filter=None):
            all_snaps = load_snapshots(database=db_filter)
            summary = []
            for s in all_snaps:
                t = s.get("totals", {})
                summary.append({
                    "timestamp": s.get("timestamp"),
                    "database": s.get("database"),
                    "documents": t.get("documents", 0),
                    "forest_disk_mb": t.get("forest_disk_mb", 0),
                    "host_forest_mb": t.get("host_forest_mb", 0),
                    "host_rss_mb": t.get("host_rss_mb", 0),
                    "file": s.get("_file"),
                })
            self._respond(200, "application/json",
                          json.dumps(summary, indent=2, default=str))

        def _serve_json_trend(self, db_filter=None):
            all_snaps = load_snapshots(database=db_filter)
            points = []
            for s in all_snaps:
                t = s.get("totals", {})
                points.append({
                    "timestamp": s.get("timestamp"),
                    "database": s.get("database"),
                    "documents": t.get("documents", 0),
                    "forest_disk_mb": t.get("forest_disk_mb", 0),
                    "host_forest_mb": t.get("host_forest_mb", 0),
                    "host_rss_mb": t.get("host_rss_mb", 0),
                    "active_fragments": t.get("active_fragments", 0),
                    "deleted_fragments": t.get("deleted_fragments", 0),
                    "system_total_mb": t.get("system_total_mb", 0),
                    "host_cache_mb": t.get("host_cache_mb", 0),
                    "host_base_mb": t.get("host_base_mb", 0),
                    "host_file_mb": t.get("host_file_mb", 0),
                })
            self._respond(200, "application/json",
                          json.dumps(points, indent=2, default=str))

        def _serve_json_databases(self):
            _SYSTEM_DBS = {
                "App-Services", "Extensions", "Fab", "Last-Login",
                "Meters", "Modules", "Schemas", "Security", "Triggers",
            }
            db_names = set(latest_snapshots.keys())
            # Fetch all databases from the cluster
            try:
                results = client.eval_javascript(
                    'Array.from(xdmp.databases()).map('
                    'function(id){return xdmp.databaseName(id)})')
                if results and isinstance(results[0], list):
                    for name in results[0]:
                        db_names.add(name)
            except Exception:
                pass  # fall back to local knowledge
            # Also include databases from saved snapshots
            for s in load_snapshots():
                db_name = s.get("database")
                if db_name:
                    db_names.add(db_name)
            # Filter out system databases
            db_names -= _SYSTEM_DBS
            # Ensure Documents is first if present
            result = sorted(db_names)
            if "Documents" in result:
                result.remove("Documents")
                result.insert(0, "Documents")
            self._respond(200, "application/json",
                          json.dumps(result, default=str))

        def _serve_json_snapshot_file(self, filename):
            """Serve full snapshot JSON for a specific file."""
            if ".." in filename or "/" in filename \
                    or not filename.endswith(".json"):
                self._respond(400, "application/json",
                              '{"error":"Invalid filename"}')
                return
            fpath = SNAPSHOT_DIR / filename
            if not fpath.exists():
                self._respond(404, "application/json",
                              '{"error":"Snapshot not found"}')
                return
            try:
                with open(fpath) as f:
                    data = json.load(f)
                self._respond(200, "application/json",
                              json.dumps(data, indent=2, default=str))
            except (json.JSONDecodeError, OSError) as e:
                self._respond(500, "application/json",
                              json.dumps({"error": str(e)}))

        def _handle_delete_snapshot(self, filename):
            """Delete a snapshot file."""
            if ".." in filename or "/" in filename \
                    or not filename.endswith(".json"):
                self._respond(400, "application/json",
                              '{"error":"Invalid filename"}')
                return
            fpath = SNAPSHOT_DIR / filename
            if not fpath.exists():
                self._respond(404, "application/json",
                              '{"error":"Snapshot not found"}')
                return
            try:
                fpath.unlink()
                self._respond(200, "application/json",
                              '{"status":"deleted"}')
            except OSError as e:
                self._respond(500, "application/json",
                              json.dumps({"error": str(e)}))

        def _handle_take_snapshot(self):
            """Trigger an immediate snapshot collection."""
            try:
                content_len = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(content_len) if content_len else b"{}"
                req = json.loads(body) if body else {}
            except (json.JSONDecodeError, ValueError):
                req = {}

            db = req.get("database") or (
                list(latest_snapshots.keys())[0] if latest_snapshots
                else databases[0] if databases else "Documents"
            )

            try:
                snap = collect_snapshot(client, db)
                save_snapshot(snap)
                with lock:
                    latest_snapshots[db] = snap
                t = snap.get("totals", {})
                summary = {
                    "status": "ok",
                    "timestamp": snap.get("timestamp"),
                    "database": db,
                    "documents": t.get("documents", 0),
                }
                print(f"  [{datetime.now(timezone.utc).strftime('%H:%M:%S')}] "
                      f"Manual snapshot {db}: "
                      f"{t.get('documents', 0):,} docs")
                self._respond(200, "application/json",
                              json.dumps(summary, indent=2, default=str))
            except Exception as e:
                self._respond(500, "application/json",
                              json.dumps({"error": str(e)}))

        def _serve_ui(self):
            self._respond(200, "text/html", _WEB_UI_HTML)

        def _respond(self, code, content_type, body):
            self.send_response(code)
            self.send_header("Content-Type", content_type)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(body.encode() if isinstance(body, str) else body)

    # Initial collection
    print(f"\n  MLCA Service starting on port {port}")
    print(f"  Monitoring: {', '.join(databases)}")
    print(f"  Collection interval: {interval_sec}s")
    print(f"  Endpoints:")
    print(f"    http://localhost:{port}/          Web UI")
    print(f"    http://localhost:{port}/metrics   Prometheus metrics")
    print(f"    http://localhost:{port}/api/      JSON API")
    print(f"    http://localhost:{port}/health    Health check")
    if otlp_endpoint:
        print(f"  OTLP push: {otlp_endpoint}")
    print()

    collect_all()

    # Start background collector
    collector = threading.Thread(target=schedule_loop, daemon=True)
    collector.start()

    # Start HTTP server
    server = HTTPServer(("", port), MLCAHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  MLCA Service stopped.")
        server.shutdown()


def push_otlp(snap, endpoint):
    """Push metrics to an OpenTelemetry Collector via OTLP HTTP JSON.

    Uses stdlib only — no opentelemetry-sdk required. Sends a minimal
    OTLP JSON payload with gauge data points.
    """
    from urllib.request import Request, urlopen

    t = snap.get("totals", {})
    db = snap.get("database", "unknown")
    now_ns = int(datetime.now(timezone.utc).timestamp() * 1e9)

    def dp(name, value, unit=""):
        return {
            "name": name,
            "unit": unit,
            "gauge": {
                "dataPoints": [{
                    "asDouble": float(value) if value else 0,
                    "timeUnixNano": str(now_ns),
                    "attributes": [
                        {"key": "database", "value": {"stringValue": db}}
                    ],
                }]
            }
        }

    metrics = [
        dp("mlca.documents.total", t.get("documents", 0)),
        dp("mlca.memory.forest", t.get("host_forest_mb", 0), "MB"),
        dp("mlca.memory.rss", t.get("host_rss_mb", 0), "MB"),
        dp("mlca.memory.headroom", t.get("system_total_mb", 0) * 0.8 -
           t.get("host_cache_mb", 0) - t.get("host_base_mb", 0) -
           t.get("host_file_mb", 0) - t.get("host_forest_mb", 0), "MB"),
        dp("mlca.disk.used", t.get("forest_disk_mb", 0), "MB"),
        dp("mlca.fragments.active", t.get("active_fragments", 0)),
        dp("mlca.fragments.deleted", t.get("deleted_fragments", 0)),
    ]

    payload = {
        "resourceMetrics": [{
            "resource": {
                "attributes": [
                    {"key": "service.name", "value": {"stringValue": "mlca"}},
                ]
            },
            "scopeMetrics": [{
                "scope": {"name": "mlca"},
                "metrics": metrics,
            }]
        }]
    }

    body = json.dumps(payload).encode()
    url = endpoint.rstrip("/")
    if not url.endswith("/v1/metrics"):
        url += "/v1/metrics"
    req = Request(url, data=body, method="POST",
                  headers={"Content-Type": "application/json"})
    with urlopen(req, timeout=10) as resp:
        return resp.status


# ── Web UI HTML ────────────────────────────────────────────────────

_WEB_UI_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>MLCA — MarkLogic Capacity Analysis</title>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body { background: #0d1117; color: #c9d1d9; font-family: 'SF Mono', 'Consolas', monospace; font-size: 14px; }
.container { max-width: 1200px; margin: 0 auto; padding: 20px; }
h1 { color: #58a6ff; font-size: 20px; margin-bottom: 4px; }
h3 { color: #58a6ff; font-size: 16px; margin: 24px 0 12px; border-bottom: 1px solid #21262d; padding-bottom: 8px; }

.header { display: flex; align-items: center; gap: 16px; margin-bottom: 4px; flex-wrap: wrap; }
.header h1 { margin-bottom: 0; }
.db-select { background: #161b22; color: #c9d1d9; border: 1px solid #30363d; border-radius: 6px;
  padding: 6px 10px; font-family: inherit; font-size: 13px; cursor: pointer; }
.db-select:focus { border-color: #58a6ff; outline: none; }
#subtitle { color: #8b949e; font-size: 14px; margin-bottom: 12px; }

.tabs { display: flex; gap: 0; border-bottom: 1px solid #21262d; margin-bottom: 20px; }
.tab { padding: 10px 20px; cursor: pointer; color: #8b949e; border-bottom: 2px solid transparent;
  font-family: inherit; font-size: 14px; background: none; border-top: none; border-left: none; border-right: none; }
.tab:hover { color: #c9d1d9; }
.tab.active { color: #58a6ff; border-bottom-color: #58a6ff; }
.tab-content { display: none; }
.tab-content.active { display: block; }

.hero { display: flex; gap: 20px; margin-bottom: 24px; flex-wrap: wrap; }
.hero-card { flex: 1; min-width: 200px; background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 20px; text-align: center; }
.hero-card.warn { border-color: #d29922; }
.hero-card.crit { border-color: #f85149; }
.hero-value { font-size: 36px; font-weight: bold; color: #58a6ff; }
.hero-card.warn .hero-value { color: #d29922; }
.hero-card.crit .hero-value { color: #f85149; }
.hero-label { color: #8b949e; font-size: 12px; margin-top: 4px; }

.bar-container { background: #21262d; border-radius: 4px; height: 20px; margin: 4px 0; }
.bar-fill { height: 100%; border-radius: 4px; transition: width 0.5s; }
.bar-fill.green { background: #3fb950; }
.bar-fill.yellow { background: #d29922; }
.bar-fill.red { background: #f85149; }

.grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(350px, 1fr)); gap: 16px; }
.card { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 16px; }
.card-title { color: #58a6ff; font-size: 13px; margin-bottom: 8px; }

.metric { display: flex; justify-content: space-between; padding: 4px 0; border-bottom: 1px solid #21262d; }
.metric:last-child { border-bottom: none; }
.metric-key { color: #8b949e; }
.metric-val { color: #c9d1d9; font-weight: bold; }
.metric-val.good { color: #3fb950; }
.metric-val.warn { color: #d29922; }
.metric-val.crit { color: #f85149; }

table { width: 100%; border-collapse: collapse; font-size: 13px; }
th { text-align: left; color: #58a6ff; border-bottom: 1px solid #30363d; padding: 6px 8px; }
td { padding: 6px 8px; border-bottom: 1px solid #21262d; }
td.num { text-align: right; font-variant-numeric: tabular-nums; }

.btn { background: #21262d; color: #c9d1d9; border: 1px solid #30363d; border-radius: 6px;
  padding: 5px 12px; cursor: pointer; font-family: inherit; font-size: 12px; }
.btn:hover { background: #30363d; border-color: #8b949e; }
.btn-primary { background: #238636; border-color: #238636; color: #fff; }
.btn-primary:hover { background: #2ea043; }
.btn-danger { color: #f85149; }
.btn-danger:hover { background: #da3633; color: #fff; border-color: #da3633; }
.btn:disabled { opacity: 0.5; cursor: not-allowed; }

.snap-toolbar { display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px; }

.chart-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
.chart-card { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 16px; }
.chart-card canvas { width: 100%; height: 220px; }
.chart-title { color: #58a6ff; font-size: 13px; margin-bottom: 10px; }
.chart-tooltip { position: fixed; background: #161b22; border: 1px solid #58a6ff; border-radius: 6px;
  padding: 8px 12px; font-size: 12px; pointer-events: none; z-index: 100; display: none; color: #c9d1d9; }

.modal-overlay { position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0,0,0,0.7);
  z-index: 200; display: none; justify-content: center; align-items: center; }
.modal-overlay.show { display: flex; }
.modal { background: #161b22; border: 1px solid #30363d; border-radius: 8px; max-width: 800px;
  width: 90%; max-height: 80vh; display: flex; flex-direction: column; }
.modal-header { display: flex; justify-content: space-between; align-items: center; padding: 16px;
  border-bottom: 1px solid #21262d; }
.modal-header h3 { margin: 0; border: none; padding: 0; }
.modal-close { background: none; border: none; color: #8b949e; font-size: 20px; cursor: pointer; padding: 4px 8px; }
.modal-close:hover { color: #c9d1d9; }
.modal-body { padding: 16px; overflow-y: auto; flex: 1; }
.modal-body pre { white-space: pre-wrap; word-break: break-all; font-size: 12px; line-height: 1.5; }

.footer { margin-top: 24px; color: #484f58; font-size: 12px; text-align: center; }
.loading { color: #8b949e; text-align: center; padding: 40px; }

@media (max-width: 768px) {
  .chart-grid { grid-template-columns: 1fr; }
  .hero { flex-direction: column; }
}
</style>
</head>
<body>
<div class="container">
  <div class="header">
    <h1>MLCA</h1>
    <select id="dbSelect" class="db-select" title="Select database"></select>
  </div>
  <div id="subtitle">Loading...</div>

  <div class="tabs">
    <button class="tab active" data-tab="dashboard">Dashboard</button>
    <button class="tab" data-tab="trends">Trends</button>
    <button class="tab" data-tab="snapshots">Snapshots</button>
  </div>

  <div id="tab-dashboard" class="tab-content active">
    <div class="hero" id="hero"></div>
    <div class="grid" id="grid"></div>
    <div id="projections-section" style="display:none">
      <h3>Capacity Projections</h3>
      <div class="grid" id="projections"></div>
    </div>
    <h3>Index Memory Usage</h3>
    <div class="card" id="indexes"><div class="loading">Loading...</div></div>
  </div>

  <div id="tab-trends" class="tab-content">
    <div class="chart-grid" id="charts">
      <div class="chart-card"><div class="chart-title">Documents</div><canvas id="chart-docs"></canvas></div>
      <div class="chart-card"><div class="chart-title">Forest Memory</div><canvas id="chart-forest"></canvas></div>
      <div class="chart-card"><div class="chart-title">Disk Usage</div><canvas id="chart-disk"></canvas></div>
      <div class="chart-card"><div class="chart-title">Memory Ceiling %</div><canvas id="chart-ceiling"></canvas></div>
    </div>
    <div id="trend-empty" class="loading" style="display:none">Not enough snapshots for trend charts. Take at least 2 snapshots.</div>
  </div>

  <div id="tab-snapshots" class="tab-content">
    <div class="snap-toolbar">
      <h3 style="margin:0;border:none;padding:0">Saved Snapshots</h3>
      <button class="btn btn-primary" id="takeSnapshotBtn" onclick="takeSnapshot()">Take Snapshot</button>
    </div>
    <div class="card" id="snap-list"><div class="loading">Loading...</div></div>
  </div>

  <div class="footer">MLCA &mdash; refreshes every 30s &mdash; build <span id="build-sha">...</span></div>
</div>

<div class="modal-overlay" id="modal">
  <div class="modal">
    <div class="modal-header">
      <h3 id="modal-title">Snapshot Detail</h3>
      <button class="modal-close" onclick="closeModal()">&times;</button>
    </div>
    <div class="modal-body"><pre id="modal-body"></pre></div>
  </div>
</div>

<div class="chart-tooltip" id="tooltip"></div>

<script>
var selectedDb = null;
var activeTab = 'dashboard';

function fmt(mb) {
  if (mb == null) return 'N/A';
  mb = Number(mb);
  if (mb >= 1024) return (mb/1024).toFixed(2) + ' GB';
  return mb.toFixed(1) + ' MB';
}
function fmtNum(n) { return (n||0).toLocaleString(); }
function barClass(p) { return p >= 90 ? 'red' : p >= 70 ? 'yellow' : 'green'; }
function renderBar(v) {
  return '<div class="bar-container"><div class="bar-fill ' + barClass(v) +
         '" style="width:' + Math.min(100, v) + '%"></div></div>';
}
function metric(key, val, cls) {
  return '<div class="metric"><span class="metric-key">' + key +
         '</span><span class="metric-val' + (cls ? ' '+cls : '') + '">' + val + '</span></div>';
}
function dbParam() { return selectedDb ? '?database=' + encodeURIComponent(selectedDb) : ''; }

// Tabs
document.querySelectorAll('.tab').forEach(function(tab) {
  tab.addEventListener('click', function() {
    document.querySelectorAll('.tab').forEach(function(t) { t.classList.remove('active'); });
    document.querySelectorAll('.tab-content').forEach(function(c) { c.classList.remove('active'); });
    tab.classList.add('active');
    activeTab = tab.dataset.tab;
    document.getElementById('tab-' + activeTab).classList.add('active');
    refreshActiveTab();
  });
});

// Database selector
async function loadDatabases() {
  try {
    var dbs = await (await fetch('/api/databases')).json();
    var sel = document.getElementById('dbSelect');
    sel.innerHTML = '';
    dbs.forEach(function(db) {
      var opt = document.createElement('option');
      opt.value = db; opt.textContent = db;
      if (db === selectedDb) opt.selected = true;
      sel.appendChild(opt);
    });
    if (!selectedDb && dbs.length > 0) selectedDb = dbs[0];
  } catch(e) {}
}
document.getElementById('dbSelect').addEventListener('change', function() {
  selectedDb = this.value;
  refreshActiveTab();
});

// Dashboard
async function refreshDashboard() {
  try {
    var snap = await (await fetch('/api/snapshot' + dbParam())).json();
    if (snap.error) { document.getElementById('subtitle').textContent = snap.error; return; }
    var t = snap.totals || {};
    var db = snap.database || '?';
    var ts = (snap.timestamp || '').substring(0,19).replace('T',' ');
    document.getElementById('subtitle').textContent = db + ' \\u2014 ' + ts;

    var sysTot = t.system_total_mb || 0;
    var cache = t.host_cache_mb || 0, base = t.host_base_mb || 0;
    var file = t.host_file_mb || 0, forest = t.host_forest_mb || 0;
    var fixed = cache + base + file;
    var ceiling = sysTot * 0.8;
    var headroom = ceiling - fixed - forest;
    var memPct = ceiling > 0 ? ((fixed + forest) / ceiling * 100) : 0;

    var heroHTML = '';
    heroHTML += '<div class="hero-card"><div class="hero-value">' +
                fmtNum(t.documents) + '</div><div class="hero-label">Documents</div></div>';
    heroHTML += '<div class="hero-card"><div class="hero-value">' +
                fmt(forest) + '</div><div class="hero-label">Forest Memory</div></div>';
    heroHTML += '<div class="hero-card"><div class="hero-value">' +
                fmt(headroom) + '</div><div class="hero-label">Memory Headroom</div></div>';
    heroHTML += '<div class="hero-card"><div class="hero-value">' +
                memPct.toFixed(1) + '%</div><div class="hero-label">Memory Ceiling</div>' +
                renderBar(memPct) + '</div>';
    document.getElementById('hero').innerHTML = heroHTML;

    var grid = '';
    grid += '<div class="card"><div class="card-title">Memory Breakdown</div>' +
      metric('Cache (list+tree)', fmt(cache)) +
      metric('Forest stands', fmt(forest)) +
      metric('Base ML overhead', fmt(base)) +
      metric('File cache', fmt(file)) +
      metric('Fixed total', fmt(fixed)) +
      metric('Ceiling (80% RAM)', fmt(ceiling)) +
      metric('Headroom', fmt(headroom), headroom < 1024 ? 'warn' : 'good') +
    '</div>';

    var hosts = snap.hosts || [];
    if (hosts.length > 0) {
      var h = hosts[0];
      var rss = h['memory-process-rss-mb'] || 0;
      var swap = h['memory-process-swap-mb'] || 0;
      grid += '<div class="card"><div class="card-title">Host: ' + (h.hostname||'?') + '</div>' +
        metric('System RAM', fmt(h['memory-system-total-mb'])) +
        metric('ML RSS', fmt(rss)) +
        metric('RSS peak', fmt(h['memory-process-rss-hwm-mb'])) +
        metric('Swap', fmt(swap), swap > 0 ? 'crit' : 'good') +
        metric('Page-out rate', (h['memory-system-pageout-rate']||0).toFixed(1) + ' MB/s') +
      '</div>';
    }

    var dbStat = snap.database_status || {};
    var diskUsed = t.forest_disk_mb || 0;
    var diskRemaining = Number(dbStat.least_remaining_mb || 0);
    var diskTotal = diskUsed + diskRemaining;
    var diskPct = diskTotal > 0 ? (diskUsed / diskTotal * 100) : 0;
    grid += '<div class="card"><div class="card-title">Disk</div>' +
      metric('Data on disk', fmt(diskUsed)) +
      metric('Remaining', fmt(diskRemaining)) +
      metric('Utilization', diskPct.toFixed(1) + '%') + renderBar(diskPct) +
      (t.documents > 0 ? metric('Bytes/doc', Math.round(diskUsed*1048576/t.documents).toLocaleString()) : '') +
    '</div>';

    var active = t.active_fragments || 0, deleted = t.deleted_fragments || 0;
    var fragTotal = active + deleted;
    var fragPct = fragTotal > 0 ? (deleted/fragTotal*100) : 0;
    grid += '<div class="card"><div class="card-title">Fragments</div>' +
      metric('Active', fmtNum(active)) +
      metric('Deleted', fmtNum(deleted), deleted > 0 ? 'warn' : '') +
      metric('Fragmentation', fragPct.toFixed(1) + '%', fragPct > 25 ? 'crit' : fragPct > 10 ? 'warn' : 'good') +
      (fragPct >= 25 ? renderBar(fragPct) : '') +
    '</div>';
    document.getElementById('grid').innerHTML = grid;

    // Index table
    var im = snap.index_memory || {};
    var indexes = im.indexes || [];
    if (indexes.length > 0) {
      indexes.sort(function(a,b) { return (b.totalMemoryBytes||0) - (a.totalMemoryBytes||0); });
      var tbl = '<table><tr><th>Index</th><th>Type</th><th>Memory</th><th>Disk</th></tr>';
      indexes.forEach(function(i) {
        var name = i.localname || i.pathExpression || i.indexType || '?';
        var mem = i.totalMemoryBytes || 0;
        var disk = i.totalOnDiskBytes || 0;
        var memStr = mem > 0 ? fmt(mem/1048576) : '<span style="color:#484f58">not cached</span>';
        var diskStr = disk > 0 ? fmt(disk/1048576) : '<span style="color:#484f58">not cached</span>';
        tbl += '<tr><td>' + name + '</td><td>' + (i.scalarType||i.indexType||'') +
               '</td><td class="num">' + memStr +
               '</td><td class="num">' + diskStr + '</td></tr>';
      });
      tbl += '</table>';

      var sums = (im.standSummaries || []);
      if (sums.length > 0) {
        var totalRange = 0;
        sums.forEach(function(s) { totalRange += (s.summary||{}).rangeIndexesBytes || 0; });
        if (totalRange > 0) {
          tbl += '<div style="margin-top:12px;padding:8px;background:#21262d;border-radius:4px">' +
            '<span class="metric-key">Total range index memory (stand-level): </span>' +
            '<span class="metric-val" style="color:#58a6ff">' + fmt(totalRange/1048576) + '</span>' +
            '<div style="color:#484f58;font-size:12px;margin-top:4px">' +
            'Per-index values show cache-warmed data only. Stand-level total includes all resident pages. ' +
            'Use --index-impact for measured per-index costs.</div></div>';
        }
      }

      document.getElementById('indexes').innerHTML = tbl;
    } else {
      document.getElementById('indexes').innerHTML = '<div class="loading">No index memory data available</div>';
    }

    // Capacity projections from trend data
    // Methodology: use linear regression on disk (stable, monotonic) as
    // primary growth signal.  Forest memory is volatile due to ML stand
    // merges/flushes, so we use regression to smooth it.  Per-doc cost
    // uses marginal cost from recent snapshots (amortisation means average
    // cost drops as the collection grows — marginal is more predictive).
    try {
      var trend = await (await fetch('/api/trend' + dbParam())).json();
      var projSec = document.getElementById('projections-section');
      // Deduplicate trend points with same doc count (keep last)
      var seen = {}; var dedupTrend = [];
      trend.forEach(function(p) {
        var key = p.documents;
        if (seen[key] !== undefined) dedupTrend[seen[key]] = p;
        else { seen[key] = dedupTrend.length; dedupTrend.push(p); }
      });
      trend = dedupTrend;

      if (trend.length >= 3) {
        var first = trend[0], last = trend[trend.length - 1];
        var t0 = new Date(first.timestamp).getTime(), t1 = new Date(last.timestamp).getTime();
        var spanDays = (t1 - t0) / 86400000;

        // Linear regression helper: returns {slope, intercept, r2}
        function linReg(xs, ys) {
          var n = xs.length, sx = 0, sy = 0, sxy = 0, sx2 = 0, sy2 = 0;
          for (var i = 0; i < n; i++) {
            sx += xs[i]; sy += ys[i]; sxy += xs[i]*ys[i];
            sx2 += xs[i]*xs[i]; sy2 += ys[i]*ys[i];
          }
          var denom = n*sx2 - sx*sx;
          if (denom === 0) return { slope: 0, intercept: 0, r2: 0 };
          var slope = (n*sxy - sx*sy) / denom;
          var intercept = (sy - slope*sx) / n;
          var ssRes = 0, ssTot = 0, yMean = sy/n;
          for (var i = 0; i < n; i++) {
            var yHat = slope*xs[i] + intercept;
            ssRes += (ys[i]-yHat)*(ys[i]-yHat);
            ssTot += (ys[i]-yMean)*(ys[i]-yMean);
          }
          return { slope: slope, intercept: intercept, r2: ssTot > 0 ? 1 - ssRes/ssTot : 0 };
        }

        // Regression on docs (x) vs disk (y) — most stable relationship
        var xDocs = trend.map(function(p) { return p.documents; });
        var yDisk = trend.map(function(p) { return p.forest_disk_mb; });
        var yForest = trend.map(function(p) { return p.host_forest_mb; });
        var diskReg = linReg(xDocs, yDisk);
        var forestReg = linReg(xDocs, yForest);

        // Marginal cost: use last 30% of snapshots for recent trend
        var recentStart = Math.max(0, Math.floor(trend.length * 0.7));
        var recent = trend.slice(recentStart);
        var rFirst = recent[0], rLast = recent[recent.length - 1];
        var rDocDelta = rLast.documents - rFirst.documents;
        var rForestDelta = rLast.host_forest_mb - rFirst.host_forest_mb;
        var rDiskDelta = rLast.forest_disk_mb - rFirst.forest_disk_mb;

        // Marginal bytes/doc from regression slope (more stable than point-to-point)
        var marginalForestBytes = forestReg.slope * 1048576;  // MB/doc -> bytes/doc
        var marginalDiskBytes = diskReg.slope * 1048576;

        // Time-based rates using regression on time
        var xTimes = trend.map(function(p) { return (new Date(p.timestamp).getTime() - t0) / 86400000; });
        var diskTimeReg = linReg(xTimes, yDisk);
        var forestTimeReg = linReg(xTimes, yForest);
        var docsTimeReg = linReg(xTimes, xDocs);

        // Account for base_mb growth (it's NOT fixed — grows with data)
        var yBase = trend.map(function(p) { return (p.host_base_mb||0) + (p.host_file_mb||0); });
        var baseTimeReg = linReg(xTimes, yBase);

        // Effective daily memory growth = forest growth + base growth
        var dailyMemGrowth = forestTimeReg.slope + baseTimeReg.slope;
        var curBase = last.host_base_mb || 0;
        var curFile = last.host_file_mb || 0;
        var effectiveHeadroom = ceiling - cache - curBase - curFile - forest;

        var proj = '';

        // Memory Runway — using regression-smoothed rates
        if (dailyMemGrowth > 0 && effectiveHeadroom > 0) {
          var daysUntilMem = effectiveHeadroom / dailyMemGrowth;
          var etaDate = new Date(Date.now() + daysUntilMem * 86400000);
          var etaStr = etaDate.getFullYear() + '-' + String(etaDate.getMonth()+1).padStart(2,'0') + '-' + String(etaDate.getDate()).padStart(2,'0');
          var runwayClass = daysUntilMem < 30 ? 'crit' : daysUntilMem < 90 ? 'warn' : 'good';
          var confidence = forestTimeReg.r2;
          var confLabel = confidence > 0.9 ? 'high' : confidence > 0.7 ? 'medium' : 'low';
          var confClass = confidence > 0.9 ? 'good' : confidence > 0.7 ? 'warn' : 'crit';

          proj += '<div class="card"><div class="card-title">Memory Runway</div>' +
            metric('Days until ceiling', '<span class="metric-val ' + runwayClass + '">' + Math.round(daysUntilMem) + ' days</span>', '') +
            metric('ETA', etaStr) +
            metric('Forest growth (regressed)', fmt(forestTimeReg.slope) + '/day') +
            metric('Base+file growth', fmt(baseTimeReg.slope) + '/day') +
            metric('Combined growth', fmt(dailyMemGrowth) + '/day') +
            metric('Confidence (R\\u00b2)', '<span class="metric-val ' + confClass + '">' + confLabel + ' (' + confidence.toFixed(2) + ')</span>', '');

          // Document-based projection using regression slope
          if (marginalForestBytes > 0) {
            var docsUntilCeiling = Math.round((effectiveHeadroom * 1048576) / marginalForestBytes);
            proj += metric('Marginal memory/doc', Math.round(marginalForestBytes).toLocaleString() + ' bytes') +
                    metric('Est. docs until ceiling', '<span class="metric-val ' + runwayClass + '">' + fmtNum(docsUntilCeiling) + '</span>', '');
          }
          proj += metric('Snapshots analyzed', trend.length + ' over ' + spanDays.toFixed(1) + ' days') +
            '</div>';
        } else if (dailyMemGrowth <= 0) {
          proj += '<div class="card"><div class="card-title">Memory Runway</div>' +
            metric('Status', 'Stable or shrinking', 'good') +
            metric('Snapshots', trend.length + ' over ' + spanDays.toFixed(1) + ' days') +
            '</div>';
        }

        // Disk Runway — most reliable projection
        var diskRemain = Number((snap.database_status || {}).least_remaining_mb || 0);
        if (diskTimeReg.slope > 0 && diskRemain > 0) {
          var daysUntilDisk = diskRemain / diskTimeReg.slope;
          var diskEta = new Date(Date.now() + daysUntilDisk * 86400000);
          var diskEtaStr = diskEta.getFullYear() + '-' + String(diskEta.getMonth()+1).padStart(2,'0') + '-' + String(diskEta.getDate()).padStart(2,'0');
          var diskClass = daysUntilDisk < 30 ? 'crit' : daysUntilDisk < 90 ? 'warn' : 'good';
          proj += '<div class="card"><div class="card-title">Disk Runway</div>' +
            metric('Growth rate (regressed)', fmt(diskTimeReg.slope) + '/day') +
            metric('Remaining', fmt(diskRemain)) +
            metric('Days until full', '<span class="metric-val ' + diskClass + '">' + Math.round(daysUntilDisk) + ' days</span>', '') +
            metric('ETA', diskEtaStr) +
            metric('Confidence (R\\u00b2)', diskTimeReg.r2.toFixed(2)) +
            metric('Disk/doc (regressed)', Math.round(diskReg.slope * 1048576).toLocaleString() + ' bytes') +
            '</div>';
        }

        // Document growth
        if (docsTimeReg.slope > 0) {
          proj += '<div class="card"><div class="card-title">Document Growth</div>' +
            metric('Current count', fmtNum(last.documents)) +
            metric('Growth rate (regressed)', fmtNum(Math.round(docsTimeReg.slope)) + '/day') +
            metric('Total growth', '+' + fmtNum(last.documents - first.documents)) +
            '</div>';
        }

        if (proj) {
          document.getElementById('projections').innerHTML = proj;
          projSec.style.display = 'block';
        } else { projSec.style.display = 'none'; }
      } else { projSec.style.display = 'none'; }
    } catch(e2) { /* projections are optional */ }
  } catch(e) {
    document.getElementById('subtitle').textContent = 'Error: ' + e.message;
  }
}

// Snapshots tab
async function refreshSnapshots() {
  try {
    var snaps = await (await fetch('/api/snapshots' + dbParam())).json();
    if (snaps.length === 0) {
      document.getElementById('snap-list').innerHTML = '<div class="loading">No snapshots found</div>';
      return;
    }
    snaps.reverse();
    var tbl = '<table><tr><th>#</th><th>Timestamp</th><th>Database</th>' +
              '<th style="text-align:right">Documents</th><th style="text-align:right">Forest Disk</th>' +
              '<th style="text-align:right">RSS</th><th>Actions</th></tr>';
    snaps.forEach(function(s, i) {
      var ts = (s.timestamp||'').substring(0,19).replace('T',' ');
      var file = s.file || '';
      tbl += '<tr><td>' + (snaps.length - i) + '</td><td>' + ts + '</td><td>' + (s.database||'') + '</td>' +
             '<td class="num">' + fmtNum(s.documents) + '</td>' +
             '<td class="num">' + fmt(s.forest_disk_mb) + '</td>' +
             '<td class="num">' + fmt(s.host_rss_mb) + '</td>' +
             '<td><button class="btn" onclick="viewSnapshot(\\'' + file + '\\')">View</button> ' +
             '<button class="btn btn-danger" onclick="deleteSnapshot(\\'' + file + '\\')">Delete</button></td></tr>';
    });
    tbl += '</table>';
    document.getElementById('snap-list').innerHTML = tbl;
  } catch(e) {
    document.getElementById('snap-list').innerHTML = '<div class="loading">Error: ' + e.message + '</div>';
  }
}

async function viewSnapshot(filename) {
  try {
    var data = await (await fetch('/api/snapshot/' + encodeURIComponent(filename))).json();
    document.getElementById('modal-title').textContent = filename;
    document.getElementById('modal-body').textContent = JSON.stringify(data, null, 2);
    document.getElementById('modal').classList.add('show');
  } catch(e) { alert('Failed to load snapshot: ' + e.message); }
}

function closeModal() { document.getElementById('modal').classList.remove('show'); }
document.addEventListener('keydown', function(e) { if (e.key === 'Escape') closeModal(); });
document.getElementById('modal').addEventListener('click', function(e) {
  if (e.target === this) closeModal();
});

async function deleteSnapshot(filename) {
  if (!confirm('Delete snapshot ' + filename + '?')) return;
  try {
    var resp = await fetch('/api/snapshots/' + encodeURIComponent(filename), { method: 'DELETE' });
    if (resp.ok) refreshSnapshots();
    else alert('Delete failed: ' + (await resp.json()).error);
  } catch(e) { alert('Delete error: ' + e.message); }
}

async function takeSnapshot() {
  var btn = document.getElementById('takeSnapshotBtn');
  btn.disabled = true; btn.textContent = 'Collecting...';
  try {
    var body = selectedDb ? JSON.stringify({database: selectedDb}) : '{}';
    var resp = await fetch('/api/snapshot', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: body
    });
    var result = await resp.json();
    if (resp.ok) {
      refreshSnapshots();
      loadDatabases();
    } else {
      alert('Snapshot failed: ' + (result.error || 'Unknown error'));
    }
  } catch(e) { alert('Snapshot error: ' + e.message); }
  btn.disabled = false; btn.textContent = 'Take Snapshot';
}

// Charts
function drawChart(canvasId, points, yKey, color, yFmt) {
  var canvas = document.getElementById(canvasId);
  if (!canvas) return;
  var rect = canvas.parentElement.getBoundingClientRect();
  var dpr = window.devicePixelRatio || 1;
  canvas.width = (rect.width - 32) * dpr;
  canvas.height = 220 * dpr;
  canvas.style.width = (rect.width - 32) + 'px';
  canvas.style.height = '220px';
  var ctx = canvas.getContext('2d');
  ctx.scale(dpr, dpr);
  var W = rect.width - 32, H = 220;
  var pad = {top: 10, right: 16, bottom: 35, left: 65};
  var plotW = W - pad.left - pad.right, plotH = H - pad.top - pad.bottom;

  if (points.length < 2) return;

  var vals = points.map(function(p) { return p[yKey] || 0; });
  var times = points.map(function(p) { return new Date(p.timestamp).getTime(); });
  var yMin = Math.min.apply(null, vals), yMax = Math.max.apply(null, vals);
  if (yMin === yMax) { yMin = yMin * 0.9; yMax = yMax * 1.1 || 1; }
  var yRange = yMax - yMin;
  var tMin = Math.min.apply(null, times), tMax = Math.max.apply(null, times);
  var tRange = tMax - tMin || 1;

  function xPos(t) { return pad.left + (t - tMin) / tRange * plotW; }
  function yPos(v) { return pad.top + plotH - (v - yMin) / yRange * plotH; }

  ctx.fillStyle = '#161b22';
  ctx.fillRect(0, 0, W, H);

  // Gridlines and Y labels
  ctx.strokeStyle = '#21262d'; ctx.lineWidth = 1;
  var ySteps = 5;
  for (var i = 0; i <= ySteps; i++) {
    var yy = pad.top + plotH * i / ySteps;
    ctx.beginPath(); ctx.moveTo(pad.left, yy); ctx.lineTo(W - pad.right, yy); ctx.stroke();
    ctx.fillStyle = '#8b949e'; ctx.font = '11px monospace'; ctx.textAlign = 'right';
    var yVal = yMax - yRange * i / ySteps;
    ctx.fillText(yFmt ? yFmt(yVal) : yVal.toFixed(0), pad.left - 6, yy + 4);
  }

  // X labels
  ctx.textAlign = 'center'; ctx.fillStyle = '#8b949e';
  var xSteps = Math.min(points.length - 1, 6);
  for (var i = 0; i <= xSteps; i++) {
    var idx = Math.round(i * (points.length - 1) / xSteps);
    var d = new Date(points[idx].timestamp);
    var label = (d.getMonth()+1) + '/' + d.getDate() + ' ' +
      String(d.getHours()).padStart(2,'0') + ':' + String(d.getMinutes()).padStart(2,'0');
    ctx.fillText(label, xPos(times[idx]), H - 5);
  }

  // Line
  ctx.strokeStyle = color; ctx.lineWidth = 2;
  ctx.beginPath();
  for (var i = 0; i < points.length; i++) {
    var x = xPos(times[i]), y = yPos(vals[i]);
    if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
  }
  ctx.stroke();

  // Dots
  ctx.fillStyle = color;
  for (var i = 0; i < points.length; i++) {
    ctx.beginPath();
    ctx.arc(xPos(times[i]), yPos(vals[i]), 3, 0, Math.PI * 2);
    ctx.fill();
  }

  // Area fill
  ctx.globalAlpha = 0.1; ctx.fillStyle = color;
  ctx.beginPath(); ctx.moveTo(xPos(times[0]), yPos(vals[0]));
  for (var i = 1; i < points.length; i++) ctx.lineTo(xPos(times[i]), yPos(vals[i]));
  ctx.lineTo(xPos(times[points.length-1]), pad.top + plotH);
  ctx.lineTo(xPos(times[0]), pad.top + plotH);
  ctx.closePath(); ctx.fill(); ctx.globalAlpha = 1.0;

  canvas._chartData = { points: points, times: times, vals: vals, yKey: yKey, yFmt: yFmt,
    xPos: xPos, yPos: yPos, pad: pad, W: W, H: H };
}

// Tooltip
document.addEventListener('mousemove', function(e) {
  var tooltip = document.getElementById('tooltip');
  var canvas = e.target;
  if (canvas.tagName !== 'CANVAS' || !canvas._chartData) { tooltip.style.display = 'none'; return; }
  var cd = canvas._chartData;
  var rect = canvas.getBoundingClientRect();
  var mx = e.clientX - rect.left, my = e.clientY - rect.top;
  if (mx < cd.pad.left || mx > cd.W - cd.pad.right) { tooltip.style.display = 'none'; return; }

  var closest = -1, closestDist = Infinity;
  for (var i = 0; i < cd.times.length; i++) {
    var dx = Math.abs(cd.xPos(cd.times[i]) - mx);
    if (dx < closestDist) { closestDist = dx; closest = i; }
  }
  if (closest < 0 || closestDist > 30) { tooltip.style.display = 'none'; return; }
  var p = cd.points[closest];
  var d = new Date(p.timestamp);
  var dateStr = d.getFullYear() + '-' + String(d.getMonth()+1).padStart(2,'0') + '-' +
    String(d.getDate()).padStart(2,'0') + ' ' + String(d.getHours()).padStart(2,'0') + ':' +
    String(d.getMinutes()).padStart(2,'0');
  var valStr = cd.yFmt ? cd.yFmt(cd.vals[closest]) : fmtNum(cd.vals[closest]);
  tooltip.innerHTML = '<div>' + dateStr + '</div><div style="color:#58a6ff;font-weight:bold">' + valStr + '</div>';
  tooltip.style.display = 'block';
  tooltip.style.left = (e.clientX + 12) + 'px';
  tooltip.style.top = (e.clientY - 10) + 'px';
});

async function refreshTrends() {
  try {
    var points = await (await fetch('/api/trend' + dbParam())).json();
    var empty = document.getElementById('trend-empty');
    var charts = document.getElementById('charts');
    if (points.length < 2) {
      empty.style.display = 'block'; charts.style.display = 'none'; return;
    }
    empty.style.display = 'none'; charts.style.display = 'grid';

    points.forEach(function(p) {
      var sysTot = p.system_total_mb || 0;
      var ceiling = sysTot * 0.8;
      var fixed = (p.host_cache_mb||0) + (p.host_base_mb||0) + (p.host_file_mb||0);
      p.ceiling_pct = ceiling > 0 ? ((fixed + (p.host_forest_mb||0)) / ceiling * 100) : 0;
    });

    drawChart('chart-docs', points, 'documents', '#58a6ff', fmtNum);
    drawChart('chart-forest', points, 'host_forest_mb', '#3fb950', fmt);
    drawChart('chart-disk', points, 'forest_disk_mb', '#d29922', fmt);
    drawChart('chart-ceiling', points, 'ceiling_pct', '#f85149', function(v) { return v.toFixed(1) + '%'; });
  } catch(e) {}
}

// Refresh
function refreshActiveTab() {
  if (activeTab === 'dashboard') refreshDashboard();
  else if (activeTab === 'trends') refreshTrends();
  else if (activeTab === 'snapshots') refreshSnapshots();
}

fetch('/api/info').then(function(r){return r.json();}).then(function(info){
  document.getElementById('build-sha').textContent = info.build || 'dev';
}).catch(function(){});

loadDatabases().then(function() { refreshActiveTab(); });
setInterval(refreshActiveTab, 30000);
window.addEventListener('resize', function() { if (activeTab === 'trends') refreshTrends(); });
</script>
</body>
</html>
"""


# ── Main ────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="MarkLogic Capacity Analyzer - Understand cluster resource utilization"
    )
    parser.add_argument("--host", default="localhost", help="MarkLogic host (default: localhost)")
    parser.add_argument("--port", type=int, default=8002, help="Management API port (default: 8002)")
    parser.add_argument("--user", default="admin", help="MarkLogic user (default: admin)")
    parser.add_argument("--password", help="MarkLogic password (prompted if not provided)")
    parser.add_argument("--database", default="Documents", help="Database to analyze (default: Documents)")
    parser.add_argument("--auth-type", choices=["digest", "basic"], default="digest", help="Auth type (default: digest)")

    # Snapshot / trend flags
    parser.add_argument("--trend", action="store_true",
                        help="Show growth trends from saved snapshots")
    parser.add_argument("--compare", type=int, metavar="N", default=None,
                        help="Compare current state to snapshot #N (use --trend to list)")
    parser.add_argument("--snapshots", action="store_true",
                        help="List saved snapshots and exit")
    parser.add_argument("--snapshot-only", action="store_true",
                        help="Save a snapshot without printing the full report")
    parser.add_argument("--no-snapshot", action="store_true",
                        help="Don't save a snapshot on this run")
    parser.add_argument("--index-impact", action="store_true",
                        help="Show index memory impact between the two most recent snapshots")
    parser.add_argument("--project-docs", type=int, metavar="N", default=None,
                        help="Project index costs at N documents (use with --index-impact)")

    # Service mode
    parser.add_argument("--serve", action="store_true",
                        help="Run as persistent service with /metrics, web UI, and JSON API")
    parser.add_argument("--serve-port", type=int, default=9090,
                        help="HTTP port for service mode (default: 9090)")
    parser.add_argument("--interval", default="15m",
                        help="Collection interval for service mode: 5m, 15m, 1h (default: 15m)")
    parser.add_argument("--retention-days", type=int, default=30,
                        help="Delete snapshots older than N days (default: 30, 0=keep all)")
    parser.add_argument("--format", choices=["text", "prometheus", "json"], default="text",
                        help="Output format: text (default), prometheus, json")
    parser.add_argument("--otlp-endpoint", default=None, metavar="URL",
                        help="Push metrics via OTLP HTTP (e.g. http://collector:4318)")

    args = parser.parse_args()

    # ── Snapshot listing (no connection needed) ──────────────────────
    if args.snapshots:
        header(f"SAVED SNAPSHOTS: {args.database}")
        list_snapshots(args.database)
        sys.exit(0)

    if not args.password:
        args.password = getpass.getpass(f"Password for {args.user}@{args.host}: ")

    client = MarkLogicClient(args.host, args.port, args.user, args.password, args.auth_type)

    # ── Service mode ─────────────────────────────────────────────
    if args.serve:
        databases = [args.database]
        interval_sec = parse_interval(args.interval)
        run_service(client, databases, interval_sec, args.serve_port,
                    otlp_endpoint=args.otlp_endpoint,
                    retention_days=args.retention_days)
        sys.exit(0)

    # ── One-shot format modes ────────────────────────────────────
    if args.format in ("prometheus", "json"):
        snap = collect_snapshot(client, args.database)
        if not args.no_snapshot:
            save_snapshot(snap)
        if args.format == "prometheus":
            print(snapshot_to_prometheus(snap))
        else:
            print(json.dumps(snap, indent=2, default=str))
        sys.exit(0)

    print(color("""
    ╔══════════════════════════════════════════════════════╗
    ║         MarkLogic Capacity Analyzer                  ║
    ║         Cluster Resource & Scaling Report             ║
    ╚══════════════════════════════════════════════════════╝
    """, CYAN))

    try:
        # ── Always collect a snapshot (used for saving + reporting) ──
        snap = collect_snapshot(client, args.database)

        # ── Save snapshot (unless --no-snapshot) ─────────────────────
        if not args.no_snapshot:
            path = save_snapshot(snap)
            print(f"    {color('Snapshot saved:', DIM)} {path}")
            removed = prune_snapshots(args.retention_days)
            if removed:
                print(f"    {color(f'Pruned {removed} snapshot(s) older than {args.retention_days} days', DIM)}")
            print()

        # ── Snapshot-only mode: save and exit ────────────────────────
        if args.snapshot_only:
            sys.exit(0)

        # ── Trend mode: show growth curves ───────────────────────────
        if args.trend:
            report_trend(args.database)
            print()
            sys.exit(0)

        # ── Compare mode: diff current vs past ───────────────────────
        if args.compare is not None:
            report_compare(args.database, args.compare)
            print()
            sys.exit(0)

        # ── Index impact mode ────────────────────────────────────────
        if args.index_impact:
            snaps = load_snapshots(args.database)
            if len(snaps) >= 2:
                report_index_impact(snaps[-2], snaps[-1],
                                    project_docs=args.project_docs)
            elif len(snaps) == 1:
                print("    Need at least 2 snapshots to compare index impact.")
                print("    Workflow: snapshot → add/remove index → snapshot → --index-impact")
            else:
                print("    No snapshots found. Run the analyzer first.")
            print()
            sys.exit(0)

        # ── Full report (existing sections, driven from snapshot) ────
        # The report_ functions still query the server directly for now;
        # future iteration could render entirely from the snapshot dict.

        # 1. Cluster overview
        report_cluster(client)

        # 2. Host memory
        host_data = report_host_memory(client)

        # 3. Database statistics
        data_size, device_space, remaining = report_database_stats(client, args.database)

        # 4. Forest health
        forest_data = report_forest_health(client, args.database)

        # 5. Index configuration
        db_props, range_count, enabled_count = report_index_config(client, args.database)

        # 6. Index memory usage (per-index and per-component breakdown)
        report_index_memory(client, args.database)

        # 7. Capacity estimation
        report_capacity_estimate(args.database, db_props, forest_data, host_data,
                                remaining_disk_mb=remaining)

        print()
        print(color("=" * 62, DIM))
        print(color(f"  Report generated for database '{args.database}'", DIM))
        print(color("=" * 62, DIM))
        print()

    except HTTPError as e:
        print(f"\n{color('ERROR', RED)}: HTTP {e.code} from MarkLogic at {args.host}:{args.port}")
        if e.code == 401:
            print("  Check your username/password and auth-type.")
        elif e.code == 404:
            print(f"  Database '{args.database}' not found.")
        else:
            print(f"  {e.read().decode()[:200]}")
        sys.exit(1)
    except URLError as e:
        print(f"\n{color('ERROR', RED)}: Cannot connect to {args.host}:{args.port}")
        print(f"  {e.reason}")
        sys.exit(1)


if __name__ == "__main__":
    main()
