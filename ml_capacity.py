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
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from base64 import b64encode


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
        kv("Total index memory",  fmt_mb(grand_mem / (1024 * 1024)))
        kv("Total index on-disk", fmt_mb(grand_disk / (1024 * 1024)))

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
            return sum(float(h.get(key, 0) or 0) for h in host_data)

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
        safe_cap = min(ml_limit if ml_limit else total_sys * 0.80,
                       total_sys * 0.80)
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

            # If fragmentation is high, estimate the "clean" bytes/doc
            if frag_pct >= 25 and total_frags > 0:
                clean_ratio = total_active / total_frags
                estimated_clean_disk = total_disk * clean_ratio
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
                   f"{color(f'(~{frag_pct:.0f}% of current disk is deleted fragments)', DIM)}",
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
        total_sys = sum(h.get("memory-system-total-mb", 0) for h in host_data)
        ml_rss = sum(h.get("memory-process-rss-mb", 0) for h in host_data)
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
        return sum(float(h.get(key, 0) or 0) for h in snap["hosts"])

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
        "system_total_mb":  hsum("memory-system-total-mb"),
        "system_free_mb":   hsum("memory-system-free-mb"),
    }

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
    """
    if a == b:
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

    args = parser.parse_args()

    # ── Snapshot listing (no connection needed) ──────────────────────
    if args.snapshots:
        header(f"SAVED SNAPSHOTS: {args.database}")
        list_snapshots(args.database)
        sys.exit(0)

    if not args.password:
        args.password = getpass.getpass(f"Password for {args.user}@{args.host}: ")

    client = MarkLogicClient(args.host, args.port, args.user, args.password, args.auth_type)

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
