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
  - Document capacity estimation (how many more docs before memory limits)

Usage:
    python ml_capacity.py [--host HOST] [--port PORT] [--user USER]
                          [--password PASSWORD] [--database DATABASE]
                          [--auth-type digest|basic]
"""

import argparse
import getpass
import json
import math
import sys
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
            kv("Fragmentation", f"{frag_pct:.1f}%  {status_badge(frag_pct < 20, 'OK', 'HIGH')}")

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


def report_capacity_estimate(database, db_props, forest_data, host_data):
    header(f"CAPACITY ESTIMATE: {database}")

    if not forest_data:
        print("    Insufficient data for capacity estimation (no forest metrics)")
        return

    # Gather current totals
    total_docs = sum(f.get("document-count", 0) or 0 for f in forest_data)
    total_active = sum(f.get("active-fragment-count", 0) or 0 for f in forest_data)
    total_memory = sum(f.get("memory-size-mb", 0) or 0 for f in forest_data)
    total_disk = sum(f.get("disk-size-mb", 0) or 0 for f in forest_data)
    num_forests = len(forest_data)

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
        # memory-forest-size is the only component that grows with documents.
        # Cache (list + compressed-tree) is pre-allocated at startup and fixed.
        # Base ML overhead (threads, code) is fixed.
        # File cache (mmap) is OS-managed and bounded separately.
        # So the marginal memory cost per additional document = forest_mem / doc_count.
        # Headroom available to forest growth = ceiling - (fixed components + forest_mem)
        print()
        print(f"    {color('Document capacity projection', BOLD)}")
        fixed_mem = cache_alloc + base_overhead + file_cache
        forest_headroom = safe_cap - fixed_mem - forest_mem

        kv("  Fixed components (cache+base+file)", fmt_mb(fixed_mem),         indent=6)
        kv("  Current forest memory",              fmt_mb(forest_mem),        indent=6)
        kv("  Remaining for forest growth",
           f"{fmt_mb(forest_headroom)}  {status_badge(forest_headroom > 512, 'OK', 'LOW')}",
           indent=6)

        if total_docs > 0 and forest_mem > 0:
            forest_bytes_per_doc = (forest_mem * 1024 * 1024) / total_docs
            kv("  Forest memory per document",
               f"{forest_bytes_per_doc:.0f} bytes", indent=6)

            if forest_headroom > 0:
                docs_remaining = int((forest_headroom * 1024 * 1024) / forest_bytes_per_doc)
                kv("  Est. additional documents until ceiling",
                   f"{color(f'{docs_remaining:,}', BOLD)}", indent=6)
                total_capacity = total_docs + docs_remaining
                kv("  Est. total documents at ceiling",
                   f"{total_capacity:,}", indent=6)
            else:
                print(f"      {color('WARNING: Forest memory headroom exhausted!', RED)}")
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
        if active > 0 and (deleted / (active + deleted)) > 0.25:
            issues.append(("High fragmentation", f"Forest '{f.get('forest-name')}' at {deleted/(active+deleted)*100:.0f}% deleted fragments"))

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

    args = parser.parse_args()

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

        # 6. Capacity estimation
        report_capacity_estimate(args.database, db_props, forest_data, host_data)

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
