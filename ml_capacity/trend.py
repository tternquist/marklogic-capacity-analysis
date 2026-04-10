from datetime import datetime
from ml_capacity.formatting import (
    color, header, sub_header, kv, bar, fmt_mb, status_badge,
    YELLOW, GREEN, RED, BOLD, DIM,
)
from ml_capacity.snapshot import load_snapshots, list_snapshots
from ml_capacity.config_drift import report_config_drift
from ml_capacity.index_analysis import diff_index_memory, _index_label


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

    # -- Configuration drift check ------------------------------------
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

    # -- Memory runway (primary) --------------------------------------
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
        kv("Forest memory growth",  f"{fmt_mb(first['forest_mb'])} \u2192 {fmt_mb(forest_now)}  "
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
        action_msg = (f'Action needed before: forest memory reaches {fmt_mb(ceiling - fixed_mem)} '
                      f'(currently {fmt_mb(forest_now)})')
        print(f"    {color('>>> ', RED)}{color(action_msg, DIM)}")
    elif days > 0 and forest_delta <= 0:
        print()
        print(f"    {color('Forest memory stable or shrinking over this period.', GREEN)}")
        print(f"    No immediate memory pressure detected.")
    elif days == 0:
        print()
        print(f"    {color('Snapshots are too close together for rate calculation.', YELLOW)}")
        print(f"    Take snapshots hours or days apart for meaningful trends.")

    # -- Growth summary -------------------------------------------------
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

        kv(label, f"{first_s} \u2192 {last_s}  {color(delta_s, GREEN if delta >= 0 else RED)}{rate_s}")

    # -- Other runway projections ---------------------------------------
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

        # -- Binding constraint summary ---------------------------------
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
        print(f"    Snapshot #{compare_idx} not found. Valid range: 0\u2013{len(snaps)-1}")
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

    # -- Configuration drift check between the two snapshots ----------
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
        kv(label, f"{old_s} \u2192 {new_s}  {color(d_s, c)}  {color(pct_s, DIM)}")

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
                kv(k, f"{ov} \u2192 {nv}")

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
