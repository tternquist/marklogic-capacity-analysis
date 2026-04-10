from ml_capacity.formatting import (
    color, header, sub_header, kv, fmt_mb,
    GREEN, RED, YELLOW, BOLD, DIM,
)


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
