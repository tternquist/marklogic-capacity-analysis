import argparse
import getpass
import json
import logging
import os
import sys
from urllib.error import HTTPError, URLError

from ml_capacity.formatting import color, header, RED, GREEN, CYAN, DIM
from ml_capacity.validation import validate_database_name
from ml_capacity.client import MarkLogicClient
from ml_capacity.snapshot import (
    SNAPSHOT_DIR, collect_snapshot, save_snapshot, prune_snapshots,
    load_snapshots, list_snapshots,
)
from ml_capacity.report import (
    report_cluster, report_host_memory, report_database_stats,
    report_forest_health, report_index_config, report_index_memory,
    report_capacity_estimate,
)
from ml_capacity.trend import report_trend, report_compare
from ml_capacity.index_analysis import report_index_impact
from ml_capacity.service import run_service
from ml_capacity.prometheus import parse_interval, snapshot_to_prometheus


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
    parser.add_argument("--api-token", default=None,
                        help="Bearer token required for service API access (env: MLCA_API_TOKEN)")

    # Import snapshots from disconnected environments
    parser.add_argument("--import-snapshot", nargs="+", metavar="FILE",
                        help="Import snapshot JSON file(s) from a disconnected environment")

    args = parser.parse_args()

    # ── Configure logging ───────────────────────────────────────────
    logging.basicConfig(
        level=logging.INFO,
        format="  [%(asctime)s] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    # ── Validate database name (prevents XQuery injection) ───────────
    try:
        validate_database_name(args.database)
    except ValueError as e:
        print(f"\n{color('ERROR', RED)}: {e}")
        sys.exit(1)

    # ── Snapshot listing (no connection needed) ──────────────────────
    if args.snapshots:
        header(f"SAVED SNAPSHOTS: {args.database}")
        list_snapshots(args.database)
        sys.exit(0)

    # ── Import snapshots from disconnected environments ─────────────
    if args.import_snapshot:
        imported = import_snapshots(args.import_snapshot)
        if imported:
            print(f"\n    {color('Import complete:', GREEN)} {imported} snapshot(s) saved to {SNAPSHOT_DIR}/")
            header(f"SAVED SNAPSHOTS: {args.database}")
            list_snapshots(args.database)
        sys.exit(0 if imported else 1)

    if not args.password:
        args.password = getpass.getpass(f"Password for {args.user}@{args.host}: ")

    # Resolve API token from arg or environment
    api_token = args.api_token or os.environ.get("MLCA_API_TOKEN")

    client = MarkLogicClient(args.host, args.port, args.user, args.password, args.auth_type)

    # ── Service mode ─────────────────────────────────────────────
    if args.serve:
        databases = [args.database]
        interval_sec = parse_interval(args.interval)
        run_service(client, databases, interval_sec, args.serve_port,
                    otlp_endpoint=args.otlp_endpoint,
                    retention_days=args.retention_days,
                    api_token=api_token)
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
