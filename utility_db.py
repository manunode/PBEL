#!/usr/bin/env python3
"""
Utility Records Database Manager (DuckDB) — CLI
=================================================
Thin CLI dispatcher that imports from:
  - utility_db_core:    config, schema, period helpers
  - utility_ingest:     file parsing, ingestion, Parquet reload
  - utility_summarize:  monthly summary, archival, queries, stats
  - utility_check:      discrepancy detection, cross-utility, ingestion checks
"""

import os
import re
import sys

from utility_db_core import (
    DEFAULT_DB_PATH, DEFAULT_ARCHIVE_DIR,
    init_db, parse_period,
)
from utility_ingest import ingest_directory, reload_parquet
from utility_summarize import (
    compute_monthly_summary, show_monthly_summary,
    archive_month_to_parquet, query_flat_readings, db_stats,
)
from utility_check import (
    detect_discrepancies, detect_summary_discrepancies,
    check_cross_utility, detect_ingestion_issues,
)


def _normalize_period(raw):
    """Normalize separators: underscores/dots/slashes → hyphens."""
    return re.sub(r'[_/.]', '-', raw.strip())


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Utility Records Database Manager (DuckDB)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Ingest all Excel files from a directory
  python utility_db.py ingest /path/to/excel/files

  # Compute monthly summary
  python utility_db.py summarize 2025-01

  # Archive a month to Parquet and purge from DB
  python utility_db.py archive 2025-01

  # Reload archived Parquet back into DB
  python utility_db.py reload archive/readings_2025-01.parquet

  # Query a flat's readings
  python utility_db.py query AurumG001 2025-01-12 2025-01-15

  # Run granular discrepancy checks (single month or period)
  python utility_db.py check 2025-01
  python utility_db.py check 2025-Q1
  python utility_db.py check 2025

  # Run monthly-summary discrepancy checks
  python utility_db.py check-summary 2025-01
  python utility_db.py check-summary 2025-H1

  # Cross-utility flat profile report
  python utility_db.py check-cross-utility 2025-Q2

  # Ingestion completeness checks
  python utility_db.py check-ingestion 2025-01

  # Show database stats
  python utility_db.py stats

Period formats:
  YYYY-MM              single month    (e.g. 2025-01)
  YYYY-MM:YYYY-MM      explicit range  (e.g. 2025-01:2025-06)
  YYYY                 full year       (e.g. 2025)
  YYYY-H1 / YYYY-H2   half year       (e.g. 2025-H1)
  YYYY-Q1..Q4          quarter         (e.g. 2025-Q3)
        """,
    )
    parser.add_argument("--db", default=DEFAULT_DB_PATH, help="Path to DuckDB database file")

    sub = parser.add_subparsers(dest="command")

    p_ingest = sub.add_parser("ingest", help="Ingest Excel files from a directory")
    p_ingest.add_argument("directory", nargs="?", default=os.path.dirname(os.path.abspath(__file__)))

    p_summarize = sub.add_parser("summarize", help="Compute monthly summary")
    p_summarize.add_argument("year_month", help="Month to summarize (YYYY-MM)")

    p_archive = sub.add_parser("archive", help="Archive month to Parquet and purge")
    p_archive.add_argument("year_month", help="Month to archive (YYYY-MM)")
    p_archive.add_argument("--archive-dir", default=DEFAULT_ARCHIVE_DIR)

    p_reload = sub.add_parser("reload", help="Reload a Parquet archive into DB")
    p_reload.add_argument("parquet_path", help="Path to Parquet file")

    p_query = sub.add_parser("query", help="Query flat readings")
    p_query.add_argument("flat_id", help="e.g. AurumG001")
    p_query.add_argument("start_date", help="YYYY-MM-DD")
    p_query.add_argument("end_date", help="YYYY-MM-DD")
    p_query.add_argument("--utility", help="Filter by utility type")

    p_check = sub.add_parser("check", help="Granular discrepancy checks (supports periods)")
    p_check.add_argument("period", help="Period to check (YYYY-MM, YYYY, YYYY-Q1, etc.)")

    p_check_summary = sub.add_parser("check-summary", help="Monthly summary discrepancy checks")
    p_check_summary.add_argument("period", nargs="?", help="Period (optional; all months if omitted)")

    p_check_cross = sub.add_parser("check-cross-utility", help="Cross-utility flat profile report")
    p_check_cross.add_argument("period", nargs="?", help="Period (optional; all months if omitted)")

    p_check_ingest = sub.add_parser("check-ingestion", help="Ingestion completeness checks")
    p_check_ingest.add_argument("year_month", help="Month to check (YYYY-MM)")
    p_check_ingest.add_argument("--expected-files", type=int, default=14,
                                help="Expected source files per day (default: 14)")

    sub.add_parser("stats", help="Show database statistics")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    con = init_db(args.db)

    try:
        if args.command == "ingest":
            ingest_directory(con, args.directory)

        elif args.command == "summarize":
            ym = _normalize_period(args.year_month)
            compute_monthly_summary(con, ym)
            show_monthly_summary(con, ym)

        elif args.command == "archive":
            ym = _normalize_period(args.year_month)
            archive_month_to_parquet(con, ym, args.archive_dir)

        elif args.command == "reload":
            reload_parquet(con, args.parquet_path)

        elif args.command == "query":
            df = query_flat_readings(con, args.flat_id, args.start_date, args.end_date, args.utility)
            if df.empty:
                print("No readings found for the given criteria.")
            else:
                print(df.to_string(index=False))

        elif args.command == "check":
            period = _normalize_period(args.period)
            start_month, end_month = parse_period(period)
            detect_discrepancies(con, start_month, end_month)

        elif args.command == "check-summary":
            if args.period:
                period = _normalize_period(args.period)
                start_month, end_month = parse_period(period)
                detect_summary_discrepancies(con, start_month, end_month)
            else:
                detect_summary_discrepancies(con)

        elif args.command == "check-cross-utility":
            if args.period:
                period = _normalize_period(args.period)
                start_month, end_month = parse_period(period)
                check_cross_utility(con, start_month, end_month)
            else:
                check_cross_utility(con)

        elif args.command == "check-ingestion":
            ym = _normalize_period(args.year_month)
            detect_ingestion_issues(con, ym, args.expected_files)

        elif args.command == "stats":
            db_stats(con)
    finally:
        con.close()


if __name__ == "__main__":
    main()
