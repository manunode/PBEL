"""
Utility DB — Core
==================
Shared configuration, database schema, and period/date-range helpers.
All other modules import from here.
"""

import os
import re
import sys

import duckdb


# ── Configuration ──────────────────────────────────────────────────────────

TOWER_MAP = {
    "A": "Platinum", "B": "Titanium", "C": "Aurum", "D": "Argentum",
    "E": "Pearl", "F": "Crystal", "G": "Jade", "H": "Turquoise",
    "J": "Amethyst", "K": "Aquamarine", "L": "Opal", "M": "Sapphire",
    "N": "Ruby", "P": "Lakeside",
}

UTILITY_MAP = {
    "Eb": "Electricity", "Dg": "Diesel", "Water": "Water", "Gas": "Gas",
}

DEFAULT_DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "utility_records.duckdb")
DEFAULT_ARCHIVE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "archive")


# ── Schema ─────────────────────────────────────────────────────────────────

def init_db(db_path=DEFAULT_DB_PATH):
    """Create/open DuckDB database and ensure tables exist."""
    con = duckdb.connect(db_path)

    con.execute("""
        CREATE TABLE IF NOT EXISTS readings (
            utility_type   VARCHAR NOT NULL,
            tower_name     VARCHAR NOT NULL,
            flat_id        VARCHAR NOT NULL,
            given_flat_id  VARCHAR NOT NULL,
            recorded_at    TIMESTAMP NOT NULL,
            reading_value  DOUBLE,
            source_file    VARCHAR NOT NULL
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS monthly_summary (
            utility_type      VARCHAR NOT NULL,
            given_flat_id     VARCHAR NOT NULL,
            year_month        VARCHAR NOT NULL,  -- 'YYYY-MM'
            opening_reading   DOUBLE,
            closing_reading   DOUBLE,
            opening_timestamp TIMESTAMP,
            closing_timestamp TIMESTAMP,
            computed_at       TIMESTAMP DEFAULT current_timestamp,
            PRIMARY KEY (utility_type, given_flat_id, year_month)
        )
    """)

    # Ingestion log with file hash for content-based deduplication
    con.execute("""
        CREATE TABLE IF NOT EXISTS ingestion_log (
            source_file   VARCHAR NOT NULL,
            file_hash     VARCHAR NOT NULL,
            ingested_at   TIMESTAMP DEFAULT current_timestamp,
            record_count  INTEGER,
            PRIMARY KEY (source_file)
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_ingestion_log_hash ON ingestion_log (file_hash)")

    # Indexes for fast queries
    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_readings_flat_time
        ON readings (given_flat_id, recorded_at)
    """)
    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_readings_time
        ON readings (recorded_at)
    """)

    # Unique index to prevent duplicate readings
    con.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_readings_unique
        ON readings (utility_type, given_flat_id, recorded_at)
    """)

    return con


# ── Period / date-range helpers ───────────────────────────────────────────

def parse_period(period_str):
    """
    Parse a flexible period string into (start_month, end_month) tuple.

    Accepted formats:
      '2026-08'           → ('2026-08', '2026-08')       single month
      '2026-01:2026-06'   → ('2026-01', '2026-06')       explicit range
      '2026'              → ('2026-01', '2026-12')        full year
      '2026-H1'           → ('2026-01', '2026-06')        first half
      '2026-H2'           → ('2026-07', '2026-12')        second half
      '2026-Q1'           → ('2026-01', '2026-03')        quarter
      '2026-Q2'           → ('2026-04', '2026-06')
      '2026-Q3'           → ('2026-07', '2026-09')
      '2026-Q4'           → ('2026-10', '2026-12')
    """
    s = re.sub(r'[_/.]', '-', period_str.strip())

    # Range: 2026-01:2026-06
    if ':' in s:
        parts = s.split(':')
        if len(parts) != 2:
            _period_error(period_str)
        start, end = parts
        if not (re.match(r'^\d{4}-\d{2}$', start) and re.match(r'^\d{4}-\d{2}$', end)):
            _period_error(period_str)
        return (start, end)

    # Full year: 2026
    if re.match(r'^\d{4}$', s):
        return (f"{s}-01", f"{s}-12")

    # Half year: 2026-H1, 2026-H2
    m = re.match(r'^(\d{4})-H([12])$', s, re.IGNORECASE)
    if m:
        year, half = m.group(1), int(m.group(2))
        if half == 1:
            return (f"{year}-01", f"{year}-06")
        else:
            return (f"{year}-07", f"{year}-12")

    # Quarter: 2026-Q1..Q4
    m = re.match(r'^(\d{4})-Q([1-4])$', s, re.IGNORECASE)
    if m:
        year, q = m.group(1), int(m.group(2))
        start_m = (q - 1) * 3 + 1
        end_m = q * 3
        return (f"{year}-{start_m:02d}", f"{year}-{end_m:02d}")

    # Single month: 2026-08
    if re.match(r'^\d{4}-\d{2}$', s):
        return (s, s)

    _period_error(period_str)


def _period_error(period_str):
    print(f"ERROR: Invalid period '{period_str}'.")
    print("  Accepted formats:")
    print("    YYYY-MM              single month    (e.g. 2026-08)")
    print("    YYYY-MM:YYYY-MM      explicit range  (e.g. 2026-01:2026-06)")
    print("    YYYY                 full year       (e.g. 2026)")
    print("    YYYY-H1 / YYYY-H2   half year       (e.g. 2026-H1)")
    print("    YYYY-Q1..Q4          quarter         (e.g. 2026-Q3)")
    sys.exit(1)


def month_filter_sql(col, start_month, end_month):
    """
    Return (sql_fragment, params) for filtering a YYYY-MM column/expression.
    Single month uses '=', ranges use 'BETWEEN'.
    """
    if start_month == end_month:
        return f"{col} = ?", [start_month]
    else:
        return f"{col} BETWEEN ? AND ?", [start_month, end_month]


def period_label(start_month, end_month):
    """Human-readable label for a period."""
    if start_month == end_month:
        return start_month
    return f"{start_month} to {end_month}"
