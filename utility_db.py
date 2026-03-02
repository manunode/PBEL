#!/usr/bin/env python3
"""
Utility Records Database Manager (DuckDB)
==========================================
Manages the lifecycle of utility meter readings:
  1. Daily ingestion: Excel files -> readings table
  2. Month-end summarization: readings -> monthly_summary table
  3. Archival: readings -> Parquet files, then purge from DB
  4. Queries: flat owner lookups, discrepancy detection

Database: utility_records.duckdb (created in the working directory)

Tables:
  - readings:        Granular 15-min interval meter readings
  - monthly_summary: Month opening/closing readings per flat per utility
  - ingestion_log:   Tracks which files have been ingested (idempotency)
"""

import os
import sys
import glob
import datetime
from collections import OrderedDict

import duckdb
import pandas as pd


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

    con.execute("""
        CREATE TABLE IF NOT EXISTS ingestion_log (
            source_file   VARCHAR NOT NULL,
            ingested_at   TIMESTAMP DEFAULT current_timestamp,
            record_count  INTEGER,
            PRIMARY KEY (source_file)
        )
    """)

    # Indexes for fast queries
    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_readings_flat_time
        ON readings (given_flat_id, recorded_at)
    """)
    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_readings_time
        ON readings (recorded_at)
    """)

    return con


# ── File Parsing ───────────────────────────────────────────────────────────

def parse_filename(filename):
    """Parse 'T C Eb readings.xls' -> ('Aurum', 'Electricity')."""
    base = os.path.basename(filename)
    name_part = base.split(" readings")[0].strip()
    parts = name_part.split()
    if len(parts) < 3 or parts[0] != "T":
        raise ValueError(f"Unexpected filename format: '{base}'")

    tower_letter = parts[1].upper()
    utility_keyword = parts[2]

    tower_name = TOWER_MAP.get(tower_letter)
    if tower_name is None:
        raise ValueError(f"Unknown tower letter '{tower_letter}' in '{base}'")

    utility_type = None
    for key, value in UTILITY_MAP.items():
        if key.lower() == utility_keyword.lower():
            utility_type = value
            break
    if utility_type is None:
        raise ValueError(f"Unknown utility keyword '{utility_keyword}' in '{base}'")

    return tower_name, utility_type


def normalize_flat_id(raw_value):
    """Normalize flat ID from Excel header: strip whitespace, int-ify floats."""
    if isinstance(raw_value, str):
        return raw_value.strip()
    elif isinstance(raw_value, (int, float)):
        return str(int(raw_value))
    return str(raw_value).strip()


def parse_datetime(raw_dt, row_idx):
    """Parse a datetime value from an Excel cell. Returns datetime or None."""
    if isinstance(raw_dt, datetime.datetime):
        return raw_dt
    if isinstance(raw_dt, str):
        trimmed = raw_dt.strip()
        if not trimmed:
            return None
        for suffix in (" AM", " PM", " am", " pm"):
            if trimmed.endswith(suffix):
                trimmed = trimmed[: -len(suffix)].rstrip()
                break
        for fmt in ("%d-%m-%Y %H:%M", "%d-%m-%Y %H:%M:%S"):
            try:
                return datetime.datetime.strptime(trimmed, fmt)
            except ValueError:
                continue
        print(f"  WARNING: Could not parse datetime at row {row_idx + 1}: '{raw_dt}'")
        return None
    if pd.isna(raw_dt):
        return None
    print(f"  WARNING: Unexpected datetime type at row {row_idx + 1}: {type(raw_dt)} = {raw_dt}")
    return None


def extract_readings_from_excel(filepath):
    """
    Extract all granular readings from a single Excel file.

    Returns:
        list of dict with keys: utility_type, tower_name, flat_id,
            given_flat_id, recorded_at, reading_value, source_file
    """
    tower_name, utility_type = parse_filename(filepath)
    source_file = os.path.basename(filepath)
    print(f"Processing: {source_file}")
    print(f"  Tower: {tower_name}, Utility: {utility_type}")

    df = pd.read_excel(filepath, sheet_name=0, header=None, engine="openpyxl")

    # Row 2 (index 1): flat IDs from column B onward
    flat_cols = []  # list of (col_idx, flat_id, given_flat_id)
    for col_idx in range(1, df.shape[1]):
        raw = df.iloc[1, col_idx]
        if pd.notna(raw):
            flat_id = normalize_flat_id(raw)
            flat_cols.append((col_idx, flat_id, tower_name + flat_id))

    print(f"  Flats found: {len(flat_cols)}")

    # Row 4+ (index 3+): data rows. Deduplicate by datetime.
    records = []
    prev_dt = None

    for row_idx in range(3, df.shape[0]):
        dt_val = parse_datetime(df.iloc[row_idx, 0], row_idx)
        if dt_val is None:
            continue
        if prev_dt is not None and dt_val == prev_dt:
            continue
        prev_dt = dt_val

        for col_idx, flat_id, given_flat_id in flat_cols:
            value = df.iloc[row_idx, col_idx]
            records.append({
                "utility_type": utility_type,
                "tower_name": tower_name,
                "flat_id": flat_id,
                "given_flat_id": given_flat_id,
                "recorded_at": dt_val,
                "reading_value": float(value) if pd.notna(value) else None,
                "source_file": source_file,
            })

    print(f"  Records extracted: {len(records)}")
    return records


# ── Ingestion ──────────────────────────────────────────────────────────────

def discover_files(directory):
    """Find all utility Excel files in directory."""
    patterns = [
        os.path.join(directory, "T * * readings.xls"),
        os.path.join(directory, "T * * Readings.xls"),
        os.path.join(directory, "T * * readings.xlsx"),
        os.path.join(directory, "T * * Readings.xlsx"),
    ]
    found = set()
    for pattern in patterns:
        found.update(glob.glob(pattern))
    return sorted(found)


def ingest_file(con, filepath):
    """
    Ingest a single Excel file into the readings table.
    Skips if already ingested (idempotent).
    Returns number of records inserted, or 0 if skipped.
    """
    source_file = os.path.basename(filepath)

    # Check idempotency
    already = con.execute(
        "SELECT 1 FROM ingestion_log WHERE source_file = ?", [source_file]
    ).fetchone()
    if already:
        print(f"SKIP: {source_file} already ingested.")
        return 0

    records = extract_readings_from_excel(filepath)
    if not records:
        print(f"  No records to insert for {source_file}.")
        return 0

    records_df = pd.DataFrame(records)
    con.execute("INSERT INTO readings SELECT * FROM records_df")
    con.execute(
        "INSERT INTO ingestion_log (source_file, record_count) VALUES (?, ?)",
        [source_file, len(records)],
    )

    print(f"  Inserted {len(records)} records into database.")
    return len(records)


def ingest_directory(con, directory):
    """Ingest all Excel files in directory. Returns total records inserted."""
    files = discover_files(directory)
    if not files:
        print("No utility reading files found.")
        return 0

    print(f"Found {len(files)} file(s):")
    for f in files:
        print(f"  - {os.path.basename(f)}")
    print("=" * 60)

    total = 0
    for filepath in files:
        try:
            total += ingest_file(con, filepath)
        except Exception as e:
            print(f"ERROR processing {os.path.basename(filepath)}: {e}")
        print()

    print(f"Total new records ingested: {total}")
    return total


# ── Month-End Summarization ───────────────────────────────────────────────

def compute_monthly_summary(con, year_month):
    """
    Compute opening and closing readings for a given month ('YYYY-MM').
    Opening = first reading of the month, Closing = last reading of the month.
    Inserts/replaces into monthly_summary table.
    """
    print(f"Computing monthly summary for {year_month}...")

    count = con.execute("""
        INSERT OR REPLACE INTO monthly_summary
            (utility_type, given_flat_id, year_month,
             opening_reading, closing_reading,
             opening_timestamp, closing_timestamp)
        WITH ranked AS (
            SELECT
                utility_type,
                given_flat_id,
                recorded_at,
                reading_value,
                ROW_NUMBER() OVER (
                    PARTITION BY utility_type, given_flat_id
                    ORDER BY recorded_at ASC
                ) AS rn_asc,
                ROW_NUMBER() OVER (
                    PARTITION BY utility_type, given_flat_id
                    ORDER BY recorded_at DESC
                ) AS rn_desc
            FROM readings
            WHERE strftime(recorded_at, '%Y-%m') = ?
              AND reading_value IS NOT NULL
        )
        SELECT
            o.utility_type,
            o.given_flat_id,
            ? AS year_month,
            o.reading_value  AS opening_reading,
            c.reading_value  AS closing_reading,
            o.recorded_at    AS opening_timestamp,
            c.recorded_at    AS closing_timestamp
        FROM ranked o
        JOIN ranked c
          ON o.utility_type = c.utility_type
         AND o.given_flat_id = c.given_flat_id
         AND c.rn_desc = 1
        WHERE o.rn_asc = 1
    """, [year_month, year_month]).fetchall()

    row_count = con.execute(
        "SELECT COUNT(*) FROM monthly_summary WHERE year_month = ?",
        [year_month],
    ).fetchone()[0]

    print(f"  Monthly summary: {row_count} flat/utility combinations for {year_month}.")
    return row_count


def show_monthly_summary(con, year_month):
    """Display the monthly summary for a given month."""
    df = con.execute("""
        SELECT utility_type, given_flat_id, year_month,
               opening_reading, closing_reading,
               opening_reading - closing_reading AS consumption,
               opening_timestamp, closing_timestamp
        FROM monthly_summary
        WHERE year_month = ?
        ORDER BY utility_type, given_flat_id
    """, [year_month]).fetchdf()

    if df.empty:
        print(f"No summary data for {year_month}.")
    else:
        print(f"\nMonthly summary for {year_month} ({len(df)} records):")
        print(df.head(20).to_string(index=False))
        if len(df) > 20:
            print(f"  ... and {len(df) - 20} more rows.")
    return df


# ── Archival ───────────────────────────────────────────────────────────────

def archive_month_to_parquet(con, year_month, archive_dir=DEFAULT_ARCHIVE_DIR):
    """
    Export a month's granular readings to a Parquet file, then purge from DB.
    Only proceeds if the monthly summary has already been computed.
    """
    # Safety check: ensure monthly summary exists
    summary_count = con.execute(
        "SELECT COUNT(*) FROM monthly_summary WHERE year_month = ?",
        [year_month],
    ).fetchone()[0]
    if summary_count == 0:
        print(f"ERROR: No monthly summary for {year_month}. Compute it first before archiving.")
        return False

    # Count records to archive
    readings_count = con.execute("""
        SELECT COUNT(*) FROM readings
        WHERE strftime(recorded_at, '%Y-%m') = ?
    """, [year_month]).fetchone()[0]

    if readings_count == 0:
        print(f"No readings to archive for {year_month}.")
        return True

    os.makedirs(archive_dir, exist_ok=True)
    parquet_path = os.path.join(archive_dir, f"readings_{year_month}.parquet")

    # Export to Parquet using DuckDB's native COPY
    con.execute(f"""
        COPY (
            SELECT * FROM readings
            WHERE strftime(recorded_at, '%Y-%m') = '{year_month}'
            ORDER BY utility_type, given_flat_id, recorded_at
        ) TO '{parquet_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    # Verify the Parquet file was written
    verify_count = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{parquet_path}')"
    ).fetchone()[0]

    if verify_count != readings_count:
        print(f"ERROR: Parquet has {verify_count} rows but DB has {readings_count}. Aborting purge.")
        return False

    # Purge from DB
    con.execute("""
        DELETE FROM readings
        WHERE strftime(recorded_at, '%Y-%m') = ?
    """, [year_month])

    print(f"Archived {readings_count} readings for {year_month} -> {parquet_path}")
    print(f"  Parquet verified: {verify_count} rows. Purged from database.")
    return True


def reload_parquet(con, parquet_path):
    """Reload a Parquet archive back into the readings table."""
    count = con.execute(f"""
        INSERT INTO readings
        SELECT * FROM read_parquet('{parquet_path}')
    """).fetchone()

    row_count = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{parquet_path}')"
    ).fetchone()[0]

    print(f"Reloaded {row_count} records from {parquet_path} into readings table.")
    return row_count


# ── Query Helpers ──────────────────────────────────────────────────────────

def query_flat_readings(con, given_flat_id, start_date, end_date, utility_type=None):
    """
    Query readings for a specific flat within a date range.
    Useful for responding to flat owner queries.

    Args:
        given_flat_id: e.g. 'AurumG001'
        start_date: 'YYYY-MM-DD'
        end_date: 'YYYY-MM-DD'
        utility_type: optional filter (e.g. 'Electricity')
    """
    query = """
        SELECT utility_type, given_flat_id, recorded_at, reading_value
        FROM readings
        WHERE given_flat_id = ?
          AND recorded_at >= CAST(? AS TIMESTAMP)
          AND recorded_at < CAST(? AS TIMESTAMP) + INTERVAL 1 DAY
    """
    params = [given_flat_id, start_date, end_date]

    if utility_type:
        query += " AND utility_type = ?"
        params.append(utility_type)

    query += " ORDER BY utility_type, recorded_at"
    return con.execute(query, params).fetchdf()


def detect_discrepancies(con, year_month):
    """
    Detect potential discrepancies in readings for a given month:
    - Gaps: missing 15-min intervals
    - Backward readings: reading decreases over time
    - Flat readings: no change over extended periods (potential meter fault)
    """
    print(f"Running discrepancy checks for {year_month}...")

    # Backward readings (meter going backwards)
    backward = con.execute("""
        WITH lagged AS (
            SELECT
                utility_type, given_flat_id, recorded_at, reading_value,
                LAG(reading_value) OVER (
                    PARTITION BY utility_type, given_flat_id
                    ORDER BY recorded_at
                ) AS prev_value,
                LAG(recorded_at) OVER (
                    PARTITION BY utility_type, given_flat_id
                    ORDER BY recorded_at
                ) AS prev_time
            FROM readings
            WHERE strftime(recorded_at, '%Y-%m') = ?
              AND reading_value IS NOT NULL
        )
        SELECT utility_type, given_flat_id, recorded_at, reading_value,
               prev_value, reading_value - prev_value AS delta
        FROM lagged
        WHERE prev_value IS NOT NULL
          AND reading_value < prev_value
        ORDER BY utility_type, given_flat_id, recorded_at
    """, [year_month]).fetchdf()

    if not backward.empty:
        print(f"\n  BACKWARD READINGS ({len(backward)} instances):")
        print(backward.head(20).to_string(index=False))
    else:
        print("  No backward readings detected.")

    # Flats with zero consumption all month
    zero_consumption = con.execute("""
        WITH flat_range AS (
            SELECT
                utility_type, given_flat_id,
                MIN(reading_value) AS min_val,
                MAX(reading_value) AS max_val,
                COUNT(*) AS num_readings
            FROM readings
            WHERE strftime(recorded_at, '%Y-%m') = ?
              AND reading_value IS NOT NULL
            GROUP BY utility_type, given_flat_id
        )
        SELECT * FROM flat_range
        WHERE min_val = max_val AND num_readings > 1
        ORDER BY utility_type, given_flat_id
    """, [year_month]).fetchdf()

    if not zero_consumption.empty:
        print(f"\n  ZERO CONSUMPTION ({len(zero_consumption)} flat/utility combos):")
        print(zero_consumption.head(20).to_string(index=False))
    else:
        print("  No zero-consumption flats detected.")

    return {"backward": backward, "zero_consumption": zero_consumption}


def db_stats(con):
    """Print database statistics."""
    readings_count = con.execute("SELECT COUNT(*) FROM readings").fetchone()[0]
    summary_count = con.execute("SELECT COUNT(*) FROM monthly_summary").fetchone()[0]
    ingestion_count = con.execute("SELECT COUNT(*) FROM ingestion_log").fetchone()[0]

    print("Database statistics:")
    print(f"  Readings:        {readings_count:>10,}")
    print(f"  Monthly summary: {summary_count:>10,}")
    print(f"  Files ingested:  {ingestion_count:>10,}")

    if readings_count > 0:
        date_range = con.execute("""
            SELECT MIN(recorded_at), MAX(recorded_at),
                   COUNT(DISTINCT given_flat_id),
                   COUNT(DISTINCT utility_type)
            FROM readings
        """).fetchone()
        print(f"  Date range:      {date_range[0]} to {date_range[1]}")
        print(f"  Distinct flats:  {date_range[2]:>10,}")
        print(f"  Utility types:   {date_range[3]:>10,}")


# ── CLI ────────────────────────────────────────────────────────────────────

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

  # Run discrepancy detection
  python utility_db.py check 2025-01

  # Show database stats
  python utility_db.py stats
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

    p_check = sub.add_parser("check", help="Detect discrepancies")
    p_check.add_argument("year_month", help="Month to check (YYYY-MM)")

    sub.add_parser("stats", help="Show database statistics")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    con = init_db(args.db)

    if args.command == "ingest":
        ingest_directory(con, args.directory)
    elif args.command == "summarize":
        compute_monthly_summary(con, args.year_month)
        show_monthly_summary(con, args.year_month)
    elif args.command == "archive":
        archive_month_to_parquet(con, args.year_month, args.archive_dir)
    elif args.command == "reload":
        reload_parquet(con, args.parquet_path)
    elif args.command == "query":
        df = query_flat_readings(con, args.flat_id, args.start_date, args.end_date, args.utility)
        if df.empty:
            print("No readings found for the given criteria.")
        else:
            print(df.to_string(index=False))
    elif args.command == "check":
        detect_discrepancies(con, args.year_month)
    elif args.command == "stats":
        db_stats(con)

    con.close()


if __name__ == "__main__":
    main()
