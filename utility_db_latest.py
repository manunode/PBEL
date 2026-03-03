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
import hashlib

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


# ── File Parsing ───────────────────────────────────────────────────────────

def parse_filename(filename):
    """Parse 'T C Eb readings.xls' -> (tower_letter, tower_name, utility_type)."""
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

    return tower_letter, tower_name, utility_type


def file_md5(filepath):
    """Compute MD5 hash of the entire file."""
    hash_md5 = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def normalize_flat_id(raw_value, tower_letter):
    """
    Normalize flat ID from Excel header:
      - Convert to string, strip whitespace.
      - If the resulting string starts with the tower_letter (case‑insensitive),
        remove that letter and any following spaces.
    Returns the cleaned flat_id (e.g., "G01").
    """
    if isinstance(raw_value, str):
        s = raw_value.strip()
    elif isinstance(raw_value, (int, float)):
        s = str(int(raw_value)).strip()
    else:
        s = str(raw_value).strip()

    # If it starts with the tower letter, strip it and following spaces
    if s.upper().startswith(tower_letter.upper()):
        s = s[len(tower_letter):].lstrip()
    return s


def parse_datetime(raw_dt, row_idx):
    """
    Parse a datetime value from an Excel cell.
    Handles both DD-MM-YYYY and YYYY-MM-DD formats, with/without seconds.
    Returns datetime or None.
    """
    if isinstance(raw_dt, (datetime.datetime, pd.Timestamp)):
        return pd.Timestamp(raw_dt).to_pydatetime()
    if isinstance(raw_dt, str):
        trimmed = raw_dt.strip()
        if not trimmed:
            return None
        # Try multiple formats
        for fmt in (
            "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M",
            "%d-%m-%Y %H:%M:%S", "%d-%m-%Y %H:%M",
            "%d-%m-%Y %I:%M %p", "%d-%m-%Y %I:%M:%S %p"
        ):
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
    Uses openpyxl first (handles .xlsx), falls back to xlrd for older .xls.
    """
    tower_letter, tower_name, utility_type = parse_filename(filepath)
    source_file = os.path.basename(filepath)
    print(f"Processing: {source_file}")
    print(f"  Tower: {tower_name}, Utility: {utility_type}")

    # Try openpyxl first (supports .xlsx, even if extension is .xls)
    try:
        df = pd.read_excel(filepath, sheet_name=0, header=None, engine='openpyxl')
    except Exception:
        # If openpyxl fails, try xlrd (older .xls format)
        df = pd.read_excel(filepath, sheet_name=0, header=None, engine='xlrd')

    # Row 2 (index 1): flat IDs from column B onward
    flat_cols = []  # list of (col_idx, flat_id, given_flat_id)
    for col_idx in range(1, df.shape[1]):
        raw = df.iloc[1, col_idx]
        if pd.notna(raw):
            flat_id = normalize_flat_id(raw, tower_letter)
            given_flat_id = tower_name + flat_id
            flat_cols.append((col_idx, flat_id, given_flat_id))

    print(f"  Flats found: {len(flat_cols)}")

    # Row 4+ (index 3+): data rows. Deduplicate consecutive timestamps.
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
    - Skips if file content already processed (using MD5 hash).
    - Inserts records, ignoring any duplicates (unique index on (utility, flat, time)).
    Returns number of new records inserted.
    """
    source_file = os.path.basename(filepath)
    file_hash = file_md5(filepath)

    # Skip if this exact file content has been processed before
    already = con.execute(
        "SELECT 1 FROM ingestion_log WHERE file_hash = ?", [file_hash]
    ).fetchone()
    if already:
        print(f"SKIP: {source_file} already ingested (hash matches).")
        return 0

    records = extract_readings_from_excel(filepath)
    if not records:
        print(f"  No records to insert for {source_file}.")
        return 0

    records_df = pd.DataFrame(records)

    # Count before insert to compute actual new rows
    count_before = con.execute("SELECT COUNT(*) FROM readings").fetchone()[0]

    # Register DataFrame as a temporary table for INSERT
    con.register("temp_readings", records_df)

    # Insert, ignoring duplicates on (utility_type, given_flat_id, recorded_at)
    con.execute("""
        INSERT INTO readings
        SELECT * FROM temp_readings
        ON CONFLICT (utility_type, given_flat_id, recorded_at) DO NOTHING
    """)

    con.unregister("temp_readings")

    count_after = con.execute("SELECT COUNT(*) FROM readings").fetchone()[0]
    inserted = count_after - count_before
    skipped = len(records_df) - inserted

    # Log the file (even if some rows were skipped)
    con.execute(
        "INSERT INTO ingestion_log (source_file, file_hash, record_count) VALUES (?, ?, ?)",
        [source_file, file_hash, inserted],
    )

    print(f"  Inserted {inserted} new records (skipped {skipped} duplicates).")
    return inserted


def ingest_directory(con, directory):
    """Ingest all Excel files in directory. Returns total new records inserted."""
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
    Uses FIRST/LAST aggregates for clarity and reliability.
    """
    print(f"Computing monthly summary for {year_month}...")

    con.execute("""
        INSERT OR REPLACE INTO monthly_summary
            (utility_type, given_flat_id, year_month,
             opening_reading, closing_reading,
             opening_timestamp, closing_timestamp)
        SELECT
            utility_type,
            given_flat_id,
            ? AS year_month,
            FIRST(reading_value ORDER BY recorded_at) AS opening_reading,
            LAST(reading_value ORDER BY recorded_at)  AS closing_reading,
            MIN(recorded_at) AS opening_timestamp,
            MAX(recorded_at) AS closing_timestamp
        FROM readings
        WHERE strftime(recorded_at, '%Y-%m') = ?
          AND reading_value IS NOT NULL
        GROUP BY utility_type, given_flat_id
    """, [year_month, year_month])

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
    summary_count = con.execute(
        "SELECT COUNT(*) FROM monthly_summary WHERE year_month = ?",
        [year_month],
    ).fetchone()[0]
    if summary_count == 0:
        print(f"ERROR: No monthly summary for {year_month}. Compute it first before archiving.")
        return False

    readings_count = con.execute("""
        SELECT COUNT(*) FROM readings
        WHERE strftime(recorded_at, '%Y-%m') = ?
    """, [year_month]).fetchone()[0]

    if readings_count == 0:
        print(f"No readings to archive for {year_month}.")
        return True

    os.makedirs(archive_dir, exist_ok=True)
    parquet_path = os.path.join(archive_dir, f"readings_{year_month}.parquet")

    con.execute(f"""
        COPY (
            SELECT * FROM readings
            WHERE strftime(recorded_at, '%Y-%m') = '{year_month}'
            ORDER BY utility_type, given_flat_id, recorded_at
        ) TO '{parquet_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    verify_count = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{parquet_path}')"
    ).fetchone()[0]

    if verify_count != readings_count:
        print(f"ERROR: Parquet has {verify_count} rows but DB has {readings_count}. Aborting purge.")
        return False

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
    Detect potential discrepancies in granular readings for a given month:
    - Backward readings (meter going backwards)
    - Zero consumption (no change all month)
    - Gap detection (missing 15-min intervals)
    - Spike detection (abnormal consumption in a single interval)
    - Stale-then-resume (flat for extended period, then resumes)
    - NULL reading values (missing data points)
    - Out-of-order timestamps (non-monotonic within a source file)
    """
    # Scan metadata
    scan_stats = con.execute("""
        SELECT COUNT(*) AS total_records,
               COUNT(DISTINCT given_flat_id) AS distinct_flats,
               COUNT(DISTINCT utility_type) AS distinct_utilities,
               MIN(recorded_at) AS earliest,
               MAX(recorded_at) AS latest
        FROM readings
        WHERE strftime(recorded_at, '%Y-%m') = ?
    """, [year_month]).fetchone()

    check_names = [
        "Backward readings",
        "Zero consumption",
        "Gap detection (missing intervals)",
        "Spike detection (>3σ)",
        "Stale-then-resume (>24h flat)",
        "NULL readings",
        "Out-of-order timestamps",
    ]

    print(f"Running granular discrepancy checks for {year_month}...")
    print(f"  Scanning: {scan_stats[0]:,} records | {scan_stats[1]:,} flats | "
          f"{scan_stats[2]} utility types | {scan_stats[3]} to {scan_stats[4]}")
    print(f"  Checks:  {', '.join(check_names)}")
    print(f"  {'─' * 72}")
    results = {}

    # 1. Backward readings (meter going backwards)
    backward = con.execute("""
        WITH lagged AS (
            SELECT
                utility_type, given_flat_id, recorded_at, reading_value,
                LAG(reading_value) OVER (
                    PARTITION BY utility_type, given_flat_id
                    ORDER BY recorded_at
                ) AS prev_value
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
    results["backward"] = backward

    if not backward.empty:
        print(f"\n  1. BACKWARD READINGS ({len(backward)} instances):")
        print(backward.head(20).to_string(index=False))
    else:
        print("\n  1. BACKWARD READINGS: None detected.")

    # 2. Zero consumption (no change all month)
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
    results["zero_consumption"] = zero_consumption

    if not zero_consumption.empty:
        print(f"\n  2. ZERO CONSUMPTION ({len(zero_consumption)} flat/utility combos):")
        print(zero_consumption.head(20).to_string(index=False))
    else:
        print("\n  2. ZERO CONSUMPTION: None detected.")

    # 3. Gap detection — flats with significantly fewer readings than expected
    # Expected: ~96/day. Flag if a flat has <80% of the median reading count.
    gaps = con.execute("""
        WITH counts AS (
            SELECT
                utility_type, given_flat_id,
                CAST(recorded_at AS DATE) AS reading_date,
                COUNT(*) AS readings_per_day
            FROM readings
            WHERE strftime(recorded_at, '%Y-%m') = ?
            GROUP BY utility_type, given_flat_id, CAST(recorded_at AS DATE)
        ),
        daily_median AS (
            SELECT
                reading_date,
                MEDIAN(readings_per_day) AS median_count
            FROM counts
            GROUP BY reading_date
        )
        SELECT c.utility_type, c.given_flat_id, c.reading_date,
               c.readings_per_day, d.median_count,
               ROUND(100.0 * c.readings_per_day / d.median_count, 1) AS pct_of_median
        FROM counts c
        JOIN daily_median d ON c.reading_date = d.reading_date
        WHERE c.readings_per_day < 0.8 * d.median_count
          AND d.median_count > 0
        ORDER BY c.reading_date, c.utility_type, c.given_flat_id
    """, [year_month]).fetchdf()
    results["gaps"] = gaps

    if not gaps.empty:
        print(f"\n  3. GAP DETECTION — missing intervals ({len(gaps)} flat/day combos below 80% of median):")
        print(gaps.head(20).to_string(index=False))
        if len(gaps) > 20:
            print(f"     ... and {len(gaps) - 20} more.")
    else:
        print("\n  3. GAP DETECTION: All flats have expected reading counts.")

    # 4. Spike detection — single-interval consumption > 3 std dev from the flat's mean
    spikes = con.execute("""
        WITH deltas AS (
            SELECT
                utility_type, given_flat_id, recorded_at, reading_value,
                reading_value - LAG(reading_value) OVER (
                    PARTITION BY utility_type, given_flat_id
                    ORDER BY recorded_at
                ) AS delta
            FROM readings
            WHERE strftime(recorded_at, '%Y-%m') = ?
              AND reading_value IS NOT NULL
        ),
        stats AS (
            SELECT
                utility_type, given_flat_id,
                AVG(delta) AS mean_delta,
                STDDEV(delta) AS std_delta
            FROM deltas
            WHERE delta IS NOT NULL AND delta >= 0
            GROUP BY utility_type, given_flat_id
            HAVING STDDEV(delta) > 0
        )
        SELECT d.utility_type, d.given_flat_id, d.recorded_at,
               ROUND(d.delta, 4) AS delta,
               ROUND(s.mean_delta, 4) AS mean_delta,
               ROUND(s.std_delta, 4) AS std_delta,
               ROUND((d.delta - s.mean_delta) / s.std_delta, 2) AS z_score
        FROM deltas d
        JOIN stats s
          ON d.utility_type = s.utility_type
         AND d.given_flat_id = s.given_flat_id
        WHERE d.delta IS NOT NULL
          AND d.delta > 0
          AND (d.delta - s.mean_delta) / s.std_delta > 3
        ORDER BY (d.delta - s.mean_delta) / s.std_delta DESC
    """, [year_month]).fetchdf()
    results["spikes"] = spikes

    if not spikes.empty:
        print(f"\n  4. SPIKE DETECTION ({len(spikes)} readings >3 std dev above mean):")
        print(spikes.head(20).to_string(index=False))
        if len(spikes) > 20:
            print(f"     ... and {len(spikes) - 20} more.")
    else:
        print("\n  4. SPIKE DETECTION: No abnormal spikes detected.")

    # 5. Stale-then-resume — reading unchanged for 24+ hours then changes
    stale = con.execute("""
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
        ),
        runs AS (
            SELECT *,
                CASE WHEN reading_value = prev_value THEN 0 ELSE 1 END AS changed,
                recorded_at - prev_time AS time_gap
            FROM lagged
            WHERE prev_value IS NOT NULL
        ),
        stale_runs AS (
            SELECT
                utility_type, given_flat_id,
                MIN(prev_time) AS stale_from,
                MAX(recorded_at) AS stale_until,
                reading_value,
                COUNT(*) AS consecutive_same,
                MAX(recorded_at) - MIN(prev_time) AS stale_duration
            FROM (
                SELECT *,
                    SUM(changed) OVER (
                        PARTITION BY utility_type, given_flat_id
                        ORDER BY recorded_at
                    ) AS run_group
                FROM runs
            ) grouped
            WHERE changed = 0
            GROUP BY utility_type, given_flat_id, run_group, reading_value
            HAVING MAX(recorded_at) - MIN(prev_time) > INTERVAL 24 HOUR
        )
        SELECT utility_type, given_flat_id, stale_from, stale_until,
               reading_value, consecutive_same,
               stale_duration
        FROM stale_runs
        ORDER BY stale_duration DESC, utility_type, given_flat_id
    """, [year_month]).fetchdf()
    results["stale"] = stale

    if not stale.empty:
        print(f"\n  5. STALE-THEN-RESUME ({len(stale)} periods >24h with unchanged reading):")
        print(stale.head(20).to_string(index=False))
        if len(stale) > 20:
            print(f"     ... and {len(stale) - 20} more.")
    else:
        print("\n  5. STALE-THEN-RESUME: No extended stale periods detected.")

    # 6. NULL reading values — timestamps where a flat has NULL while others don't
    nulls = con.execute("""
        WITH null_readings AS (
            SELECT utility_type, given_flat_id, recorded_at, source_file
            FROM readings
            WHERE strftime(recorded_at, '%Y-%m') = ?
              AND reading_value IS NULL
        )
        SELECT utility_type, given_flat_id,
               COUNT(*) AS null_count,
               MIN(recorded_at) AS first_null,
               MAX(recorded_at) AS last_null
        FROM null_readings
        GROUP BY utility_type, given_flat_id
        ORDER BY null_count DESC, utility_type, given_flat_id
    """, [year_month]).fetchdf()
    results["nulls"] = nulls

    if not nulls.empty:
        print(f"\n  6. NULL READINGS ({len(nulls)} flat/utility combos with missing values):")
        print(nulls.head(20).to_string(index=False))
        if len(nulls) > 20:
            print(f"     ... and {len(nulls) - 20} more.")
    else:
        print("\n  6. NULL READINGS: No missing values detected.")

    # 7. Out-of-order timestamps — within a source file, timestamps should be monotonic
    out_of_order = con.execute("""
        WITH sequenced AS (
            SELECT
                utility_type, given_flat_id, recorded_at, source_file,
                LAG(recorded_at) OVER (
                    PARTITION BY source_file, given_flat_id
                    ORDER BY recorded_at
                ) AS prev_time
            FROM readings
            WHERE strftime(recorded_at, '%Y-%m') = ?
        )
        SELECT source_file, utility_type, given_flat_id,
               prev_time, recorded_at,
               recorded_at - prev_time AS time_gap
        FROM sequenced
        WHERE prev_time IS NOT NULL
          AND recorded_at < prev_time
        ORDER BY source_file, given_flat_id, recorded_at
    """, [year_month]).fetchdf()
    results["out_of_order"] = out_of_order

    if not out_of_order.empty:
        print(f"\n  7. OUT-OF-ORDER TIMESTAMPS ({len(out_of_order)} instances):")
        print(out_of_order.head(20).to_string(index=False))
    else:
        print("\n  7. OUT-OF-ORDER TIMESTAMPS: All timestamps monotonically increasing.")

    # Summary report
    total_issues = sum(len(v) for v in results.values())
    flagged = sum(1 for v in results.values() if len(v) > 0)
    print(f"\n  {'─' * 72}")
    print(f"  SCAN SUMMARY — {year_month}")
    print(f"  Records scanned : {scan_stats[0]:>10,}")
    print(f"  Flats scanned   : {scan_stats[1]:>10,}")
    print(f"  Total issues    : {total_issues:>10,}")
    print(f"  Categories hit  : {flagged}/{len(check_names)}")
    print()
    result_keys = ["backward", "zero_consumption", "gaps", "spikes", "stale", "nulls", "out_of_order"]
    for i, (name, key) in enumerate(zip(check_names, result_keys), 1):
        count = len(results.get(key, []))
        status = f"{count:,} found" if count > 0 else "PASS"
        marker = "!!" if count > 0 else "ok"
        print(f"    [{marker}] {i}. {name:<35s} {status}")
    print(f"  {'─' * 72}")
    return results


def _summary_where(year_month, base_condition=""):
    """Helper to build WHERE clause with optional year_month filter."""
    clauses = []
    params = []
    if year_month:
        clauses.append("year_month = ?")
        params.append(year_month)
    if base_condition:
        clauses.append(base_condition)
    where = "WHERE " + " AND ".join(clauses) if clauses else ""
    return where, params


def detect_summary_discrepancies(con, year_month=None):
    """
    Detect anomalies in the monthly_summary table:
    - Negative consumption
    - Zero consumption
    - Late opening reading (after day 2)
    - Missing months (gaps in coverage)
    - Month continuity (prev closing != current opening)
    - Outlier consumption (>3 std dev from flat's historical avg)
    - Early closing timestamp (before day 28)
    - Cross-utility correlation (electricity zero but water high, etc.)
    """
    scope = f"for {year_month}" if year_month else "across all months"

    # Scan metadata
    where_count, params_count = _summary_where(year_month)
    scan_stats = con.execute(f"""
        SELECT COUNT(*) AS total_rows,
               COUNT(DISTINCT given_flat_id) AS distinct_flats,
               COUNT(DISTINCT utility_type) AS distinct_utilities,
               COUNT(DISTINCT year_month) AS distinct_months
        FROM monthly_summary {where_count}
    """, params_count).fetchone()

    check_names = [
        "Negative consumption",
        "Zero consumption",
        "Late opening reading (day > 2)",
        "Early closing timestamp (day < 28)",
        "Month continuity breaks",
        "Outlier consumption (>3σ)",
        "Cross-utility mismatch",
        "Missing months",
    ]

    print(f"Running monthly summary discrepancy checks {scope}...")
    print(f"  Scanning: {scan_stats[0]:,} summary rows | {scan_stats[1]:,} flats | "
          f"{scan_stats[2]} utility types | {scan_stats[3]} months")
    print(f"  Checks:  {', '.join(check_names)}")
    print(f"  {'─' * 72}")
    results = {}

    # 1. Negative consumption
    where, params = _summary_where(year_month, "(closing_reading - opening_reading) < 0")
    neg = con.execute(f"""
        SELECT utility_type, given_flat_id, year_month,
               opening_reading, closing_reading,
               closing_reading - opening_reading AS consumption
        FROM monthly_summary {where}
        ORDER BY consumption ASC
    """, params).fetchdf()
    results["negative"] = neg

    if not neg.empty:
        print(f"\n  1. NEGATIVE CONSUMPTION ({len(neg)} rows):")
        print(neg.head(20).to_string(index=False))
    else:
        print("\n  1. NEGATIVE CONSUMPTION: None.")

    # 2. Zero consumption
    where, params = _summary_where(year_month, "closing_reading = opening_reading")
    zero = con.execute(f"""
        SELECT utility_type, given_flat_id, year_month,
               opening_reading, closing_reading
        FROM monthly_summary {where}
        ORDER BY utility_type, given_flat_id
    """, params).fetchdf()
    results["zero"] = zero

    if not zero.empty:
        print(f"\n  2. ZERO CONSUMPTION ({len(zero)} rows):")
        print(zero.head(20).to_string(index=False))
    else:
        print("\n  2. ZERO CONSUMPTION: None.")

    # 3. Late opening reading (after day 2)
    where, params = _summary_where(year_month, "CAST(strftime(opening_timestamp, '%d') AS INT) > 2")
    late_start = con.execute(f"""
        SELECT utility_type, given_flat_id, year_month,
               opening_timestamp,
               CAST(strftime(opening_timestamp, '%d') AS INT) AS opening_day
        FROM monthly_summary {where}
        ORDER BY opening_day DESC, utility_type, given_flat_id
    """, params).fetchdf()
    results["late_start"] = late_start

    if not late_start.empty:
        print(f"\n  3. LATE OPENING READING — day > 2 ({len(late_start)} rows):")
        print(late_start.head(20).to_string(index=False))
    else:
        print("\n  3. LATE OPENING READING: All within first 2 days.")

    # 4. Early closing timestamp (before day 28)
    where, params = _summary_where(year_month, "CAST(strftime(closing_timestamp, '%d') AS INT) < 28")
    early_close = con.execute(f"""
        SELECT utility_type, given_flat_id, year_month,
               closing_timestamp,
               CAST(strftime(closing_timestamp, '%d') AS INT) AS closing_day
        FROM monthly_summary {where}
        ORDER BY closing_day ASC, utility_type, given_flat_id
    """, params).fetchdf()
    results["early_close"] = early_close

    if not early_close.empty:
        print(f"\n  4. EARLY CLOSING TIMESTAMP — day < 28 ({len(early_close)} rows):")
        print(early_close.head(20).to_string(index=False))
        if len(early_close) > 20:
            print(f"     ... and {len(early_close) - 20} more.")
    else:
        print("\n  4. EARLY CLOSING TIMESTAMP: All closing readings at or after day 28.")

    # 5. Month continuity — previous month's closing should equal current month's opening
    if year_month:
        # Derive previous month
        y, m = map(int, year_month.split("-"))
        if m == 1:
            prev_month = f"{y - 1:04d}-12"
        else:
            prev_month = f"{y:04d}-{m - 1:02d}"
        continuity = con.execute("""
            SELECT
                c.utility_type, c.given_flat_id, c.year_month,
                p.closing_reading AS prev_closing,
                c.opening_reading AS curr_opening,
                c.opening_reading - p.closing_reading AS gap
            FROM monthly_summary c
            JOIN monthly_summary p
              ON c.utility_type = p.utility_type
             AND c.given_flat_id = p.given_flat_id
             AND p.year_month = ?
            WHERE c.year_month = ?
              AND ABS(c.opening_reading - p.closing_reading) > 0.001
            ORDER BY ABS(c.opening_reading - p.closing_reading) DESC
        """, [prev_month, year_month]).fetchdf()
    else:
        continuity = con.execute("""
            WITH ordered AS (
                SELECT *,
                    LAG(closing_reading) OVER (
                        PARTITION BY utility_type, given_flat_id
                        ORDER BY year_month
                    ) AS prev_closing,
                    LAG(year_month) OVER (
                        PARTITION BY utility_type, given_flat_id
                        ORDER BY year_month
                    ) AS prev_month
                FROM monthly_summary
            )
            SELECT utility_type, given_flat_id, year_month,
                   prev_month, prev_closing, opening_reading AS curr_opening,
                   opening_reading - prev_closing AS gap
            FROM ordered
            WHERE prev_closing IS NOT NULL
              AND ABS(opening_reading - prev_closing) > 0.001
            ORDER BY ABS(opening_reading - prev_closing) DESC
        """).fetchdf()
    results["continuity"] = continuity

    if not continuity.empty:
        print(f"\n  5. MONTH CONTINUITY BREAKS ({len(continuity)} — prev closing != curr opening):")
        print(continuity.head(20).to_string(index=False))
        if len(continuity) > 20:
            print(f"     ... and {len(continuity) - 20} more.")
    else:
        print("\n  5. MONTH CONTINUITY: All opening readings match previous closing.")

    # 6. Outlier consumption — >3 std dev from the flat's own historical average
    # Only meaningful with multiple months of data
    if year_month:
        outliers = con.execute("""
            WITH consumption AS (
                SELECT utility_type, given_flat_id, year_month,
                       closing_reading - opening_reading AS consumption
                FROM monthly_summary
            ),
            stats AS (
                SELECT utility_type, given_flat_id,
                       AVG(consumption) AS mean_consumption,
                       STDDEV(consumption) AS std_consumption,
                       COUNT(*) AS num_months
                FROM consumption
                GROUP BY utility_type, given_flat_id
                HAVING COUNT(*) >= 3 AND STDDEV(consumption) > 0
            )
            SELECT c.utility_type, c.given_flat_id, c.year_month,
                   ROUND(c.consumption, 4) AS consumption,
                   ROUND(s.mean_consumption, 4) AS historical_mean,
                   ROUND(s.std_consumption, 4) AS historical_std,
                   ROUND((c.consumption - s.mean_consumption) / s.std_consumption, 2) AS z_score
            FROM consumption c
            JOIN stats s
              ON c.utility_type = s.utility_type
             AND c.given_flat_id = s.given_flat_id
            WHERE c.year_month = ?
              AND ABS(c.consumption - s.mean_consumption) / s.std_consumption > 3
            ORDER BY ABS((c.consumption - s.mean_consumption) / s.std_consumption) DESC
        """, [year_month]).fetchdf()
    else:
        outliers = con.execute("""
            WITH consumption AS (
                SELECT utility_type, given_flat_id, year_month,
                       closing_reading - opening_reading AS consumption
                FROM monthly_summary
            ),
            stats AS (
                SELECT utility_type, given_flat_id,
                       AVG(consumption) AS mean_consumption,
                       STDDEV(consumption) AS std_consumption,
                       COUNT(*) AS num_months
                FROM consumption
                GROUP BY utility_type, given_flat_id
                HAVING COUNT(*) >= 3 AND STDDEV(consumption) > 0
            )
            SELECT c.utility_type, c.given_flat_id, c.year_month,
                   ROUND(c.consumption, 4) AS consumption,
                   ROUND(s.mean_consumption, 4) AS historical_mean,
                   ROUND(s.std_consumption, 4) AS historical_std,
                   ROUND((c.consumption - s.mean_consumption) / s.std_consumption, 2) AS z_score
            FROM consumption c
            JOIN stats s
              ON c.utility_type = s.utility_type
             AND c.given_flat_id = s.given_flat_id
            WHERE ABS(c.consumption - s.mean_consumption) / s.std_consumption > 3
            ORDER BY ABS((c.consumption - s.mean_consumption) / s.std_consumption) DESC
        """).fetchdf()
    results["outliers"] = outliers

    if not outliers.empty:
        print(f"\n  6. OUTLIER CONSUMPTION ({len(outliers)} — >3 std dev from historical avg):")
        print(outliers.head(20).to_string(index=False))
        if len(outliers) > 20:
            print(f"     ... and {len(outliers) - 20} more.")
    else:
        print("\n  6. OUTLIER CONSUMPTION: None detected (requires >=3 months of history).")

    # 7. Cross-utility correlation — zero consumption in one utility but not others
    where, params = _summary_where(year_month)
    cross_utility = con.execute(f"""
        WITH consumption AS (
            SELECT utility_type, given_flat_id, year_month,
                   closing_reading - opening_reading AS consumption
            FROM monthly_summary {where}
        ),
        flat_month AS (
            SELECT given_flat_id, year_month,
                   COUNT(*) AS num_utilities,
                   COUNT(*) FILTER (WHERE consumption = 0) AS zero_count,
                   COUNT(*) FILTER (WHERE consumption > 0) AS active_count,
                   LIST(utility_type) FILTER (WHERE consumption = 0) AS zero_utilities,
                   LIST(utility_type) FILTER (WHERE consumption > 0) AS active_utilities
            FROM consumption
            GROUP BY given_flat_id, year_month
            HAVING COUNT(*) > 1
        )
        SELECT given_flat_id, year_month,
               zero_utilities, active_utilities,
               zero_count, active_count
        FROM flat_month
        WHERE zero_count > 0 AND active_count > 0
        ORDER BY given_flat_id, year_month
    """, params).fetchdf()
    results["cross_utility"] = cross_utility

    if not cross_utility.empty:
        print(f"\n  7. CROSS-UTILITY MISMATCH ({len(cross_utility)} — zero in some utilities but active in others):")
        print(cross_utility.head(20).to_string(index=False))
        if len(cross_utility) > 20:
            print(f"     ... and {len(cross_utility) - 20} more.")
    else:
        print("\n  7. CROSS-UTILITY MISMATCH: None detected.")

    # 8. Missing months (gap detection across expected flat/utility/month combos)
    if not year_month:
        missing = con.execute("""
            WITH combos AS (
                SELECT DISTINCT given_flat_id, utility_type FROM monthly_summary
            ),
            months AS (
                SELECT DISTINCT year_month FROM monthly_summary
            )
            SELECT c.given_flat_id, c.utility_type, m.year_month
            FROM combos c
            CROSS JOIN months m
            LEFT JOIN monthly_summary s
                ON c.given_flat_id = s.given_flat_id
                AND c.utility_type = s.utility_type
                AND m.year_month = s.year_month
            WHERE s.given_flat_id IS NULL
            ORDER BY c.given_flat_id, c.utility_type, m.year_month
        """).fetchdf()
        results["missing_months"] = missing

        if not missing.empty:
            print(f"\n  8. MISSING MONTHS ({len(missing)} missing flat/utility/month entries):")
            print(missing.head(20).to_string(index=False))
            if len(missing) > 20:
                print(f"     ... and {len(missing) - 20} more.")
        else:
            print("\n  8. MISSING MONTHS: Full coverage across all flat/utility/month combos.")

    # Summary report
    total_issues = sum(len(v) for v in results.values())
    flagged = sum(1 for v in results.values() if len(v) > 0)
    total_checks = len(results)
    result_keys = ["negative", "zero", "late_start", "early_close",
                   "continuity", "outliers", "cross_utility", "missing_months"]
    print(f"\n  {'─' * 72}")
    print(f"  SCAN SUMMARY — monthly summaries {scope}")
    print(f"  Rows scanned    : {scan_stats[0]:>10,}")
    print(f"  Flats scanned   : {scan_stats[1]:>10,}")
    print(f"  Months covered  : {scan_stats[3]:>10,}")
    print(f"  Total issues    : {total_issues:>10,}")
    print(f"  Categories hit  : {flagged}/{total_checks}")
    print()
    for i, (name, key) in enumerate(zip(check_names, result_keys), 1):
        count = len(results.get(key, []))
        status = f"{count:,} found" if count > 0 else "PASS" if key in results else "SKIPPED"
        marker = "!!" if count > 0 else "ok" if key in results else "--"
        print(f"    [{marker}] {i}. {name:<35s} {status}")
    print(f"  {'─' * 72}")
    return results


def detect_ingestion_issues(con, year_month, expected_files_per_day=14):
    """
    Detect operational issues with data ingestion for a given month:
    - File completeness: expected N files/day but fewer arrived
    - Row count anomalies: files with significantly fewer rows than peers
    """
    # Scan metadata
    scan_stats = con.execute("""
        SELECT COUNT(*) AS total_records,
               COUNT(DISTINCT source_file) AS distinct_files,
               COUNT(DISTINCT CAST(recorded_at AS DATE)) AS distinct_days
        FROM readings
        WHERE strftime(recorded_at, '%Y-%m') = ?
    """, [year_month]).fetchone()

    check_names = [
        "File completeness (files/day)",
        "Row count anomalies (<50% median)",
        "Ingestion log",
    ]

    print(f"Running ingestion checks for {year_month} (expecting {expected_files_per_day} files/day)...")
    print(f"  Scanning: {scan_stats[0]:,} records | {scan_stats[1]:,} source files | "
          f"{scan_stats[2]} days with data")
    print(f"  Checks:  {', '.join(check_names)}")
    print(f"  {'─' * 72}")
    results = {}

    # 1. File completeness — how many distinct source files contributed to each day?
    file_completeness = con.execute("""
        WITH daily_files AS (
            SELECT
                CAST(recorded_at AS DATE) AS reading_date,
                COUNT(DISTINCT source_file) AS files_received
            FROM readings
            WHERE strftime(recorded_at, '%Y-%m') = ?
            GROUP BY CAST(recorded_at AS DATE)
        )
        SELECT reading_date, files_received, ? AS expected_files,
               ? - files_received AS missing_files
        FROM daily_files
        WHERE files_received < ?
        ORDER BY reading_date
    """, [year_month, expected_files_per_day, expected_files_per_day, expected_files_per_day]).fetchdf()
    results["file_completeness"] = file_completeness

    if not file_completeness.empty:
        print(f"\n  1. INCOMPLETE DAYS ({len(file_completeness)} days with fewer than {expected_files_per_day} files):")
        print(file_completeness.to_string(index=False))
    else:
        print(f"\n  1. FILE COMPLETENESS: All days have {expected_files_per_day}+ source files.")

    # 2. Row count anomalies — files with significantly fewer rows than the median
    row_anomalies = con.execute("""
        WITH file_stats AS (
            SELECT source_file,
                   COUNT(*) AS row_count,
                   MIN(recorded_at) AS first_reading,
                   MAX(recorded_at) AS last_reading,
                   COUNT(DISTINCT given_flat_id) AS num_flats
            FROM readings
            WHERE strftime(recorded_at, '%Y-%m') = ?
            GROUP BY source_file
        ),
        overall AS (
            SELECT MEDIAN(row_count) AS median_rows
            FROM file_stats
        )
        SELECT f.source_file, f.row_count, o.median_rows,
               ROUND(100.0 * f.row_count / o.median_rows, 1) AS pct_of_median,
               f.num_flats, f.first_reading, f.last_reading
        FROM file_stats f, overall o
        WHERE f.row_count < 0.5 * o.median_rows
          AND o.median_rows > 0
        ORDER BY f.row_count ASC
    """, [year_month]).fetchdf()
    results["row_anomalies"] = row_anomalies

    if not row_anomalies.empty:
        print(f"\n  2. ROW COUNT ANOMALIES ({len(row_anomalies)} files with <50% of median row count):")
        print(row_anomalies.to_string(index=False))
    else:
        print("\n  2. ROW COUNT ANOMALIES: All files have consistent row counts.")

    # 3. Show ingestion log summary for the period
    log_summary = con.execute("""
        SELECT source_file, file_hash, ingested_at, record_count
        FROM ingestion_log
        ORDER BY ingested_at DESC
    """).fetchdf()
    results["ingestion_log"] = log_summary

    if not log_summary.empty:
        print(f"\n  3. INGESTION LOG ({len(log_summary)} files total):")
        print(log_summary.to_string(index=False))
    else:
        print("\n  3. INGESTION LOG: No files ingested yet.")

    total_issues = len(file_completeness) + len(row_anomalies)
    flagged = (1 if len(file_completeness) > 0 else 0) + (1 if len(row_anomalies) > 0 else 0)
    result_keys = ["file_completeness", "row_anomalies", "ingestion_log"]
    print(f"\n  {'─' * 72}")
    print(f"  SCAN SUMMARY — ingestion checks {year_month}")
    print(f"  Records scanned : {scan_stats[0]:>10,}")
    print(f"  Source files    : {scan_stats[1]:>10,}")
    print(f"  Days with data  : {scan_stats[2]:>10,}")
    print(f"  Total issues    : {total_issues:>10,}")
    print(f"  Categories hit  : {flagged}/2")
    print()
    for i, (name, key) in enumerate(zip(check_names, result_keys), 1):
        count = len(results.get(key, []))
        if key == "ingestion_log":
            print(f"    [--] {i}. {name:<35s} {count:,} files logged")
        else:
            status = f"{count:,} found" if count > 0 else "PASS"
            marker = "!!" if count > 0 else "ok"
            print(f"    [{marker}] {i}. {name:<35s} {status}")
    print(f"  {'─' * 72}")
    return results


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

  # Run discrepancy detection on granular data for a month
  python utility_db.py check 2025-01

  # Run discrepancy detection on monthly summaries
  python utility_db.py check-summary [2025-01]

  # Run ingestion completeness checks
  python utility_db.py check-ingestion 2025-01 --expected-files 14

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

    p_check = sub.add_parser("check", help="Detect discrepancies in granular data")
    p_check.add_argument("year_month", help="Month to check (YYYY-MM)")

    p_check_summary = sub.add_parser("check-summary", help="Detect anomalies in monthly summaries")
    p_check_summary.add_argument("year_month", nargs="?", help="Limit to a specific month (YYYY-MM) – optional")

    p_check_ingest = sub.add_parser("check-ingestion", help="Check ingestion completeness and row counts")
    p_check_ingest.add_argument("year_month", help="Month to check (YYYY-MM)")
    p_check_ingest.add_argument("--expected-files", type=int, default=14, help="Expected files per day (default: 14)")

    sub.add_parser("stats", help="Show database statistics")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    # Normalize year_month: accept 2026_08, 2026/08, 2026.08 → 2026-08
    if hasattr(args, "year_month") and args.year_month:
        import re
        normalized = re.sub(r'[_/.]', '-', args.year_month)
        if not re.match(r'^\d{4}-\d{2}$', normalized):
            print(f"ERROR: Invalid month format '{args.year_month}'. Expected YYYY-MM (e.g. 2026-08).")
            sys.exit(1)
        if normalized != args.year_month:
            print(f"  (normalized '{args.year_month}' → '{normalized}')")
        args.year_month = normalized

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
    elif args.command == "check-summary":
        detect_summary_discrepancies(con, args.year_month)
    elif args.command == "check-ingestion":
        detect_ingestion_issues(con, args.year_month, args.expected_files)
    elif args.command == "stats":
        db_stats(con)

    con.close()


if __name__ == "__main__":
    main()