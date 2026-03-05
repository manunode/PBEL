"""
Utility DB — Summarization & Queries
======================================
Monthly summary computation, archival to Parquet, flat queries, and DB stats.
"""

import os

from utility_db_core import DEFAULT_ARCHIVE_DIR


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
               opening_reading, closing_reading, monthly_reading,
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


# ── Database Stats ─────────────────────────────────────────────────────────

def db_stats(con):
    """Print database statistics."""
    readings_count = con.execute("SELECT COUNT(*) FROM readings").fetchone()[0]
    summary_count = con.execute("SELECT COUNT(*) FROM monthly_summary").fetchone()[0]
    discrepancy_count = con.execute("SELECT COUNT(*) FROM monthly_discrepancy").fetchone()[0]
    ingestion_count = con.execute("SELECT COUNT(*) FROM ingestion_log").fetchone()[0]

    print("Database statistics:")
    print(f"  Readings:        {readings_count:>10,}")
    print(f"  Monthly summary: {summary_count:>10,}")
    print(f"  Discrepancies:   {discrepancy_count:>10,}")
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
