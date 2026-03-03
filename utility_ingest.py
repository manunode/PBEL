"""
Utility DB — Ingestion
=======================
Excel file parsing, ingestion into DuckDB, and Parquet reload.
"""

import os
import glob
import datetime
import hashlib

import pandas as pd

from utility_db_core import TOWER_MAP, UTILITY_MAP


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
      - If the resulting string starts with the tower_letter (case-insensitive),
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
