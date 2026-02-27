#!/usr/bin/env python3
"""
Utility Records Processor
=========================
Processes Excel utility meter reading files (Electricity, Diesel, Water, Gas)
and consolidates them into daily starting/closing readings per flat.

Input:  Excel files named like "T <tower_letter> <utility> readings.xls"
Output: CSV file with columns: Type_of_utility, given_flat_id, date, starting_reading, closing_reading
"""

import os
import sys
import glob
import datetime
import pandas as pd


# ----- Configuration / Mappings -----

TOWER_MAP = {
    "A": "Platinum",
    "B": "Titanium",
    "C": "Aurum",
    "D": "Argentum",
    "E": "Pearl",
    "F": "Crystal",
    "G": "Jade",
    "H": "Turquoise",
    "J": "Amethyst",
    "K": "Aquamarine",
    "L": "Opal",
    "M": "Sapphire",
    "N": "Ruby",
    "P": "Lakeside",
}

UTILITY_MAP = {
    "Eb": "Electricity",
    "Dg": "Diesel",
    "Water": "Water",
    "Gas": "Gas",
}


def parse_filename(filename):
    """
    Parse a filename like 'T C Eb readings.xls' to extract tower letter and utility type.

    Returns:
        tuple: (tower_name, utility_type) e.g. ('Aurum', 'Electricity')

    Raises:
        ValueError: If filename cannot be parsed or mappings are unknown.
    """
    base = os.path.basename(filename)
    # Remove the ' readings.xls' suffix (case-insensitive)
    name_part = base.split(" readings")[0].strip()
    # Expected format: "T <tower_letter> <utility_keyword>"
    parts = name_part.split()
    if len(parts) < 3 or parts[0] != "T":
        raise ValueError(f"Unexpected filename format: '{base}'. Expected 'T <letter> <utility> readings.xls'")

    tower_letter = parts[1].upper()
    utility_keyword = parts[2]  # preserve original case for matching

    tower_name = TOWER_MAP.get(tower_letter)
    if tower_name is None:
        raise ValueError(f"Unknown tower letter '{tower_letter}' in filename '{base}'")

    # Match utility keyword case-insensitively
    utility_type = None
    for key, value in UTILITY_MAP.items():
        if key.lower() == utility_keyword.lower():
            utility_type = value
            break
    if utility_type is None:
        raise ValueError(f"Unknown utility keyword '{utility_keyword}' in filename '{base}'")

    return tower_name, utility_type


def normalize_flat_id(raw_value):
    """
    Normalize a flat ID from the Excel header row.
    - String values: strip whitespace (e.g. ' G001' -> 'G001')
    - Numeric values: convert to integer string (e.g. 101.0 -> '101')
    """
    if isinstance(raw_value, str):
        return raw_value.strip()
    elif isinstance(raw_value, (int, float)):
        return str(int(raw_value))
    else:
        return str(raw_value).strip()


def process_file(filepath):
    """
    Process a single utility Excel file and return consolidated daily records.

    Returns:
        list of dict: Each dict has keys:
            Type_of_utility, given_flat_id, date, starting_reading, closing_reading
    """
    tower_name, utility_type = parse_filename(filepath)
    print(f"Processing: {os.path.basename(filepath)}")
    print(f"  Tower: {tower_name}, Utility: {utility_type}")

    # Read the first sheet with no header (raw access)
    df = pd.read_excel(filepath, sheet_name=0, header=None, engine="openpyxl")

    # A1: parameter text
    parameter_name = str(df.iloc[0, 0]).strip() if pd.notna(df.iloc[0, 0]) else ""
    print(f"  Parameter: {parameter_name}")

    # Row 2 (index 1): headers — A2 = 'Recorded Date/Time', B2 onward = flat IDs
    flat_ids = []
    for col_idx in range(1, df.shape[1]):
        raw = df.iloc[1, col_idx]
        if pd.notna(raw):
            flat_id = normalize_flat_id(raw)
            given_flat_id = tower_name + flat_id
            flat_ids.append((col_idx, given_flat_id))

    print(f"  Flats found: {len(flat_ids)}")

    # Row 3 (index 2): empty — skip
    # Rows 4+ (index 3+): data rows

    # Collect valid (non-duplicate) data rows
    # A row is duplicate if its datetime in column A equals the previous row's datetime
    valid_rows = []  # list of (datetime_val, row_index)
    prev_dt = None

    for row_idx in range(3, df.shape[0]):
        raw_dt = df.iloc[row_idx, 0]

        # Parse datetime
        if isinstance(raw_dt, datetime.datetime):
            dt_val = raw_dt
        elif isinstance(raw_dt, str):
            trimmed = raw_dt.strip()
            if not trimmed:
                continue
            try:
                dt_val = datetime.datetime.strptime(trimmed, "%d-%m-%Y %H:%M")
            except ValueError:
                try:
                    dt_val = datetime.datetime.strptime(trimmed, "%d-%m-%Y %H:%M:%S")
                except ValueError:
                    print(f"  WARNING: Could not parse datetime at row {row_idx + 1}: '{raw_dt}'")
                    continue
        elif pd.isna(raw_dt):
            continue
        else:
            print(f"  WARNING: Unexpected datetime type at row {row_idx + 1}: {type(raw_dt)} = {raw_dt}")
            continue

        # Duplicate check: skip if same datetime as previous row
        if prev_dt is not None and dt_val == prev_dt:
            continue

        prev_dt = dt_val
        valid_rows.append((dt_val, row_idx))

    print(f"  Valid data rows: {len(valid_rows)} (out of {df.shape[0] - 3} total)")

    # Group valid rows by date
    from collections import OrderedDict
    daily_groups = OrderedDict()  # date -> list of (datetime_val, row_idx)
    for dt_val, row_idx in valid_rows:
        day = dt_val.date()
        if day not in daily_groups:
            daily_groups[day] = []
        daily_groups[day].append((dt_val, row_idx))

    print(f"  Days found: {len(daily_groups)}")

    # Build consolidated records
    records = []
    for day, rows_in_day in daily_groups.items():
        first_row_idx = rows_in_day[0][1]   # first valid reading of the day
        last_row_idx = rows_in_day[-1][1]    # last valid reading of the day
        date_str = day.strftime("%d-%m-%Y")

        for col_idx, given_flat_id in flat_ids:
            starting_reading = df.iloc[first_row_idx, col_idx]
            closing_reading = df.iloc[last_row_idx, col_idx]

            # Convert readings to appropriate format
            if pd.notna(starting_reading) and pd.notna(closing_reading):
                records.append({
                    "Type_of_utility": utility_type,
                    "given_flat_id": given_flat_id,
                    "date": date_str,
                    "starting_reading": starting_reading,
                    "closing_reading": closing_reading,
                })

    print(f"  Records generated: {len(records)}")
    return records


def discover_files(directory):
    """
    Discover all utility reading Excel files in the given directory.
    Matches pattern: T * * readings.xls (case-insensitive for 'readings').
    """
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


def main():
    # Determine input directory (default: directory of this script)
    if len(sys.argv) > 1:
        input_dir = sys.argv[1]
    else:
        input_dir = os.path.dirname(os.path.abspath(__file__))

    print(f"Input directory: {input_dir}")
    print("=" * 60)

    files = discover_files(input_dir)
    if not files:
        print("No utility reading files found.")
        sys.exit(1)

    print(f"Found {len(files)} file(s):")
    for f in files:
        print(f"  - {os.path.basename(f)}")
    print("=" * 60)

    all_records = []
    for filepath in files:
        try:
            records = process_file(filepath)
            all_records.extend(records)
        except Exception as e:
            print(f"ERROR processing {os.path.basename(filepath)}: {e}")
        print()

    if not all_records:
        print("No records generated.")
        sys.exit(1)

    # Write output CSV
    output_path = os.path.join(input_dir, "utility_records_output.csv")
    output_df = pd.DataFrame(all_records)
    output_df.to_csv(output_path, index=False)

    print("=" * 60)
    print(f"Total records: {len(all_records)}")
    print(f"Output written to: {output_path}")
    print("\nSample output (first 10 rows):")
    print(output_df.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
