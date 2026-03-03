"""
Utility DB — Discrepancy & Quality Checks
===========================================
All check commands: granular discrepancies, summary anomalies,
cross-utility analysis, and ingestion completeness.
"""

from utility_db_core import month_filter_sql, period_label


# ── Helpers ────────────────────────────────────────────────────────────────

def _summary_where(start_month, end_month, base_condition=""):
    """Helper to build WHERE clause with optional period filter for monthly_summary."""
    clauses = []
    params = []
    if start_month:
        frag, p = month_filter_sql("year_month", start_month, end_month or start_month)
        clauses.append(frag)
        params.extend(p)
    if base_condition:
        clauses.append(base_condition)
    where = "WHERE " + " AND ".join(clauses) if clauses else ""
    return where, params


def _print_repeat_offenders(results, check_col_map, month_col_map, check_names):
    """Print flats that appear in multiple months across check categories."""
    result_keys = list(check_col_map.keys())
    any_repeats = False

    for key, name in zip(result_keys, check_names):
        df = results.get(key)
        if df is None or df.empty:
            continue

        flat_col = check_col_map[key]
        month_info = month_col_map.get(key)

        if flat_col not in df.columns:
            continue

        # For checks that already have a month column, count distinct months per flat
        if month_info and month_info in df.columns and month_info != "months_affected":
            # Derive month from date column if needed
            if df[month_info].dtype == 'object' or 'date' in month_info.lower():
                df = df.copy()
                df['_month'] = df[month_info].astype(str).str[:7]
            else:
                df['_month'] = df[month_info].astype(str).str[:7]

            repeat = df.groupby(flat_col).agg(
                times_flagged=('_month', 'count'),
                months_flagged=('_month', 'nunique'),
                month_list=('_month', lambda x: ', '.join(sorted(x.unique())))
            ).reset_index()
            repeat = repeat[repeat['months_flagged'] > 1].sort_values('months_flagged', ascending=False)
        elif month_info == "months_affected" and month_info in df.columns:
            # Already aggregated (e.g. nulls)
            repeat = df[df[month_info] > 1][[flat_col, month_info]].copy()
            repeat = repeat.rename(columns={month_info: 'months_flagged'})
            repeat['times_flagged'] = repeat['months_flagged']
            repeat = repeat.sort_values('months_flagged', ascending=False)
        else:
            continue

        if not repeat.empty:
            any_repeats = True
            print(f"\n    {name} — {len(repeat)} repeat offenders:")
            print(f"    {repeat.head(15).to_string(index=False)}")
            if len(repeat) > 15:
                print(f"    ... and {len(repeat) - 15} more.")

    if not any_repeats:
        print("\n    No repeat offenders found across months.")


# ── Granular Discrepancy Checks ───────────────────────────────────────────

def detect_discrepancies(con, start_month, end_month=None):
    """
    Detect potential discrepancies in granular readings for a period.
    Accepts single month or a range. When spanning multiple months,
    also produces a 'repeat offenders' summary.
    """
    if end_month is None:
        end_month = start_month
    label = period_label(start_month, end_month)
    multi = start_month != end_month
    mf_sql, mf_params = month_filter_sql("strftime(recorded_at, '%Y-%m')", start_month, end_month)

    # Scan metadata
    scan_stats = con.execute(f"""
        SELECT COUNT(*) AS total_records,
               COUNT(DISTINCT given_flat_id) AS distinct_flats,
               COUNT(DISTINCT utility_type) AS distinct_utilities,
               MIN(recorded_at) AS earliest,
               MAX(recorded_at) AS latest,
               COUNT(DISTINCT strftime(recorded_at, '%Y-%m')) AS months_covered
        FROM readings
        WHERE {mf_sql}
    """, mf_params).fetchone()

    check_names = [
        "Backward readings",
        "Zero consumption",
        "Gap detection (missing intervals)",
        "Spike detection (>3\u03c3)",
        "Stale-then-resume (>24h flat)",
        "NULL readings",
        "Out-of-order timestamps",
    ]

    print(f"Running granular discrepancy checks for {label}...")
    print(f"  Scanning: {scan_stats[0]:,} records | {scan_stats[1]:,} flats | "
          f"{scan_stats[2]} utility types | {scan_stats[5]} months | {scan_stats[3]} to {scan_stats[4]}")
    print(f"  Checks:  {', '.join(check_names)}")
    print(f"  {'─' * 72}")
    results = {}

    # 1. Backward readings
    backward = con.execute(f"""
        WITH lagged AS (
            SELECT
                utility_type, given_flat_id, recorded_at, reading_value,
                strftime(recorded_at, '%Y-%m') AS month,
                LAG(reading_value) OVER (
                    PARTITION BY utility_type, given_flat_id
                    ORDER BY recorded_at
                ) AS prev_value
            FROM readings
            WHERE {mf_sql}
              AND reading_value IS NOT NULL
        )
        SELECT utility_type, given_flat_id, month, recorded_at, reading_value,
               prev_value, reading_value - prev_value AS delta
        FROM lagged
        WHERE prev_value IS NOT NULL
          AND reading_value < prev_value
        ORDER BY utility_type, given_flat_id, recorded_at
    """, mf_params).fetchdf()
    results["backward"] = backward

    if not backward.empty:
        print(f"\n  1. BACKWARD READINGS ({len(backward)} instances):")
        print(backward.head(20).to_string(index=False))
        if len(backward) > 20:
            print(f"     ... and {len(backward) - 20} more.")
    else:
        print("\n  1. BACKWARD READINGS: None detected.")

    # 2. Zero consumption — per month per flat
    zero_consumption = con.execute(f"""
        WITH flat_range AS (
            SELECT
                utility_type, given_flat_id,
                strftime(recorded_at, '%Y-%m') AS month,
                MIN(reading_value) AS min_val,
                MAX(reading_value) AS max_val,
                COUNT(*) AS num_readings
            FROM readings
            WHERE {mf_sql}
              AND reading_value IS NOT NULL
            GROUP BY utility_type, given_flat_id, strftime(recorded_at, '%Y-%m')
        )
        SELECT * FROM flat_range
        WHERE min_val = max_val AND num_readings > 1
        ORDER BY month, utility_type, given_flat_id
    """, mf_params).fetchdf()
    results["zero_consumption"] = zero_consumption

    if not zero_consumption.empty:
        print(f"\n  2. ZERO CONSUMPTION ({len(zero_consumption)} flat/utility/month combos):")
        print(zero_consumption.head(20).to_string(index=False))
        if len(zero_consumption) > 20:
            print(f"     ... and {len(zero_consumption) - 20} more.")
    else:
        print("\n  2. ZERO CONSUMPTION: None detected.")

    # 3. Gap detection
    gaps = con.execute(f"""
        WITH counts AS (
            SELECT
                utility_type, given_flat_id,
                CAST(recorded_at AS DATE) AS reading_date,
                COUNT(*) AS readings_per_day
            FROM readings
            WHERE {mf_sql}
            GROUP BY utility_type, given_flat_id, CAST(recorded_at AS DATE)
        ),
        daily_median AS (
            SELECT reading_date,
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
    """, mf_params).fetchdf()
    results["gaps"] = gaps

    if not gaps.empty:
        print(f"\n  3. GAP DETECTION — missing intervals ({len(gaps)} flat/day combos below 80% of median):")
        print(gaps.head(20).to_string(index=False))
        if len(gaps) > 20:
            print(f"     ... and {len(gaps) - 20} more.")
    else:
        print("\n  3. GAP DETECTION: All flats have expected reading counts.")

    # 4. Spike detection
    spikes = con.execute(f"""
        WITH deltas AS (
            SELECT
                utility_type, given_flat_id, recorded_at, reading_value,
                strftime(recorded_at, '%Y-%m') AS month,
                reading_value - LAG(reading_value) OVER (
                    PARTITION BY utility_type, given_flat_id
                    ORDER BY recorded_at
                ) AS delta
            FROM readings
            WHERE {mf_sql}
              AND reading_value IS NOT NULL
        ),
        stats AS (
            SELECT utility_type, given_flat_id,
                   AVG(delta) AS mean_delta,
                   STDDEV(delta) AS std_delta
            FROM deltas
            WHERE delta IS NOT NULL AND delta >= 0
            GROUP BY utility_type, given_flat_id
            HAVING STDDEV(delta) > 0
        )
        SELECT d.utility_type, d.given_flat_id, d.month, d.recorded_at,
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
    """, mf_params).fetchdf()
    results["spikes"] = spikes

    if not spikes.empty:
        print(f"\n  4. SPIKE DETECTION ({len(spikes)} readings >3 std dev above mean):")
        print(spikes.head(20).to_string(index=False))
        if len(spikes) > 20:
            print(f"     ... and {len(spikes) - 20} more.")
    else:
        print("\n  4. SPIKE DETECTION: No abnormal spikes detected.")

    # 5. Stale-then-resume
    stale = con.execute(f"""
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
            WHERE {mf_sql}
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
    """, mf_params).fetchdf()
    results["stale"] = stale

    if not stale.empty:
        print(f"\n  5. STALE-THEN-RESUME ({len(stale)} periods >24h with unchanged reading):")
        print(stale.head(20).to_string(index=False))
        if len(stale) > 20:
            print(f"     ... and {len(stale) - 20} more.")
    else:
        print("\n  5. STALE-THEN-RESUME: No extended stale periods detected.")

    # 6. NULL readings
    nulls = con.execute(f"""
        WITH null_readings AS (
            SELECT utility_type, given_flat_id, recorded_at, source_file,
                   strftime(recorded_at, '%Y-%m') AS month
            FROM readings
            WHERE {mf_sql}
              AND reading_value IS NULL
        )
        SELECT utility_type, given_flat_id,
               COUNT(*) AS null_count,
               COUNT(DISTINCT month) AS months_affected,
               MIN(recorded_at) AS first_null,
               MAX(recorded_at) AS last_null
        FROM null_readings
        GROUP BY utility_type, given_flat_id
        ORDER BY null_count DESC, utility_type, given_flat_id
    """, mf_params).fetchdf()
    results["nulls"] = nulls

    if not nulls.empty:
        print(f"\n  6. NULL READINGS ({len(nulls)} flat/utility combos with missing values):")
        print(nulls.head(20).to_string(index=False))
        if len(nulls) > 20:
            print(f"     ... and {len(nulls) - 20} more.")
    else:
        print("\n  6. NULL READINGS: No missing values detected.")

    # 7. Out-of-order timestamps
    out_of_order = con.execute(f"""
        WITH sequenced AS (
            SELECT
                utility_type, given_flat_id, recorded_at, source_file,
                LAG(recorded_at) OVER (
                    PARTITION BY source_file, given_flat_id
                    ORDER BY recorded_at
                ) AS prev_time
            FROM readings
            WHERE {mf_sql}
        )
        SELECT source_file, utility_type, given_flat_id,
               prev_time, recorded_at,
               recorded_at - prev_time AS time_gap
        FROM sequenced
        WHERE prev_time IS NOT NULL
          AND recorded_at < prev_time
        ORDER BY source_file, given_flat_id, recorded_at
    """, mf_params).fetchdf()
    results["out_of_order"] = out_of_order

    if not out_of_order.empty:
        print(f"\n  7. OUT-OF-ORDER TIMESTAMPS ({len(out_of_order)} instances):")
        print(out_of_order.head(20).to_string(index=False))
    else:
        print("\n  7. OUT-OF-ORDER TIMESTAMPS: All timestamps monotonically increasing.")

    # ── Repeat offenders (only for multi-month ranges) ────────────────────
    if multi:
        print(f"\n  {'═' * 72}")
        print(f"  REPEAT OFFENDERS — flats flagged in multiple months ({label})")
        print(f"  {'─' * 72}")
        _print_repeat_offenders(results, check_col_map={
            "backward": "given_flat_id",
            "zero_consumption": "given_flat_id",
            "gaps": "given_flat_id",
            "spikes": "given_flat_id",
            "stale": "given_flat_id",
            "nulls": "given_flat_id",
            "out_of_order": "given_flat_id",
        }, month_col_map={
            "backward": "month",
            "zero_consumption": "month",
            "gaps": "reading_date",
            "spikes": "month",
            "stale": "stale_from",
            "nulls": "months_affected",
            "out_of_order": None,
        }, check_names=check_names)

    # ── Summary report ────────────────────────────────────────────────────
    total_issues = sum(len(v) for v in results.values())
    flagged = sum(1 for v in results.values() if len(v) > 0)
    print(f"\n  {'─' * 72}")
    print(f"  SCAN SUMMARY — {label}")
    print(f"  Records scanned : {scan_stats[0]:>10,}")
    print(f"  Flats scanned   : {scan_stats[1]:>10,}")
    print(f"  Months covered  : {scan_stats[5]:>10,}")
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


# ── Summary Discrepancy Checks ────────────────────────────────────────────

def detect_summary_discrepancies(con, start_month=None, end_month=None):
    """
    Detect anomalies in the monthly_summary table.
    Accepts a period range; shows repeat offenders when spanning multiple months.
    """
    if start_month and not end_month:
        end_month = start_month
    multi = start_month and start_month != end_month
    label = period_label(start_month, end_month) if start_month else "all months"
    scope = f"for {label}" if start_month else "across all months"

    # Scan metadata
    where_count, params_count = _summary_where(start_month, end_month)
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
        "Outlier consumption (>3\u03c3)",
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
    where, params = _summary_where(start_month, end_month, "(closing_reading - opening_reading) < 0")
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
    where, params = _summary_where(start_month, end_month, "closing_reading = opening_reading")
    zero = con.execute(f"""
        SELECT utility_type, given_flat_id, year_month,
               opening_reading, closing_reading
        FROM monthly_summary {where}
        ORDER BY year_month, utility_type, given_flat_id
    """, params).fetchdf()
    results["zero"] = zero

    if not zero.empty:
        print(f"\n  2. ZERO CONSUMPTION ({len(zero)} rows):")
        print(zero.head(20).to_string(index=False))
    else:
        print("\n  2. ZERO CONSUMPTION: None.")

    # 3. Late opening reading (after day 2)
    where, params = _summary_where(start_month, end_month, "CAST(strftime(opening_timestamp, '%d') AS INT) > 2")
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
    where, params = _summary_where(start_month, end_month, "CAST(strftime(closing_timestamp, '%d') AS INT) < 28")
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

    # 5. Month continuity
    continuity_where, continuity_params = _summary_where(start_month, end_month)
    continuity = con.execute(f"""
        WITH scoped AS (
            SELECT * FROM monthly_summary {continuity_where}
        ),
        ordered AS (
            SELECT s.*,
                LAG(s.closing_reading) OVER (
                    PARTITION BY s.utility_type, s.given_flat_id
                    ORDER BY s.year_month
                ) AS prev_closing,
                LAG(s.year_month) OVER (
                    PARTITION BY s.utility_type, s.given_flat_id
                    ORDER BY s.year_month
                ) AS prev_month
            FROM monthly_summary s
        )
        SELECT o.utility_type, o.given_flat_id, o.year_month,
               o.prev_month, o.prev_closing, o.opening_reading AS curr_opening,
               o.opening_reading - o.prev_closing AS gap
        FROM ordered o
        INNER JOIN scoped sc
            ON o.utility_type = sc.utility_type
           AND o.given_flat_id = sc.given_flat_id
           AND o.year_month = sc.year_month
        WHERE o.prev_closing IS NOT NULL
          AND ABS(o.opening_reading - o.prev_closing) > 0.001
        ORDER BY ABS(o.opening_reading - o.prev_closing) DESC
    """, continuity_params).fetchdf()
    results["continuity"] = continuity

    if not continuity.empty:
        print(f"\n  5. MONTH CONTINUITY BREAKS ({len(continuity)} — prev closing != curr opening):")
        print(continuity.head(20).to_string(index=False))
        if len(continuity) > 20:
            print(f"     ... and {len(continuity) - 20} more.")
    else:
        print("\n  5. MONTH CONTINUITY: All opening readings match previous closing.")

    # 6. Outlier consumption
    mf_sql, mf_params = month_filter_sql("c.year_month", start_month, end_month) if start_month else ("1=1", [])
    outliers = con.execute(f"""
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
        WHERE {mf_sql}
          AND ABS(c.consumption - s.mean_consumption) / s.std_consumption > 3
        ORDER BY ABS((c.consumption - s.mean_consumption) / s.std_consumption) DESC
    """, mf_params).fetchdf()
    results["outliers"] = outliers

    if not outliers.empty:
        print(f"\n  6. OUTLIER CONSUMPTION ({len(outliers)} — >3 std dev from historical avg):")
        print(outliers.head(20).to_string(index=False))
        if len(outliers) > 20:
            print(f"     ... and {len(outliers) - 20} more.")
    else:
        print("\n  6. OUTLIER CONSUMPTION: None detected (requires >=3 months of history).")

    # 7. Cross-utility correlation
    where, params = _summary_where(start_month, end_month)
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

    # 8. Missing months
    if not start_month or multi:
        where_mm, params_mm = _summary_where(start_month, end_month)
        missing = con.execute(f"""
            WITH scoped AS (
                SELECT * FROM monthly_summary {where_mm}
            ),
            combos AS (
                SELECT DISTINCT given_flat_id, utility_type FROM scoped
            ),
            months AS (
                SELECT DISTINCT year_month FROM scoped
            )
            SELECT c.given_flat_id, c.utility_type, m.year_month
            FROM combos c
            CROSS JOIN months m
            LEFT JOIN scoped s
                ON c.given_flat_id = s.given_flat_id
                AND c.utility_type = s.utility_type
                AND m.year_month = s.year_month
            WHERE s.given_flat_id IS NULL
            ORDER BY c.given_flat_id, c.utility_type, m.year_month
        """, params_mm).fetchdf()
        results["missing_months"] = missing

        if not missing.empty:
            print(f"\n  8. MISSING MONTHS ({len(missing)} missing flat/utility/month entries):")
            print(missing.head(20).to_string(index=False))
            if len(missing) > 20:
                print(f"     ... and {len(missing) - 20} more.")
        else:
            print("\n  8. MISSING MONTHS: Full coverage across all flat/utility/month combos.")

    # ── Repeat offenders ──────────────────────────────────────────────────
    if multi:
        print(f"\n  {'═' * 72}")
        print(f"  REPEAT OFFENDERS — flats flagged in multiple months ({label})")
        print(f"  {'─' * 72}")
        _print_repeat_offenders(results, check_col_map={
            "negative": "given_flat_id",
            "zero": "given_flat_id",
            "late_start": "given_flat_id",
            "early_close": "given_flat_id",
            "continuity": "given_flat_id",
            "outliers": "given_flat_id",
            "cross_utility": "given_flat_id",
            "missing_months": "given_flat_id",
        }, month_col_map={
            "negative": "year_month",
            "zero": "year_month",
            "late_start": "year_month",
            "early_close": "year_month",
            "continuity": "year_month",
            "outliers": "year_month",
            "cross_utility": "year_month",
            "missing_months": "year_month",
        }, check_names=check_names)

    # ── Summary report ────────────────────────────────────────────────────
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


# ── Cross-Utility Flat Profile ────────────────────────────────────────────

def check_cross_utility(con, start_month=None, end_month=None):
    """
    Cross-utility flat profile report.
    Shows which utilities have data (and consumption) per flat across months.
    """
    if start_month and not end_month:
        end_month = start_month
    label = period_label(start_month, end_month) if start_month else "all months"
    where, params = _summary_where(start_month, end_month)

    # Scan metadata
    scan_stats = con.execute(f"""
        SELECT COUNT(*) AS total_rows,
               COUNT(DISTINCT given_flat_id) AS distinct_flats,
               COUNT(DISTINCT utility_type) AS distinct_utilities,
               COUNT(DISTINCT year_month) AS distinct_months
        FROM monthly_summary {where}
    """, params).fetchone()

    all_utilities = con.execute(f"""
        SELECT DISTINCT utility_type FROM monthly_summary {where}
        ORDER BY utility_type
    """, params).fetchdf()['utility_type'].tolist()

    print(f"Cross-utility flat profile report — {label}")
    print(f"  Scanning: {scan_stats[0]:,} summary rows | {scan_stats[1]:,} flats | "
          f"{scan_stats[2]} utility types | {scan_stats[3]} months")
    print(f"  Utilities in scope: {', '.join(all_utilities)}")
    print(f"  {'─' * 72}")

    # 1. Utility presence pattern
    pattern_df = con.execute(f"""
        WITH flat_utils AS (
            SELECT given_flat_id,
                   LIST(DISTINCT utility_type ORDER BY utility_type) AS utilities_present,
                   COUNT(DISTINCT utility_type) AS num_utilities,
                   COUNT(DISTINCT year_month) AS months_active,
                   ROUND(SUM(closing_reading - opening_reading), 2) AS total_consumption
            FROM monthly_summary {where}
            GROUP BY given_flat_id
        )
        SELECT utilities_present, num_utilities,
               COUNT(*) AS num_flats,
               LIST(given_flat_id ORDER BY given_flat_id) AS flat_ids
        FROM flat_utils
        GROUP BY utilities_present, num_utilities
        ORDER BY num_flats DESC
    """, params).fetchdf()

    print(f"\n  1. UTILITY PRESENCE PATTERNS ({len(pattern_df)} distinct patterns across {scan_stats[1]:,} flats):")
    for _, row in pattern_df.iterrows():
        present = row['utilities_present']
        n = row['num_flats']
        is_complete = (row['num_utilities'] == len(all_utilities))
        marker = "  " if is_complete else "!!"
        flat_preview = str(row['flat_ids'])
        if len(flat_preview) > 80:
            flat_preview = flat_preview[:77] + "..."
        print(f"    [{marker}] {str(present):<45s} {n:>5,} flats  {flat_preview}")

    # 2. Incomplete coverage
    if scan_stats[3] > 1:
        incomplete = con.execute(f"""
            WITH scoped AS (
                SELECT * FROM monthly_summary {where}
            ),
            all_combos AS (
                SELECT DISTINCT f.given_flat_id, u.utility_type, m.year_month
                FROM (SELECT DISTINCT given_flat_id FROM scoped) f
                CROSS JOIN (SELECT DISTINCT utility_type FROM scoped) u
                CROSS JOIN (SELECT DISTINCT year_month FROM scoped) m
            ),
            actual AS (
                SELECT given_flat_id, utility_type, year_month, 1 AS present
                FROM scoped
            ),
            gaps AS (
                SELECT a.given_flat_id, a.utility_type, a.year_month
                FROM all_combos a
                LEFT JOIN actual act
                    ON a.given_flat_id = act.given_flat_id
                   AND a.utility_type = act.utility_type
                   AND a.year_month = act.year_month
                WHERE act.present IS NULL
            )
            SELECT given_flat_id,
                   COUNT(*) AS missing_slots,
                   LIST(DISTINCT utility_type ORDER BY utility_type) AS missing_utilities,
                   LIST(DISTINCT year_month ORDER BY year_month) AS missing_months
            FROM gaps
            GROUP BY given_flat_id
            ORDER BY missing_slots DESC, given_flat_id
        """, params).fetchdf()

        if not incomplete.empty:
            print(f"\n  2. INCOMPLETE COVERAGE ({len(incomplete)} flats missing utility/month slots):")
            print(incomplete.head(30).to_string(index=False))
            if len(incomplete) > 30:
                print(f"     ... and {len(incomplete) - 30} more.")
        else:
            print("\n  2. INCOMPLETE COVERAGE: All flats have all utilities for all months.")
    else:
        print("\n  2. INCOMPLETE COVERAGE: Skipped (need >1 month for coverage analysis).")

    # 3. Zero-consumption utility profiles
    zero_profiles = con.execute(f"""
        WITH consumption AS (
            SELECT given_flat_id, utility_type,
                   SUM(closing_reading - opening_reading) AS total_consumption,
                   COUNT(*) AS months_present
            FROM monthly_summary {where}
            GROUP BY given_flat_id, utility_type
        ),
        flat_profiles AS (
            SELECT given_flat_id,
                   LIST(utility_type ORDER BY utility_type) FILTER (WHERE total_consumption = 0) AS zero_utilities,
                   LIST(utility_type ORDER BY utility_type) FILTER (WHERE total_consumption > 0) AS active_utilities,
                   COUNT(*) FILTER (WHERE total_consumption = 0) AS zero_count,
                   COUNT(*) FILTER (WHERE total_consumption > 0) AS active_count
            FROM consumption
            GROUP BY given_flat_id
        )
        SELECT given_flat_id, zero_utilities, active_utilities,
               zero_count, active_count
        FROM flat_profiles
        WHERE zero_count > 0 AND active_count > 0
        ORDER BY zero_count DESC, given_flat_id
    """, params).fetchdf()

    if not zero_profiles.empty:
        print(f"\n  3. ZERO-CONSUMPTION PROFILES ({len(zero_profiles)} flats — zero total usage in some utilities):")
        print(zero_profiles.head(30).to_string(index=False))
        if len(zero_profiles) > 30:
            print(f"     ... and {len(zero_profiles) - 30} more.")

        # Group by pattern
        pattern_summary = zero_profiles.groupby(
            [zero_profiles['zero_utilities'].astype(str), zero_profiles['active_utilities'].astype(str)]
        ).size().reset_index(name='count').sort_values('count', ascending=False)
        print(f"\n    Grouped by pattern:")
        for _, row in pattern_summary.iterrows():
            print(f"      Zero: {row['zero_utilities']:<30s} Active: {row['active_utilities']:<30s} \u2192 {row['count']:,} flats")
    else:
        print("\n  3. ZERO-CONSUMPTION PROFILES: All flats have active consumption on all their utilities.")

    # Summary
    print(f"\n  {'─' * 72}")
    print(f"  CROSS-UTILITY SUMMARY — {label}")
    print(f"  Flats scanned       : {scan_stats[1]:>8,}")
    print(f"  Utility types       : {scan_stats[2]:>8}")
    print(f"  Presence patterns   : {len(pattern_df):>8}")
    n_full = sum(1 for _, r in pattern_df.iterrows() if r['num_utilities'] == len(all_utilities))
    n_partial = len(pattern_df) - n_full
    print(f"  Full-utility flats  : {pattern_df[pattern_df['num_utilities'] == len(all_utilities)]['num_flats'].sum() if n_full else 0:>8,}")
    print(f"  Partial patterns    : {n_partial:>8}")
    if not zero_profiles.empty:
        print(f"  Zero-on-some flats  : {len(zero_profiles):>8,}")
    print(f"  {'─' * 72}")


# ── Ingestion Checks ──────────────────────────────────────────────────────

def detect_ingestion_issues(con, year_month, expected_files_per_day=14):
    """
    Detect operational issues with data ingestion for a given month.
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

    # 1. File completeness
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

    # 2. Row count anomalies
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

    # 3. Ingestion log
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
