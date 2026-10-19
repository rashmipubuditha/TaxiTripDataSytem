"""Data quality checks.

Critical checks (QC1-QC6) run INSIDE the publish transaction, before COMMIT. If any
fails, the caller rolls back, so the month keeps its previous data and bad data is
never published.

Warning checks (QC7-QC9) run after COMMIT. A breach is stored in etl_quality_results
and logged as a WARNING (event "quality_warning"), but the run still succeeds: the data
is valid, just unusual enough that a person should look at it.

    python -m pipeline.quality --months 2023-09,2023-10   # (re)run QC7-QC9 for loaded months
"""
from __future__ import annotations

import argparse
import calendar
import logging
from dataclasses import dataclass
from datetime import datetime

import psycopg

from pipeline.config import QualityThresholds, get_settings, month_bounds, parse_months

logger = logging.getLogger(__name__)

FACT_FOREIGN_KEYS = 7  # date, hour, pickup zone, drop-off zone, payment type, vendor, source file


@dataclass(frozen=True)
class CheckResult:
    name: str
    severity: str              # 'CRITICAL' or 'WARNING'
    passed: bool
    observed: float | None
    threshold: float | None
    details: str


def run_critical_checks(
    cur: psycopg.Cursor,
    *,
    source_file_id: int,
    month_start: datetime,
    month_end: datetime,
    rows_read: int,
    rows_staged: int,
    rows_deduplicated: int,
    rows_rejected: int,
) -> list[CheckResult]:
    """QC1-QC6 for one month. Must be called inside the publish transaction."""
    start_key = int(month_start.strftime("%Y%m%d"))
    end_key = int(month_end.strftime("%Y%m%d"))  # exclusive

    # One scan of the month's fact rows answers QC2, QC4 and QC5.
    cur.execute(
        """
        SELECT count(*),
               count(*) FILTER (WHERE pickup_datetime < %(start)s OR pickup_datetime >= %(end)s),
               count(*) FILTER (WHERE pickup_datetime IS NULL OR trip_distance IS NULL
                                   OR fare_amount IS NULL OR total_amount IS NULL),
               coalesce(sum(total_amount), 0)
        FROM fact_taxi_trips
        WHERE source_file_id = %(fid)s
        """,
        {"start": month_start, "end": month_end, "fid": source_file_id},
    )
    rows_loaded, outside_month, null_required, fact_revenue = cur.fetchone()

    # QC3: every fact row must point to an existing dimension row. One set-based pass
    # (hash joins) checks all 7 relationships; this is what guarantees integrity when the
    # publish INSERT skips PostgreSQL's row-by-row foreign-key checks (fast_fk_load).
    cur.execute(
        """
        SELECT count(*) FILTER (WHERE d.date_key IS NULL OR t.hour_key IS NULL OR pu.location_key IS NULL
                                   OR dz.location_key IS NULL OR p.payment_type_key IS NULL
                                   OR v.vendor_key IS NULL OR sf.source_file_id IS NULL)
        FROM fact_taxi_trips f
        LEFT JOIN dim_date d          ON d.date_key = f.pickup_date_key
        LEFT JOIN dim_time t          ON t.hour_key = f.pickup_hour_key
        LEFT JOIN dim_location pu     ON pu.location_key = f.pu_location_key
        LEFT JOIN dim_location dz     ON dz.location_key = f.do_location_key
        LEFT JOIN dim_payment_type p  ON p.payment_type_key = f.payment_type_key
        LEFT JOIN dim_vendor v        ON v.vendor_key = f.vendor_key
        LEFT JOIN etl_source_files sf ON sf.source_file_id = f.source_file_id
        WHERE f.source_file_id = %(fid)s
        """,
        {"fid": source_file_id},
    )
    orphan_rows = cur.fetchone()[0]
    # The foreign-key constraints themselves must still be declared on the table.
    cur.execute(
        "SELECT count(*) FROM pg_constraint WHERE conrelid = 'fact_taxi_trips'::regclass AND contype = 'f'"
    )
    declared_fks = cur.fetchone()[0]

    # QC6a: no duplicates left (rule D1 key; drop-off time = pickup time + duration).
    cur.execute(
        """
        SELECT count(*) FROM (
            SELECT 1 FROM fact_taxi_trips
            WHERE source_file_id = %(fid)s
            GROUP BY vendor_key, pickup_datetime, coalesce(trip_duration_min, 0),
                     pu_location_key, do_location_key, trip_distance, fare_amount
            HAVING count(*) > 1
        ) d
        """,
        {"fid": source_file_id},
    )
    remaining_duplicates = cur.fetchone()[0]

    # QC6b: the summary table must match the fact table for this month.
    cur.execute(
        """
        SELECT coalesce(sum(trip_count), 0), coalesce(sum(sum_total), 0)
        FROM agg_trips_daily_hourly
        WHERE date_key >= %(s)s AND date_key < %(e)s
        """,
        {"s": start_key, "e": end_key},
    )
    agg_trips, agg_revenue = cur.fetchone()

    return [
        CheckResult("QC1", "CRITICAL", rows_read == rows_loaded + rows_rejected,
                    rows_loaded + rows_rejected, rows_read,
                    f"rows_read={rows_read:,} vs loaded+rejected={rows_loaded + rows_rejected:,}"),
        CheckResult("QC2", "CRITICAL", rows_loaded == rows_staged - rows_deduplicated,
                    rows_loaded, rows_staged - rows_deduplicated,
                    f"fact rows={rows_loaded:,} vs staged-deduplicated={rows_staged - rows_deduplicated:,}"),
        CheckResult("QC3", "CRITICAL", orphan_rows == 0 and declared_fks == FACT_FOREIGN_KEYS,
                    orphan_rows, 0,
                    f"fact rows without a matching dimension row={orphan_rows:,}; "
                    f"declared foreign keys={declared_fks}/{FACT_FOREIGN_KEYS}"),
        CheckResult("QC4", "CRITICAL", outside_month == 0, outside_month, 0,
                    "fact rows with pickup outside the month"),
        CheckResult("QC5", "CRITICAL", null_required == 0, null_required, 0,
                    "fact rows with NULL in a required column"),
        CheckResult("QC6", "CRITICAL",
                    remaining_duplicates == 0 and agg_trips == rows_loaded and agg_revenue == fact_revenue,
                    remaining_duplicates, 0,
                    f"remaining duplicate groups={remaining_duplicates}; summary trips={agg_trips:,} "
                    f"vs fact={rows_loaded:,}; summary revenue={agg_revenue} vs fact={fact_revenue}"),
    ]


def save_check_results(cur: psycopg.Cursor, run_id: int, results: list[CheckResult]) -> None:
    """Store results (insert or replace) in etl_quality_results."""
    cur.executemany(
        """
        INSERT INTO etl_quality_results (run_id, check_name, severity, passed, observed_value,
                                         threshold_value, details)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (run_id, check_name) DO UPDATE
            SET passed = EXCLUDED.passed, observed_value = EXCLUDED.observed_value,
                threshold_value = EXCLUDED.threshold_value, details = EXCLUDED.details,
                checked_at = now()
        """,
        [(run_id, r.name, r.severity, r.passed, r.observed, r.threshold, r.details) for r in results],
    )


# --- Warning checks QC7-QC9 ----------------------------------------------------------------

@dataclass(frozen=True)
class MonthStats:
    """Inputs for the warning checks of one month."""
    month: str
    rows_read: int
    rows_staged: int
    rows_rejected: int          # R1-R13 rejects + duplicates
    rows_deduplicated: int
    days_in_month: int
    days_with_trips: int
    fare_per_mile: float | None  # weighted: total fare / total miles


def evaluate_warning_checks(stats: MonthStats, limits: QualityThresholds) -> list[CheckResult]:
    """Pure function (no database): compare one month's statistics with the thresholds."""
    reject_pct = 100.0 * stats.rows_rejected / stats.rows_read if stats.rows_read else 0.0
    duplicate_pct = 100.0 * stats.rows_deduplicated / stats.rows_staged if stats.rows_staged else 0.0
    fpm = stats.fare_per_mile
    fpm_ok = fpm is not None and limits.min_fare_per_mile <= fpm <= limits.max_fare_per_mile
    all_days = stats.days_with_trips == stats.days_in_month
    fpm_text = "n/a" if fpm is None else f"{fpm:.2f}"
    return [
        CheckResult("QC7", "WARNING", reject_pct <= limits.max_reject_rate_pct,
                    round(reject_pct, 4), limits.max_reject_rate_pct,
                    f"reject rate {reject_pct:.2f}% ({stats.rows_rejected:,} of {stats.rows_read:,} rows) "
                    f"vs limit {limits.max_reject_rate_pct}%"),
        CheckResult("QC8", "WARNING", duplicate_pct <= limits.max_duplicate_rate_pct,
                    round(duplicate_pct, 4), limits.max_duplicate_rate_pct,
                    f"duplicate rate {duplicate_pct:.4f}% ({stats.rows_deduplicated:,} rows) "
                    f"vs limit {limits.max_duplicate_rate_pct}%"),
        CheckResult("QC9", "WARNING", all_days and fpm_ok, fpm, None,
                    f"days with trips {stats.days_with_trips}/{stats.days_in_month}; weighted fare per mile "
                    f"{fpm_text} (expected {limits.min_fare_per_mile}-{limits.max_fare_per_mile})"),
    ]


def collect_month_stats(cur: psycopg.Cursor, month: str, *, rows_read: int, rows_staged: int,
                        rows_rejected: int, rows_deduplicated: int) -> MonthStats:
    """Read day coverage and fare per mile for the month from the summary table (fast)."""
    start, end = month_bounds(month)
    cur.execute(
        """
        SELECT count(DISTINCT date_key), sum(sum_fare) / nullif(sum(sum_distance), 0)
        FROM agg_trips_daily_hourly
        WHERE date_key >= %s AND date_key < %s
        """,
        (int(start.strftime("%Y%m%d")), int(end.strftime("%Y%m%d"))),
    )
    days_with_trips, fpm = cur.fetchone()
    return MonthStats(month=month, rows_read=rows_read, rows_staged=rows_staged, rows_rejected=rows_rejected,
                      rows_deduplicated=rows_deduplicated,
                      days_in_month=calendar.monthrange(start.year, start.month)[1],
                      days_with_trips=days_with_trips or 0, fare_per_mile=float(fpm) if fpm is not None else None)


def run_warning_checks(conn: psycopg.Connection, run_id: int, stats: MonthStats,
                       limits: QualityThresholds) -> list[CheckResult]:
    """Evaluate QC7-QC9, store the results, and log each breach as a WARNING."""
    results = evaluate_warning_checks(stats, limits)
    with conn.transaction(), conn.cursor() as cur:
        save_check_results(cur, run_id, results)
    for r in results:
        if not r.passed:
            logger.warning("quality warning %s for %s: %s", r.name, stats.month, r.details,
                           extra={"event": "quality_warning", "check": r.name, "month": stats.month,
                                  "observed": r.observed, "threshold": r.threshold})
    return results


def main() -> None:
    """Re-run the warning checks for already loaded months (latest successful run of each)."""
    from pipeline.db import connect
    from pipeline.logging_config import setup_logging

    parser = argparse.ArgumentParser(description="Run warning checks QC7-QC9 for loaded months.")
    parser.add_argument("--months", help="comma-separated YYYY-MM (default: MONTHS from .env)")
    args = parser.parse_args()
    setup_logging(console=True)
    settings = get_settings()
    months = parse_months(args.months) if args.months else settings.months

    with connect(settings) as conn:
        for month in months:
            row = conn.execute(
                """SELECT run_id, rows_read, rows_staged, rows_rejected, rows_deduplicated
                   FROM etl_pipeline_runs WHERE status = 'SUCCESS' AND source_month = %s
                   ORDER BY run_id DESC LIMIT 1""", (month,)).fetchone()
            if row is None:
                print(f"{month}: no successful run — load it first")
                continue
            run_id, read, staged, rejected, dedup = row
            with conn.cursor() as cur:
                stats = collect_month_stats(cur, month, rows_read=read, rows_staged=staged,
                                            rows_rejected=rejected, rows_deduplicated=dedup)
            results = run_warning_checks(conn, run_id, stats, settings.quality)
            summary = "  ".join(f"{r.name}:{'ok' if r.passed else 'WARN'}" for r in results)
            print(f"{month} (run {run_id}): {summary}")


if __name__ == "__main__":
    main()
