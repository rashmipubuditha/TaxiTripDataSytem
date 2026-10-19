"""Load: dimensions, staging (COPY), and the all-or-nothing monthly publish.

Step functions here are plain Python; the Prefect tasks in pipeline/flows.py wrap them.
`python -m pipeline.load --months 2023-01 [--limit 50000]` runs them without Prefect
(useful for debugging); the normal entry point is `python -m pipeline.flows`.
"""
from __future__ import annotations

import argparse
import csv
import logging
import time
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd
import psycopg
from psycopg import sql

from pipeline.config import PROJECT_ROOT, Settings, get_settings, load_zone_ids, month_bounds, parse_months
from pipeline.db import connect, copy_dataframe
from pipeline.exceptions import DataQualityError, EtlError, LoadError
from pipeline.extract import SourceFileInfo, iter_batches, validate_source_file
from pipeline.logging_config import log_context, setup_logging
from pipeline.quality import collect_month_stats, run_critical_checks, run_warning_checks, save_check_results
from pipeline.transform import STAGING_COLUMNS, TransformContext, process_batch

logger = logging.getLogger(__name__)

# Fact columns = staging columns minus dropoff_datetime (stored only in staging, for rule D1).
FACT_COLUMNS = tuple(c for c in STAGING_COLUMNS if c != "dropoff_datetime")

# Rule D1: rows that describe the same trip. Columns that may be NULL in staging are
# counted so the most complete copy is kept.
D1_KEY = ("vendor_key", "pickup_datetime", "dropoff_datetime", "pu_location_key",
          "do_location_key", "trip_distance", "fare_amount")
NULLABLE_STAGING_COLUMNS = ("passenger_count", "store_and_fwd_flag", "trip_duration_min",
                            "congestion_surcharge", "airport_fee", "cbd_congestion_fee", "request_source")


@dataclass
class StageResult:
    rows_read: int = 0
    rows_staged: int = 0
    rejects: Counter = field(default_factory=Counter)
    warnings: Counter = field(default_factory=Counter)
    rejected_file: Path | None = None   # Parquet file with the rejected rows (if saved)


@dataclass
class PublishResult:
    rows_deduplicated: int
    rows_loaded: int
    rows_replaced: int        # fact rows of this file that existed before (a reload)


# --- Dimensions -------------------------------------------------------------------

def load_dim_location(conn: psycopg.Connection, csv_path: Path) -> int:
    """Upsert the 265 TLC zones. If the CSV is missing, insert IDs 1-265 named 'Unknown'."""
    if csv_path.is_file():
        with csv_path.open(newline="", encoding="utf-8") as fh:
            rows = [(int(r["LocationID"]), r["Borough"] or "Unknown", r["Zone"] or "Unknown",
                     r["service_zone"] or "Unknown") for r in csv.DictReader(fh)]
    else:
        logger.warning("Zone lookup %s not found — loading zone IDs without names", csv_path)
        rows = [(zone_id, "Unknown", "Unknown", "Unknown") for zone_id in sorted(load_zone_ids(csv_path))]
    with conn.transaction(), conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO dim_location (location_key, borough, zone, service_zone)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (location_key) DO UPDATE
                SET borough = EXCLUDED.borough, zone = EXCLUDED.zone, service_zone = EXCLUDED.service_zone
            """,
            rows,
        )
    return len(rows)


# --- Lineage and run tracking --------------------------------------------------------

def register_source_file(conn: psycopg.Connection, info: SourceFileInfo, month: str) -> int:
    """Insert or update the file in etl_source_files and return its small id."""
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO etl_source_files (file_name, source_month, file_size_bytes, row_count)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (file_name) DO UPDATE
                SET file_size_bytes = EXCLUDED.file_size_bytes, row_count = EXCLUDED.row_count
            RETURNING source_file_id
            """,
            (info.path.name, month, info.size_bytes, info.row_count),
        )
        return cur.fetchone()[0]


def partition_name(source_file_id: int, month: str) -> str:
    """Name of the fact partition for one source file, e.g. fact_taxi_trips_f001_2023_01."""
    return f"fact_taxi_trips_f{source_file_id:03d}_{month.replace('-', '_')}"


def ensure_fact_partition(conn: psycopg.Connection, source_file_id: int, month: str) -> str:
    """Create the fact partition for this source file if it does not exist yet."""
    name = partition_name(source_file_id, month)
    with conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE TABLE IF NOT EXISTS {} PARTITION OF fact_taxi_trips FOR VALUES IN ({})")
                    .format(sql.Identifier(name), sql.Literal(source_file_id)))
    return name


def start_run(conn: psycopg.Connection, month: str, prefect_flow_run_id: str | None = None) -> int:
    """Create a RUNNING row in etl_pipeline_runs (committed at once, so it is visible even if the run fails)."""
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO etl_pipeline_runs (source_month, prefect_flow_run_id) VALUES (%s, %s) RETURNING run_id",
            (month, prefect_flow_run_id),
        )
        return cur.fetchone()[0]


def finish_run(conn: psycopg.Connection, run_id: int, *, source_file_id: int | None,
               stage: StageResult | None, publish: PublishResult | None) -> None:
    rejected = None
    if stage is not None and publish is not None:
        rejected = sum(stage.rejects.values()) + publish.rows_deduplicated
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE etl_pipeline_runs
            SET status = 'SUCCESS', ended_at = now(),
                duration_seconds = extract(epoch FROM now() - started_at),
                source_file_id = %s, rows_read = %s, rows_staged = %s,
                rows_deduplicated = %s, rows_loaded = %s, rows_rejected = %s
            WHERE run_id = %s
            """,
            (source_file_id,
             stage.rows_read if stage else None, stage.rows_staged if stage else None,
             publish.rows_deduplicated if publish else None, publish.rows_loaded if publish else None,
             rejected, run_id),
        )


def fail_run(conn: psycopg.Connection, run_id: int, step: str, error: BaseException | str,
             source_file_id: int | None = None) -> None:
    """Mark a run FAILED with the step and message (called from error handlers).
    `error` is an exception, or an already formatted 'ErrorType: message' string."""
    message = error if isinstance(error, str) else f"{type(error).__name__}: {error}"
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE etl_pipeline_runs
            SET status = 'FAILED', ended_at = now(),
                duration_seconds = extract(epoch FROM now() - started_at),
                source_file_id = coalesce(%s, source_file_id),
                error_step = %s, error_message = %s
            WHERE run_id = %s
            """,
            (source_file_id, step, message[:4000], run_id),
        )


# --- Staging -----------------------------------------------------------------------

def rejected_rows_path(info: SourceFileInfo) -> Path:
    """data/rejected/<file>_rejected.parquet — overwritten on every run of that file."""
    return PROJECT_ROOT / "data" / "rejected" / f"{info.path.stem}_rejected.parquet"


def stage_month(conn: psycopg.Connection, info: SourceFileInfo, ctx: TransformContext,
                batch_size: int, limit: int | None = None, save_rejected: bool = False) -> StageResult:
    """Empty stg_trips, then extract -> clean -> transform -> COPY batch by batch.

    Only counts are returned (no DataFrames), so this works well as one Prefect task.
    save_rejected: also write the rejected source rows (with reject_reason) to Parquet.
    """
    result = StageResult()
    rejected_parts = []
    with conn.cursor() as cur:
        cur.execute("TRUNCATE stg_trips")
        for number, batch in enumerate(iter_batches(info, batch_size, limit), start=1):
            try:
                out = process_batch(batch, ctx)
                with conn.transaction():
                    copy_dataframe(cur, "stg_trips", STAGING_COLUMNS, out.staged)
            except EtlError:
                raise
            except Exception as exc:
                raise LoadError(f"Staging failed for {info.path.name}, batch {number}: {exc}") from exc
            result.rows_read += len(batch)
            result.rows_staged += len(out.staged)
            result.rejects.update(out.rejects)
            result.warnings.update(out.warnings)
            if save_rejected and not out.rejected.empty:
                rejected_parts.append(out.rejected)
            logger.info("staged batch %d of %s: %s rows read, %s staged",
                        number, info.path.name, f"{len(batch):,}", f"{len(out.staged):,}",
                        extra={"event": "batch_staged", "batch": number, "rows_read": len(batch),
                               "rows_staged": len(out.staged), "rows_rejected": len(out.rejected)})
    if save_rejected:
        path = rejected_rows_path(info)
        path.parent.mkdir(parents=True, exist_ok=True)
        if rejected_parts:
            pd.concat(rejected_parts, ignore_index=True).to_parquet(path, index=False)
            result.rejected_file = path
        elif path.exists():
            path.unlink()  # a previous run's file would now be misleading
    logger.info("staging finished for %s: %s read, %s staged", info.path.name,
                f"{result.rows_read:,}", f"{result.rows_staged:,}",
                extra={"event": "stage_finished", "rows_read": result.rows_read,
                       "rows_staged": result.rows_staged, "rejects": dict(result.rejects),
                       "warnings": dict(result.warnings)})
    return result


# --- Publish (one transaction) ---------------------------------------------------------

def _dedup_sql() -> str:
    null_count = " + ".join(f"({c} IS NULL)::int" for c in NULLABLE_STAGING_COLUMNS)
    return f"""
        DELETE FROM stg_trips s
        USING (
            SELECT ctid,
                   row_number() OVER (
                       PARTITION BY {', '.join(D1_KEY)}
                       ORDER BY {null_count}, total_amount, source_row_number
                   ) AS copy_number
            FROM stg_trips
        ) d
        WHERE s.ctid = d.ctid AND d.copy_number > 1
    """


def deduplicate_staging(cur: psycopg.Cursor) -> int:
    """Rule D1: keep one row per trip (most complete, then lowest total, then first in file).
    Returns the number of extra copies removed."""
    cur.execute(_dedup_sql())
    return cur.rowcount


def _rebuild_summaries(cur: psycopg.Cursor, month: str, start_key: int, end_key: int) -> None:
    cur.execute("DELETE FROM agg_trips_daily_hourly WHERE date_key >= %s AND date_key < %s",
                (start_key, end_key))
    cur.execute(
        """
        INSERT INTO agg_trips_daily_hourly (date_key, hour_key, payment_type_key, trip_count,
                                            sum_fare, sum_distance, sum_total, sum_tip,
                                            sum_fare_per_mile_ratio)
        SELECT pickup_date_key, pickup_hour_key, payment_type_key, count(*),
               sum(fare_amount), sum(trip_distance), sum(total_amount), sum(tip_amount),
               sum(fare_amount / trip_distance)
        FROM fact_taxi_trips
        WHERE pickup_date_key >= %s AND pickup_date_key < %s
        GROUP BY pickup_date_key, pickup_hour_key, payment_type_key
        """,
        (start_key, end_key),
    )
    cur.execute("DELETE FROM agg_monthly_fare_stats WHERE source_month = %s", (month,))
    # GROUPING SETS: one row per payment type plus one row (payment_type_key NULL) for all types.
    cur.execute(
        """
        INSERT INTO agg_monthly_fare_stats (source_month, payment_type_key, trip_count, median_fare_per_mile)
        SELECT %s, payment_type_key, count(*),
               percentile_cont(0.5) WITHIN GROUP (ORDER BY fare_amount / trip_distance)
        FROM fact_taxi_trips
        WHERE pickup_date_key >= %s AND pickup_date_key < %s
        GROUP BY GROUPING SETS ((payment_type_key), ())
        """,
        (month, start_key, end_key),
    )


def _disable_fk_triggers(cur: psycopg.Cursor) -> bool:
    """Skip PostgreSQL's row-by-row foreign-key checks for the rest of this transaction.

    Foreign keys are enforced by internal triggers; session_replication_role = replica
    turns triggers off (primary-key, unique and CHECK constraints stay enforced). QC3
    then verifies every relationship with one set-based query before COMMIT, so a bad
    key still rolls the month back. Needs superuser; returns False (normal checking)
    if the role is not allowed to change the setting.
    """
    try:
        with cur.connection.transaction():  # savepoint: a permission error must not abort the publish
            cur.execute("SET LOCAL session_replication_role = replica")
        return True
    except psycopg.errors.InsufficientPrivilege:
        logger.warning("No permission to skip row-by-row FK checks; using normal (slower) checking")
        return False


def publish_month(conn: psycopg.Connection, *, month: str, source_file_id: int, run_id: int,
                  stage: StageResult, fast_fk_load: bool = True) -> PublishResult:
    """All-or-nothing: de-duplicate, replace the month's facts, rebuild summaries, run QC1-QC6.

    If any critical check fails the transaction is rolled back (the month keeps its
    previous data) and DataQualityError is raised.
    """
    start, end = month_bounds(month)
    start_key, end_key = int(start.strftime("%Y%m%d")), int(end.strftime("%Y%m%d"))
    fact_cols = ", ".join(FACT_COLUMNS)
    partition = sql.Identifier(ensure_fact_partition(conn, source_file_id, month))
    failed_checks = None

    try:
        with conn.transaction(), conn.cursor() as cur:
            deduplicated = deduplicate_staging(cur)
            # Replace the month: empty only this file's partition. TRUNCATE is transactional
            # in PostgreSQL, so a rollback brings the old rows back; unlike DELETE it leaves
            # no dead rows behind, so the table does not grow on reloads.
            cur.execute(sql.SQL("SELECT count(*) FROM {}").format(partition))
            replaced = cur.fetchone()[0]
            cur.execute(sql.SQL("TRUNCATE {}").format(partition))
            fk_checks_skipped = fast_fk_load and _disable_fk_triggers(cur)
            # Insert in date order so the BRIN index on pickup_date_key stays effective.
            cur.execute(
                f"INSERT INTO fact_taxi_trips ({fact_cols}) "
                f"SELECT {fact_cols} FROM stg_trips ORDER BY pickup_date_key, pickup_hour_key"
            )
            loaded = cur.rowcount
            if fk_checks_skipped:
                cur.execute("SET LOCAL session_replication_role = DEFAULT")  # back to normal for the rest
            _rebuild_summaries(cur, month, start_key, end_key)

            checks = run_critical_checks(
                cur, source_file_id=source_file_id, month_start=start, month_end=end,
                rows_read=stage.rows_read, rows_staged=stage.rows_staged,
                rows_deduplicated=deduplicated, rows_rejected=sum(stage.rejects.values()) + deduplicated,
            )
            failed = [c for c in checks if not c.passed]
            if failed:
                failed_checks = checks
                raise DataQualityError("; ".join(f"{c.name}: {c.details}" for c in failed))

            save_check_results(cur, run_id, checks)
            reject_rows = [(run_id, reason, count) for reason, count in stage.rejects.items() if count]
            if deduplicated:
                reject_rows.append((run_id, "duplicate_record", deduplicated))
            cur.execute("DELETE FROM etl_rejected_summary WHERE run_id = %s", (run_id,))
            cur.executemany("INSERT INTO etl_rejected_summary (run_id, reason, row_count) VALUES (%s, %s, %s)",
                            reject_rows)
            cur.execute(
                """
                UPDATE etl_source_files
                SET first_loaded_at = coalesce(first_loaded_at, now()), last_loaded_at = now(),
                    last_run_id = %s
                WHERE source_file_id = %s
                """,
                (run_id, source_file_id),
            )
    except DataQualityError as exc:
        # The transaction is rolled back; store the failing check results separately.
        if failed_checks:
            with conn.transaction(), conn.cursor() as cur:
                save_check_results(cur, run_id, failed_checks)
        logger.error("critical checks failed for %s; month rolled back: %s", month, exc,
                     extra={"event": "publish_rolled_back"})
        raise
    except psycopg.Error as exc:
        raise LoadError(f"Publishing {month} failed and was rolled back: {exc}") from exc

    # After COMMIT: empty staging and refresh planner statistics for the new partition.
    with conn.cursor() as cur:
        cur.execute("TRUNCATE stg_trips")
        cur.execute(sql.SQL("ANALYZE {}").format(partition))
    logger.info("published %s: %s rows loaded, %s duplicates removed, %s rows replaced",
                month, f"{loaded:,}", f"{deduplicated:,}", f"{replaced:,}",
                extra={"event": "publish_committed", "rows_loaded": loaded,
                       "rows_deduplicated": deduplicated, "rows_replaced": replaced,
                       "critical_checks_passed": len(checks)})
    return PublishResult(rows_deduplicated=deduplicated, rows_loaded=loaded, rows_replaced=replaced)


def check_published_month(conn: psycopg.Connection, *, month: str, run_id: int, stage: StageResult,
                          publish: PublishResult, settings: Settings) -> list:
    """Warning checks QC7-QC9 after COMMIT: stored and logged, never blocking."""
    with conn.cursor() as cur:
        stats = collect_month_stats(cur, month, rows_read=stage.rows_read, rows_staged=stage.rows_staged,
                                    rows_rejected=sum(stage.rejects.values()) + publish.rows_deduplicated,
                                    rows_deduplicated=publish.rows_deduplicated)
    return run_warning_checks(conn, run_id, stats, settings.quality)


# --- Run one month end-to-end (without Prefect) ------------------------------------------

def load_month(month: str, limit: int | None = None, settings: Settings | None = None) -> dict:
    """Validate -> dimensions -> stage -> publish -> finish, recording the run in the database."""
    settings = settings or get_settings()
    began = time.perf_counter()
    with connect(settings) as conn:
        run_id = start_run(conn, month)
        step, source_file_id = "validate_inputs", None
        try:
            info = validate_source_file(settings.source_file_path(month))
            step = "load_dimensions"
            load_dim_location(conn, settings.zone_lookup_path)
            step = "register_file"
            source_file_id = register_source_file(conn, info, month)
            start, end = month_bounds(month)
            ctx = TransformContext(month_start=start, month_end=end,
                                   valid_zone_ids=load_zone_ids(settings.zone_lookup_path),
                                   rules=settings.rules, source_file_id=source_file_id)
            step = "stage_month"
            stage = stage_month(conn, info, ctx, settings.batch_size, limit, settings.save_rejected_rows)
            step = "publish_month"
            publish = publish_month(conn, month=month, source_file_id=source_file_id,
                                    run_id=run_id, stage=stage, fast_fk_load=settings.fast_fk_load)
            step = "warning_checks"
            check_published_month(conn, month=month, run_id=run_id, stage=stage, publish=publish,
                                  settings=settings)
            finish_run(conn, run_id, source_file_id=source_file_id, stage=stage, publish=publish)
        except Exception as exc:
            fail_run(conn, run_id, step, exc, source_file_id)
            logger.error("run %s for %s FAILED at %s: %s", run_id, month, step, exc)
            raise
    return {"month": month, "run_id": run_id, "stage": stage, "publish": publish,
            "seconds": time.perf_counter() - began}


def main() -> None:
    parser = argparse.ArgumentParser(description="Load months into PostgreSQL (without Prefect).")
    parser.add_argument("--months", help="comma-separated YYYY-MM (default: MONTHS from .env)")
    parser.add_argument("--limit", type=int, help="load only the first N rows of each file")
    args = parser.parse_args()
    setup_logging(console=True)

    months = parse_months(args.months) if args.months else get_settings().months
    for month in months:
        with log_context(month=month):
            r = load_month(month, args.limit)
        s, p = r["stage"], r["publish"]
        print(f"\n=== {month}: run {r['run_id']} SUCCESS in {r['seconds']:.1f} s ===")
        print(f"  read {s.rows_read:,} | staged {s.rows_staged:,} | rejected by rules {sum(s.rejects.values()):,}"
              f" | de-duplicated {p.rows_deduplicated:,} | loaded {p.rows_loaded:,}"
              f" | replaced {p.rows_replaced:,}")


if __name__ == "__main__":
    main()
