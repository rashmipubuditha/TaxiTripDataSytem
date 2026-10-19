"""Prefect orchestration for the NYC Taxi batch ETL.

    python -m pipeline.flows                          # all months in MONTHS (.env)
    python -m pipeline.flows --months 2023-01,2023-02 # dev profile
    python -m pipeline.flows --months 2023-03 --limit 50000

Structure (visible in the Prefect UI at http://127.0.0.1:4200 when the server runs):
  taxi-etl (parent flow)
    └─ process-<month> (one sub-flow per month, in order)
         start-run → validate-inputs → load-dimensions → register-file
         → stage-month → publish-month → run-warning-checks → finish-run

Failure handling:
  * Each task records its step; any error marks the run FAILED in etl_pipeline_runs.
  * on_failure / on_crashed hooks log a CRITICAL "alert" record and, as a safety net,
    close any run row still in RUNNING state.
  * Only "database unreachable" errors are retried (3x, 10 s apart); data errors are not.
  * If one month fails, the remaining months still run; the parent flow then ends Failed.
Tasks exchange only small values (ids, counts, file info) — never DataFrames.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
import urllib.request

from pipeline.config import PROJECT_ROOT, get_settings, load_zone_ids, month_bounds, parse_months, validate_month

DEFAULT_PREFECT_API_URL = "http://127.0.0.1:4200/api"


def _configure_prefect() -> str | None:
    """Prepare Prefect's environment. Must run BEFORE `import prefect`.

    Uses the local Prefect server if it is running (run history visible in the UI);
    otherwise Prefect falls back to a temporary in-process server and the run still works.
    """
    get_settings()  # loads .env into the environment
    os.environ.setdefault("PREFECT_HOME", str(PROJECT_ROOT / ".prefect"))
    os.environ.setdefault("PREFECT_LOGGING_EXTRA_LOGGERS", "pipeline")  # our logs -> Prefect UI/console
    api_url = os.environ.get("PREFECT_API_URL") or DEFAULT_PREFECT_API_URL
    try:
        with urllib.request.urlopen(f"{api_url.rstrip('/')}/health", timeout=3):
            os.environ["PREFECT_API_URL"] = api_url
            return api_url
    except OSError:
        os.environ.pop("PREFECT_API_URL", None)
        return None


PREFECT_API_URL = _configure_prefect()

import psycopg  # noqa: E402
from prefect import flow, task  # noqa: E402
from prefect.cache_policies import NO_CACHE  # noqa: E402
from prefect.runtime import flow_run as current_flow_run  # noqa: E402

from pipeline import load  # noqa: E402
from pipeline.db import connect  # noqa: E402
from pipeline.exceptions import DatabaseUnavailableError, EtlError  # noqa: E402
from pipeline.extract import SourceFileInfo, validate_source_file  # noqa: E402
from pipeline.logging_config import bind_context, log_context, setup_logging  # noqa: E402
from pipeline.transform import TransformContext  # noqa: E402

logger = logging.getLogger("pipeline.flows")


# --- Retry policy ------------------------------------------------------------------------

def _retry_only_if_database_unavailable(task, task_run, state) -> bool:
    """Prefect retry condition: retry temporary connection problems, never data errors."""
    try:
        state.result()
    except (DatabaseUnavailableError, psycopg.OperationalError):
        return True
    except Exception:
        return False
    return False


DB_TASK = dict(cache_policy=NO_CACHE, persist_result=False, retries=3, retry_delay_seconds=10,
               retry_condition_fn=_retry_only_if_database_unavailable)
DATA_TASK = dict(cache_policy=NO_CACHE, persist_result=False)  # no retries: rerunning won't fix data


# --- Tasks (thin wrappers around the plain functions in load.py / extract.py) ----------

@task(name="start-run", **DB_TASK)
def start_run_task(month: str, flow_run_id: str | None) -> int:
    with connect() as conn:
        return load.start_run(conn, month, flow_run_id)


@task(name="validate-inputs", **DATA_TASK)
def validate_inputs_task(month: str) -> SourceFileInfo:
    info = validate_source_file(get_settings().source_file_path(month))
    logger.info("source file OK: %s (%s rows, %.1f MB)", info.path.name, f"{info.row_count:,}",
                info.size_bytes / 1e6, extra={"event": "file_validated", "file_rows": info.row_count})
    return info


@task(name="load-dimensions", **DB_TASK)
def load_dimensions_task() -> int:
    with connect() as conn:
        return load.load_dim_location(conn, get_settings().zone_lookup_path)


@task(name="register-file", **DB_TASK)
def register_file_task(info: SourceFileInfo, month: str) -> int:
    with connect() as conn:
        source_file_id = load.register_source_file(conn, info, month)
        load.ensure_fact_partition(conn, source_file_id, month)
        return source_file_id


@task(name="stage-month", **DATA_TASK)
def stage_month_task(month: str, info: SourceFileInfo, source_file_id: int, limit: int | None) -> load.StageResult:
    settings = get_settings()
    start, end = month_bounds(month)
    ctx = TransformContext(month_start=start, month_end=end,
                           valid_zone_ids=load_zone_ids(settings.zone_lookup_path),
                           rules=settings.rules, source_file_id=source_file_id)
    with connect() as conn:
        return load.stage_month(conn, info, ctx, settings.batch_size, limit, settings.save_rejected_rows)


@task(name="publish-month", **DATA_TASK)
def publish_month_task(month: str, source_file_id: int, run_id: int, stage: load.StageResult) -> load.PublishResult:
    with connect() as conn:
        return load.publish_month(conn, month=month, source_file_id=source_file_id, run_id=run_id,
                                  stage=stage, fast_fk_load=get_settings().fast_fk_load)


@task(name="run-warning-checks", **DB_TASK)
def warning_checks_task(month: str, run_id: int, stage: load.StageResult, publish: load.PublishResult) -> int:
    """QC7-QC9 after COMMIT. Returns the number of warnings (the run still succeeds)."""
    with connect() as conn:
        results = load.check_published_month(conn, month=month, run_id=run_id, stage=stage,
                                             publish=publish, settings=get_settings())
    return sum(not r.passed for r in results)


@task(name="finish-run", **DB_TASK)
def finish_run_task(run_id: int, source_file_id: int, stage: load.StageResult, publish: load.PublishResult) -> None:
    with connect() as conn:
        load.finish_run(conn, run_id, source_file_id=source_file_id, stage=stage, publish=publish)


@task(name="record-failure", **DB_TASK)
def record_failure_task(run_id: int, step: str, error: str, source_file_id: int | None) -> None:
    with connect() as conn:
        load.fail_run(conn, run_id, step, error, source_file_id)


# --- Failure hooks ------------------------------------------------------------------------

def on_month_failure(flow, flow_run, state) -> None:
    """Runs when a month's sub-flow fails or crashes: CRITICAL alert + close any RUNNING row."""
    month = flow_run.parameters.get("month")
    try:
        with connect() as conn:
            conn.execute(
                """
                UPDATE etl_pipeline_runs
                SET status = 'FAILED', ended_at = now(),
                    duration_seconds = extract(epoch FROM now() - started_at),
                    error_step = coalesce(error_step, 'unknown'),
                    error_message = coalesce(error_message, %s)
                WHERE prefect_flow_run_id = %s AND status = 'RUNNING'
                """,
                (state.message, flow_run.id),
            )
    except Exception:  # the alert below must still be emitted
        logger.exception("could not update etl_pipeline_runs from the failure hook")
    logger.critical("ALERT: pipeline run for %s FAILED (%s): %s", month, state.name, state.message,
                    extra={"event": "alert", "alert": True, "month": month,
                           "flow_run_id": str(flow_run.id), "state": state.name})


def on_pipeline_failure(flow, flow_run, state) -> None:
    logger.critical("ALERT: taxi-etl finished with failures: %s", state.message,
                    extra={"event": "alert", "alert": True, "flow_run_id": str(flow_run.id)})


# --- Flows ----------------------------------------------------------------------------------

@flow(name="process-month", flow_run_name="process-{month}", log_prints=True,
      on_failure=[on_month_failure], on_crashed=[on_month_failure])
def process_month(month: str, limit: int | None = None) -> dict:
    """Load one monthly file: every step is a task, and every outcome is recorded."""
    validate_month(month)
    flow_run_id = str(current_flow_run.id) if current_flow_run.id else None
    began = time.perf_counter()
    with log_context(month=month, flow_run_id=flow_run_id):
        run_id = start_run_task(month, flow_run_id)
        bind_context(run_id=run_id)
        logger.info("month %s started (run %s)", month, run_id, extra={"event": "month_started"})
        step, source_file_id = "validate_inputs", None
        try:
            info = validate_inputs_task(month)
            step = "load_dimensions"
            load_dimensions_task()
            step = "register_file"
            source_file_id = register_file_task(info, month)
            step = "stage_month"
            stage = stage_month_task(month, info, source_file_id, limit)
            step = "publish_month"
            publish = publish_month_task(month, source_file_id, run_id, stage)
            step = "warning_checks"
            warnings = warning_checks_task(month, run_id, stage, publish)
            step = "finish_run"
            finish_run_task(run_id, source_file_id, stage, publish)
        except Exception as exc:
            record_failure_task(run_id, step, f"{type(exc).__name__}: {exc}", source_file_id)
            logger.error("month %s FAILED at step %s: %s", month, step, exc,
                         extra={"event": "month_failed", "error_step": step})
            raise

        summary = {
            "month": month, "run_id": run_id, "rows_read": stage.rows_read,
            "rows_staged": stage.rows_staged, "rows_rejected": sum(stage.rejects.values()) + publish.rows_deduplicated,
            "rows_deduplicated": publish.rows_deduplicated, "rows_loaded": publish.rows_loaded,
            "quality_warnings": warnings, "seconds": round(time.perf_counter() - began, 1),
        }
        logger.info("month %s SUCCESS: %s rows loaded in %.1f s", month, f"{publish.rows_loaded:,}",
                    summary["seconds"], extra={"event": "month_succeeded", **summary})
        return summary


@flow(name="taxi-etl", log_prints=True, on_failure=[on_pipeline_failure])
def taxi_etl(months: list[str] | None = None, limit: int | None = None) -> dict[str, str]:
    """Parent flow: one process-month sub-flow per month, in order.
    A failed month does not stop the others; the parent fails at the end if any month failed."""
    setup_logging()
    month_list = list(parse_months(",".join(months))) if months else list(get_settings().months)
    began = time.perf_counter()
    logger.info("pipeline started for %d month(s): %s", len(month_list), ", ".join(month_list),
                extra={"event": "pipeline_started", "months": month_list})

    outcome: dict[str, str] = {}
    for month in month_list:
        state = process_month(month, limit, return_state=True)
        outcome[month] = "SUCCESS" if state.is_completed() else "FAILED"

    failed = [m for m, status in outcome.items() if status == "FAILED"]
    logger.info("pipeline finished in %.1f s: %d succeeded, %d failed", time.perf_counter() - began,
                len(month_list) - len(failed), len(failed),
                extra={"event": "pipeline_finished", "outcome": outcome,
                       "seconds": round(time.perf_counter() - began, 1)})
    if failed:
        raise EtlError(f"{len(failed)} of {len(month_list)} month(s) failed: {', '.join(failed)}")
    return outcome


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the NYC Taxi ETL with Prefect.")
    parser.add_argument("--months", help="comma-separated YYYY-MM (default: MONTHS from .env)")
    parser.add_argument("--limit", type=int, help="process only the first N rows of each file")
    parser.add_argument("--serve", action="store_true",
                        help="register a scheduled deployment and keep running (needs the Prefect server); "
                             "schedule = SCHEDULE_CRON in .env, default 06:00 on the 2nd of each month")
    args = parser.parse_args()

    setup_logging()
    if args.serve:
        if not PREFECT_API_URL:
            sys.exit("--serve needs the Prefect server: bash scripts/start_prefect_server.sh")
        cron = os.environ.get("SCHEDULE_CRON", "0 6 2 * *")
        print(f"Serving deployment 'taxi-etl/monthly' with schedule '{cron}' — Ctrl+C to stop.\n"
              f"Trigger a run now from the UI: {PREFECT_API_URL.removesuffix('/api')}/deployments")
        taxi_etl.serve(name="monthly", cron=cron, parameters={"months": None, "limit": args.limit})
        return
    months = list(parse_months(args.months)) if args.months else None  # bad input fails here, before any run
    if PREFECT_API_URL:
        print(f"Prefect server: {PREFECT_API_URL.removesuffix('/api')}  (runs appear in the UI)")
    else:
        print("Prefect server not running -> using a temporary local server (no UI history).\n"
              "Start it with: bash scripts/start_prefect_server.sh")
    try:
        outcome = taxi_etl(months=months, limit=args.limit)
    except Exception as exc:
        print(f"\nPIPELINE FAILED: {exc}\nDetails: logs/pipeline.jsonl and table etl_pipeline_runs")
        sys.exit(1)
    print(f"\nPIPELINE SUCCEEDED: {outcome}")


if __name__ == "__main__":
    main()
