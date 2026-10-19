"""End-to-end loads of small real files into taxi_test: extract -> clean -> stage -> publish -> checks."""
import pytest

from pipeline.db import connect
from pipeline.exceptions import SourceFileError
from pipeline.load import load_month
from tests.load_fixtures import INJECTED, SAMPLE_ROWS, month_totals

pytestmark = pytest.mark.integration


def latest_run(settings, month):
    with connect(settings) as conn:
        return conn.execute("""SELECT run_id, status, error_step, error_message, rows_read, rows_staged,
                                      rows_deduplicated, rows_loaded, rows_rejected
                               FROM etl_pipeline_runs WHERE source_month = %s ORDER BY run_id DESC LIMIT 1""",
                            (month,)).fetchone()


def test_month_loads_end_to_end(load_env):
    result = load_month("2023-01", settings=load_env)
    stage, publish = result["stage"], result["publish"]

    assert stage.rows_read == SAMPLE_ROWS + 3
    assert stage.rejects["fare_too_high"] == INJECTED["fare_too_high"]
    assert stage.rejects["pickup_outside_month"] >= INJECTED["pickup_outside_month"]
    assert publish.rows_deduplicated == INJECTED["duplicates"]
    assert stage.rows_read == publish.rows_loaded + sum(stage.rejects.values()) + publish.rows_deduplicated

    run_id, status, _, _, read, staged, dedup, loaded, rejected = latest_run(load_env, "2023-01")
    assert status == "SUCCESS"
    assert (read, staged, dedup, loaded) == (stage.rows_read, stage.rows_staged, 1, publish.rows_loaded)
    assert read == loaded + rejected

    totals = month_totals(load_env, "2023-01")
    assert totals["trips"] == publish.rows_loaded
    assert (totals["summary_trips"], totals["summary_revenue"]) == (totals["trips"], totals["revenue"])

    with connect(load_env) as conn:
        reasons = dict(conn.execute("SELECT reason, row_count FROM etl_rejected_summary WHERE run_id = %s",
                                    (run_id,)).fetchall())
        checks = conn.execute("""SELECT count(*) FILTER (WHERE severity = 'CRITICAL' AND passed),
                                        count(*) FILTER (WHERE severity = 'WARNING')
                                 FROM etl_quality_results WHERE run_id = %s""", (run_id,)).fetchone()
        zones = conn.execute("SELECT count(*) FROM dim_location").fetchone()[0]
    assert sum(reasons.values()) == rejected and reasons["duplicate_record"] == 1
    assert checks == (6, 3)          # all 6 critical checks passed; 3 warning checks recorded
    assert zones == 265


def test_both_2023_schema_variants_load(load_env):
    jan = load_month("2023-01", settings=load_env)   # airport_fee, double columns
    feb = load_month("2023-02", settings=load_env)   # Airport_fee, integer columns
    assert jan["publish"].rows_loaded > 0 and feb["publish"].rows_loaded > 0
    with connect(load_env) as conn:
        partitions = conn.execute("SELECT count(*) FROM pg_inherits "
                                  "WHERE inhparent = 'fact_taxi_trips'::regclass").fetchone()[0]
        airport_fees = conn.execute("SELECT count(*) FROM fact_taxi_trips WHERE airport_fee IS NOT NULL").fetchone()[0]
    assert partitions == 2
    assert airport_fees > 0          # the fee column was found in both spellings


def test_missing_file_is_recorded_as_failed_run(load_env):
    with pytest.raises(SourceFileError, match="not found"):
        load_month("2024-01", settings=load_env)
    _, status, error_step, error_message, *_ = latest_run(load_env, "2024-01")
    assert (status, error_step) == ("FAILED", "validate_inputs")
    assert error_message.startswith("SourceFileError:")
