"""Re-running the pipeline must give the same result as running it once (idempotency)."""
import psycopg
import pytest

from pipeline import load
from pipeline.db import connect
from pipeline.exceptions import LoadError
from pipeline.load import load_month
from tests.load_fixtures import month_totals

pytestmark = pytest.mark.integration


def partition_size(settings, month):
    with connect(settings) as conn:
        return conn.execute("""SELECT pg_total_relation_size(inhrelid) FROM pg_inherits
                               WHERE inhparent = 'fact_taxi_trips'::regclass
                                 AND inhrelid::regclass::text LIKE %s""",
                            (f"%{month.replace('-', '_')}",)).fetchone()[0]


def test_loading_the_same_month_twice_gives_identical_results(load_env):
    first = load_month("2023-01", settings=load_env)
    totals_first, size_first = month_totals(load_env, "2023-01"), partition_size(load_env, "2023-01")

    second = load_month("2023-01", settings=load_env)
    assert month_totals(load_env, "2023-01") == totals_first          # same rows, revenue, fare, summaries
    assert second["publish"].rows_replaced == first["publish"].rows_loaded
    assert partition_size(load_env, "2023-01") == size_first           # no growth on reload


def test_reloading_one_month_leaves_other_months_untouched(load_env):
    load_month("2023-01", settings=load_env)
    load_month("2023-02", settings=load_env)
    feb_before = month_totals(load_env, "2023-02")
    load_month("2023-01", settings=load_env)
    assert month_totals(load_env, "2023-02") == feb_before


def test_database_refuses_a_second_copy_of_a_source_row(load_env):
    load_month("2023-01", settings=load_env)
    with connect(load_env) as conn, pytest.raises(psycopg.errors.UniqueViolation):
        conn.execute("""INSERT INTO fact_taxi_trips
                        SELECT * FROM fact_taxi_trips WHERE source_row_number = 1""")


def test_error_during_publish_keeps_previous_data(load_env, monkeypatch):
    load_month("2023-01", settings=load_env)
    before = month_totals(load_env, "2023-01")

    def broken_summary_rebuild(*args, **kwargs):
        raise psycopg.errors.DiskFull("simulated: disk full while rebuilding summaries")

    monkeypatch.setattr(load, "_rebuild_summaries", broken_summary_rebuild)
    with pytest.raises(LoadError, match="rolled back"):
        load_month("2023-01", settings=load_env)

    assert month_totals(load_env, "2023-01") == before                 # old data and summaries intact
    with connect(load_env) as conn:
        status, step = conn.execute("""SELECT status, error_step FROM etl_pipeline_runs
                                       ORDER BY run_id DESC LIMIT 1""").fetchone()
    assert (status, step) == ("FAILED", "publish_month")
