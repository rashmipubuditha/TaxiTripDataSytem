"""Publish transaction: a failed critical check must roll the whole month back."""
from collections import Counter
from datetime import datetime

import pytest

from pipeline.exceptions import DataQualityError
from pipeline.load import StageResult, ensure_fact_partition, publish_month, start_run

pytestmark = pytest.mark.integration

ROW = {
    "source_row_number": 1, "pickup_datetime": datetime(2023, 3, 5, 8, 0),
    "dropoff_datetime": datetime(2023, 3, 5, 8, 20), "pickup_date_key": 20230305, "pickup_hour_key": 8,
    "pu_location_key": 1, "do_location_key": 2, "payment_type_key": 1, "vendor_key": 2,
    "rate_code_id": 1, "trip_duration_min": 20.0, "trip_distance": 3.1, "fare_amount": 17.0,
    "extra": 0, "mta_tax": 0.5, "tip_amount": 3, "tolls_amount": 0, "improvement_surcharge": 1,
    "total_amount": 21.5,
}


@pytest.fixture
def month_setup(test_conn):
    """Two zones, one registered source file (with its partition) and a RUNNING run."""
    conn = test_conn
    conn.execute("INSERT INTO dim_location VALUES (1,'EWR','Newark Airport','EWR'), "
                 "(2,'Queens','Jamaica Bay','Boro Zone') ON CONFLICT DO NOTHING")
    file_id = conn.execute(
        "INSERT INTO etl_source_files (file_name, source_month) VALUES ('test_2023-03.parquet', '2023-03') "
        "ON CONFLICT (file_name) DO UPDATE SET source_month = EXCLUDED.source_month RETURNING source_file_id"
    ).fetchone()[0]
    ensure_fact_partition(conn, file_id, "2023-03")
    conn.execute("DELETE FROM fact_taxi_trips WHERE source_file_id = %s", (file_id,))
    return conn, file_id, start_run(conn, "2023-03")


def stage(conn, file_id, *overrides):
    for number, override in enumerate(overrides, start=1):
        data = {**ROW, "source_row_number": number, "source_file_id": file_id, **override}
        conn.execute(f"INSERT INTO stg_trips ({', '.join(data)}) VALUES ({', '.join(['%s'] * len(data))})",
                     list(data.values()))
    return StageResult(rows_read=len(overrides), rows_staged=len(overrides), rejects=Counter())


def fact_count(conn, file_id):
    return conn.execute("SELECT count(*) FROM fact_taxi_trips WHERE source_file_id = %s", (file_id,)).fetchone()[0]


def test_valid_month_is_published(month_setup):
    conn, file_id, run_id = month_setup
    result = publish_month(conn, month="2023-03", source_file_id=file_id, run_id=run_id,
                           stage=stage(conn, file_id, {}, {"fare_amount": 25.0, "total_amount": 29.5}))
    assert result.rows_loaded == 2 and fact_count(conn, file_id) == 2


def test_duplicate_rows_are_removed_on_publish(month_setup):
    conn, file_id, run_id = month_setup
    result = publish_month(conn, month="2023-03", source_file_id=file_id, run_id=run_id,
                           stage=stage(conn, file_id, {}, {}))         # same trip twice
    assert (result.rows_deduplicated, result.rows_loaded) == (1, 1)


@pytest.mark.parametrize("fast_fk_load", [True, False])
def test_unknown_zone_rolls_back_and_keeps_previous_data(month_setup, fast_fk_load):
    conn, file_id, run_id = month_setup
    publish_month(conn, month="2023-03", source_file_id=file_id, run_id=run_id,
                  stage=stage(conn, file_id, {}))                     # previous good load: 1 row
    conn.execute("TRUNCATE stg_trips")

    # Zone 999 does not exist. It bypasses the Python rules because it is staged directly.
    # fast_fk_load=True: QC3 must catch it. False: PostgreSQL's FK check must catch it.
    bad = stage(conn, file_id, {}, {"source_row_number": 2, "pu_location_key": 999})
    with pytest.raises(Exception) as caught:
        publish_month(conn, month="2023-03", source_file_id=file_id, run_id=run_id,
                      stage=bad, fast_fk_load=fast_fk_load)
    if fast_fk_load:
        assert isinstance(caught.value, DataQualityError) and "QC3" in str(caught.value)
    assert fact_count(conn, file_id) == 1                              # previous data intact
    if fast_fk_load:
        stored = conn.execute("SELECT passed FROM etl_quality_results WHERE run_id = %s AND check_name = 'QC3'",
                              (run_id,)).fetchone()
        assert stored == (False,)                                       # failure recorded for monitoring
