"""Fixtures for end-to-end load tests: small Parquet files cut from the real 2023 data.

Each fixture file keeps the real file's schema variant (January: lower-case airport_fee
and double columns; February: Airport_fee and integers) and adds three hand-made rows:
  * an exact duplicate of the first trip         -> removed by rule D1
  * a trip with fare_amount = 2,000              -> rejected by R8 (fare_too_high)
  * a trip with a pickup in the previous year    -> rejected by R2 (pickup_outside_month)
Tests run against the throw-away taxi_test database with a settings copy pointing at
these files, so the real data and the real database are never touched.
"""
from __future__ import annotations

import dataclasses
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pipeline.config import PROJECT_ROOT, Settings, get_settings
from pipeline.db import connect
from tests.db_fixtures import TEST_DB, reset_data

SAMPLE_ROWS = 2_000
INJECTED = {"duplicates": 1, "fare_too_high": 1, "pickup_outside_month": 1}


def write_fixture_file(month: str, folder: Path) -> Path:
    """First SAMPLE_ROWS rows of the real month + the three injected rows, same schema."""
    source = PROJECT_ROOT / f"Dataset_{month[:4]}" / f"yellow_tripdata_{month}.parquet"
    if not source.is_file():
        pytest.skip(f"real data file {source.name} not available")
    parquet = pq.ParquetFile(source)
    batch = next(parquet.iter_batches(batch_size=SAMPLE_ROWS))
    schema = parquet.schema_arrow
    frame = pa.Table.from_batches([batch], schema=schema).to_pandas()

    cols = {c.lower(): c for c in frame.columns}
    # Base the injected rows on trips that certainly pass every rule, so each injected
    # problem is counted under exactly the intended reason.
    pickup, dropoff = frame[cols["tpep_pickup_datetime"]], frame[cols["tpep_dropoff_datetime"]]
    start = pd.Timestamp(f"{month}-01")
    valid = frame[frame[cols["fare_amount"]].between(5, 100) & frame[cols["trip_distance"]].between(0.5, 50)
                  & frame[cols["total_amount"]].gt(0) & (dropoff > pickup)
                  & (dropoff - pickup < pd.Timedelta(hours=3))
                  & (pickup >= start) & (pickup < start + pd.offsets.MonthBegin(1))
                  & frame[cols["vendorid"]].isin([1, 2]) & frame[cols["payment_type"]].isin([1, 2])]
    duplicate = valid.iloc[[0]].copy()
    too_expensive = valid.iloc[[1]].copy()
    too_expensive[cols["fare_amount"]] = 2000.0
    old_pickup = valid.iloc[[2]].copy()
    old_pickup[cols["tpep_pickup_datetime"]] = datetime(2008, 12, 31, 23, 0)
    frame = pd.concat([frame, duplicate, too_expensive, old_pickup], ignore_index=True)

    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"yellow_tripdata_{month}.parquet"
    pq.write_table(pa.Table.from_pandas(frame, schema=schema, preserve_index=False), path)
    return path


@pytest.fixture
def load_env(tmp_path: Path, test_db: str) -> Settings:
    """Settings for taxi_test + fixture files for 2023-01 and 2023-02, on a freshly emptied database."""
    for month in ("2023-01", "2023-02"):
        write_fixture_file(month, tmp_path / "Dataset_2023")
    settings = dataclasses.replace(
        get_settings(), postgres_db=TEST_DB, raw_dir_pattern=str(tmp_path / "Dataset_{year}"),
        save_rejected_rows=False)
    with connect(settings) as conn:
        reset_data(conn)
    return settings


def month_totals(settings: Settings, month: str) -> dict:
    """Fact and summary totals for one month, used to compare runs."""
    key = int(month.replace("-", "")) * 100  # e.g. 20230100
    with connect(settings) as conn:
        fact = conn.execute("""SELECT count(*), coalesce(sum(total_amount), 0), coalesce(sum(fare_amount), 0)
                               FROM fact_taxi_trips WHERE pickup_date_key > %s AND pickup_date_key < %s""",
                            (key, key + 100)).fetchone()
        summary = conn.execute("""SELECT coalesce(sum(trip_count), 0), coalesce(sum(sum_total), 0)
                                  FROM agg_trips_daily_hourly WHERE date_key > %s AND date_key < %s""",
                               (key, key + 100)).fetchone()
    return {"trips": fact[0], "revenue": fact[1], "fare": fact[2],
            "summary_trips": summary[0], "summary_revenue": summary[1]}
