"""Shared test fixtures: small hand-made trips and Parquet files in both 2023 schema variants."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pipeline.config import DEFAULT_ZONE_IDS, SOURCE_SCHEMA, CleaningRules, month_bounds
from pipeline.extract import normalise_batch
from pipeline.transform import TransformContext

# Fixture modules for database tests (shared by several test files).
pytest_plugins = ["tests.db_fixtures", "tests.load_fixtures"]

# One valid January 2023 trip; tests override single fields to break one rule at a time.
VALID_TRIP = {
    "vendorid": 2,
    "tpep_pickup_datetime": datetime(2023, 1, 15, 10, 0, 0),
    "tpep_dropoff_datetime": datetime(2023, 1, 15, 10, 15, 0),
    "passenger_count": 1,
    "trip_distance": 2.5,
    "ratecodeid": 1,
    "store_and_fwd_flag": "N",
    "pulocationid": 161,
    "dolocationid": 236,
    "payment_type": 1,
    "fare_amount": 14.2,
    "extra": 0.0,
    "mta_tax": 0.5,
    "tip_amount": 3.0,
    "tolls_amount": 0.0,
    "improvement_surcharge": 1.0,
    "total_amount": 21.2,          # = sum of the parts below (incl. congestion 2.5)
    "congestion_surcharge": 2.5,
    "airport_fee": 0.0,
    "cbd_congestion_fee": None,
    "request_source": None,
}


def make_frame(*overrides: dict) -> pd.DataFrame:
    """Build a DataFrame shaped exactly like extract.iter_batches output (canonical columns,
    target types, 1-based source_row_number). One row per overrides dict."""
    rows = [{**VALID_TRIP, **o} for o in (overrides or ({},))]
    arrow_schema = pa.schema([(name, spec.arrow_type) for name, spec in SOURCE_SCHEMA.items()])
    batch = pa.RecordBatch.from_pylist(rows, schema=arrow_schema)
    table = normalise_batch(batch, {c: c for c in SOURCE_SCHEMA}, "test")
    frame = table.to_pandas(types_mapper={pa.int32(): pd.Int32Dtype(), pa.string(): pd.StringDtype()}.get)
    frame.insert(0, "source_row_number", range(1, len(frame) + 1))
    return frame


@pytest.fixture
def ctx() -> TransformContext:
    start, end = month_bounds("2023-01")
    return TransformContext(month_start=start, month_end=end, valid_zone_ids=DEFAULT_ZONE_IDS,
                            rules=CleaningRules(), source_file_id=7)


def _write(path: Path, columns: dict[str, pa.Array]) -> Path:
    pq.write_table(pa.table(columns), path)
    return path


@pytest.fixture
def jan_style_file(tmp_path: Path) -> Path:
    """Mimics January 2023: lower-case airport_fee, double passenger_count/RatecodeID, int64 IDs."""
    n = 5
    return _write(tmp_path / "jan_style.parquet", {
        "VendorID": pa.array([1, 2, 2, 1, 2], pa.int64()),
        "tpep_pickup_datetime": pa.array([datetime(2023, 1, 1, h) for h in range(n)], pa.timestamp("us")),
        "tpep_dropoff_datetime": pa.array([datetime(2023, 1, 1, h, 20) for h in range(n)], pa.timestamp("us")),
        "passenger_count": pa.array([1.0, 2.0, None, 0.0, 1.0], pa.float64()),
        "trip_distance": pa.array([1.1, 2.2, 3.3, 4.4, 5.5]),
        "RatecodeID": pa.array([1.0, 1.0, None, 99.0, 2.0], pa.float64()),
        "store_and_fwd_flag": pa.array(["N", "N", None, "Y", "N"], pa.string()),
        "PULocationID": pa.array([161, 43, 48, 138, 132], pa.int64()),
        "DOLocationID": pa.array([141, 237, 238, 7, 265], pa.int64()),
        "payment_type": pa.array([2, 1, 0, 1, 1], pa.int64()),
        **{c: pa.array([10.0] * n) for c in ("fare_amount", "extra", "mta_tax", "tip_amount",
                                              "tolls_amount", "improvement_surcharge", "total_amount",
                                              "congestion_surcharge")},
        "airport_fee": pa.array([0.0, 1.25, None, 0.0, 0.0]),
    })


@pytest.fixture
def feb_style_file(tmp_path: Path) -> Path:
    """Mimics February-December 2023: capitalised Airport_fee, int64 counts, int32 IDs, large_string."""
    n = 5
    return _write(tmp_path / "feb_style.parquet", {
        "VendorID": pa.array([1, 2, 2, 1, 2], pa.int32()),
        "tpep_pickup_datetime": pa.array([datetime(2023, 1, 1, h) for h in range(n)], pa.timestamp("us")),
        "tpep_dropoff_datetime": pa.array([datetime(2023, 1, 1, h, 20) for h in range(n)], pa.timestamp("us")),
        "passenger_count": pa.array([1, 2, None, 0, 1], pa.int64()),
        "trip_distance": pa.array([1.1, 2.2, 3.3, 4.4, 5.5]),
        "RatecodeID": pa.array([1, 1, None, 99, 2], pa.int64()),
        "store_and_fwd_flag": pa.array(["N", "N", None, "Y", "N"], pa.large_string()),
        "PULocationID": pa.array([161, 43, 48, 138, 132], pa.int32()),
        "DOLocationID": pa.array([141, 237, 238, 7, 265], pa.int32()),
        "payment_type": pa.array([2, 1, 0, 1, 1], pa.int64()),
        **{c: pa.array([10.0] * n) for c in ("fare_amount", "extra", "mta_tax", "tip_amount",
                                              "tolls_amount", "improvement_surcharge", "total_amount",
                                              "congestion_surcharge")},
        "Airport_fee": pa.array([0.0, 1.25, None, 0.0, 0.0]),
        "unused_extra_column": pa.array(["x"] * n),
    })
