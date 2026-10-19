from datetime import datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pipeline.config import SOURCE_SCHEMA
from pipeline.exceptions import SchemaError, SourceFileError
from pipeline.extract import iter_batches, validate_source_file


def read_all(path, batch_size=1000, limit=None):
    return pd.concat(iter_batches(validate_source_file(path), batch_size, limit), ignore_index=True)


def test_both_2023_schema_variants_normalise_to_the_same_result(jan_style_file, feb_style_file):
    jan, feb = read_all(jan_style_file), read_all(feb_style_file)
    pd.testing.assert_frame_equal(jan, feb)
    assert list(jan.columns) == ["source_row_number", *SOURCE_SCHEMA]
    assert jan["airport_fee"].tolist()[:2] == [0.0, 1.25]          # airport_fee AND Airport_fee found
    assert str(jan["passenger_count"].dtype) == "Int32"            # double 1.0 -> integer, NULL kept
    assert jan["passenger_count"].isna().sum() == 1


def test_missing_optional_columns_are_added_as_null(jan_style_file):
    frame = read_all(jan_style_file)
    assert frame["cbd_congestion_fee"].isna().all()
    assert frame["request_source"].isna().all()


def test_unused_columns_are_reported_and_dropped(feb_style_file):
    info = validate_source_file(feb_style_file)
    assert info.ignored_columns == ("unused_extra_column",)
    assert "unused_extra_column" not in read_all(feb_style_file).columns


def test_missing_required_column_raises_schema_error(tmp_path):
    path = tmp_path / "no_fare.parquet"
    pq.write_table(pa.table({"VendorID": [1], "trip_distance": [1.0]}), path)
    with pytest.raises(SchemaError, match="fare_amount"):
        validate_source_file(path)


def test_missing_file_raises_source_file_error(tmp_path):
    with pytest.raises(SourceFileError, match="not found"):
        validate_source_file(tmp_path / "yellow_tripdata_2024-01.parquet")


def test_empty_file_raises_source_file_error(tmp_path):
    path = tmp_path / "empty.parquet"
    path.touch()
    with pytest.raises(SourceFileError, match="empty"):
        validate_source_file(path)


def test_non_parquet_file_raises_source_file_error(tmp_path):
    path = tmp_path / "fake.parquet"
    path.write_text("this is not parquet")
    with pytest.raises(SourceFileError, match="not a readable Parquet"):
        validate_source_file(path)


def test_lossy_conversion_raises_schema_error(jan_style_file, tmp_path):
    table = pq.read_table(jan_style_file)
    idx = table.column_names.index("passenger_count")
    bad = table.set_column(idx, "passenger_count", pa.array([2.5, 1, 1, 1, 1], pa.float64()))
    path = tmp_path / "fractional_passengers.parquet"
    pq.write_table(bad, path)
    with pytest.raises(SchemaError, match="passenger_count"):
        read_all(path)


def test_batches_keep_continuous_row_numbers(jan_style_file):
    batches = list(iter_batches(validate_source_file(jan_style_file), batch_size=2))
    assert [len(b) for b in batches] == [2, 2, 1]
    assert pd.concat(batches)["source_row_number"].tolist() == [1, 2, 3, 4, 5]


def test_limit_stops_early(jan_style_file):
    frame = read_all(jan_style_file, batch_size=2, limit=3)
    assert frame["source_row_number"].tolist() == [1, 2, 3]


def test_timestamps_are_kept_exactly(jan_style_file):
    assert read_all(jan_style_file)["tpep_pickup_datetime"].iloc[3] == datetime(2023, 1, 1, 3)
