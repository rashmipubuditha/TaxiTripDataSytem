from datetime import datetime

import pandas as pd
import pytest

from pipeline.transform import STAGING_COLUMNS, assign_reject_reasons, process_batch, round_values
from tests.conftest import make_frame


def reason_for(ctx, **override):
    return assign_reject_reasons(round_values(make_frame(override)), ctx).iloc[0]


def test_valid_trip_passes_every_rule(ctx):
    assert pd.isna(reason_for(ctx))


# One case per rule R1-R13: change one field so exactly that rule fails.
@pytest.mark.parametrize("override, expected", [
    ({"fare_amount": None},                                          "missing_required_field"),
    ({"pulocationid": None},                                         "missing_required_field"),
    ({"tpep_pickup_datetime": datetime(2022, 12, 31, 23, 59),
      "tpep_dropoff_datetime": datetime(2023, 1, 1, 0, 10)},         "pickup_outside_month"),
    ({"tpep_pickup_datetime": datetime(2023, 2, 1, 0, 0),
      "tpep_dropoff_datetime": datetime(2023, 2, 1, 0, 10)},         "pickup_outside_month"),
    ({"tpep_dropoff_datetime": datetime(2023, 1, 15, 9, 59)},       "negative_duration"),
    ({"tpep_dropoff_datetime": datetime(2023, 1, 16, 10, 0, 1)},    "duration_too_long"),
    ({"trip_distance": 0.0},                                         "distance_not_positive"),
    ({"trip_distance": 100.01},                                      "distance_too_long"),
    ({"fare_amount": -7.2},                                          "fare_not_positive"),
    ({"fare_amount": 1000.01},                                       "fare_too_high"),
    ({"total_amount": 0.0},                                          "total_not_positive"),
    ({"dolocationid": 999},                                          "unknown_location"),
    ({"payment_type": 9},                                            "unknown_payment_type"),
    ({"vendorid": 5},                                                "unknown_vendor"),
    ({"tolls_amount": 1e9},                                          "amount_out_of_range"),
])
def test_each_rule_rejects_its_case(ctx, override, expected):
    assert reason_for(ctx, **override) == expected


def test_first_failing_rule_wins(ctx):
    # Fails distance_not_positive (R5) AND fare_not_positive (R7): counted once, under R5.
    assert reason_for(ctx, trip_distance=0.0, fare_amount=-5.0) == "distance_not_positive"


def test_boundaries_are_inclusive_where_the_plan_says_so(ctx):
    assert pd.isna(reason_for(ctx, tpep_pickup_datetime=datetime(2023, 1, 1, 0, 0),
                              tpep_dropoff_datetime=datetime(2023, 1, 1, 0, 5)))   # month start kept
    assert pd.isna(reason_for(ctx, tpep_dropoff_datetime=datetime(2023, 1, 16, 10, 0)))  # exactly 24 h kept
    assert pd.isna(reason_for(ctx, trip_distance=100.0))
    assert pd.isna(reason_for(ctx, fare_amount=1000.0))


def test_rounding_happens_before_rules(ctx):
    # 0.004 miles rounds to 0.00, which the database would refuse (> 0), so it must be rejected here.
    assert reason_for(ctx, trip_distance=0.004) == "distance_not_positive"


def test_counts_always_add_up(ctx):
    frame = make_frame({}, {"trip_distance": 0.0}, {"fare_amount": -1.0}, {}, {"vendorid": 5})
    result = process_batch(frame, ctx)
    assert len(result.staged) + len(result.rejected) == len(frame)
    assert sum(result.rejects.values()) == len(result.rejected) == 3
    assert result.rejects == {"distance_not_positive": 1, "fare_not_positive": 1, "unknown_vendor": 1}


def test_staged_rows_have_star_schema_keys(ctx):
    frame = make_frame({"tpep_pickup_datetime": datetime(2023, 1, 31, 23, 59, 59),
                        "tpep_dropoff_datetime": datetime(2023, 2, 1, 0, 10, 0)},
                       {"tpep_pickup_datetime": datetime(2023, 1, 1, 0, 0, 0),
                        "tpep_dropoff_datetime": datetime(2023, 1, 1, 0, 30, 0)})
    staged = process_batch(frame, ctx).staged
    assert tuple(staged.columns) == STAGING_COLUMNS
    assert staged["pickup_date_key"].tolist() == [20230131, 20230101]
    assert staged["pickup_hour_key"].tolist() == [23, 0]
    assert staged["trip_duration_min"].tolist() == [10.02, 30.0]
    assert (staged["source_file_id"] == 7).all()
    assert staged["source_row_number"].tolist() == [1, 2]


def test_zero_duration_trip_is_kept_with_null_duration(ctx):
    frame = make_frame({"tpep_dropoff_datetime": datetime(2023, 1, 15, 10, 0, 0)})
    result = process_batch(frame, ctx)
    assert len(result.staged) == 1
    assert pd.isna(result.staged["trip_duration_min"].iloc[0])
    assert result.warnings["zero_duration"] == 1


def test_rate_code_null_and_invalid_become_99(ctx):
    result = process_batch(make_frame({"ratecodeid": None}, {"ratecodeid": 7}, {"ratecodeid": 2}), ctx)
    assert result.staged["rate_code_id"].tolist() == [99, 99, 2]
    assert result.warnings["rate_code_invalid_set_to_99"] == 1
    assert result.warnings["rate_code_unknown_99"] == 2


def test_passenger_count_rules(ctx):
    result = process_batch(make_frame({"passenger_count": 0}, {"passenger_count": None},
                                      {"passenger_count": 42}, {"passenger_count": 3}), ctx)
    assert result.staged["passenger_count"].tolist()[0] == 0
    assert result.staged["passenger_count"].isna().tolist() == [False, True, True, False]
    assert result.warnings["passenger_count_zero"] == 1
    assert result.warnings["passenger_count_invalid_set_null"] == 1


def test_store_and_forward_flag_becomes_boolean(ctx):
    staged = process_batch(make_frame({"store_and_fwd_flag": "Y"}, {"store_and_fwd_flag": "N"},
                                      {"store_and_fwd_flag": None}), ctx).staged
    assert staged["store_and_fwd_flag"].tolist()[:2] == [True, False]
    assert pd.isna(staged["store_and_fwd_flag"].iloc[2])


def test_flex_fare_row_with_nulls_is_kept(ctx):
    # 2023 Flex Fare rows have NULL passenger_count, rate code, flag, congestion and airport fee.
    flex = {"payment_type": 0, "passenger_count": None, "ratecodeid": None, "store_and_fwd_flag": None,
            "congestion_surcharge": None, "airport_fee": None, "total_amount": 18.7}
    result = process_batch(make_frame(flex), ctx)
    assert len(result.staged) == 1
    assert result.staged["payment_type_key"].iloc[0] == 0


def test_total_mismatch_is_only_a_warning(ctx):
    # Vendor 1 pattern: congestion surcharge listed but not included in the total (plan Q15).
    result = process_batch(make_frame({"vendorid": 1, "total_amount": 18.7}), ctx)
    assert len(result.staged) == 1
    assert result.warnings["total_not_equal_sum_of_parts"] == 1


def test_money_is_rounded_to_cents(ctx):
    staged = process_batch(make_frame({"fare_amount": 14.199999999, "total_amount": 21.2000001}), ctx).staged
    assert staged["fare_amount"].iloc[0] == 14.2
    assert staged["total_amount"].iloc[0] == 21.2
