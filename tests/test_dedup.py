"""Rule D1 (de-duplication) runs in SQL on stg_trips, so these tests use the test database."""
from datetime import datetime, timedelta

import pytest

from pipeline.load import deduplicate_staging

pytestmark = pytest.mark.integration

PICKUP = datetime(2023, 5, 10, 3, 39, 29)
BASE = {
    "vendor_key": 1, "pickup_datetime": PICKUP, "dropoff_datetime": PICKUP + timedelta(minutes=25),
    "pu_location_key": 232, "do_location_key": 143, "trip_distance": 5.7, "fare_amount": 27.73,
    "total_amount": 41.48, "passenger_count": 1, "congestion_surcharge": 2.5, "airport_fee": 0,
    "trip_duration_min": 25.0, "store_and_fwd_flag": False,
}


def insert(conn, *rows):
    for number, row in enumerate(rows, start=1):
        data = {**BASE, "source_row_number": number, **row}
        cols = ", ".join(data)
        conn.execute(f"INSERT INTO stg_trips ({cols}) VALUES ({', '.join(['%s'] * len(data))})",
                     list(data.values()))


def remaining(conn):
    return [r[0] for r in conn.execute("SELECT source_row_number FROM stg_trips ORDER BY 1").fetchall()]


def run_dedup(conn):
    with conn.cursor() as cur:
        return deduplicate_staging(cur)


def test_identical_pair_keeps_the_first_row(test_conn):
    insert(test_conn, {}, {})
    assert run_dedup(test_conn) == 1
    assert remaining(test_conn) == [1]


def test_same_trip_times_but_different_fare_are_both_kept(test_conn):
    insert(test_conn, {}, {"fare_amount": 30.00})
    assert run_dedup(test_conn) == 0
    assert remaining(test_conn) == [1, 2]


def test_most_complete_row_wins_even_with_a_higher_total(test_conn):
    # Row 1 has a lower total but is missing passenger_count -> row 2 (complete) is kept.
    insert(test_conn, {"passenger_count": None, "total_amount": 30.0}, {"total_amount": 41.48})
    assert run_dedup(test_conn) == 1
    assert remaining(test_conn) == [2]


def test_doubled_total_copy_is_removed(test_conn):
    # May 2026 pattern: second copy has total_amount exactly x2.
    insert(test_conn, {"total_amount": 82.96}, {"total_amount": 41.48})
    assert run_dedup(test_conn) == 1
    assert remaining(test_conn) == [2]


def test_card_row_is_kept_over_flex_fare_copy(test_conn):
    # July/August 2026 pattern: same trip as credit card (complete) and Flex Fare (NULLs).
    insert(test_conn,
           {"payment_type_key": 0, "passenger_count": None, "congestion_surcharge": None,
            "airport_fee": None, "store_and_fwd_flag": None, "total_amount": 32.20},
           {"payment_type_key": 1, "total_amount": 31.19})
    assert run_dedup(test_conn) == 1
    assert remaining(test_conn) == [2]


def test_three_copies_leave_one(test_conn):
    insert(test_conn, {}, {}, {})
    assert run_dedup(test_conn) == 2
    assert remaining(test_conn) == [1]


def test_zero_duration_duplicates_are_detected(test_conn):
    # Helix-style rows: drop-off = pickup and NULL duration; NULLs must not hide duplicates.
    same = {"dropoff_datetime": PICKUP, "trip_duration_min": None}
    insert(test_conn, same, same)
    assert run_dedup(test_conn) == 1
