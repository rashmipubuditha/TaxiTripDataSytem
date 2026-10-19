"""Transform: clean one batch (reject rules R1-R13) and shape it for the star schema.

Plain functions (no Prefect, no database) so they are easy to unit-test.
Rule order matters: a row that fails several rules is counted once, under the
first rule it fails, so rows_read = rows_staged + rejected always holds.
"""
from __future__ import annotations

from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime

import numpy as np
import pandas as pd

from pipeline.config import (
    MONEY_COLUMNS,
    TOTAL_PARTS,
    UNKNOWN_RATE_CODE,
    VALID_PAYMENT_TYPES,
    VALID_RATE_CODES,
    VALID_VENDORS,
    CleaningRules,
)

# Columns that must have a value; NULL in any of them -> rule R1.
# (The fee columns are NOT NULL in the fact table, so they are required too.)
REQUIRED_VALUE_COLUMNS = (
    "vendorid", "tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance",
    "pulocationid", "dolocationid", "payment_type", "fare_amount", "extra", "mta_tax",
    "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount",
)

# Column order of the stg_trips table (used by COPY in the load step).
STAGING_COLUMNS = (
    "source_file_id", "source_row_number", "pickup_datetime", "dropoff_datetime",
    "pickup_date_key", "pickup_hour_key", "pu_location_key", "do_location_key",
    "payment_type_key", "vendor_key", "rate_code_id", "passenger_count",
    "store_and_fwd_flag", "trip_duration_min", "trip_distance", "fare_amount", "extra",
    "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
    "congestion_surcharge", "airport_fee", "total_amount", "cbd_congestion_fee",
    "request_source",
)

MAX_PASSENGERS = 9  # largest value seen in 2023; anything outside 0-9 becomes NULL (warning)


@dataclass(frozen=True)
class TransformContext:
    """Everything the rules need to know about the month being processed."""

    month_start: datetime
    month_end: datetime            # exclusive
    valid_zone_ids: frozenset[int]
    rules: CleaningRules
    source_file_id: int


@dataclass
class BatchResult:
    staged: pd.DataFrame           # rows ready for stg_trips, columns = STAGING_COLUMNS
    rejected: pd.DataFrame         # rejected source rows + 'reject_reason'
    rejects: Counter = field(default_factory=Counter)   # reason -> count
    warnings: Counter = field(default_factory=Counter)  # warning -> count (rows kept)


# --- Reject rules ---------------------------------------------------------------
# Each rule returns a boolean mask: True = reject. Order = priority (plan §7.2).
Rule = Callable[[pd.DataFrame, TransformContext], pd.Series]


def _duration_s(df: pd.DataFrame) -> pd.Series:
    return (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]).dt.total_seconds()


REJECT_RULES: tuple[tuple[str, Rule], ...] = (
    ("missing_required_field", lambda df, c: df[list(REQUIRED_VALUE_COLUMNS)].isna().any(axis=1)),
    ("pickup_outside_month", lambda df, c: (df["tpep_pickup_datetime"] < c.month_start)
                                          | (df["tpep_pickup_datetime"] >= c.month_end)),
    ("negative_duration", lambda df, c: _duration_s(df) < 0),
    ("duration_too_long", lambda df, c: _duration_s(df) > c.rules.max_duration_hours * 3600),
    ("distance_not_positive", lambda df, c: df["trip_distance"] <= 0),
    ("distance_too_long", lambda df, c: df["trip_distance"] > c.rules.max_trip_distance_mi),
    ("fare_not_positive", lambda df, c: df["fare_amount"] <= 0),
    ("fare_too_high", lambda df, c: df["fare_amount"] > c.rules.max_fare_amount),
    ("total_not_positive", lambda df, c: df["total_amount"] <= 0),
    ("unknown_location", lambda df, c: ~df["pulocationid"].isin(c.valid_zone_ids)
                                      | ~df["dolocationid"].isin(c.valid_zone_ids)),
    ("unknown_payment_type", lambda df, c: ~df["payment_type"].isin(VALID_PAYMENT_TYPES)),
    ("unknown_vendor", lambda df, c: ~df["vendorid"].isin(VALID_VENDORS)),
    ("amount_out_of_range", lambda df, c: (df[list(MONEY_COLUMNS)].abs() > c.rules.max_abs_amount).any(axis=1)),
)
REJECT_REASONS = tuple(name for name, _ in REJECT_RULES)


def round_values(df: pd.DataFrame) -> pd.DataFrame:
    """Round money and distance to 2 decimals BEFORE the rules run, so the rules check
    exactly the values that will be stored in NUMERIC(…, 2) columns."""
    df = df.copy()
    for col in (*MONEY_COLUMNS, "trip_distance"):
        df[col] = df[col].round(2)
    return df


def assign_reject_reasons(df: pd.DataFrame, ctx: TransformContext) -> pd.Series:
    """Return the first failing rule's name per row, or <NA> if the row passes all rules."""
    codes = np.zeros(len(df), dtype=np.int8)  # 0 = passed so far
    for position, (_, rule) in enumerate(REJECT_RULES, start=1):
        # Masks can contain <NA> for NULL values; those rows were already caught by R1.
        mask = rule(df, ctx).fillna(False).to_numpy(dtype=bool)
        codes[mask & (codes == 0)] = position
    labels = np.array([pd.NA, *REJECT_REASONS], dtype=object)
    return pd.Series(labels[codes], index=df.index, dtype="string")


def to_star(df: pd.DataFrame, ctx: TransformContext) -> tuple[pd.DataFrame, Counter]:
    """Derive star-schema keys and clean attribute values for accepted rows."""
    warnings: Counter = Counter()
    pickup = df["tpep_pickup_datetime"]
    duration_s = _duration_s(df)

    rate_code = df["ratecodeid"]
    invalid_rate = rate_code.notna() & ~rate_code.isin(VALID_RATE_CODES)
    warnings["rate_code_invalid_set_to_99"] = int(invalid_rate.sum())
    rate_code = rate_code.mask(invalid_rate).fillna(UNKNOWN_RATE_CODE).astype("int16")
    warnings["rate_code_unknown_99"] = int((rate_code == UNKNOWN_RATE_CODE).sum())

    passengers = df["passenger_count"]
    invalid_pax = passengers.notna() & ((passengers < 0) | (passengers > MAX_PASSENGERS))
    warnings["passenger_count_invalid_set_null"] = int(invalid_pax.sum())
    passengers = passengers.mask(invalid_pax).astype("Int16")
    warnings["passenger_count_null"] = int(passengers.isna().sum())
    warnings["passenger_count_zero"] = int((passengers == 0).sum())

    zero_duration = duration_s == 0
    warnings["zero_duration"] = int(zero_duration.sum())

    parts = df[list(TOTAL_PARTS)].fillna(0).sum(axis=1)
    warnings["total_not_equal_sum_of_parts"] = int(((df["total_amount"] - parts).abs() > 0.01).sum())

    staged = pd.DataFrame({
        "source_file_id": np.int16(ctx.source_file_id),
        "source_row_number": df["source_row_number"].astype("int32"),
        "pickup_datetime": pickup,
        "dropoff_datetime": df["tpep_dropoff_datetime"],
        "pickup_date_key": (pickup.dt.year * 10000 + pickup.dt.month * 100 + pickup.dt.day).astype("int32"),
        "pickup_hour_key": pickup.dt.hour.astype("int16"),
        "pu_location_key": df["pulocationid"].astype("int16"),
        "do_location_key": df["dolocationid"].astype("int16"),
        "payment_type_key": df["payment_type"].astype("int16"),
        "vendor_key": df["vendorid"].astype("int16"),
        "rate_code_id": rate_code,
        "passenger_count": passengers,
        "store_and_fwd_flag": df["store_and_fwd_flag"].map({"Y": True, "N": False}).astype("boolean"),
        "trip_duration_min": (duration_s / 60).round(2).mask(zero_duration),
        **{col: df[col] for col in ("trip_distance", "fare_amount", "extra", "mta_tax", "tip_amount",
                                     "tolls_amount", "improvement_surcharge", "congestion_surcharge",
                                     "airport_fee", "total_amount", "cbd_congestion_fee",
                                     "request_source")},
    }, index=df.index)
    return staged[list(STAGING_COLUMNS)], warnings


def process_batch(df: pd.DataFrame, ctx: TransformContext) -> BatchResult:
    """Round -> apply rules R1-R13 -> shape accepted rows for staging."""
    df = round_values(df)
    reasons = assign_reject_reasons(df, ctx)
    passed = reasons.isna().to_numpy()

    staged, warnings = to_star(df[passed], ctx)
    rejected = df[~passed].assign(reject_reason=reasons[~passed])
    rejects = Counter(rejected["reject_reason"].value_counts().to_dict())
    return BatchResult(staged=staged, rejected=rejected, rejects=rejects, warnings=warnings)
