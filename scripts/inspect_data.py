"""Re-runnable profile of the raw monthly files (the checks behind plan §3.6).

Usage:  python scripts/inspect_data.py [--months 2023-01,2023-02] [--sample 5]

Reads one month at a time (memory stays at about one month of data), using the same
schema normalisation as the pipeline, and prints one column per month.
Read-only: never modifies the source files.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # allow 'import pipeline'

from pipeline.config import get_settings, month_bounds, parse_months  # noqa: E402
from pipeline.extract import iter_batches, validate_source_file  # noqa: E402

DUP_KEY = ["vendorid", "tpep_pickup_datetime", "tpep_dropoff_datetime", "pulocationid", "dolocationid"]


def profile_month(month: str, sample: int) -> dict[str, object]:
    settings = get_settings()
    info = validate_source_file(settings.source_file_path(month))
    df = pd.concat(iter_batches(info, batch_size=1_000_000), ignore_index=True)
    start, end = month_bounds(month)
    pickup, dropoff = df["tpep_pickup_datetime"], df["tpep_dropoff_datetime"]
    duration = (dropoff - pickup).dt.total_seconds()

    if sample:
        print(f"\n--- {month}: first {sample} rows ---")
        print(df.head(sample).T.to_string())

    return {
        "file size (MB)": round(info.size_bytes / 1e6, 1),
        "rows": len(df),
        "row groups": info.row_groups,
        "rows with NULL passenger_count": int(df["passenger_count"].isna().sum()),
        "payment_type 0 (Flex Fare)": int((df["payment_type"] == 0).sum()),
        "pickup outside month": int(((pickup < start) | (pickup >= end)).sum()),
        "earliest pickup": str(pickup.min()),
        "drop-off <= pickup": int((duration <= 0).sum()),
        "duration > 24 h": int((duration > 86400).sum()),
        "trip_distance = 0": int((df["trip_distance"] == 0).sum()),
        "trip_distance > 100": int((df["trip_distance"] > 100).sum()),
        "max trip_distance": round(float(df["trip_distance"].max()), 2),
        "fare_amount < 0": int((df["fare_amount"] < 0).sum()),
        "fare_amount > 1000": int((df["fare_amount"] > 1000).sum()),
        "max fare_amount": round(float(df["fare_amount"].max()), 2),
        "RatecodeID = 99": int((df["ratecodeid"] == 99).sum()),
        "location IDs outside 1-265": int((~df["pulocationid"].between(1, 265)
                                           | ~df["dolocationid"].between(1, 265)).sum()),
        "same vendor+times+zones": int(df.duplicated(subset=DUP_KEY).sum()),
        "exact duplicate rows": int(df.drop(columns="source_row_number").duplicated().sum()),
        "vendors": ",".join(str(v) for v in sorted(df["vendorid"].dropna().unique())),
        "days with trips": int(pickup[(pickup >= start) & (pickup < end)].dt.date.nunique()),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--months", help="comma-separated YYYY-MM (default: MONTHS from .env)")
    parser.add_argument("--sample", type=int, default=0, help="also print the first N rows of each month")
    args = parser.parse_args()

    months = parse_months(args.months) if args.months else get_settings().months
    results = {}
    for month in months:
        print(f"profiling {month} ...", flush=True)
        results[month] = profile_month(month, args.sample)
    table = pd.DataFrame(results)
    with pd.option_context("display.max_columns", None, "display.width", 250):
        print("\n" + table.to_string())


if __name__ == "__main__":
    main()
