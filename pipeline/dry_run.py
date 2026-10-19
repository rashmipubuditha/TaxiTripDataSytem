"""Dry run: extract + transform whole months without touching the database.

Usage:  python -m pipeline.dry_run --months 2023-01,2023-02 [--limit 50000]

Prints rows read / staged / rejected by reason / warnings per month, so the cleaning
rules can be checked against the plan's preview numbers before anything is loaded.
"""
from __future__ import annotations

import argparse
import logging
import time
from collections import Counter

from pipeline.config import get_settings, load_zone_ids, month_bounds, parse_months
from pipeline.extract import iter_batches, validate_source_file
from pipeline.transform import REJECT_REASONS, TransformContext, process_batch


def dry_run_month(month: str, limit: int | None = None) -> dict:
    settings = get_settings()
    info = validate_source_file(settings.source_file_path(month))
    start, end = month_bounds(month)
    ctx = TransformContext(
        month_start=start,
        month_end=end,
        valid_zone_ids=load_zone_ids(settings.zone_lookup_path),
        rules=settings.rules,
        source_file_id=0,  # not registered in a dry run
    )

    began = time.perf_counter()
    rows_read = rows_staged = 0
    rejects: Counter = Counter()
    warnings: Counter = Counter()
    sum_fare = sum_distance = 0.0
    for batch in iter_batches(info, settings.batch_size, limit):
        result = process_batch(batch, ctx)
        rows_read += len(batch)
        rows_staged += len(result.staged)
        rejects.update(result.rejects)
        warnings.update(result.warnings)
        sum_fare += float(result.staged["fare_amount"].sum())
        sum_distance += float(result.staged["trip_distance"].sum())

    return {
        "month": month,
        "file_rows": info.row_count,
        "rows_read": rows_read,
        "rows_staged": rows_staged,
        "rows_rejected": sum(rejects.values()),
        "rejects": rejects,
        "warnings": warnings,
        "fare_per_mile": sum_fare / sum_distance if sum_distance else float("nan"),
        "seconds": time.perf_counter() - began,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--months", help="comma-separated YYYY-MM (default: MONTHS from .env)")
    parser.add_argument("--limit", type=int, help="process only the first N rows of each file")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    months = parse_months(args.months) if args.months else get_settings().months
    for month in months:
        r = dry_run_month(month, args.limit)
        reject_pct = 100 * r["rows_rejected"] / r["rows_read"] if r["rows_read"] else 0
        print(f"\n=== {month}  ({r['seconds']:.1f} s) ===")
        print(f"  rows in file      {r['file_rows']:>12,}")
        print(f"  rows read         {r['rows_read']:>12,}")
        print(f"  rows staged       {r['rows_staged']:>12,}   (before D1 de-duplication, done in SQL)")
        print(f"  rows rejected     {r['rows_rejected']:>12,}   ({reject_pct:.2f}%)")
        for reason in REJECT_REASONS:
            if r["rejects"].get(reason):
                print(f"    {reason:<26}{r['rejects'][reason]:>10,}")
        print("  warnings (rows kept):")
        for name, count in sorted(r["warnings"].items()):
            print(f"    {name:<34}{count:>10,}")
        print(f"  check: read = staged + rejected -> {r['rows_read'] == r['rows_staged'] + r['rows_rejected']}")
        print(f"  weighted fare per mile (staged)   {r['fare_per_mile']:.4f}")


if __name__ == "__main__":
    main()
