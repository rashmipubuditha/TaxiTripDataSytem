"""Central configuration: settings from .env, cleaning thresholds and the target schema.

Everything that might change between environments lives in .env; everything that
defines the data contract (column names, types, valid codes) lives here.
"""
from __future__ import annotations

import csv
import os
import re
from dataclasses import dataclass, field
from datetime import datetime
from functools import lru_cache
from pathlib import Path

import pyarrow as pa
from dotenv import load_dotenv

from pipeline.exceptions import ConfigError

PROJECT_ROOT = Path(__file__).resolve().parent.parent

MONTH_PATTERN = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")

# --- Valid codes from the TLC data dictionary (March 18, 2025) ----------------
VALID_VENDORS = frozenset({1, 2, 6, 7})
VALID_PAYMENT_TYPES = frozenset(range(0, 7))          # 0 = Flex Fare ... 6 = Voided trip
VALID_RATE_CODES = frozenset({1, 2, 3, 4, 5, 6, 99})  # 99 = Null/unknown
UNKNOWN_RATE_CODE = 99
DEFAULT_ZONE_IDS = frozenset(range(1, 266))           # used only if the zone CSV is missing

# --- Target schema ------------------------------------------------------------
# Every source file is converted to exactly these columns and types (after
# lower-casing names, so 'Airport_fee' and 'airport_fee' both become 'airport_fee').
# required=True: the file must contain the column (its values may still be NULL).
# required=False: added as NULL when missing (newer columns not present in 2023).
# IDs are int32 (not int16) so an out-of-range value is rejected by a rule
# instead of crashing the conversion.


@dataclass(frozen=True)
class ColumnSpec:
    arrow_type: pa.DataType
    required: bool = True


SOURCE_SCHEMA: dict[str, ColumnSpec] = {
    "vendorid": ColumnSpec(pa.int32()),
    "tpep_pickup_datetime": ColumnSpec(pa.timestamp("us")),
    "tpep_dropoff_datetime": ColumnSpec(pa.timestamp("us")),
    "passenger_count": ColumnSpec(pa.int32()),
    "trip_distance": ColumnSpec(pa.float64()),
    "ratecodeid": ColumnSpec(pa.int32()),
    "store_and_fwd_flag": ColumnSpec(pa.string()),
    "pulocationid": ColumnSpec(pa.int32()),
    "dolocationid": ColumnSpec(pa.int32()),
    "payment_type": ColumnSpec(pa.int32()),
    "fare_amount": ColumnSpec(pa.float64()),
    "extra": ColumnSpec(pa.float64()),
    "mta_tax": ColumnSpec(pa.float64()),
    "tip_amount": ColumnSpec(pa.float64()),
    "tolls_amount": ColumnSpec(pa.float64()),
    "improvement_surcharge": ColumnSpec(pa.float64()),
    "total_amount": ColumnSpec(pa.float64()),
    "congestion_surcharge": ColumnSpec(pa.float64()),
    "airport_fee": ColumnSpec(pa.float64()),
    "cbd_congestion_fee": ColumnSpec(pa.float64(), required=False),
    "request_source": ColumnSpec(pa.string(), required=False),
}

MONEY_COLUMNS = (
    "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
    "improvement_surcharge", "congestion_surcharge", "airport_fee",
    "total_amount", "cbd_congestion_fee",
)
# Parts that should add up to total_amount (used only for a warning count).
TOTAL_PARTS = (
    "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
    "improvement_surcharge", "congestion_surcharge", "airport_fee", "cbd_congestion_fee",
)


@dataclass(frozen=True)
class CleaningRules:
    """Thresholds for the reject rules R1-R13 (plan §7.2). Overridable from .env."""

    max_duration_hours: float = 24.0
    max_trip_distance_mi: float = 100.0
    max_fare_amount: float = 1000.0
    # Largest absolute value NUMERIC(10,2) can hold; anything beyond is impossible data.
    max_abs_amount: float = 99_999_999.99


@dataclass(frozen=True)
class QualityThresholds:
    """Limits for the warning checks QC7-QC9 (plan §10.4). A breach is logged and stored,
    but never blocks a load. Overridable from .env."""

    max_reject_rate_pct: float = 4.0       # QC7
    max_duplicate_rate_pct: float = 1.0    # QC8
    min_fare_per_mile: float = 3.0         # QC9
    max_fare_per_mile: float = 12.0        # QC9


@dataclass(frozen=True)
class Settings:
    postgres_user: str
    postgres_password: str
    postgres_db: str
    postgres_host: str
    postgres_port: int
    months: tuple[str, ...]
    raw_dir_pattern: str
    reference_dir: Path
    batch_size: int
    rules: CleaningRules = field(default_factory=CleaningRules)
    # True: skip PostgreSQL's row-by-row foreign-key checks during the publish INSERT and
    # verify all relationships with one set-based query (QC3) before COMMIT instead.
    # Measured on January 2023: 354 s -> 48 s for the insert. See load.publish_month.
    fast_fk_load: bool = True
    quality: QualityThresholds = field(default_factory=QualityThresholds)
    # Save rejected source rows (with their reason) to data/rejected/ for inspection.
    save_rejected_rows: bool = True

    def source_file_path(self, month: str) -> Path:
        """Path of the monthly Parquet file, e.g. Dataset_2023/yellow_tripdata_2023-01.parquet."""
        validate_month(month)
        folder = self.raw_dir_pattern.replace("{year}", month[:4])
        return PROJECT_ROOT / folder / f"yellow_tripdata_{month}.parquet"

    @property
    def zone_lookup_path(self) -> Path:
        return self.reference_dir / "taxi_zone_lookup.csv"


def validate_month(month: str) -> str:
    if not MONTH_PATTERN.match(month):
        raise ConfigError(f"Invalid month '{month}': expected YYYY-MM, e.g. 2023-01")
    return month


def parse_months(value: str) -> tuple[str, ...]:
    """'2023-01, 2023-02' -> ('2023-01', '2023-02'), validated, duplicates removed, order kept."""
    months = [m.strip() for m in value.split(",") if m.strip()]
    if not months:
        raise ConfigError("No months given")
    return tuple(dict.fromkeys(validate_month(m) for m in months))


def month_bounds(month: str) -> tuple[datetime, datetime]:
    """Start of the month (inclusive) and start of the next month (exclusive)."""
    validate_month(month)
    year, mon = int(month[:4]), int(month[5:])
    start = datetime(year, mon, 1)
    end = datetime(year + (mon == 12), mon % 12 + 1, 1)
    return start, end


def load_zone_ids(path: Path) -> frozenset[int]:
    """Valid LocationIDs from the TLC zone lookup CSV; falls back to 1-265 if it is missing."""
    if not path.is_file():
        return DEFAULT_ZONE_IDS
    with path.open(newline="", encoding="utf-8") as fh:
        return frozenset(int(row["LocationID"]) for row in csv.DictReader(fh))


def _env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        raise ConfigError(f"Setting {name} is missing — check your .env file")
    return value


@lru_cache(maxsize=1)
def get_settings(env_file: str | None = None) -> Settings:
    """Read .env once and return validated settings."""
    load_dotenv(env_file or PROJECT_ROOT / ".env", override=False)
    try:
        rules = CleaningRules(
            max_duration_hours=float(_env("MAX_DURATION_HOURS", "24")),
            max_trip_distance_mi=float(_env("MAX_TRIP_DISTANCE_MI", "100")),
            max_fare_amount=float(_env("MAX_FARE_AMOUNT", "1000")),
        )
        return Settings(
            postgres_user=_env("POSTGRES_USER"),
            postgres_password=_env("POSTGRES_PASSWORD"),
            postgres_db=_env("POSTGRES_DB"),
            postgres_host=_env("POSTGRES_HOST", "localhost"),
            postgres_port=int(_env("POSTGRES_PORT", "5432")),
            months=parse_months(_env("MONTHS")),
            raw_dir_pattern=_env("RAW_DIR_PATTERN", "Dataset_{year}"),
            reference_dir=PROJECT_ROOT / _env("REFERENCE_DIR", "reference"),
            batch_size=int(_env("BATCH_SIZE", "250000")),
            rules=rules,
            fast_fk_load=_env("FAST_FK_LOAD", "true").strip().lower() in ("1", "true", "yes"),
            quality=QualityThresholds(
                max_reject_rate_pct=float(_env("MAX_REJECT_RATE_PCT", "4")),
                max_duplicate_rate_pct=float(_env("MAX_DUPLICATE_RATE_PCT", "1")),
                min_fare_per_mile=float(_env("MIN_FARE_PER_MILE", "3")),
                max_fare_per_mile=float(_env("MAX_FARE_PER_MILE", "12")),
            ),
            save_rejected_rows=_env("SAVE_REJECTED_ROWS", "true").strip().lower() in ("1", "true", "yes"),
        )
    except ValueError as exc:  # e.g. a non-numeric port or threshold
        raise ConfigError(f"Invalid number in .env: {exc}") from exc
