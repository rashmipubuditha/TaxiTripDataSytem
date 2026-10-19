"""Warning checks QC7-QC9: pure threshold logic, no database needed."""
from dataclasses import replace

import pytest

from pipeline.config import QualityThresholds
from pipeline.quality import MonthStats, evaluate_warning_checks

LIMITS = QualityThresholds()  # 4% rejects, 1% duplicates, fare per mile 3-12

# A normal month, based on January 2023.
NORMAL = MonthStats(month="2023-01", rows_read=3_066_766, rows_staged=2_998_234, rows_rejected=68_532,
                    rows_deduplicated=0, days_in_month=31, days_with_trips=31, fare_per_mile=5.43)


def by_name(stats):
    return {r.name: r for r in evaluate_warning_checks(stats, LIMITS)}


def test_normal_month_has_no_warnings():
    assert all(r.passed for r in by_name(NORMAL).values())
    assert all(r.severity == "WARNING" for r in by_name(NORMAL).values())


def test_qc7_fires_for_september_2023_reject_rate():
    # Real September 2023 numbers: 4.42% rejected (zero-distance spike).
    sep = replace(NORMAL, month="2023-09", rows_read=2_846_722, rows_staged=2_720_768,
                  rows_rejected=125_954, days_in_month=30, days_with_trips=30, fare_per_mile=5.77)
    qc7 = by_name(sep)["QC7"]
    assert not qc7.passed
    assert qc7.observed == pytest.approx(4.4245, abs=1e-3)
    assert "4.42%" in qc7.details


def test_qc7_limit_is_inclusive():
    exactly_4pct = replace(NORMAL, rows_read=1000, rows_rejected=40)
    assert by_name(exactly_4pct)["QC7"].passed


def test_qc8_fires_for_a_duplicate_burst():
    # July 2026 pattern: 57,087 duplicates in 3.39 M staged rows = 1.68%.
    burst = replace(NORMAL, rows_staged=3_386_698, rows_deduplicated=57_087)
    assert not by_name(burst)["QC8"].passed


@pytest.mark.parametrize("change", [
    {"days_with_trips": 30},          # one day without any trips
    {"fare_per_mile": 2.5},           # implausibly low
    {"fare_per_mile": 12.5},          # implausibly high
    {"fare_per_mile": None},          # nothing loaded
])
def test_qc9_fires_for_missing_days_or_implausible_fare(change):
    assert not by_name(replace(NORMAL, **change))["QC9"].passed


def test_empty_month_does_not_divide_by_zero():
    empty = replace(NORMAL, rows_read=0, rows_staged=0, rows_rejected=0, days_with_trips=0, fare_per_mile=None)
    results = by_name(empty)
    assert results["QC7"].passed and results["QC8"].passed and not results["QC9"].passed
