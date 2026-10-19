from datetime import datetime

import pytest

from pipeline.config import DEFAULT_ZONE_IDS, load_zone_ids, month_bounds, parse_months
from pipeline.exceptions import ConfigError


def test_parse_months_trims_and_removes_duplicates():
    assert parse_months(" 2023-01, 2023-02 ,2023-01") == ("2023-01", "2023-02")


@pytest.mark.parametrize("bad", ["2023-13", "2023-1", "23-01", "2023/01", ""])
def test_parse_months_rejects_bad_values(bad):
    with pytest.raises(ConfigError):
        parse_months(bad)


def test_month_bounds_normal_month():
    assert month_bounds("2023-02") == (datetime(2023, 2, 1), datetime(2023, 3, 1))


def test_month_bounds_december_rolls_into_next_year():
    assert month_bounds("2023-12") == (datetime(2023, 12, 1), datetime(2024, 1, 1))


def test_zone_ids_fall_back_to_1_to_265_when_csv_missing(tmp_path):
    assert load_zone_ids(tmp_path / "missing.csv") == DEFAULT_ZONE_IDS


def test_zone_ids_read_from_csv(tmp_path):
    csv_file = tmp_path / "zones.csv"
    csv_file.write_text('"LocationID","Borough","Zone","service_zone"\n'
                        '1,"EWR","Newark Airport","EWR"\n'
                        '2,"Queens","Jamaica Bay","Boro Zone"\n')
    assert load_zone_ids(csv_file) == frozenset({1, 2})
