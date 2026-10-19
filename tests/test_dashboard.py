"""The dashboard must show exactly what SQL on the fact table returns for the same filters.

Runs the real Streamlit page (AppTest) against the loaded project database, so it is
skipped when no data is loaded. Slow-ish (scans the fact table): marked integration.
"""
import pytest
from streamlit.testing.v1 import AppTest

from pipeline.config import PROJECT_ROOT
from pipeline.db import connect
from pipeline.exceptions import LoadError

pytestmark = pytest.mark.integration

APP = str(PROJECT_ROOT / "dashboard" / "app.py")


@pytest.fixture(scope="module")
def project_conn():
    try:
        conn = connect()
    except LoadError as exc:
        pytest.skip(f"PostgreSQL not available: {exc}")
    loaded = conn.execute("SELECT EXISTS (SELECT 1 FROM fact_taxi_trips "
                          "WHERE pickup_date_key BETWEEN 20230101 AND 20231231)").fetchone()[0]
    if not loaded:
        pytest.skip("No 2023 data loaded — run python -m pipeline.flows first")
    yield conn
    conn.close()


def fact_kpis(conn, first_key: int, last_key: int, payment_types: list[int]) -> dict[str, str]:
    """The headline KPIs computed straight from fact_taxi_trips, formatted like the dashboard."""
    trips, revenue, fpm = conn.execute(
        """SELECT count(*), sum(total_amount), sum(fare_amount) / sum(trip_distance)
           FROM fact_taxi_trips
           WHERE pickup_date_key BETWEEN %s AND %s AND payment_type_key = ANY(%s)""",
        (first_key, last_key, payment_types)).fetchone()
    peak = conn.execute(
        """SELECT lpad(pickup_hour_key::text, 2, '0') || ':00' FROM fact_taxi_trips
           WHERE pickup_date_key BETWEEN %s AND %s AND payment_type_key = ANY(%s)
           GROUP BY pickup_hour_key ORDER BY count(*) DESC LIMIT 1""",
        (first_key, last_key, payment_types)).fetchone()[0]
    return {"Total trips": f"{trips:,}", "Total revenue": f"${float(revenue) / 1e6:,.2f} M",
            "Fare per mile": f"${float(fpm):,.2f}", "Peak hour": peak}


def dashboard_kpis(at: AppTest) -> dict[str, str]:
    headline = ("Total trips", "Total revenue", "Fare per mile", "Peak hour")
    return {m.label: m.value for m in at.metric if m.label in headline}


def test_dashboard_matches_fact_table_for_full_year(project_conn):
    at = AppTest.from_file(APP, default_timeout=120).run()
    at.sidebar.selectbox[0].set_value(2023).run()
    assert not at.exception
    all_types = list(at.sidebar.multiselect[2].value)
    assert dashboard_kpis(at) == fact_kpis(project_conn, 20230101, 20231231, all_types)


def test_dashboard_matches_fact_table_for_q1_credit_card(project_conn):
    at = AppTest.from_file(APP, default_timeout=120).run()
    at.sidebar.selectbox[0].set_value(2023).run()
    at.sidebar.multiselect[2].set_value([1]).run()   # payment type: Credit card
    at.sidebar.multiselect[0].set_value([1]).run()   # quarter: Q1
    assert not at.exception
    assert dashboard_kpis(at) == fact_kpis(project_conn, 20230101, 20230331, [1])
