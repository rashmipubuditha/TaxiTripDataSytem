"""Fixtures for integration tests: a throw-away 'taxi_test' database with the real schema.

Tests using these fixtures are skipped automatically when PostgreSQL is not reachable.
The project database (taxi_dw) is never touched.
"""
from __future__ import annotations

import psycopg
import pytest
from psycopg import sql

from pipeline.config import PROJECT_ROOT, get_settings
from pipeline.db import connect
from pipeline.exceptions import LoadError

TEST_DB = "taxi_test"


@pytest.fixture(scope="session")
def test_db() -> str:
    """Create taxi_test from scratch once per test session and apply sql/schema.sql."""
    settings = get_settings()
    try:
        admin = connect(settings, dbname="postgres")
    except LoadError as exc:
        pytest.skip(f"PostgreSQL not available: {exc}")
    with admin:
        admin.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(TEST_DB)))
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(TEST_DB)))
    with connect(settings, dbname=TEST_DB) as conn:
        conn.execute((PROJECT_ROOT / "sql" / "schema.sql").read_text(encoding="utf-8"))
    return TEST_DB


@pytest.fixture
def test_conn(test_db: str) -> psycopg.Connection:
    """Connection to taxi_test with an empty staging table."""
    with connect(get_settings(), dbname=test_db) as conn:
        conn.execute("TRUNCATE stg_trips")
        yield conn


def reset_data(conn: psycopg.Connection) -> None:
    """Empty every data and run table in taxi_test (dimensions stay).

    Fact partitions are dropped, not just emptied: source-file ids restart at 1, and a
    leftover partition for id 1 of another month would clash with the new one.
    """
    partitions = conn.execute(
        "SELECT inhrelid::regclass::text FROM pg_inherits WHERE inhparent = 'fact_taxi_trips'::regclass"
    ).fetchall()
    for (name,) in partitions:
        conn.execute(sql.SQL("DROP TABLE {}").format(sql.Identifier(name)))
    conn.execute("""TRUNCATE etl_quality_results, etl_rejected_summary, etl_pipeline_runs, etl_source_files,
                             agg_trips_daily_hourly, agg_monthly_fare_stats, stg_trips RESTART IDENTITY CASCADE""")
