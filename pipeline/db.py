"""Database helpers: connections and fast bulk loading with PostgreSQL COPY."""
from __future__ import annotations

import io
from collections.abc import Sequence

import pandas as pd
import psycopg
import pyarrow as pa
import pyarrow.csv as pacsv

from pipeline.config import Settings, get_settings
from pipeline.exceptions import DatabaseUnavailableError


def connect(settings: Settings | None = None, dbname: str | None = None) -> psycopg.Connection:
    """Open a connection in autocommit mode.

    Autocommit means every statement commits on its own unless it runs inside an explicit
    `with conn.transaction():` block. That makes transaction boundaries visible in the
    code, and allows VACUUM (which cannot run inside a transaction).
    """
    s = settings or get_settings()
    try:
        return psycopg.connect(
            host=s.postgres_host,
            port=s.postgres_port,
            dbname=dbname or s.postgres_db,
            user=s.postgres_user,
            password=s.postgres_password,
            autocommit=True,
            connect_timeout=10,
            application_name="taxi_etl",
        )
    except psycopg.OperationalError as exc:
        raise DatabaseUnavailableError(
            f"Cannot connect to PostgreSQL at {s.postgres_host}:{s.postgres_port} — "
            f"is the container running? (bash setup.sh) Details: {exc}"
        ) from exc


def copy_dataframe(cur: psycopg.Cursor, table: str, columns: Sequence[str], frame: pd.DataFrame) -> int:
    """Bulk-insert a DataFrame with COPY ... FROM STDIN (CSV), much faster than INSERT.

    Missing values (NaN / <NA> / None) are written as empty fields, which COPY reads as NULL.
    The CSV text is produced by pyarrow, which is about 5x faster than pandas.to_csv
    (measured: 1.3 s vs 7.6 s per 250,000 rows).
    Returns the number of rows copied.
    """
    if frame.empty:
        return 0
    table_data = pa.Table.from_pandas(frame[list(columns)], preserve_index=False)
    buffer = io.BytesIO()
    pacsv.write_csv(table_data, buffer, pacsv.WriteOptions(include_header=False, quoting_style="needed"))
    sql = f"COPY {table} ({', '.join(columns)}) FROM STDIN WITH (FORMAT csv, NULL '')"
    with cur.copy(sql) as copy:
        copy.write(buffer.getvalue())
    return len(frame)
