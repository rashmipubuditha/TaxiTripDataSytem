"""Extract: validate a monthly Parquet file and read it in batches with one fixed schema.

Plain functions (no Prefect, no database) so they are easy to unit-test.
"""
from __future__ import annotations

import logging
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from pipeline.config import SOURCE_SCHEMA
from pipeline.exceptions import SchemaError, SourceFileError

logger = logging.getLogger(__name__)

# Arrow -> pandas types: nullable integers keep NULLs without turning into floats.
_PANDAS_TYPES = {
    pa.int32(): pd.Int32Dtype(),
    pa.string(): pd.StringDtype(),
}


@dataclass(frozen=True)
class SourceFileInfo:
    path: Path
    size_bytes: int
    row_count: int
    row_groups: int
    column_map: dict[str, str]        # canonical (lower-case) name -> name in the file
    ignored_columns: tuple[str, ...]  # columns in the file that we do not use


def validate_source_file(path: Path) -> SourceFileInfo:
    """Check that the file exists, is a readable Parquet file and has every required column.

    Raises SourceFileError or SchemaError with a message that says what to do.
    """
    if not path.is_file():
        raise SourceFileError(f"{path} not found — run setup.sh or check MONTHS")
    size = path.stat().st_size
    if size == 0:
        raise SourceFileError(f"{path} is empty — delete it and run setup.sh to download it again")
    try:
        parquet = pq.ParquetFile(path)
    except Exception as exc:  # pyarrow raises several error types for corrupt files
        raise SourceFileError(f"{path} is not a readable Parquet file: {exc}") from exc

    column_map: dict[str, str] = {}
    ignored: list[str] = []
    for name in parquet.schema_arrow.names:
        canonical = name.lower()
        if canonical in SOURCE_SCHEMA:
            column_map[canonical] = name
        else:
            ignored.append(name)

    missing = [c for c, spec in SOURCE_SCHEMA.items() if spec.required and c not in column_map]
    if missing:
        raise SchemaError(f"{path.name} is missing required column(s): {', '.join(missing)}")

    return SourceFileInfo(
        path=path,
        size_bytes=size,
        row_count=parquet.metadata.num_rows,
        row_groups=parquet.metadata.num_row_groups,
        column_map=column_map,
        ignored_columns=tuple(ignored),
    )


def normalise_batch(batch: pa.RecordBatch, column_map: dict[str, str], file_name: str) -> pa.Table:
    """Rename columns to lower case, cast to the target types and add missing optional columns."""
    arrays, names = [], []
    for canonical, spec in SOURCE_SCHEMA.items():
        source_name = column_map.get(canonical)
        if source_name is None:
            column = pa.nulls(batch.num_rows, type=spec.arrow_type)
        else:
            column = batch.column(source_name)
            if column.type != spec.arrow_type:
                try:
                    # safe=True: refuse lossy casts, e.g. 2.5 passengers -> 2.
                    column = pc.cast(column, spec.arrow_type, safe=True)
                except pa.ArrowInvalid as exc:
                    raise SchemaError(
                        f"{file_name}: column '{source_name}' cannot be converted to "
                        f"{spec.arrow_type}: {exc}"
                    ) from exc
        arrays.append(column)
        names.append(canonical)
    return pa.Table.from_arrays(arrays, names=names)


def iter_batches(info: SourceFileInfo, batch_size: int, limit: int | None = None) -> Iterator[pd.DataFrame]:
    """Yield the file as pandas DataFrames of at most batch_size rows.

    Each DataFrame has the canonical columns plus 'source_row_number' (1-based row
    position in the file), which becomes part of the fact table's primary key.
    limit: stop after this many rows (for quick development runs).
    """
    if info.ignored_columns:
        logger.info("Ignoring unused columns in %s: %s", info.path.name, ", ".join(info.ignored_columns))

    parquet = pq.ParquetFile(info.path)
    rows_done = 0
    for batch in parquet.iter_batches(batch_size=batch_size, columns=list(info.column_map.values())):
        if limit is not None and rows_done >= limit:
            break
        if limit is not None and rows_done + batch.num_rows > limit:
            batch = batch.slice(0, limit - rows_done)

        table = normalise_batch(batch, info.column_map, info.path.name)
        frame = table.to_pandas(types_mapper=_PANDAS_TYPES.get)
        frame.insert(0, "source_row_number", range(rows_done + 1, rows_done + len(frame) + 1))
        rows_done += len(frame)
        yield frame
