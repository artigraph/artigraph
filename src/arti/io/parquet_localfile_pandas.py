"""
Read and write Pandas DataFrames as local Parquet files.

A Parquet file holds a table, so the Artifact type must be a `Collection`. Only the DataFrame's
columns are stored, not its index; call `reset_index()` first to keep it.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Protocol, cast

try:
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    from arti.formats.parquet import Parquet
    from arti.views.pandas import DataFrame
except ModuleNotFoundError as error:  # pragma: no cover - dependency-free subprocess
    if (error.name or "").split(".", 1)[0] not in {"numpy", "pandas", "pyarrow"}:
        raise
    # IO discovery imports every implementation. Optional Parquet dependencies must not prevent
    # JSON/Pickle IO from working in a minimal installation.
else:
    from arti.internal.utils import atomic_write_path
    from arti.io import register_reader, register_writer
    from arti.storage.local import LocalFilePartition
    from arti.types import Collection

    class _ParquetFileReader(Protocol):  # pragma: no cover - static typing protocol
        def read(self) -> pa.Table: ...

    class _TableToPandas(Protocol):  # pragma: no cover - static typing protocol
        def to_pandas(self) -> pd.DataFrame: ...

    class _ParquetWriter(Protocol):  # pragma: no cover - static typing protocol
        def write_table(self, table: pa.Table, where: Path) -> None: ...

    @register_reader
    def read_parquet_localfile_pandas(
        type_: Collection,
        format: Parquet,
        storage_partitions: Sequence[LocalFilePartition],
        view: DataFrame,
    ) -> pd.DataFrame:
        # NOTE: Read each file directly rather than with `pq.read_table(paths)`, which infers hive
        # partition columns from the paths and silently reconciles differing file schemas.
        tables = [
            cast(_ParquetFileReader, pq.ParquetFile(partition.path)).read()
            for partition in storage_partitions
        ]
        return cast(_TableToPandas, pa.concat_tables(tables)).to_pandas()

    def _has_time_of_day(value: datetime) -> bool:
        timestamp = pd.Timestamp(value)
        # Arrow takes the local date of `datetime` objects, but the UTC date of datetime64 columns.
        # Require UTC midnight for both, so the dtype doesn't decide which date is written.
        if timestamp.tz is not None:
            timestamp = timestamp.tz_convert(None)
        return timestamp != timestamp.normalize()

    @register_writer
    def write_parquet_localfile_pandas(
        data: pd.DataFrame,
        type_: Collection,
        format: Parquet,
        storage_partition: LocalFilePartition,
        view: DataFrame,
    ) -> None:
        if not isinstance(data, pd.DataFrame):  # pyright: ignore[reportUnnecessaryIsInstance]
            raise TypeError(f"Expected a pandas DataFrame, got {type(data)}")
        schema = format.type_system.to_system(type_, hints={})
        assert isinstance(schema, pa.Schema)
        extra_columns = [column for column in data.columns if column not in schema.names]
        if extra_columns:
            raise ValueError(f"DataFrame has columns not in the Artifact type: {extra_columns}")
        for name, arrow_type in zip(schema.names, schema.types, strict=True):
            if pa.types.is_date(arrow_type) and name in data.columns:
                values = data[name].dropna()
                if values.dtype == object and any(
                    _has_time_of_day(value) for value in values if isinstance(value, datetime)
                ):
                    raise ValueError(f"Date column {name!r} has times of day that would be dropped")
                if isinstance(values.dtype, pd.DatetimeTZDtype):
                    values = values.dt.tz_convert(None)  # Arrow writes the UTC date
                if (
                    pd.api.types.is_datetime64_dtype(values)
                    and (values != values.dt.normalize()).any()
                ):
                    raise ValueError(f"Date column {name!r} has times of day that would be dropped")
        table = pa.Table.from_pandas(data, schema=schema, preserve_index=False, safe=True)
        with atomic_write_path(Path(storage_partition.path)) as path:
            cast(_ParquetWriter, pq).write_table(table, path)
