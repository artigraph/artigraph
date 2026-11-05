"""
Read and write Python rows (a `list` of `dict`) as local Parquet files.

A Parquet file holds a table, so the Artifact type must be a `Collection`. This lets the Python View
read Parquet, eg: to compute Statistics of raw Parquet Artifacts written outside of Artigraph.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any, Protocol, cast

try:
    import pyarrow as pa
    import pyarrow.parquet as pq

    from arti.formats.parquet import Parquet
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
    from arti.views.python import List

    class _ParquetFileReader(Protocol):  # pragma: no cover - static typing protocol
        def read(self) -> pa.Table: ...

    class _ParquetWriter(Protocol):  # pragma: no cover - static typing protocol
        def write_table(self, table: pa.Table, where: Path) -> None: ...

    @register_reader
    def read_parquet_localfile_python(
        type_: Collection,
        format: Parquet,
        storage_partitions: Sequence[LocalFilePartition],
        view: List,
    ) -> list[dict[str, Any]]:
        # NOTE: Read each file directly rather than with `pq.read_table(paths)`, which infers hive
        # partition columns from the paths and silently reconciles differing file schemas.
        return [
            row
            for partition in storage_partitions
            for row in cast(_ParquetFileReader, pq.ParquetFile(partition.path)).read().to_pylist()
        ]

    @register_writer
    def write_parquet_localfile_python(
        data: list[dict[str, Any]],
        type_: Collection,
        format: Parquet,
        storage_partition: LocalFilePartition,
        view: List,
    ) -> None:
        schema = format.type_system.to_system(type_, hints={})
        assert isinstance(schema, pa.Schema)
        if extra := sorted({key for row in data for key in row} - set(schema.names)):
            raise ValueError(f"Rows have fields not in the Artifact type: {extra}")
        table = pa.Table.from_pylist(data, schema=schema)
        with atomic_write_path(Path(storage_partition.path)) as path:
            cast(_ParquetWriter, pq).write_table(table, path)
