from __future__ import annotations

try:
    from arti.types.pyarrow import pyarrow_type_system
except ModuleNotFoundError as error:  # pragma: no cover - exercised in a dependency-free subprocess
    raise ModuleNotFoundError(  # pragma: no cover
        "Parquet support requires the `parquet` extra: `pip install 'arti[parquet]'`",
        name=error.name,
    ) from error

from arti.formats import Format


class Parquet(Format):
    extension: str = ".parquet"
    type_system = pyarrow_type_system


__all__ = ["Parquet"]
