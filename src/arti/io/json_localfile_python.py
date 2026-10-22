from __future__ import annotations

import json
from collections.abc import Sequence
from itertools import chain
from pathlib import Path
from typing import Any

from arti.formats.json import JSON
from arti.internal.utils import atomic_write_path
from arti.io import register_reader, register_writer
from arti.storage.local import LocalFilePartition
from arti.types import Type, is_partitioned
from arti.views.python import PythonBuiltin

# TODO: Do I need to inject the partition keys into the returned data? Likely useful...
# Maybe a View option?


def _read_json_file(path: str) -> Any:
    with Path(path).open() as file:
        return json.load(file)


@register_reader
def read_json_localfile_python(
    type_: Type,
    format: JSON,
    storage_partitions: Sequence[LocalFilePartition],
    view: PythonBuiltin,
) -> Any:
    if is_partitioned(type_):
        return list(
            chain.from_iterable(
                _read_json_file(storage_partition.path) for storage_partition in storage_partitions
            )
        )
    assert len(storage_partitions) == 1  # Better error handled in base read
    return _read_json_file(storage_partitions[0].path)


@register_writer
def write_json_localfile_python(
    data: Any, type_: Type, format: JSON, storage_partition: LocalFilePartition, view: PythonBuiltin
) -> None:
    with atomic_write_path(Path(storage_partition.path)) as path, path.open("w") as file:
        json.dump(data, file)
