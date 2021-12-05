import json
from collections.abc import Sequence
from itertools import chain
from pathlib import Path
from typing import Any

from arti.formats.json import JSON
from arti.io import register_reader, register_writer
from arti.storage.local import LocalFilePartition
from arti.types import Collection, Type
from arti.views.python import PythonBuiltin

# TODO: Do I need to inject the partition keys into the returned data? Likely useful...
# Maybe a View option?


def _read_json_file(path: str) -> Any:
    with open(path, "rb") as file:
        return json.load(file)


@register_reader
def _read_json_localfile_python(
    type_: Type,
    format: JSON,
    storage_partitions: Sequence[LocalFilePartition],
    view: PythonBuiltin,
) -> Any:
    if isinstance(type_, Collection) and type_.is_partitioned:
        return list(
            chain.from_iterable(
                _read_json_file(storage_partition.path) for storage_partition in storage_partitions
            )
        )
    else:
        assert len(storage_partitions) == 1  # Better error handled in base read
        return _read_json_file(storage_partitions[0].path)


@register_writer
def _write_json_localfile_python(
    data: Any, type_: Type, format: JSON, storage_partition: LocalFilePartition, view: PythonBuiltin
) -> None:
    path = Path(storage_partition.path)
    path.parent.mkdir(exist_ok=True, parents=True)
    with path.open("w") as file:
        json.dump(data, file)
