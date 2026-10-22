from __future__ import annotations

import json
from collections.abc import Sequence
from itertools import chain
from typing import Any

from arti.formats.json import JSON
from arti.io import register_reader, register_writer
from arti.storage.google.cloud.storage import GCSFilePartition, gcs_file_system
from arti.types import Type, is_partitioned
from arti.views.python import PythonBuiltin

# TODO: Do I need to inject the partition keys into the returned data? Likely useful...
# Maybe a View option?


def _read_json_file(path: str) -> Any:
    # TODO: GCSFileSystem needs to be injected somehow
    with gcs_file_system().open(path, "r") as file:
        return json.load(file)


@register_reader
def read_json_gcsfile_python(
    type_: Type,
    format: JSON,
    storage_partitions: Sequence[GCSFilePartition],
    view: PythonBuiltin,
) -> Any:
    if is_partitioned(type_):
        return list(
            chain.from_iterable(
                _read_json_file(storage_partition.qualified_path)
                for storage_partition in storage_partitions
            )
        )
    assert len(storage_partitions) == 1  # Better error handled in base read
    return _read_json_file(storage_partitions[0].qualified_path)


@register_writer
def write_json_gcsfile_python(
    data: Any, type_: Type, format: JSON, storage_partition: GCSFilePartition, view: PythonBuiltin
) -> None:
    # Encode first: closing the file (even after an error) uploads whatever was written. A single
    # upload replaces the object atomically.
    encoded = json.dumps(data)
    with gcs_file_system().open(storage_partition.qualified_path, "w") as file:
        file.write(encoded)
