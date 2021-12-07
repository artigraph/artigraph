import json
from collections.abc import Sequence
from typing import Any

from arti.formats.json import JSON
from arti.io import register_reader, register_writer
from arti.storage.literal import (
    StringLiteralPartition,
    _cannot_be_partitioned_err,
    _not_written_err,
)
from arti.types import Type
from arti.views.python import PythonBuiltin


@register_reader
def _read_json_stringliteral_python(
    type_: Type,
    format: JSON,
    storage_partitions: Sequence[StringLiteralPartition],
    view: PythonBuiltin,
) -> Any:
    storage_partition, *tail = storage_partitions
    if tail:
        raise _cannot_be_partitioned_err
    if storage_partition.value is None:
        raise _not_written_err
    return json.loads(storage_partition.value)


@register_writer
def _write_json_stringliteral_python(
    data: Any,
    type_: Type,
    format: JSON,
    storage_partition: StringLiteralPartition,
    view: PythonBuiltin,
) -> StringLiteralPartition:
    return storage_partition.copy(update={"value": json.dumps(data)})
