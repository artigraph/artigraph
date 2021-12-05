import json
from collections.abc import Sequence
from typing import Any

from arti.formats.json import JSON
from arti.io import register_reader
from arti.storage.literal import StringLiteralPartition
from arti.types import Type
from arti.views.python import PythonBuiltin

# NOTE: Literal storage is read-only.


@register_reader
def _read_json_stringliteral_python(
    type_: Type,
    format: JSON,
    storage_partitions: Sequence[StringLiteralPartition],
    view: PythonBuiltin,
) -> Any:
    storage_partition, *tail = storage_partitions
    if tail:
        raise ValueError("Literals cannot be partitioned")
    return json.loads(storage_partition.value)
