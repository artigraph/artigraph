from __future__ import annotations

import json
from collections.abc import Sequence
from itertools import chain
from typing import Any

from arti.formats.json import JSON
from arti.io import register_reader, register_writer
from arti.storage.literal import StringLiteralPartition
from arti.types import Type, is_partitioned
from arti.views.python import PythonBuiltin


def _read_json_literal(partition: StringLiteralPartition) -> Any:
    # Though we take in a Partition (which has `value` as optional), we should be operating within a
    # Snapshot, which will ensure the `value` is set as part of
    # StringLiteralPartition.compute_content_fingerprint.
    assert partition.value is not None
    return json.loads(partition.value)


@register_reader
def read_json_stringliteral_python(
    type_: Type,
    format: JSON,
    storage_partitions: Sequence[StringLiteralPartition],
    view: PythonBuiltin,
) -> Any:
    if is_partitioned(type_):
        return list(
            chain.from_iterable(
                _read_json_literal(storage_partition) for storage_partition in storage_partitions
            )
        )
    assert len(storage_partitions) == 1  # Better error handled in base read
    return _read_json_literal(storage_partitions[0])


@register_writer
def write_json_stringliteral_python(
    data: Any,
    type_: Type,
    format: JSON,
    storage_partition: StringLiteralPartition,
    view: PythonBuiltin,
) -> StringLiteralPartition:
    return storage_partition.with_value(json.dumps(data))
