from __future__ import annotations

import pickle
from base64 import b64decode, b64encode
from collections.abc import Sequence
from itertools import chain
from typing import Any

from arti.formats.pickle import Pickle
from arti.io import register_reader, register_writer
from arti.storage.literal import StringLiteralPartition
from arti.types import Type, is_partitioned
from arti.views.python import PythonBuiltin

# Pickled literals are stored base64 encoded in the Backend, so reading them executes payloads from
# the Backend's metadata. Only use with Backends you trust as much as the code itself.


def _read_pickle_literal(partition: StringLiteralPartition) -> Any:
    # Though we take in a Partition (which has `value` as optional), we should be operating within a
    # Snapshot, which will ensure the `value` is set as part of
    # StringLiteralPartition.compute_content_fingerprint.
    assert partition.value is not None
    return pickle.loads(b64decode(partition.value))  # noqa: S301 # User opted into pickle, ignore bandit check


@register_reader
def read_pickle_stringliteral_python(
    type_: Type,
    format: Pickle,
    storage_partitions: Sequence[StringLiteralPartition],
    view: PythonBuiltin,
) -> Any:
    if is_partitioned(type_):
        return list(
            chain.from_iterable(
                _read_pickle_literal(storage_partition) for storage_partition in storage_partitions
            )
        )
    assert len(storage_partitions) == 1  # Better error handled in base read
    return _read_pickle_literal(storage_partitions[0])


@register_writer
def write_pickle_stringliteral_python(
    data: Any,
    type_: Type,
    format: Pickle,
    storage_partition: StringLiteralPartition,
    view: PythonBuiltin,
) -> StringLiteralPartition:
    return storage_partition.with_value(b64encode(pickle.dumps(data)).decode())
