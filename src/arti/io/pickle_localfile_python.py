from __future__ import annotations

import pickle
from collections.abc import Sequence
from itertools import chain
from pathlib import Path
from typing import Any

from arti.formats.pickle import Pickle
from arti.io import register_reader, register_writer
from arti.storage.local import LocalFilePartition
from arti.types import Type, is_partitioned
from arti.views.python import PythonBuiltin


def _read_pickle_file(path: str) -> Any:
    with open(path, "rb") as file:
        return pickle.load(file)  # nosec # User opted into pickle, ignore bandit check


@register_reader
def _read_pickle_localfile_python(
    type_: Type,
    format: Pickle,
    storage_partitions: Sequence[LocalFilePartition],
    view: PythonBuiltin,
) -> Any:
    if is_partitioned(type_):
        return list(
            chain.from_iterable(
                _read_pickle_file(storage_partition.path)
                for storage_partition in storage_partitions
            )
        )
    assert len(storage_partitions) == 1  # Better error handled in base read
    return _read_pickle_file(storage_partitions[0].path)


@register_writer
def _write_pickle_localfile_python(
    data: Any,
    type_: Type,
    format: Pickle,
    storage_partition: LocalFilePartition,
    view: PythonBuiltin,
) -> None:
    path = Path(storage_partition.path)
    path.parent.mkdir(exist_ok=True, parents=True)
    with path.open("wb") as file:
        pickle.dump(data, file)
