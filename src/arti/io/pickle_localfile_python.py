import pickle
from collections.abc import Sequence
from itertools import chain
from pathlib import Path
from typing import Any

from arti.formats.pickle import Pickle
from arti.io import read, write
from arti.storage.local import LocalFilePartition
from arti.types import Collection, Type
from arti.views.python import PythonBuiltin


def _read_pickle_file(path: str) -> Any:
    with open(path, "rb") as file:
        return pickle.load(file)


# TODO: Need to handle partitioned lists. Should I separate read and read_partitions?


@read.register
def _read_pickle_localfile_python(
    type_: Type,
    format: Pickle,
    storage_partitions: Sequence[LocalFilePartition],
    view: PythonBuiltin,
) -> Any:
    if isinstance(type_, Collection) and type_.is_partitioned:
        return list(
            chain.from_iterable(
                _read_pickle_file(storage_partition.path)
                for storage_partition in storage_partitions
            )
        )
    else:
        if len(storage_partitions) != 1:
            raise ValueError(f"Multiple partitions can only be read into a list, not {view}")
        return _read_pickle_file(storage_partitions[0].path)


@write.register
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
