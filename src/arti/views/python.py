import pickle
from datetime import date, datetime
from typing import Any

from arti.formats.pickle import Pickle
from arti.internal.type_hints import NoneType
from arti.io import read, write
from arti.storage.local import LocalFilePartition
from arti.types.python import python_type_system
from arti.views import View


class _PythonBuiltin(View):
    _abstract_ = True

    type_system = python_type_system


class Date(_PythonBuiltin):
    python_type = date


class Datetime(_PythonBuiltin):
    python_type = datetime


class Dict(_PythonBuiltin):
    python_type = dict


class Float(_PythonBuiltin):
    python_type = float


class Int(_PythonBuiltin):
    python_type = int


class List(_PythonBuiltin):
    python_type = list


class Null(_PythonBuiltin):
    python_type = NoneType


class Str(_PythonBuiltin):
    python_type = str


def _read_pickle_file(path: str) -> Any:
    with open(path, "rb") as file:
        return pickle.load(file)


@read.register
def _read_pickle_localfile_python(
    *, format: Pickle, storage_partitions: list[LocalFilePartition], view: _PythonBuiltin
) -> Any:
    return [_read_pickle_file(storage_partition.path) for storage_partition in storage_partitions]


@write.register
def _write_pickle_localfile_python(
    data: Any, *, format: Pickle, storage_partition: LocalFilePartition, view: _PythonBuiltin
) -> None:
    with open(storage_partition.path, "wb") as file:
        pickle.dump(data, file)
