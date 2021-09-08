import pickle
from datetime import date, datetime
from typing import Any, ClassVar

from arti.formats.pickle import Pickle
from arti.internal.type_hints import NoneType
from arti.io import read, write
from arti.storage.local import LocalFile
from arti.types import TypeSystem
from arti.types.python import python_type_system
from arti.views import View


class _PythonBuiltin(View):
    _abstract_ = True

    type_system: ClassVar[TypeSystem] = python_type_system


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


class Null(_PythonBuiltin):
    python_type = NoneType


class Str(_PythonBuiltin):
    python_type = str


@read.register
def _read_pickle_localfile_python(
    *, format: Pickle, storage: LocalFile, view: _PythonBuiltin
) -> Any:
    with open(storage.path, "rb") as file:
        return pickle.load(file)


@write.register
def _write_pickle_localfile_python(
    data: Any, *, format: Pickle, storage: LocalFile, view: _PythonBuiltin
) -> None:
    with open(storage.path, "wb") as file:
        pickle.dump(data, file)
