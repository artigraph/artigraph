import pickle
from typing import Any, ClassVar

from arti.formats.pickle import Pickle
from arti.io.core import read, write
from arti.storage.local import LocalFile
from arti.types.core import TypeSystem
from arti.types.python import python
from arti.views.core import View


class PythonObject(View):
    __abstract__ = True

    type_system: ClassVar[TypeSystem] = python


class Int(PythonObject):
    build_type = int


@read.register
def _read_pickle_localfile_python(format: Pickle, storage: LocalFile, view: PythonObject) -> Any:
    with open(storage.path, "rb") as file:
        return pickle.load(file)


@write.register
def _write_pickle_localfile_python(
    data: Any, format: Pickle, storage: LocalFile, view: PythonObject
) -> None:
    with open(storage.path, "wb") as file:
        pickle.dump(data, file)
