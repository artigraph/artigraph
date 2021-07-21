import pickle
from typing import Any, ClassVar

from arti.formats.pickle import Pickle
from arti.storage.local import LocalFile
from arti.types.core import TypeSystem
from arti.types.python import python
from arti.views.core import View, read, write


class Python(View):
    type_system: ClassVar[TypeSystem] = python


@read.register
def _read_pickle_localfile_python(format: Pickle, storage: LocalFile, view: Python) -> Any:
    with open(storage.path, "rb") as file:
        return pickle.load(file)


@write.register
def _write_pickle_localfile_python(
    data: Any, format: Pickle, storage: LocalFile, view: Python
) -> None:
    with open(storage.path, "wb") as file:
        pickle.dump(data, file)
