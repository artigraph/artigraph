import pickle
from typing import ClassVar

from arti.formats.pickle import Pickle
from arti.storage.local import LocalFile
from arti.types.core import TypeSystem
from arti.types.python import python
from arti.views.core import View, read, write


class Python(View):
    type_system: ClassVar[TypeSystem] = python


@read.register
def _read(view: Python, format: Pickle, storage: LocalFile):
    with open(storage.path, "rb") as file:
        view.data = pickle.load(file)
    return view


@write.register
def _write(view: Python, format: Pickle, storage: LocalFile):
    with open(storage.path, "wb") as file:
        pickle.dump(view.data, file)
