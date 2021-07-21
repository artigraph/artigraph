import pickle

from arti.artifacts.core import Artifact
from arti.formats.pickle import Pickle
from arti.internal.utils import named_temporary_file
from arti.storage.local import LocalFile
from arti.views.python import Python


def test_Python_View() -> None:
    binary = pickle.dumps(1)
    with named_temporary_file("w+b") as f:
        f.write(binary)
        f.seek(0)

        a = Artifact()
        a.storage = LocalFile(path=f.name)
        a.format = Pickle()

        view = Python.read(a)
        assert view.data == 1

        f.truncate()
        view.write(a)
        assert f.read() == binary
