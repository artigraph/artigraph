import pickle

from arti.formats.pickle import Pickle
from arti.internal.utils import named_temporary_file
from arti.io.core import read, write
from arti.storage.local import LocalFile
from arti.views.python import Python


def test_Python_View() -> None:
    val = 1
    binary = pickle.dumps(val)
    with named_temporary_file("w+b") as f:
        f.write(binary)
        f.seek(0)

        test_format = Pickle()
        test_storage = LocalFile(path=f.name)
        test_view = Python()
        data = read(test_format, test_storage, test_view)
        assert data == val

        f.truncate()
        write(data, test_format, test_storage, test_view)
        assert f.read() == binary
