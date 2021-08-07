import pickle
from datetime import date, datetime

from arti.formats.pickle import Pickle
from arti.internal.utils import named_temporary_file
from arti.io import read, write
from arti.storage.local import LocalFile
from arti.views import View
from arti.views.python import Date, Datetime, Dict, Float, Int, Null, Str


def test_python_View() -> None:
    for val, view, python_type in [
        (date(1970, 1, 1), Date, date),
        (datetime(1970, 1, 1, 0), Datetime, datetime),
        (dict(a=1), Dict, dict),
        (1.0, Float, float),
        (1, Int, int),
        (None, Null, type(None)),
        ("", Str, str),
    ]:
        binary = pickle.dumps(val)
        with named_temporary_file("w+b") as f:
            f.write(binary)
            f.seek(0)

            test_format = Pickle()
            test_storage = LocalFile(path=f.name)
            test_view = view()
            assert View._registry_[python_type] is view
            data = read(test_format, test_storage, test_view)
            assert data == val

            f.truncate()
            write(data, test_format, test_storage, test_view)
            assert f.read() == binary
