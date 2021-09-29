import pickle
from datetime import date, datetime

from arti.formats.pickle import Pickle
from arti.internal.type_hints import NoneType
from arti.internal.utils import named_temporary_file
from arti.io import read, write
from arti.storage.local import LocalFilePartition
from arti.views import View
from arti.views.python import Date, Datetime, Dict, Float, Int, Null, Str


def test_python_View() -> None:
    for val, view, python_type in [
        (date(1970, 1, 1), Date, date),
        (datetime(1970, 1, 1, 0), Datetime, datetime),
        (dict(a=1), Dict, dict),
        (1.0, Float, float),
        (1, Int, int),
        (None, Null, NoneType),
        ("", Str, str),
    ]:
        binary = pickle.dumps(val)
        with named_temporary_file("w+b") as f:
            f.write(binary)
            f.seek(0)

            test_format = Pickle()
            test_storage_partition = LocalFilePartition(keys={}, path=f.name)
            test_view = view()
            assert View._registry_[python_type] is view
            # read returns a list, matching the passed partitions
            data, *tail = read(
                format=test_format, storage_partitions=[test_storage_partition], view=test_view
            )
            assert data == val
            assert not tail

            f.truncate()
            write(
                data, format=test_format, storage_partition=test_storage_partition, view=test_view
            )
            assert f.read() == binary
