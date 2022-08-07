import pickle
from datetime import date, datetime

from arti import Artifact, View, read, write
from arti.formats.pickle import Pickle
from arti.internal.type_hints import NoneType
from arti.internal.utils import named_temporary_file
from arti.storage.local import LocalFilePartition
from arti.views.python import Date, Datetime, Dict, Float, Int, Null, Str


def test_python_View() -> None:
    for val, view_class, python_type in [
        (date(1970, 1, 1), Date, date),
        (datetime(1970, 1, 1, 0), Datetime, datetime),
        (dict(a=1), Dict, dict[str, int]),
        (1.0, Float, float),
        (1, Int, int),
        (None, Null, NoneType),
        ("", Str, str),
    ]:
        view = View.from_annotation(python_type, mode="READWRITE")
        assert isinstance(view, view_class)
        assert view.artifact_class is Artifact
        assert view.type == view.type_system.to_artigraph(python_type, hints={})

        test_format = Pickle()

        binary = pickle.dumps(val)
        with named_temporary_file("w+b") as f:
            test_storage_partition = LocalFilePartition(
                format=test_format, path=f.name, type=view.type
            )

            f.write(binary)
            f.seek(0)

            # read returns a list, matching the passed partitions
            data = read(
                type_=view.type,
                format=test_format,
                storage_partitions=(test_storage_partition,),
                view=view,
            )
            assert data == val

            f.truncate()
            write(
                data,
                type_=view.type,
                format=test_format,
                storage_partition=test_storage_partition,
                view=view,
            )
            assert f.read() == binary
