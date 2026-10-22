import pickle
from datetime import date, datetime
from pathlib import Path
from types import NoneType
from typing import Annotated

from arti import Artifact, Type, View, read, types, write
from arti.formats.json import JSON
from arti.formats.pickle import Pickle
from arti.storage.local import LocalFile, LocalFilePartition
from arti.views.python import Date, Datetime, Dict, Float, Int, Null, Str


def test_python_View(tmp_path: Path) -> None:
    for val, view_class, python_type in [
        ("", Str, str),
        (1, Int, int),
        (1.0, Float, float),
        (None, Null, None),
        (date(1970, 1, 1), Date, date),
        (datetime(1970, 1, 1, 0), Datetime, datetime),
        ({"a": 1}, Dict, dict[str, int]),
    ]:
        view = View.from_annotation(python_type, mode="READWRITE")
        assert isinstance(view, view_class)
        assert view.artifact_class is Artifact
        assert view.type == view.type_system.to_artigraph(python_type, hints={})

        test_format = Pickle()
        binary = pickle.dumps(val)
        path = tmp_path / f"{view_class.__name__}.pickle"
        path.write_bytes(binary)
        test_storage_partition = LocalFilePartition(path=str(path), storage=LocalFile())

        # read returns a list, matching the passed partitions
        data = read(
            type_=view.type,
            format=test_format,
            storage_partition_snapshots=(test_storage_partition.snapshot(),),
            view=view,
        )
        assert data == val

        path.unlink()
        write(
            data,
            type_=view.type,
            format=test_format,
            storage_partition=test_storage_partition,
            view=view,
        )
        assert path.read_bytes() == binary


class NullArtifact(Artifact):
    type: Type = types.Null()


def test_python_View_null(tmp_path: Path) -> None:
    artifact = NullArtifact(format=JSON(), storage=LocalFile(path=str(tmp_path / "null.json")))
    # `Annotated` and `get_type_hints` convert a `None` hint to `NoneType`.
    for annotation in [None, NoneType, Annotated[None, NullArtifact]]:
        view = View.from_annotation(annotation, mode="READWRITE")
        assert isinstance(view, Null)
        view.check_artifact_compatibility(artifact)
