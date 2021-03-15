from typing import Optional

import pytest

from arti.artifacts.core import Artifact
from arti.formats.core import Format
from arti.storage.core import Storage
from arti.types.core import Int32, String, Struct, Type
from tests.arti.dummies import A1, A2, P1, P2, DummyFormat


def test_cast() -> None:
    assert isinstance(Artifact.cast(A1()), A1)
    assert isinstance(Artifact.cast(P1(a1=A1())), A2)
    with pytest.raises(ValueError, match="P2 produces 2 Artifacts"):
        Artifact.cast(P2(a2=A2()))


@pytest.mark.xfail  # Artifact casting to python objects is not implemented yet.
def test_cast_todo() -> None:
    Artifact.cast(5)
    Artifact.cast("hi")
    Artifact.cast([1, 2, 3])


def test_class_validation() -> None:
    class F1(DummyFormat):
        def __init__(self, error: bool) -> None:
            self.error = error

        def validate(self, schema: Optional[Type]) -> None:
            if self.error:
                raise ValueError("Format - Boo!")

    class S1(Storage):
        def __init__(self, error: bool) -> None:
            self.error = error

        def validate(self, schema: Optional[Type], format: Optional[Format]) -> None:
            if self.error:
                raise ValueError("Storage - Boo!")

    with pytest.raises(ValueError, match="Format - Boo!"):

        class BadFormatArtifact(Artifact):
            format = F1(error=True)
            storage = S1(error=False)

    with pytest.raises(ValueError, match="Storage - Boo!"):

        class BadStorageArtifact(Artifact):
            format = F1(error=False)
            storage = S1(error=True)


def test_instantiate() -> None:
    format = Format("csv")
    storage = Storage("GCS")
    schema = Struct({"int_col": Int32(), "str_col": String()})

    artifact = Artifact(schema=schema, format=format, storage=storage)
    assert isinstance(artifact.format, Format)
    assert artifact.format.type == "csv"
    assert isinstance(artifact.schema, Struct)
    assert isinstance(artifact.schema.fields["str_col"], String)


def test_to_from_dict() -> None:
    artifact_dict = {
        "key": "test",
        "fingerprint": "123",
        "schema": {
            "type": "Struct",
            "params": {"fields": {"int_col": {"type": "Int32"}, "str_col": {"type": "String"}}},
        },
        "format": {"type": "csv"},
        "storage": {"type": "GCS", "path": "a/b/c"},
    }
    artifact = Artifact.from_dict(artifact_dict)
    assert isinstance(artifact, Artifact)
    assert isinstance(artifact.storage, Storage)
    assert isinstance(artifact.schema, Struct)
    assert isinstance(artifact.schema.fields["int_col"], Int32)

    assert artifact_dict.items() <= artifact.to_dict().items()
