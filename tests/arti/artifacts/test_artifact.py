from typing import Optional

import pytest

from arti.artifacts.core import Artifact
from arti.formats.core import Format
from arti.storage.core import Storage
from arti.types.core import Type
from tests.arti.dummies import A1, A2, P1, P2, TestFormat


def test_Artifact_cast() -> None:
    assert isinstance(Artifact.cast(A1()), A1)
    assert isinstance(Artifact.cast(P1(input_artifact=A1())), A2)
    with pytest.raises(ValueError, match="P2 produces 2 Artifacts"):
        Artifact.cast(P2(input_artifact=A2()))


def test_Artifact_class_validation() -> None:
    class F1(TestFormat):
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


# TODO: Artifact casting to python objects is not implemented yet.
@pytest.mark.xfail
def test_Artifact_cast_todo() -> None:
    Artifact.cast(5)
    Artifact.cast("hi")
    Artifact.cast([1, 2, 3])
