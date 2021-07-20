import pytest

from arti.artifacts.core import Artifact
from arti.formats.core import Format
from arti.storage.core import Storage
from arti.types.core import Int64, Type
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
        error: bool

        def supports(self, type_: Type) -> None:
            if self.error:
                raise ValueError("Format - Boo!")

    class S1(Storage):
        error: bool

        def supports(self, type_: Type, format: Format) -> None:
            if self.error:
                raise ValueError("Storage - Boo!")

    with pytest.raises(ValueError, match="Format - Boo!"):

        class BadFormatArtifact(Artifact):
            type = Int64()
            format = F1(error=True)
            storage = S1(error=False)

    with pytest.raises(ValueError, match="Storage - Boo!"):

        class BadStorageArtifact(Artifact):
            type = Int64()
            format = F1(error=False)
            storage = S1(error=True)

    # Check Artifact can validate w/o storage
    class NoStorage(Artifact):
        type = Int64()
        format = F1(error=False)
