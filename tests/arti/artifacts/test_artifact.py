import pytest

from arti.annotations.core import Annotation
from arti.artifacts.core import Artifact
from arti.formats.core import Format
from arti.statistics.core import Statistic
from arti.types.core import Int64, Type
from tests.arti.dummies import A1, A2, P1, P2, DummyFormat, DummyStatistic, DummyStorage


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

    class S1(DummyStorage):
        error: bool

        def supports(self, type_: Type, format: Format) -> None:
            if self.error:
                raise ValueError("Storage - Boo!")

    with pytest.raises(ValueError, match="Format - Boo!"):

        class BadFormatArtifact(Artifact):
            type: Int64 = Int64()
            format: F1 = F1(error=True)
            storage: S1 = S1(error=False)

        BadFormatArtifact()

    with pytest.raises(ValueError, match="Storage - Boo!"):

        class BadStorageArtifact(Artifact):
            type: Int64 = Int64()
            format: F1 = F1(error=False)
            storage: S1 = S1(error=True)

        BadStorageArtifact()


def test_instance_attr_merging() -> None:
    class Ann1(Annotation):
        x: int

    class Ann2(Annotation):
        y: int

    class Stat1(DummyStatistic):
        pass

    class Stat2(DummyStatistic):
        pass

    class MyArtifact(A1):
        annotations: tuple[Annotation, ...] = (Ann1(x=5),)
        statistics: tuple[Statistic, ...] = (Stat1(),)

    artifact = MyArtifact(annotations=[Ann2(y=10)], statistics=[Stat2()])
    assert tuple(type(a) for a in artifact.annotations) == (Ann1, Ann2)
    assert tuple(type(s) for s in artifact.statistics) == (Stat1, Stat2)
