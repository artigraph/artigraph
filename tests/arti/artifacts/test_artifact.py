import pytest

from arti.artifacts.core import Artifact
from tests.arti.dummies import A1, A2, P1, P2


def test_Artifact_cast() -> None:
    assert isinstance(Artifact.cast(A1()), A1)
    assert isinstance(Artifact.cast(P1(input_artifact=A1())), A2)
    with pytest.raises(ValueError, match="P2 produces 2 Artifacts"):
        Artifact.cast(P2(input_artifact=A2()))


# TODO: Artifact casting to python objects is not implemented yet.
@pytest.mark.xfail
def test_Artifact_cast_todo() -> None:
    Artifact.cast(5)
    Artifact.cast("hi")
    Artifact.cast([1, 2, 3])
