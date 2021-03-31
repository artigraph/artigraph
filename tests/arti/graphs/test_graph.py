import pytest
from box import BoxError

from arti.graphs.core import Graph
from tests.arti.dummies import A1, P1, P2


def test_Graph() -> None:
    # NOTE: .out() supports strict Artifact subclass mypy typing with the mypy_plugin, but Producers
    # also support simple iteration (eg: `a, b = MyProducer(...)`).
    with Graph("test") as graph:
        graph.artifacts.a = A1()
        graph.artifacts.b.c = P1(input_artifact=graph.artifacts.a).out()
        graph.artifacts.c, graph.artifacts.d = P2(input_artifact=graph.artifacts.b.c).out()

    with pytest.raises(BoxError, match="Box is frozen"):
        graph.artifacts.a = A1()
    with pytest.raises(AttributeError, match="has no attribute"):
        graph.artifacts.z
