import pytest
from box import BoxError

from arti.graphs.core import Graph
from tests.arti.dummies import A1, A2, A3, A4, P1, P2, P3


def test_Graph() -> None:
    # NOTE: .out() supports strict Artifact subclass mypy typing with the mypy_plugin, but Producers
    # also support simple iteration (eg: `a, b = MyProducer(...)`).
    with Graph("test") as g1:
        g1.artifacts.a = A1()
        g1.artifacts.b.c = P1(a1=g1.artifacts.a).out()
        g1.artifacts.d, g1.artifacts.e = P2(a2=g1.artifacts.b.c).out()
        g1.artifacts.f, g1.artifacts.g = P3(a1=g1.artifacts.a, a2=g1.artifacts.b.c).out()

    with Graph("test-2") as g2:
        g2.artifacts.upstream.a = g1.artifacts.a

    assert isinstance(g1.artifacts.a, A1)
    assert isinstance(g1.artifacts.b.c, A2)
    assert isinstance(g1.artifacts.d, A3)
    assert isinstance(g1.artifacts.e, A4)
    assert isinstance(g1.artifacts.f, A3)
    assert isinstance(g1.artifacts.g, A4)
    assert g1.artifacts.a is g2.artifacts.upstream.a


def test_Graph_errors() -> None:
    with Graph("test") as graph:
        graph.artifacts.a = A1()
        graph.artifacts.b = P1(a1=graph.artifacts.a).out()

    with pytest.raises(BoxError, match="Box is frozen"):
        graph.artifacts.a = A1()
    with pytest.raises(AttributeError, match="has no attribute"):
        graph.artifacts.z
