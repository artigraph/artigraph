import pytest
from box import BoxError  # type: ignore

from arti.graphs.core import Graph
from tests.arti.dummies import A1, P1, P2


def test_Graph() -> None:
    with Graph("test") as graph:
        graph.artifacts.a = A1()
        graph.artifacts.b.c = P1(input_artifact=graph.artifacts.a)
        # mypy thinks (understandably) that b.c is a Producer instance and didn't catch the casting.
        # We may need to make a mypy plugin[1] to parse these, unless there's a way to type hint Box
        # appropriately (even for these nested creations).
        #
        # 1:https://mypy.readthedocs.io/en/stable/extending_mypy.html#extending-mypy-using-plugins
        graph.artifacts.c, graph.artifacts.d = P2(input_artifact=graph.artifacts.b.c)  # type: ignore

    with pytest.raises(BoxError, match="Box is frozen"):
        graph.artifacts.a = A1()
    with pytest.raises(AttributeError, match="has no attribute"):
        graph.artifacts.z
