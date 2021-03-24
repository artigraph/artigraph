import pytest

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
    id_graph_a = id(graph.artifacts.a)

    with pytest.raises(Exception, match="Box is frozen"):
        graph.artifacts.a = A1()

    with graph.extend("test 2") as graph2:
        graph2.artifacts.z = A1()
    # TODO: Verify the Producers have TypedProxies pointing to this graph instead of the original.
    assert set(graph2.artifacts) == set(["a", "b", "c", "d", "z"])
    assert id_graph_a != id(graph2.artifacts.a)
