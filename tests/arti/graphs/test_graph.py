from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Annotated, cast

import pytest
from box import BoxError

from arti.artifacts import Artifact
from arti.executors.local import LocalExecutor
from arti.fingerprints import Fingerprint
from arti.formats.json import JSON
from arti.graphs import Graph
from arti.internal.utils import frozendict
from arti.partitions import CompositeKey
from arti.producers import Producer
from arti.storage.local import LocalFile, LocalFilePartition
from arti.types import Int64
from arti.views import python as python_views
from tests.arti.dummies import A1, A2, A3, A4, P1, P2


class Num(Artifact):
    type: Int64 = Int64()
    format: JSON = JSON()
    # Require callers to set the storage instance in a separate tempdir.
    storage: LocalFile


@pytest.fixture
def graph() -> Graph:
    # NOTE: .out() supports strict Artifact subclass mypy typing with the mypy_plugin, but Producers
    # also support simple iteration (eg: `a, b = MyProducer(...)`).
    with Graph(name="test") as g:
        g.artifacts.a = A1()
        g.artifacts.b = P1(a1=g.artifacts.a).out()
        g.artifacts.c.a, g.artifacts.c.b = P2(a2=g.artifacts.b).out()
    return g


def test_Graph(graph: Graph) -> None:
    assert isinstance(graph.artifacts.a, A1)
    assert isinstance(graph.artifacts.b, A2)
    assert isinstance(graph.artifacts.c.a, A3)
    assert isinstance(graph.artifacts.c.b, A4)


def test_Graph_build() -> None:
    side_effect = 0

    class Increment(Producer):
        i: Num

        @staticmethod
        def build(i: int) -> Annotated[int, Num]:
            nonlocal side_effect
            side_effect = i + 1
            return side_effect

    with TemporaryDirectory() as _dir:
        dir = Path(_dir)
        with Graph(name="test") as g:
            g.artifacts.a = Num(storage=LocalFile(path=str(dir / "a.json")))
            g.artifacts.b = Increment(i=g.artifacts.a).out(
                Num(storage=LocalFile(path=str(dir / "b.json")))
            )

        a, b = g.artifacts.a, cast(A2, g.artifacts.b)
        # Bootstrap the initial artifact
        g.write(side_effect, artifact=a)
        g.build()
        assert side_effect == 1
        assert 1 == g.read(b, annotation=int)
        g.build(executor=LocalExecutor())
        # Second build should no-op
        assert side_effect == 1


def test_Graph_dependencies(graph: Graph) -> None:
    p1 = graph.artifacts.b.producer_output.producer
    p2 = graph.artifacts.c.a.producer_output.producer
    assert graph.dependencies == frozendict(
        {
            graph.artifacts.a: frozenset(),
            p1: frozenset({graph.artifacts.a}),
            graph.artifacts.b: frozenset({p1}),
            p2: frozenset({graph.artifacts.b}),
            graph.artifacts.c.a: frozenset({p2}),
            graph.artifacts.c.b: frozenset({p2}),
        }
    )


def test_Graph_errors() -> None:
    with Graph(name="test") as graph:
        graph.artifacts.a = A1()
        graph.artifacts.b = P1(a1=graph.artifacts.a).out()

    with pytest.raises(BoxError, match="Box is frozen"):
        graph.artifacts.a = A1()
    with pytest.raises(AttributeError, match="has no attribute"):
        graph.artifacts.z

    with Graph(name="outer"):
        with pytest.raises(ValueError, match="Another graph is being defined"):
            with Graph(name="inner"):
                pass


def test_Graph_producers(graph: Graph) -> None:
    p1 = graph.artifacts.b.producer_output.producer
    p2 = graph.artifacts.c.a.producer_output.producer
    assert graph.producers == frozenset({p1, p2})


def test_Graph_producer_output(graph: Graph) -> None:
    p1 = graph.artifacts.b.producer_output.producer
    p2 = graph.artifacts.c.a.producer_output.producer
    assert graph.producer_outputs == frozendict(
        {
            p1: (graph.artifacts.b,),
            p2: (graph.artifacts.c.a, graph.artifacts.c.b),
        }
    )

    with Graph(name="test") as g:
        with pytest.raises(
            ValueError,
            match="producer_outputs cannot be used while the Graph is still being defined",
        ):
            g.producer_outputs


def test_Graph_read_write() -> None:
    with TemporaryDirectory() as _dir:
        dir = Path(_dir)
        with Graph(name="test") as g:
            g.artifacts.i = Num(storage=LocalFile(path=str(dir / "i.json")))

        i = g.artifacts.i
        # Test write
        storage_partition = g.write(5, artifact=i)
        assert isinstance(storage_partition, LocalFilePartition)
        assert storage_partition.content_fingerprint != Fingerprint.empty()
        assert storage_partition.input_fingerprint == Fingerprint.empty()
        assert storage_partition.keys == CompositeKey()
        assert storage_partition.path.endswith(i.format.extension)
        # Test read
        assert g.read(i, annotation=int) == 5
        assert g.read(i, view=python_views.Int()) == 5
        assert g.read(i, annotation=int, storage_partitions=[storage_partition]) == 5
        with pytest.raises(ValueError, match="Either `annotation` or `view` must be passed"):
            g.read(i)
        with pytest.raises(ValueError, match="Only one of `annotation` or `view` may be passed"):
            g.read(i, annotation=int, view=python_views.Int())


def test_Graph_references(graph: Graph) -> None:
    with Graph(name="test-2") as g2:
        g2.artifacts.upstream.a = graph.artifacts.a
    assert graph.artifacts.a == g2.artifacts.upstream.a
