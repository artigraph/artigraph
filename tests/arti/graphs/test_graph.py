import re
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
from arti.producers import producer
from arti.storage.local import LocalFile, LocalFilePartition
from arti.types import Int64
from arti.views import python as python_views
from tests.arti.dummies import A1, A2, A3, A4, P1, P2


# TODO: Add a test-scoped fixture to generate these (may need to be a func that can generate multiple).
class Num(Artifact):
    type: Int64 = Int64()
    format: JSON = JSON()
    # Require callers to set the storage instance in a separate tempdir.
    storage: LocalFile


@producer()
def div(a: Annotated[int, Num], b: Annotated[int, Num]) -> Annotated[int, Num]:
    return a // b


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
    assert not graph.artifacts.a.storage.includes_input_fingerprint_template
    assert graph.artifacts.b.storage.includes_input_fingerprint_template
    assert graph.artifacts.c.a.storage.includes_input_fingerprint_template
    assert graph.artifacts.c.b.storage.includes_input_fingerprint_template


def test_Graph_compute_id() -> None:
    with Graph(name="test") as g:
        g.artifacts.a = A1()
        p1 = P1(a1=g.artifacts.a)
        g.artifacts.b = cast(A2, p1.out())

    id_components = [
        g.fingerprint,
        Fingerprint.from_string("a"),
        Fingerprint.from_string("b"),
        g.artifacts.a.fingerprint,
        g.artifacts.b.fingerprint,
        p1.fingerprint,
        *(
            storage_partition.with_content_fingerprint().fingerprint
            for storage_partition in g.artifacts.a.discover_storage_partitions()
        ),
    ]

    assert g.compute_id() == Fingerprint.combine(*id_components)
    # Ensure order independence
    assert g.compute_id() == Fingerprint.combine(*reversed(id_components))


def test_Graph_compute_id_missing_input_artifact() -> None:
    with TemporaryDirectory() as _dir:
        dir = Path(_dir)
        with Graph(name="test") as g:
            g.artifacts.a = Num(storage=LocalFile(path=str(dir / "a.json")))

        with pytest.raises(ValueError, match=re.escape("No data found for `a`")):
            assert g.compute_id()


def test_Graph_compute_id_producer_arg_order() -> None:
    with TemporaryDirectory() as _dir:
        dir = Path(_dir)
        a = Num(storage=LocalFile(path=str(dir / "a.json")))
        with open(a.storage.path, "w") as f:
            f.write("10")
        b = Num(storage=LocalFile(path=str(dir / "b.json")))
        with open(b.storage.path, "w") as f:
            f.write("5")
        c = Num(storage=LocalFile(path=str(dir / "{input_fingerprint}/c.json")))

        # Create two Graphs, varying only by the arg order to the Producer.
        with Graph(name="test") as g_ab:
            g_ab.artifacts.c = div(a=a, b=b).out(c)
        with Graph(name="test") as g_ba:
            g_ba.artifacts.c = div(a=b, b=a).out(c)

        assert g_ab.compute_id() != g_ba.compute_id()


def test_Graph_build() -> None:
    side_effect = 0

    @producer()
    def increment(i: Annotated[int, Num]) -> Annotated[int, Num]:
        nonlocal side_effect
        side_effect += 1
        return i + 1

    @producer()
    def dup(i: Annotated[int, Num]) -> tuple[Annotated[int, Num], Annotated[int, Num]]:
        return i, i

    with TemporaryDirectory() as _dir:
        dir = Path(_dir)
        with Graph(name="test") as g:
            g.artifacts.a = Num(storage=LocalFile(path=str(dir / "a.json")))
            g.artifacts.b = increment(i=g.artifacts.a).out(
                Num(storage=LocalFile(path=str(dir / "b/{input_fingerprint}.json")))
            )
            # Test multiple return values
            g.artifacts.c, g.artifacts.d = dup(i=g.artifacts.a).out(
                Num(storage=LocalFile(path=str(dir / "c/{input_fingerprint}.json"))),
                Num(storage=LocalFile(path=str(dir / "d/{input_fingerprint}.json"))),
            )

        a, b, c, d = (
            g.artifacts.a,
            cast(Num, g.artifacts.b),
            cast(Num, g.artifacts.c),
            cast(Num, g.artifacts.d),
        )
        # Bootstrap the initial artifact and build
        g.write(0, artifact=a)
        g.build()
        assert side_effect == 1
        assert g.read(b, annotation=int) == 1
        assert g.read(c, annotation=int) == g.read(d, annotation=int) == 0
        # A second build should no-op
        g.build(executor=LocalExecutor())
        assert side_effect == 1
        assert g.read(b, annotation=int) == 1
        assert g.read(c, annotation=int) == g.read(d, annotation=int) == 0
        # Changing the raw Artifact data should trigger a rerun
        g.write(1, artifact=a)
        g.build()
        assert side_effect == 2
        assert g.read(b, annotation=int) == 2
        assert g.read(c, annotation=int) == g.read(d, annotation=int) == 1
        # Changing back to the original data should no-op
        g.write(0, artifact=a)
        g.build()
        assert side_effect == 2
        assert g.read(b, annotation=int) == 1
        assert g.read(c, annotation=int) == g.read(d, annotation=int) == 0


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
        with pytest.raises(
            ValueError,
            match=re.escape("Writing to a raw Artifact (`i`) would cause a `graph_id` change."),
        ):
            g.write(10, artifact=i, graph_id=Fingerprint.from_string("junk"))
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


def test_Graph_storage_resolution() -> None:
    with Graph(name="test", path_tags={"tag": "value"}) as g:
        g.artifacts.root.a = Num(storage=LocalFile())
        g.artifacts.root.b = Num(storage=LocalFile())
        g.artifacts.c = cast(
            Num, div(a=g.artifacts.root.a, b=g.artifacts.root.b).out(Num(storage=LocalFile()))
        )
        with pytest.raises(
            ValueError,
            match=re.escape(
                "Produced Artifacts must have a '{input_fingerprint}' template in their Storage"
            ),
        ):
            g.artifacts.d = div(a=g.artifacts.root.a, b=g.artifacts.root.b).out(
                Num(storage=LocalFile(path="junk"))
            )

    assert g.artifacts.root.a.storage.path.endswith("/test/tag=value/root/a/a.json")
    assert g.artifacts.root.b.storage.path.endswith("/test/tag=value/root/b/b.json")
    assert g.artifacts.c.storage.path.endswith("/test/tag=value/c/{input_fingerprint}/c.json")
