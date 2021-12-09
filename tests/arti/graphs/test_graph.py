import re
from pathlib import Path
from typing import Annotated, cast

import pytest
from box import BoxError

from arti.artifacts import Artifact
from arti.executors.local import LocalExecutor
from arti.fingerprints import Fingerprint
from arti.graphs import Graph
from arti.internal.utils import frozendict
from arti.partitions import CompositeKey
from arti.producers import producer
from arti.storage.literal import StringLiteral
from arti.storage.local import LocalFile, LocalFilePartition
from arti.types import Int64
from arti.views import python as python_views
from tests.arti.dummies import A1, A2, A3, A4, P1, P2
from tests.arti.dummies import Num as _Num
from tests.arti.dummies import div


class Num(_Num):
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
    assert not graph.artifacts.a.storage.includes_input_fingerprint_template
    assert graph.artifacts.b.storage.includes_input_fingerprint_template
    assert graph.artifacts.c.a.storage.includes_input_fingerprint_template
    assert graph.artifacts.c.b.storage.includes_input_fingerprint_template


def test_Graph_literals(tmp_path: Path) -> None:
    n_add_runs = 0

    @producer()
    def add(x: int, y: Annotated[int, Num]) -> int:
        nonlocal n_add_runs
        n_add_runs += 1
        return x + y

    with Graph(name="Test") as g:
        g.artifacts.x = 1
        g.artifacts.y = Num(storage=LocalFile(path=str(tmp_path / "y.json")))
        g.artifacts.z = add(x=g.artifacts.x, y=g.artifacts.y).out()
        # Changes to `phase` will cause a new snapshot_id. However, since `phase` isn't an input to
        # `add`, we *shouldn't* have to recompute `z` - assuming the backend properly stores
        # storage->storage_partitions separate from the set of storage_partitions associated with a
        # snapshot_id.
        g.artifacts.phase = Num(storage=LocalFile(path=str(tmp_path / "phase.json")))

    Int64Artifact = Artifact.from_type(Int64())
    x, y, z, phase = g.artifacts.x, g.artifacts.y, g.artifacts.z, g.artifacts.phase
    assert isinstance(x, Int64Artifact)
    assert isinstance(x.storage, StringLiteral)
    assert x.storage.value == "1"
    assert isinstance(z, Int64Artifact)
    assert isinstance(z.storage, StringLiteral)
    assert z.storage.value is None

    g.write(1, artifact=y)
    g.write(1, artifact=phase)
    with pytest.raises(FileNotFoundError, match="No data"):
        g.read(z, annotation=int)

    # Run the initial build to compute z
    g.build()
    assert g.read(z, annotation=int) == 2
    assert n_add_runs == 1
    assert len(g.backend.read_graph_partitions(g.name, g.get_snapshot_id(), "z")) == 1
    assert len(g.backend.read_storage_partitions(z.storage)) == 1
    # A subsequent build shouldn't require a rerun, ensuring we properly lookup existing literals.
    g.build()
    assert g.read(z, annotation=int) == 2
    assert n_add_runs == 1
    assert len(g.backend.read_graph_partitions(g.name, g.get_snapshot_id(), "z")) == 1
    assert len(g.backend.read_storage_partitions(z.storage)) == 1
    # Changing an input should trigger a rerun. There will still only be 1 z literal for this graph,
    # but now 2 overall for the storage (with different `input_fingerprint`s).
    g.write(2, artifact=y)
    g.build()
    assert g.read(z, annotation=int) == 3
    assert n_add_runs == 2
    assert len(g.backend.read_graph_partitions(g.name, g.get_snapshot_id(), "z")) == 1
    assert len(g.backend.read_storage_partitions(z.storage)) == 2
    # After getting a new snapshot_id, but no changes to `add`s inputs, ensure we properly lookup
    # existing literals - even though the snapshot_id will change, the input_fingerprint for `z`
    # will not.
    g.write(2, artifact=phase)
    g.build()
    assert g.read(z, annotation=int) == 3
    assert n_add_runs == 2
    assert len(g.backend.read_graph_partitions(g.name, g.get_snapshot_id(), "z")) == 1
    assert len(g.backend.read_storage_partitions(z.storage)) == 2


def test_Graph_snapshot() -> None:
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

    snapshot = g.snapshot()
    assert snapshot.snapshot_id == Fingerprint.combine(*id_components)
    # Ensure order independence
    assert snapshot.snapshot_id == Fingerprint.combine(*reversed(id_components))
    # Ensure snapshot of a snapshot doesn't copy
    assert snapshot.snapshot() is snapshot


def test_Graph_get_snapshot_id(tmp_path: Path) -> None:
    with Graph(name="test") as g:
        g.artifacts.a = 5

    assert g.snapshot_id is None
    snapshot = g.snapshot()
    assert snapshot.snapshot_id is not None
    assert snapshot.snapshot_id == g.get_snapshot_id()
    # Confirm snapshot_id is still unset on the original graph
    assert g.snapshot_id is None


def test_Graph_snapshot_missing_input_artifact(tmp_path: Path) -> None:
    with Graph(name="test") as g:
        g.artifacts.a = Num(storage=LocalFile(path=str(tmp_path / "a.json")))

    with pytest.raises(ValueError, match=re.escape("No data found for `a`")):
        assert g.snapshot()


def test_Graph_snapshot_id_producer_arg_order(tmp_path: Path) -> None:
    a = Num(storage=LocalFile(path=str(tmp_path / "a.json")))
    with open(a.storage.path, "w") as f:
        f.write("10")
    b = Num(storage=LocalFile(path=str(tmp_path / "b.json")))
    with open(b.storage.path, "w") as f:
        f.write("5")
    c = Num(storage=LocalFile.rooted_at(tmp_path))

    # Create two Graphs, varying only by the arg order to the Producer.
    with Graph(name="test") as g_ab:
        g_ab.artifacts.c = div(a=a, b=b).out(c)
    with Graph(name="test") as g_ba:
        g_ba.artifacts.c = div(a=b, b=a).out(c)

    assert g_ab.get_snapshot_id() != g_ba.get_snapshot_id()


def test_Graph_build(tmp_path: Path) -> None:
    n_builds = 0

    @producer()
    def increment(i: Annotated[int, Num]) -> Annotated[int, Num]:
        nonlocal n_builds
        n_builds += 1
        return i + 1

    @producer()
    def dup(i: Annotated[int, Num]) -> tuple[Annotated[int, Num], Annotated[int, Num]]:
        return i, i

    with Graph(name="test") as g:
        g.artifacts.a = Num(storage=LocalFile(path=str(tmp_path / "a.json")))
        g.artifacts.b = increment(i=g.artifacts.a).out(Num(storage=LocalFile.rooted_at(tmp_path)))
        # Test multiple return values
        g.artifacts.c, g.artifacts.d = dup(i=g.artifacts.a).out(
            Num(storage=LocalFile.rooted_at(tmp_path)),
            Num(storage=LocalFile.rooted_at(tmp_path)),
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
    assert n_builds == 1
    assert g.read(b, annotation=int) == 1
    assert g.read(c, annotation=int) == g.read(d, annotation=int) == 0
    # A second build should no-op
    g.build(executor=LocalExecutor())
    assert n_builds == 1
    assert g.read(b, annotation=int) == 1
    assert g.read(c, annotation=int) == g.read(d, annotation=int) == 0
    # Changing the raw Artifact data should trigger a rerun
    g.write(1, artifact=a)
    g.build()
    assert n_builds == 2
    assert g.read(b, annotation=int) == 2
    assert g.read(c, annotation=int) == g.read(d, annotation=int) == 1
    # Changing back to the original data should no-op
    g.write(0, artifact=a)
    g.build()
    assert n_builds == 2
    assert g.read(b, annotation=int) == 1
    assert g.read(c, annotation=int) == g.read(d, annotation=int) == 0


def test_Graph_build_failed_validation(tmp_path: Path) -> None:
    failed_validation_msg = "This is junk data!"

    @producer(validate_outputs=lambda i: (False, failed_validation_msg))
    def angry_add(i: Annotated[int, Num]) -> Annotated[int, Num]:
        return i + 1

    num = Num(storage=LocalFile.rooted_at(tmp_path))  # Immutable, thus can reuse
    with Graph(name="test") as g:
        g.artifacts.a = num
        g.artifacts.b = cast(Num, angry_add(i=g.artifacts.a).out(num))

    g.write(0, artifact=g.artifacts.a)
    with pytest.raises(ValueError, match=failed_validation_msg):
        g.build()
    with pytest.raises(FileNotFoundError, match="No data"):
        g.read(g.artifacts.b, annotation=int)


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


def test_Graph_read_write(tmp_path: Path) -> None:
    with Graph(name="test") as g:
        g.artifacts.i = Num(storage=LocalFile(path=str(tmp_path / "i.json")))

    i = g.artifacts.i
    # Test write
    storage_partition = g.write(5, artifact=i)
    assert isinstance(storage_partition, LocalFilePartition)
    assert storage_partition.content_fingerprint != Fingerprint.empty()
    assert storage_partition.input_fingerprint == Fingerprint.empty()
    assert storage_partition.keys == CompositeKey()
    assert storage_partition.path.endswith(i.format.extension)
    # Once snapshotted, writing to the raw Artifacts would result in a different snapshot.
    with pytest.raises(
        ValueError,
        match=re.escape("Writing to a raw Artifact (`i`) would cause a `snapshot_id` change."),
    ):
        g.snapshot().write(10, artifact=i)
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
