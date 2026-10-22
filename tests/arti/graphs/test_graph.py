# pyright: reportPrivateUsage=false
import json
import pickle
import re
import shutil
from collections.abc import Callable
from pathlib import Path
from typing import Annotated, Any, assert_type, cast
from unittest.mock import PropertyMock, patch

import pytest

from arti import (
    Artifact,
    Fingerprint,
    Format,
    Graph,
    GraphSnapshot,
    PartitionDependencies,
    PartitionKey,
    StoragePartitionSnapshots,
    Type,
    View,
    producer,
)
from arti.backends.memory import MemoryBackend
from arti.executors.local import LocalExecutor
from arti.formats.json import JSON
from arti.graphs import ArtifactBox
from arti.partitions import Int64Field
from arti.producers import InputPartitions
from arti.storage.literal import StringLiteral
from arti.storage.local import LocalFile, LocalFilePartition
from arti.types import Collection, Int64, Struct
from tests.arti.dummies import A1, A2, A3, A4, P1, P2, div
from tests.arti.dummies import Num as _Num


class Num(_Num):
    storage: LocalFile = LocalFile()


def test_ArtifactBox() -> None:
    with Graph(name="test") as g:
        g.artifacts.a.b.c = 5
        g.artifacts.x.y.z = Artifact(type=Int64())

    assert (
        cast(StringLiteral, cast(Artifact, g.artifacts["a.b.c"]).storage).id == "test/a/b/c/c.json"
    )
    assert (
        cast(StringLiteral, cast(Artifact, g.artifacts["x.y.z"]).storage).id == "test/x/y/z/z.json"
    )

    assert g.artifacts == pickle.loads(pickle.dumps(g.artifacts))


@pytest.mark.xfail(raises=AssertionError, reason="Templates are not supported yet.")
def test_ArtifactBox_template() -> None:
    templates = ArtifactBox(a=5)
    # We don't expect this part to fail, so avoid an assert to prevent false xfails.
    if (
        cast(StringLiteral, cast(Artifact, templates.a).storage).id
        != "{graph_name}/{path_tags}/{names}/{name}.json"
    ):
        raise ValueError("Sanity check in xfail failed!")

    with Graph(name="test", artifacts=templates) as g:
        g.artifacts.z = 5
    assert g.artifacts.a.storage.id == "test/a/a.json"
    assert cast(StringLiteral, cast(Artifact, g.artifacts.z).storage).id == "test/z/z.json"


@pytest.fixture
def graph() -> Graph:
    # NOTE: .out() preserves concrete Artifact subclasses, while Producers also support simple
    # iteration (eg: `a, b = MyProducer(...)`).
    with Graph(name="test") as g:
        g.artifacts.a = A1()
        g.artifacts.b = P1(a1=g.artifacts.a)
        g.artifacts.c.a, g.artifacts.c.b = P2(a2=cast(A2, g.artifacts.b))

    return g


def test_Graph(graph: Graph) -> None:
    assert graph.artifacts._status == "closed"
    assert isinstance(graph.artifacts.a, A1)
    assert isinstance(graph.artifacts.b, A2)
    assert isinstance(graph.artifacts.c.a, A3)
    assert isinstance(graph.artifacts.c.b, A4)
    assert not cast(Artifact, graph.artifacts.a).storage.includes_input_fingerprint_template
    assert cast(Artifact, graph.artifacts.b).storage.includes_input_fingerprint_template
    assert cast(Artifact, graph.artifacts.c.a).storage.includes_input_fingerprint_template
    assert cast(Artifact, graph.artifacts.c.b).storage.includes_input_fingerprint_template
    assert "backend" not in graph.model_dump()


def _model_copy_graph(graph: Graph) -> Graph:
    return graph.model_copy()


def _pickle_graph(graph: Graph) -> Graph:
    return cast(Graph, pickle.loads(pickle.dumps(graph)))


def test_Graph_serialization_is_stable(graph: Graph) -> None:
    # Pin the serialized form and fingerprints so refactors can't silently change Graph identity.
    assert graph.artifacts.b.fingerprint == 7954616637240085606
    assert graph.artifacts.b.producer_output.producer.fingerprint == -46238299892531143
    assert graph.fingerprint == -5172931774348243978

    # Artifact subclass fields and concrete Storage fields must be included.
    class Noted(Artifact):
        type: Type = Int64()
        note: str = "hi"

    with Graph(name="test") as g:
        g.artifacts.n = Noted(storage=StringLiteral(value="1"))
    assert json.loads(g.model_dump_json())["artifacts"] == {
        "n": {
            "_arti_type_": "Noted",
            "annotations": [],
            "format": {"_arti_type_": "JSON", "extension": ".json"},
            "note": "hi",
            "statistics": [],
            "storage": {"_arti_type_": "StringLiteral", "id": "test/n/n.json", "value": "1"},
            "type": {"_arti_type_": "Int64", "nullable": False},
        }
    }


@pytest.mark.parametrize(
    "copier",
    [_model_copy_graph, _pickle_graph],
    ids=["model-copy", "pickle"],
)
def test_Graph_copy(graph: Graph, copier: Callable[[Graph], Graph]) -> None:
    # There have been edge cases in pydantic when copying a model with a mapping subclass field[1],
    # so double check things are ok.
    #
    # 1: https://github.com/pydantic/pydantic/issues/5225
    copy = copier(graph)

    assert isinstance(copy.artifacts, ArtifactBox)
    assert copy.artifacts._status == "closed"
    assert graph.artifacts == copy.artifacts

    assert graph.model_dump() == copy.model_dump()
    assert graph == copy

    assert graph.fingerprint == copy.fingerprint
    assert hash(graph) == hash(copy)
    # The copy must remain sealed (eg: unpickled status strings are equal, but not identical).
    assert copy.producer_outputs == graph.producer_outputs


def test_Graph_user_names_do_not_collide_with_internals() -> None:
    # Producer inputs and Graph artifact names are user-defined, so internal metadata must not
    # shadow them.
    @producer()
    def add(
        outputs: int,
        build_inputs: int,
        map_inputs: int,
        input_artifact_classes: int,
        build_signature: int,
        map_signature: int,
        iter_outputs: int,
        arti_type_key: int,
        arti_fingerprint_fields: int,
    ) -> int:
        return (
            outputs
            + build_inputs
            + map_inputs
            + input_artifact_classes
            + build_signature
            + map_signature
            + iter_outputs
            + arti_type_key
            + arti_fingerprint_fields
        )

    names = [
        "outputs",
        "build_inputs",
        "map_inputs",
        "input_artifact_classes",
        "build_signature",
        "map_signature",
        "iter_outputs",
        "arti_type_key",
        "arti_fingerprint_fields",
    ]
    with Graph(name="test") as g:
        g.artifacts.status = 1
        g.artifacts.total = cast(Any, add)(**dict.fromkeys(names, g.artifacts.status))

    assert isinstance(g.artifacts.status, Artifact)
    g.build()
    assert g.read(g.artifacts.total, annotation=int) == len(names)


def test_Graph_literals(tmp_path: Path) -> None:
    n_add_runs = 0

    @producer()
    def add(x: int, y: int) -> int:
        nonlocal n_add_runs
        n_add_runs += 1
        return x + y

    with Graph(name="Test") as g:
        g.artifacts.x = 1
        g.artifacts.y = Num(storage=LocalFile(path=str(tmp_path / "y.json")))
        g.artifacts.z = cast(Any, add)(x=g.artifacts.x, y=g.artifacts.y).out()
        # Changes to `phase` will cause a new GraphSnapshot. However, since `phase` isn't an input
        # to `add`, we *shouldn't* have to recompute `z` - assuming the backend properly stores
        # storage->storage_partitions separate from the set of storage_partitions associated with a
        # GraphSnapshot.
        g.artifacts.phase = Num(storage=LocalFile(path=str(tmp_path / "phase.json")))

    x, y, z, phase = g.artifacts.x, g.artifacts.y, g.artifacts.z, g.artifacts.phase
    assert isinstance(x, Artifact)
    assert isinstance(y, Artifact)
    assert isinstance(z, Artifact)
    assert isinstance(phase, Artifact)
    assert isinstance(x.storage, StringLiteral)
    assert x.storage.value == "1"
    assert isinstance(z.storage, StringLiteral)
    assert z.storage.value is None

    # Ensure we can read raw Artifacts, even if others are not populated yet.
    assert g.read(x, annotation=int) == 1
    with pytest.raises(FileNotFoundError, match="No data"):
        assert g.read(y, annotation=int) == 1
    g.write(1, artifact=y)
    g.write(1, artifact=phase)
    with pytest.raises(FileNotFoundError, match="No data"):
        g.read(z, annotation=int)

    # Run the initial build to compute z
    s = g.build()
    assert g.read(z, annotation=int) == 2
    assert n_add_runs == 1
    with g.backend.connect() as conn:
        assert len(conn.read_snapshot_partitions(s, "z", z)) == 1
        assert len(conn.read_artifact_partitions(z)) == 1
    # A subsequent build shouldn't require a rerun, ensuring we properly lookup existing literals.
    s = g.build()
    assert g.read(z, annotation=int) == 2
    assert n_add_runs == 1
    with g.backend.connect() as conn:
        assert len(conn.read_snapshot_partitions(s, "z", z)) == 1
        assert len(conn.read_artifact_partitions(z)) == 1
    # Changing an input should trigger a rerun. There will still only be 1 z literal for this graph,
    # but now 2 overall for the storage (with different `input_fingerprint`s).
    g.write(2, artifact=y)
    s = g.build()
    assert g.read(z, annotation=int) == 3
    assert n_add_runs == 2
    with g.backend.connect() as conn:
        assert len(conn.read_snapshot_partitions(s, "z", z)) == 1
        assert len(conn.read_artifact_partitions(z)) == 2
    # After getting a new GraphSnapshot, but no changes to `add`s inputs, ensure we properly lookup
    # existing literals - even though the GraphSnapshot will change, the input_fingerprint for `z`
    # will not.
    g.write(2, artifact=phase)
    s = g.build()
    assert g.read(z, annotation=int) == 3
    assert n_add_runs == 2
    with g.backend.connect() as conn:
        assert len(conn.read_snapshot_partitions(s, "z", z)) == 1
        assert len(conn.read_artifact_partitions(z)) == 2


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
            storage_partition_snapshot.fingerprint
            for storage_partition_snapshot in cast(
                Artifact, g.artifacts.a
            ).storage.discover_partitions()
        ),
    ]

    s = g.snapshot()
    assert s.id == Fingerprint.combine(*id_components)
    # Ensure order independence
    assert s.id == Fingerprint.combine(*reversed(id_components))

    assert g.backend is s.backend  # Ensure the backend is not copied
    # Ensure metadata is written
    with g.backend.connect() as conn:
        assert conn.read_graph(g.name, g.fingerprint) == g
        assert conn.read_snapshot(s.name, s.fingerprint) == s
        assert conn.read_snapshot_partitions(s, "a", s.artifacts.a)


def test_Graph_snapshot_missing_input_artifact(tmp_path: Path) -> None:
    with Graph(name="test") as g:
        g.artifacts.a = Num(storage=LocalFile(path=str(tmp_path / "a.json")))

    with pytest.raises(ValueError, match=re.escape("No data found for `a`")):
        assert g.snapshot()


def test_Graph_snapshot_id_producer_arg_order(tmp_path: Path) -> None:
    a = Num(storage=LocalFile(path=str(tmp_path / "a.json")))
    with Path(a.storage.path).open("w") as f:
        f.write("10")
    b = Num(storage=LocalFile(path=str(tmp_path / "b.json")))
    with Path(b.storage.path).open("w") as f:
        f.write("5")
    c = Num(storage=LocalFile.rooted_at(tmp_path))

    # Create two Graphs, varying only by the arg order to the Producer.
    with Graph(name="test") as g_ab:
        g_ab.artifacts.c = cast(Any, div)(a=a, b=b).out(c)
    with Graph(name="test") as g_ba:
        g_ba.artifacts.c = cast(Any, div)(a=b, b=a).out(c)

    assert g_ab.snapshot().id != g_ba.snapshot().id


def test_Graph_tagging(tmp_path: Path) -> None:
    tag = "test"

    with Graph(name="Test") as g:
        g.artifacts.x = Num(storage=LocalFile(path=str(tmp_path / "x.json")))

    g.write(1, artifact=g.artifacts.x)
    s1 = g.build()
    s1.tag(tag)
    assert GraphSnapshot.from_tag(g.name, tag, connectable=g.backend) == s1

    g.write(2, artifact=g.artifacts.x)
    s2 = g.build()
    assert s1.id != s2.id

    with pytest.raises(
        ValueError,
        match=re.escape(f"Existing `{tag}` tag for Graph `{g.name}` points to GraphSnapshot"),
    ):
        s2.tag(tag)

    s2.tag(tag, overwrite=True)
    with g.backend.connect() as conn:
        assert GraphSnapshot.from_tag(g.name, tag, connectable=conn) == s2

    with pytest.raises(ValueError, match=re.escape("No known `fake` tag for GraphSnapshot `Test`")):
        GraphSnapshot.from_tag(g.name, "fake", connectable=g.backend)


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
        g.artifacts.root.a = Num(storage=LocalFile(path=str(tmp_path / "a.json")))
        # NOTE: We're using a Num w/ LocalFile output so that the file can be discovered even if we
        # wipe the default MemoryBackend.
        g.artifacts.b = cast(Any, increment)(i=g.artifacts.root.a).out(
            Num(storage=LocalFile.rooted_at(tmp_path))
        )
        # Test multiple return values
        g.artifacts.c, g.artifacts.d = cast(Any, dup)(i=g.artifacts.root.a).out(
            Num(storage=LocalFile.rooted_at(tmp_path)),
            Num(storage=LocalFile.rooted_at(tmp_path)),
        )

    a, b, c, d = (
        g.artifacts.root.a,
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

    # Test that the MemoryBackend will discover existing StoragePartitions (*except for Literals*),
    # even when empty. Other backends are persistent, so this isn't necessary. This is really a
    # MemoryBackend test, but easiest to test in a Graph context.
    #
    # Running a build should no-op (ie: num_builds shouldn't increment), but we unfortunately can't
    # read *immediately* because we won't know the input_fingerprints for all the generated
    # Artifacts until build. Eventually, we need to allow the Artifact to access the backend
    # directly and automatically compute the input_fingerprints (ie: sync on the fly), which would
    # allow us to read automatically.
    s = g.model_copy(update={"backend": MemoryBackend()}).build()
    assert n_builds == 2
    assert s.read(b, annotation=int) == 1
    assert s.read(c, annotation=int) == s.read(d, annotation=int) == 0


def test_Graph_build_failed_validation(tmp_path: Path) -> None:
    failed_validation_msg = "This is junk data!"

    def reject_output(output: int) -> tuple[bool, str]:
        return False, failed_validation_msg

    @producer(validate_outputs=reject_output)
    def angry_add(i: Annotated[int, Num]) -> Annotated[int, Num]:
        return i + 1

    num = Num(storage=LocalFile.rooted_at(tmp_path))  # Immutable, thus can reuse
    with Graph(name="test") as g:
        g.artifacts.a = num
        g.artifacts.b = cast(Num, cast(Any, angry_add)(i=g.artifacts.a).out(num))

    g.write(0, artifact=g.artifacts.a)
    with pytest.raises(ValueError, match=failed_validation_msg):
        g.build()
    with pytest.raises(FileNotFoundError, match="No data"):
        g.read(g.artifacts.b, annotation=int)


def test_Graph_build_empty_map_links_no_partitions(tmp_path: Path) -> None:
    class Rows(Artifact):
        type: Type = Collection(element=Struct(fields={"k": Int64()}), partition_by=("k",))
        format: Format = JSON()
        storage: LocalFile = LocalFile.rooted_at(tmp_path / "produced")

    def positive_keys(rows: StoragePartitionSnapshots) -> PartitionDependencies:
        return PartitionDependencies(
            {
                partition.partition_key: InputPartitions(rows=(partition,))
                for partition in rows
                if cast(Int64Field, partition.partition_key["k"]).value > 0
            }
        )

    @producer(map=positive_keys)
    def copy_rows(
        rows: Annotated[list[dict[Any, Any]], Rows],
    ) -> Annotated[list[dict[Any, Any]], Rows]:
        return rows

    with Graph(name="test") as g:
        g.artifacts.raw = Rows(storage=LocalFile.rooted_at(tmp_path / "raw"))
        g.artifacts.copied = cast(Any, copy_rows)(rows=g.artifacts.raw).out()

    raw, copied = g.artifacts.raw, cast(Rows, g.artifacts.copied)
    g.write([{"k": 1}], artifact=raw, partition_key=PartitionKey(k=Int64Field(value=1)))
    assert g.build().read(copied, annotation=list) == [{"k": 1}]
    # Replace the raw data with a partition that `positive_keys` skips. The output built from the
    # old input must not be linked to the new snapshot.
    shutil.rmtree(tmp_path / "raw")
    g.write([{"k": -1}], artifact=raw, partition_key=PartitionKey(k=Int64Field(value=-1)))
    snapshot = g.build()
    with g.backend.connect() as conn:
        assert conn.read_snapshot_partitions(snapshot, "copied", copied) == ()


def test_Graph_dependencies(graph: Graph) -> None:
    p1 = graph.artifacts.b.producer_output.producer
    p2 = graph.artifacts.c.a.producer_output.producer
    assert graph.dependencies == {
        graph.artifacts.a: frozenset(),
        p1: frozenset({graph.artifacts.a}),
        graph.artifacts.b: frozenset({p1}),
        p2: frozenset({graph.artifacts.b}),
        graph.artifacts.c.a: frozenset({p2}),
        graph.artifacts.c.b: frozenset({p2}),
    }


def test_Graph_errors() -> None:
    with Graph(name="test") as graph:
        graph.artifacts.a = A1()
        graph.artifacts.b = P1(a1=graph.artifacts.a).out()

    with pytest.raises(ValueError, match="ArtifactBox is frozen"):
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
    assert graph.producer_outputs == {
        p1: (graph.artifacts.b,),
        p2: (graph.artifacts.c.a, graph.artifacts.c.b),
    }

    with Graph(name="test") as g:
        with pytest.raises(
            ValueError,
            match="producer_outputs cannot be used while the Graph is still being defined",
        ):
            g.producer_outputs


def test_Graph_read_write(tmp_path: Path) -> None:
    @producer()
    def plus1(i: int) -> int:
        return i + 1

    with Graph(name="test") as g:
        g.artifacts.i = Num(storage=LocalFile(path=str(tmp_path / "i.json")))
        g.artifacts.j = cast(Any, plus1)(i=g.artifacts.i).out()

    i, j = g.artifacts.i, g.artifacts.j
    assert isinstance(j, Artifact)
    # Test write
    snapshot = g.write(5, artifact=i)
    assert snapshot.content_fingerprint is not None
    partition = snapshot.storage_partition
    assert isinstance(partition, LocalFilePartition)
    assert partition.input_fingerprint is None
    assert partition.partition_key == PartitionKey()
    assert partition.path.endswith(i.format.extension)

    # Once snapshotted, writing to the raw Artifacts would result in a different snapshot.
    with pytest.raises(
        ValueError,
        match=re.escape("Writing to a raw Artifact (`i`) with a GraphSnapshot is not supported."),
    ):
        g.snapshot().write(10, artifact=i)
    g.snapshot().write(5, artifact=j, input_fingerprint=Fingerprint.from_int(1))

    # Test read
    assert assert_type(g.read(i, annotation=int), int) == 5
    assert assert_type(g.read(i, annotation=Annotated[int, "hint"]), Any) == 5
    assert assert_type(g.read(i, view=View.from_annotation(int, mode="READ")), Any) == 5
    assert g.read(i, annotation=int, storage_partition_snapshots=(snapshot,)) == 5
    with pytest.raises(ValueError, match="Either `annotation` or `view` must be passed"):
        g.read(i)
    with pytest.raises(ValueError, match="Only one of `annotation` or `view` may be passed"):
        g.read(i, annotation=int, view=View.from_annotation(int, mode="READ"))
    # An explicit `view` must still match the Artifact.
    with pytest.raises(ValueError, match="is not compatible with the Artifact's Type"):
        g.read(i, view=View.from_annotation(str, mode="READ"))
    assert g.read(j, annotation=int) == 5

    # Test that we can use an existing connection
    with (
        g.backend.connect() as conn,
        patch.object(type(g.backend), "connect", new_callable=PropertyMock) as connect_mock,
    ):
        g.write(10, artifact=i, connection=conn)
        assert g.read(i, annotation=int, connection=conn) == 10

        s = g.snapshot(connection=conn)
        s.write(10, artifact=j, connection=conn, input_fingerprint=Fingerprint.from_int(1))
        assert assert_type(s.read(j, annotation=int, connection=conn), int) == 10
        assert g.read(j, annotation=int, connection=conn) == 10

        assert not connect_mock.called
        g.backend.connect()  # Confirm we are mocking correctly
        assert connect_mock.called


def test_Graph_references(graph: Graph) -> None:
    with Graph(name="test-2") as g2:
        g2.artifacts.upstream.a = graph.artifacts.a
    assert graph.artifacts.a == g2.artifacts.upstream.a


def test_Graph_storage_resolution() -> None:
    with Graph(name="test", path_tags={"tag": "value"}) as g:
        g.artifacts.root.a = Num(storage=LocalFile())
        g.artifacts.root.b = Num(storage=LocalFile())
        g.artifacts.c = cast(
            Num,
            cast(Any, div)(a=g.artifacts.root.a, b=g.artifacts.root.b).out(
                Num(storage=LocalFile())
            ),
        )
        with pytest.raises(
            ValueError,
            match=re.escape(
                "Produced Artifacts must have a '{input_fingerprint}' template in their Storage"
            ),
        ):
            g.artifacts.d = cast(Any, div)(a=g.artifacts.root.a, b=g.artifacts.root.b).out(
                Num(storage=LocalFile(path="junk"))
            )

    assert g.artifacts.root.a.storage.path.endswith("/test/tag=value/root/a/a.json")
    assert g.artifacts.root.b.storage.path.endswith("/test/tag=value/root/b/b.json")
    assert g.artifacts.c.storage.path.endswith("/test/tag=value/c/{input_fingerprint}/c.json")


def test_Graph_storage_resolution_after_fingerprint() -> None:
    # Fingerprinting a Storage template must not leak into the Storages resolved from it.
    template = LocalFile()
    assert template.fingerprint
    with Graph(name="test") as g:
        g.artifacts.a = Num(storage=template)
        g.artifacts.b = Num(storage=template)

    a, b = g.artifacts.a.storage, g.artifacts.b.storage
    assert a.path != b.path
    assert a.fingerprint != b.fingerprint
