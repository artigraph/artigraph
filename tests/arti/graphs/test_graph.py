import json
import pickle
import re
from pathlib import Path
from typing import Annotated, Any, cast

import pytest
from box import BoxError

from arti import Artifact, CompositeKey, Fingerprint, Graph, View, producer
from arti.backends.memory import MemoryBackend
from arti.executors.local import LocalExecutor
from arti.internal.utils import frozendict
from arti.storage.local import LocalFile
from tests.arti.dummies import A1, A2, A3, A4, P1, P2
from tests.arti.dummies import Num as _Num
from tests.arti.dummies import div


def write_local_json_file(artifact: Artifact, val: Any) -> None:
    assert hasattr(artifact.storage, "path")
    path = Path(artifact.storage.path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        json.dump(val, f)


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


def test_Graph_pickle(graph: Graph) -> None:
    assert graph == pickle.loads(pickle.dumps(graph))


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
        g.artifacts.z = add(x=g.artifacts.x, y=g.artifacts.y)  # type: ignore[call-arg]
        # Changes to `phase` will cause a new snapshot_id. However, since `phase` isn't an input to
        # `add`, we *shouldn't* have to recompute `z` - assuming the backend properly stores
        # storage->storage_partitions separate from the set of storage_partitions associated with a
        # snapshot_id.
        g.artifacts.phase = Num(storage=LocalFile(path=str(tmp_path / "phase.json")))

    x, y, z, phase = g.artifacts.x, g.artifacts.y, g.artifacts.z, g.artifacts.phase
    assert isinstance(x, Artifact)
    assert isinstance(y, Artifact)
    assert isinstance(z, Artifact)
    assert isinstance(phase, Artifact)
    assert x.storage.value == "1"
    assert z.storage.value is None

    # Ensure we can read raw Artifacts, even if others are not populated yet.
    assert g.read(x, annotation=int) == 1
    with pytest.raises(FileNotFoundError, match="No data"):
        assert g.read(y, annotation=int) == 1
    write_local_json_file(y, 1)
    write_local_json_file(phase, 1)
    s = g.snapshot()
    with pytest.raises(FileNotFoundError, match="No data"):
        s.read(z, annotation=int)

    # Run the initial build to compute z
    s = g.build()
    assert s.read(z, annotation=int) == 2
    assert n_add_runs == 1
    assert len(s.backend.read_graph_partitions(s.name, s.id, "z", z)) == 1
    assert len(s.backend.read_artifact_partitions(z)) == 1
    # A subsequent build shouldn't require a rerun, ensuring we properly lookup existing literals.
    s = g.build()
    assert s.read(z, annotation=int) == 2
    assert n_add_runs == 1
    assert len(s.backend.read_graph_partitions(s.name, s.id, "z", z)) == 1
    assert len(s.backend.read_artifact_partitions(z)) == 1
    # Changing an input should trigger a rerun. There will still only be 1 z literal for this graph,
    # but now 2 overall for the storage (with different `input_fingerprint`s).
    write_local_json_file(y, 2)
    s = g.build()  # TODO: Check that s.build() errors b/c the raw data changed
    assert s.read(z, annotation=int) == 3
    assert n_add_runs == 2
    assert len(s.backend.read_graph_partitions(s.name, s.id, "z", z)) == 1
    assert len(s.backend.read_artifact_partitions(z)) == 2
    # After getting a new snapshot id, but no changes to `add`s inputs, ensure we properly lookup
    # existing literals - even though the snapshot id will change, the input_fingerprint for `z`
    # will not.
    write_local_json_file(phase, 2)
    s = g.build()
    assert s.read(z, annotation=int) == 3
    assert n_add_runs == 2
    assert len(s.backend.read_graph_partitions(s.name, s.id, "z", z)) == 1
    assert len(s.backend.read_artifact_partitions(z)) == 2


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
            for storage_partition in g.artifacts.a.storage.discover_partitions()
        ),
    ]

    s = g.snapshot()
    assert s.id == Fingerprint.combine(*id_components)
    # Ensure order independence
    assert s.id == Fingerprint.combine(*reversed(id_components))


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
        g_ab.artifacts.c = div(a=a, b=b).out(c)  # type: ignore[call-arg]
    with Graph(name="test") as g_ba:
        g_ba.artifacts.c = div(a=b, b=a).out(c)  # type: ignore[call-arg]

    assert g_ab.snapshot().id != g_ba.snapshot().id


def test_Graph_tagging(tmp_path: Path) -> None:
    @producer()
    def plus1(x: int) -> int:
        return x + 1

    name, tag = "Test", "test"

    with Graph(name=name) as g1:
        g1.artifacts.x = 1
        g1.artifacts.y = plus1(x=g1.artifacts.x)  # type: ignore[call-arg]

    with Graph(name=name) as g2:
        g2.artifacts.x = 2
        g2.artifacts.y = plus1(x=g2.artifacts.x)  # type: ignore[call-arg]

    s1 = g1.build()
    s1.tag(tag)
    assert s1.backend.read_graph_tag(name, tag) == s1.id

    s2 = g2.build()
    s2.tag(tag, overwrite=True)
    assert s1.id != s2.id
    assert s2.backend.read_graph_tag(name, tag) == s2.id

    with pytest.raises(
        ValueError,
        match=re.escape(f"Existing `{tag}` tag for Graph `{name}` points to Fingerprint"),
    ):
        s2.tag(tag)
    with pytest.raises(ValueError, match=re.escape("No known `fake` tag for Graph `Test`")):
        s2.backend.read_graph_tag(name, "fake")


def test_Graph_build(tmp_path: Path) -> None:
    n_builds = 0

    @producer()
    def increment(i: int) -> Annotated[int, Num]:
        nonlocal n_builds
        n_builds += 1
        return i + 1

    @producer()
    def dup(i: int) -> tuple[int, int]:
        return i, i

    with Graph(name="test") as g:
        g.artifacts.root.a = Num(storage=LocalFile(path=str(tmp_path / "a.json")))
        # NOTE: We're using a Num w/ LocalFile output so that the file can be discovered even if we
        # wipe the default MemoryBackend.
        g.artifacts.b = increment(i=g.artifacts.root.a).out(  # type: ignore[call-arg]
            Num(storage=LocalFile.rooted_at(tmp_path))
        )
        # Test multiple return values
        g.artifacts.c, g.artifacts.d = dup(i=g.artifacts.root.a)  # type: ignore[call-arg]

    write_local_json_file(g.artifacts.root.a, 0)

    s = g.build()
    b, c, d = (
        s.artifacts.b,
        s.artifacts.c,
        s.artifacts.d,
    )

    # Bootstrap the initial artifact and build
    assert n_builds == 1
    assert s.read(b, annotation=int) == 1
    assert s.read(c, annotation=int) == s.read(d, annotation=int) == 0
    # A second build should no-op
    g.build(executor=LocalExecutor())
    assert n_builds == 1
    assert s.read(b, annotation=int) == 1
    assert s.read(c, annotation=int) == s.read(d, annotation=int) == 0
    # Changing the raw Artifact data should trigger a rerun
    write_local_json_file(g.artifacts.root.a, 1)
    s = g.build()
    assert n_builds == 2
    assert s.read(b, annotation=int) == 2
    assert s.read(c, annotation=int) == s.read(d, annotation=int) == 1
    # Changing back to the original data should no-op
    write_local_json_file(g.artifacts.root.a, 0)
    s = g.build()
    assert n_builds == 2
    assert s.read(b, annotation=int) == 1
    assert s.read(c, annotation=int) == s.read(d, annotation=int) == 0

    # Test that the MemoryBackend will discover existing StoragePartitions (*except for Literals*),
    # even when empty. Other backends are persistent, so this isn't necessary. This is really a
    # MemoryBackend test, but easiest to test in a Graph context.
    #
    # Running a build should no-op (ie: num_builds shouldn't increment), but we unfortunately can't
    # read *immediately* because we won't know the input_fingerprints for all the generated
    # Artifacts until build. Eventually, we need to allow the Artifact to access the backend
    # directly and automatically compute the input_fingerprints (ie: sync on the fly), which would
    # allow us to read automatically.
    s = g.copy(update={"backend": MemoryBackend()}).build()
    assert n_builds == 2
    assert s.read(b, annotation=int) == 1
    assert s.read(c, annotation=int) == s.read(d, annotation=int) == 0


def test_Graph_build_failed_validation(tmp_path: Path) -> None:
    failed_validation_msg = "This is junk data!"

    @producer(validate_outputs=lambda i: (False, failed_validation_msg))
    def angry_add(i: int) -> int:
        return i + 1

    with Graph(name="test") as g:
        g.artifacts.a = Num(storage=LocalFile.rooted_at(tmp_path))
        g.artifacts.b = angry_add(i=g.artifacts.a)  # type: ignore[call-arg]

    write_local_json_file(g.artifacts.a, 0)
    with pytest.raises(ValueError, match=failed_validation_msg):
        g.build()
    s = g.snapshot()
    with pytest.raises(FileNotFoundError, match="No data"):
        s.read(s.artifacts.b, annotation=int)


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
    @producer()
    def plus1(x: int) -> int:
        return x + 1

    with Graph(name="test") as g:
        g.artifacts.x = Num(storage=LocalFile(path=str(tmp_path / "i.json")))
        g.artifacts.y = plus1(x=g.artifacts.x)  # type: ignore[call-arg]

    x, y = g.artifacts.x, g.artifacts.y
    write_local_json_file(x, 5)
    s = g.build()

    # Test read
    assert s.read(x, annotation=int) == 5
    assert s.read(y, annotation=int) == 6  # type: ignore[arg-type]
    assert s.read(x, view=View.from_annotation(int, mode="READ")) == 5
    # assert s.read(x, annotation=int, storage_partitions=[storage_partition]) == 5
    with pytest.raises(ValueError, match="Either `annotation` or `view` must be passed"):
        s.read(x)
    with pytest.raises(ValueError, match="Only one of `annotation` or `view` may be passed"):
        s.read(x, annotation=int, view=View.from_annotation(int, mode="READ"))

    # Test write
    y_input_fingerprint = y.producer_output.producer.compute_input_fingerprint(  # type: ignore[attr-defined]
        {"x": x.storage.discover_partitions()}
    )
    y_storage_partition = s.write(5, artifact=y, input_fingerprint=y_input_fingerprint)  # type: ignore[arg-type]
    assert y_storage_partition.content_fingerprint != Fingerprint.empty()
    assert y_storage_partition.input_fingerprint == y_input_fingerprint
    assert y_storage_partition.keys == CompositeKey()
    # Once snapshotted, writing to the raw Artifacts would result in a different snapshot.
    with pytest.raises(
        ValueError,
        match=re.escape("Writing to a raw Artifact (`x`) with a GraphSnapshot is not supported"),
    ):
        s.write(10, artifact=x)


def test_Graph_references(graph: Graph) -> None:
    with Graph(name="test-2") as g2:
        g2.artifacts.upstream.a = graph.artifacts.a
    assert graph.artifacts.a == g2.artifacts.upstream.a


def test_Graph_storage_resolution() -> None:
    with Graph(name="test", path_tags={"tag": "value"}) as g:
        g.artifacts.root.a = Num(storage=LocalFile())
        g.artifacts.root.b = Num(storage=LocalFile())
        g.artifacts.c = cast(
            Num, div(a=g.artifacts.root.a, b=g.artifacts.root.b).out(Num(storage=LocalFile()))  # type: ignore[call-arg]
        )
        with pytest.raises(
            ValueError,
            match=re.escape(
                "Produced Artifacts must have a '{input_fingerprint}' template in their Storage"
            ),
        ):
            g.artifacts.d = div(a=g.artifacts.root.a, b=g.artifacts.root.b).out(  # type: ignore[call-arg]
                Num(storage=LocalFile(path="junk"))
            )

    assert g.artifacts.root.a.storage.path.endswith("/test/tag=value/root/a/a.json")
    assert g.artifacts.root.b.storage.path.endswith("/test/tag=value/root/b/b.json")
    assert g.artifacts.c.storage.path.endswith("/test/tag=value/c/{input_fingerprint}/c.json")


def test_ArtifactBox() -> None:
    with Graph(name="test") as g:
        g.artifacts.a.b.c = 5  # test chained assignment
        g.artifacts.x = {"y": {"z": 5}}  # test direct nested assignment
    assert g.artifacts.a.b.c.storage.id == "test/a/b/c/c.json"  # type: ignore[attr-defined]
    assert g.artifacts.x.y.z.storage.id == "test/x/y/z/z.json"  # type: ignore[attr-defined]
    assert g.artifacts == pickle.loads(pickle.dumps(g.artifacts))
