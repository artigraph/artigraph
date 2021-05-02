from arti.artifacts.core import Artifact
from arti.backends.core import MemoryBackend
from arti.formats.core import Format
from arti.graphs.core import Graph
from arti.storage.core import Storage
from arti.types.core import Int32, Struct


def _sample_artifact() -> Artifact:
    format = Format("csv")
    storage = Storage("gcs")
    schema = Struct({"x": Int32()})
    return Artifact(key="a", schema=schema, format=format, storage=storage)


def test_memory_artifact_store() -> None:
    backend = MemoryBackend()

    a = _sample_artifact()
    backend.write_artifact(a)
    assert len(backend.artifact_store) == 1

    loaded_a1 = backend.load_artifact(a.id)
    assert loaded_a1.id == a.id


def test_memory_graph_store() -> None:
    backend = MemoryBackend()
    a = _sample_artifact()

    with Graph("test", backend=backend) as g1:
        g1.artifacts.a = a
        g1.write()
    assert len(backend.graph_store) == 1
    assert len(backend.artifact_store) == 1

    with Graph("load", backend=backend) as g2:
        g2.load(g1.id)
        assert g2.artifacts.a.id == a.id
