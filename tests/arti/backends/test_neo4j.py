import os
from collections.abc import Iterator
from pathlib import Path

import pytest

from arti import Artifact, Graph, producer
from arti.backends.neo4j import Neo4jBackend
from arti.partitions import Int64Key
from arti.storage.local import LocalFile
from arti.types import Collection, Int64, Struct


@producer()
def div(a: int, b: int) -> float:
    return a / b


@pytest.fixture(scope="session")
def base_neo4j_backend() -> Neo4jBackend:
    return Neo4jBackend(
        host=os.getenv("ARTIGRAPH_TEST_NEO4J_HOST", "localhost"),
        port=int(os.getenv("ARTIGRAPH_TEST_NEO4J_PORT", "7687")),
        username=os.getenv("ARTIGRAPH_TEST_NEO4J_USERNAME", "neo4j"),
        password=os.getenv("ARTIGRAPH_TEST_NEO4J_PASSWORD", "neo4j"),
        database=os.getenv("ARTIGRAPH_TEST_NEO4J_BASE_DATABASE", "neo4j"),
    )


@pytest.fixture()
def neo4j_backend(base_neo4j_backend: Neo4jBackend, clean_test_name: str) -> Iterator[Neo4jBackend]:
    db_name = f"artigraph-test-{clean_test_name.replace('_', '-')}"
    with base_neo4j_backend.connect() as connection:
        connection.run_cypher("CREATE DATABASE $db_name", db_name=db_name)
        try:
            yield base_neo4j_backend.copy(update={"database": db_name})
        finally:
            pass
            connection.run_cypher("DROP DATABASE $db_name", db_name=db_name)


@pytest.fixture()
def graph(neo4j_backend: Neo4jBackend, tmp_path: Path) -> Graph:
    with Graph(name="test", backend=neo4j_backend) as g:
        g.artifacts.a = 5
        g.artifacts.b = 10
        g.artifacts.c = Artifact(
            type=Collection(
                element=Struct(fields={"id": Int64(), "value": Int64()}), partition_by=("id",)
            ),
            storage=LocalFile(path=str(tmp_path / "{id.key}.{extension}")),
        )
        g.artifacts.namespace.data = div(a=g.artifacts.a, b=g.artifacts.b)  # type: ignore[call-arg]

    g.write([{"value": 1}], artifact=g.artifacts.c, keys={"id": Int64Key(key=1)})
    g.write([{"value": 2}], artifact=g.artifacts.c, keys={"id": Int64Key(key=2)})

    return g


def test_neo4j_connect(graph: Graph) -> None:
    assert isinstance(graph.backend, Neo4jBackend)
    with graph.backend.connect() as connection:
        connection.run_cypher("MATCH (n) RETURN n")


def test_neo4j_read_write_model(graph: Graph) -> None:
    assert isinstance(graph.backend, Neo4jBackend)
    with graph.backend.connect() as connection:
        with pytest.raises(ValueError, match="No Artifact node with fingerprint"):
            connection.read_model(Artifact, fingerprint=graph.artifacts.a.fingerprint)

        connection.write_model(graph.artifacts.a)
        connection.write_model(graph.artifacts.b)
        connection.write_model(graph.artifacts.c)
        connection.write_model(graph.artifacts.namespace.data)

        artifact, _ = connection.read_model(Artifact, fingerprint=graph.artifacts.a.fingerprint)
        assert artifact == graph.artifacts.a

        artifacts, _ = connection.read_models(
            Artifact,
            fingerprints=[
                graph.artifacts.a.fingerprint,
                graph.artifacts.b.fingerprint,
                graph.artifacts.c.fingerprint,
                graph.artifacts.namespace.data.fingerprint,
            ],
        )
        assert set(artifacts) == {
            graph.artifacts.a,
            graph.artifacts.b,
            graph.artifacts.c,
            graph.artifacts.namespace.data,
        }


def test_neo4j_write_graph(graph: Graph) -> None:
    with graph.backend.connect() as connection:
        connection.write_graph(graph)


def test_neo4j_graph_build(graph: Graph) -> None:
    graph.build()
    a = graph.read(graph.artifacts.a, annotation=int)
    b = graph.read(graph.artifacts.b, annotation=int)
    expected = a / b
    assert graph.read(graph.artifacts.namespace.data, annotation=float) == expected


def test_neo4j_graph_snapshot(graph: Graph) -> None:
    graph.snapshot()
