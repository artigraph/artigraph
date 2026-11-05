"""
An importable Graph for executor tests.

Executors that build in other processes import the Graph by reference (`GRAPH_REF`). Those
processes build `graph` from the environment variables below, which the tests set before starting
them. Tests in the current process can instead assign `graph` directly.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Annotated, Any, cast

from arti import (
    Artifact,
    Backend,
    Graph,
    PartitionKey,
    Storage,
    Type,
    one_to_one,
    producer,
)
from arti.backends.postgres import PostgresBackend
from arti.backends.sqlite import SQLiteBackend
from arti.formats.json import JSON
from arti.partitions import Int32Field
from arti.storage.google.cloud.storage import GCSFile
from arti.storage.literal import StringLiteral
from arti.storage.local import LocalFile
from arti.types import Collection, Int32, Int64, Struct

GRAPH_REF = "tests.arti.executors.graphs:graph"
BACKEND_ENV = "ARTI_TEST_EXECUTOR_BACKEND"  # A SQLite file path or a Postgres URL
RAW_ROOT_ENV = "ARTI_TEST_EXECUTOR_RAW_ROOT"  # A local directory or `gs://{bucket}`
N_PARTITIONS = 4

_row = Struct(fields={"i": Int32(), "value": Int64()})


class Rows(Artifact):
    type: Type = Collection(element=_row, partition_by=("i",))
    format: JSON = JSON()
    storage: Storage = StringLiteral()


class Number(Artifact):
    type: Type = Int64()
    format: JSON = JSON()
    storage: Storage = StringLiteral()


@producer(map=one_to_one("rows"))
def square(rows: Annotated[list[dict[Any, Any]], Rows]) -> Annotated[list[dict[Any, Any]], Rows]:
    return [{"i": row["i"], "value": row["value"] ** 2} for row in rows]


@producer()
def total(rows: Annotated[list[dict[Any, Any]], Rows]) -> Annotated[int, Number]:
    return sum(row["value"] for row in rows)


@producer()
def double(value: Annotated[int, Number]) -> Annotated[int, Number]:
    return value * 2


@producer()
def neighbors(
    value: Annotated[int, Number],
) -> tuple[Annotated[int, Number], Annotated[int, Number]]:
    return value - 1, value + 1


def raw_storage(root: str) -> Storage:
    if root.startswith("gs://"):
        return GCSFile(bucket=root.removeprefix("gs://"), path="raw/{i.value}.json")
    return LocalFile(path=str(Path(root) / "raw" / "{i.value}.json"))


def make_graph(backend: Backend[Any], raw_root: str) -> Graph:
    """Build a partitioned, branching Graph with a multi-output Producer."""
    with Graph(name="executor-conformance", backend=backend) as graph:
        graph.artifacts.raw = Rows(storage=raw_storage(raw_root))
        graph.artifacts.squared = square(rows=graph.artifacts.raw).out()
        graph.artifacts.total = total(rows=cast(Artifact, graph.artifacts.squared)).out()
        # `doubled` and `neighbors` only depend on `total`, so they may be built concurrently.
        graph.artifacts.doubled = double(value=cast(Artifact, graph.artifacts.total)).out()
        graph.artifacts.previous, graph.artifacts.next = neighbors(
            value=cast(Artifact, graph.artifacts.total)
        ).out()
    return graph


def write_raw(graph: Graph) -> None:
    for i in range(N_PARTITIONS):
        graph.write(
            [{"i": i, "value": i + 1}],
            artifact=graph.artifacts.raw,
            partition_key=PartitionKey(i=Int32Field(value=i)),
        )


def backend_from(value: str) -> Backend[Any]:
    if value.endswith(".sqlite"):
        return SQLiteBackend(path=Path(value))
    return PostgresBackend.from_url(value)


graph: Graph | None = None
if BACKEND_ENV in os.environ:
    graph = make_graph(backend_from(os.environ[BACKEND_ENV]), os.environ[RAW_ROOT_ENV])
