from __future__ import annotations

from typing import Annotated, Any, cast

from arti import Artifact, Backend, Graph, GraphSnapshot, Storage, Type, producer
from arti.formats.json import JSON
from arti.storage.literal import StringLiteral
from arti.types import Int64


class Number(Artifact):
    type: Type = Int64()
    format: JSON = JSON()
    storage: Storage = StringLiteral()


@producer()
def increment(value: Annotated[int, Number]) -> Annotated[int, Number]:
    return value + 1


@producer()
def double(value: Annotated[int, Number]) -> Annotated[int, Number]:
    return value * 2


@producer()
def neighbors(
    value: Annotated[int, Number],
) -> tuple[Annotated[int, Number], Annotated[int, Number]]:
    return value - 1, value + 1


def build_pipeline(backend: Backend[Any], name: str) -> GraphSnapshot:
    """Build a small branched, multi-output pipeline suitable for backend conformance tests."""
    with Graph(name=name, backend=backend) as graph:
        graph.artifacts.value = Number(storage=StringLiteral(value="1"))
        incremented = increment(value=graph.artifacts.value).out()
        assert isinstance(incremented, Artifact)
        graph.artifacts.incremented = incremented
        assigned_incremented = cast(Artifact, graph.artifacts["incremented"])
        doubled = double(value=assigned_incremented).out()
        assert isinstance(doubled, Artifact)
        graph.artifacts.doubled = doubled
        assigned_doubled = cast(Artifact, graph.artifacts["doubled"])
        previous, next_ = cast(
            tuple[Artifact, Artifact],
            neighbors(value=assigned_doubled).out(),
        )
        (
            graph.artifacts.previous,
            graph.artifacts.next,
        ) = previous, next_
    snapshot = graph.build()
    snapshot.tag("latest")
    return snapshot
