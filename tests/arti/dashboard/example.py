"""A small, documented Graph with partitioned data, for dashboard tests."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated, Any, TypedDict, cast

from arti import Annotation, Artifact, Backend, Format, Graph, Storage, Type, producer
from arti.formats.json import JSON
from arti.storage.literal import StringLiteral
from arti.storage.local import LocalFile
from arti.types import Collection, Date, Float64, Int64, Struct
from arti.versions import SemVer


class Vendor(Annotation):
    name: str


class Transactions(Artifact):
    """Transactions, partitioned by day."""

    type: Type = Collection(
        element=Struct(fields={"date": Date(), "amount": Float64()}), partition_by=("date",)
    )
    format: Format = JSON()


class TotalSpend(Artifact):
    """Aggregate spend over all time."""

    type: Type = Float64()
    format: Format = JSON()


class Limit(Artifact):
    type: Type = Int64()
    format: Format = JSON()
    storage: Storage = StringLiteral(value="100")


class Transaction(TypedDict):
    amount: float


@producer(version=SemVer(major=1, minor=0, patch=0))
def total_spend(
    transactions: Annotated[list[Transaction], Transactions],
) -> Annotated[float, TotalSpend]:
    """Sum every transaction."""
    return float(sum(txn["amount"] for txn in transactions))


@producer()
def over_limit(
    spend: Annotated[float, TotalSpend], limit: Annotated[int, Limit]
) -> Annotated[int, Limit]:
    return int(spend > limit)


def write_transactions(root: Path) -> None:
    (root / "transactions").mkdir(parents=True, exist_ok=True)
    for day, amount in [("2026-01-01", 1.5), ("2026-01-02", 3.0)]:
        (root / "transactions" / f"{day}.json").write_text(json.dumps([{"amount": amount}]))


def build_graph(root: Path, backend: Backend[Any], *, name: str = "spend") -> Graph:
    with Graph(name=name, backend=backend) as graph:
        graph.artifacts.vendor.transactions = Transactions(
            annotations=(Vendor(name="Acme"),),
            storage=LocalFile(path=str(root / "transactions" / "{date.iso}.json")),
        )
        graph.artifacts.spend = total_spend(transactions=graph.artifacts.vendor.transactions)
    return graph


def build_graph_with_external_input(root: Path, backend: Backend[Any]) -> Graph:
    """Build a Graph where a Producer reads an Artifact that was never assigned to the Graph."""
    with Graph(name="external", backend=backend) as graph:
        graph.artifacts.vendor.transactions = Transactions(
            storage=LocalFile(path=str(root / "transactions" / "{date.iso}.json"))
        )
        graph.artifacts.spend = total_spend(transactions=graph.artifacts.vendor.transactions)
        graph.artifacts.over_limit = over_limit(
            spend=cast(Artifact, graph.artifacts["spend"]), limit=Limit()
        )
    return graph
