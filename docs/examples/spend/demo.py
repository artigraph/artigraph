import logging
from pathlib import Path
from typing import Annotated

from arti import Annotation, Artifact, Graph, producer
from arti.formats.json import JSON
from arti.storage.local import LocalFile
from arti.types import Collection, Date, Float64, Int64, Struct
from arti.versions import SemVer

DIR = Path(__file__).parent


class Vendor(Annotation):
    name: str


class Transactions(Artifact):
    """Transactions partitioned by day."""

    type = Collection(
        element=Struct(fields={"id": Int64(), "date": Date(), "amount": Float64()}),
        partition_by=("date",),
    )


class TotalSpend(Artifact):
    """Aggregate spend over all time."""

    type = Float64()
    format = JSON()
    storage = LocalFile()


@producer(version=SemVer(major=1, minor=0, patch=0))
def aggregate_transactions(
    transactions: Annotated[list[dict], Transactions]  # type: ignore[type-arg]
) -> Annotated[float, TotalSpend]:
    return sum(txn["amount"] for txn in transactions)  # type: ignore[no-any-return]


with Graph(name="test-graph") as g:
    g.artifacts.vendor.transactions = Transactions(
        annotations=[Vendor(name="Acme")],
        format=JSON(),
        storage=LocalFile(path=str(DIR / "transactions" / "{date.iso}.json")),
    )
    g.artifacts.spend = aggregate_transactions(transactions=g.artifacts.vendor.transactions)  # type: ignore[call-arg]


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    snapshot = g.build()

    logging.info(
        f"Transactions: {snapshot.read(snapshot.artifacts.vendor.transactions, annotation=list)}"
    )
    logging.info(f"Total Spend: {snapshot.read(snapshot.artifacts.spend, annotation=float)}")
