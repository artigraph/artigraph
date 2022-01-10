import logging
from datetime import date
from pathlib import Path
from typing import Annotated

from arti import Annotation, Artifact, Graph, producer
from arti.formats.json import JSON
from arti.partitions import DateKey
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
    transactions: Annotated[list[dict], Transactions]
) -> Annotated[float, TotalSpend]:
    return sum(txn["amount"] for txn in transactions)


with Graph(name="test") as g:
    g.artifacts.vendor.transactions = Transactions(
        annotations=[Vendor(name="Acme")],
        format=JSON(),
        storage=LocalFile(path=str(DIR / "transactions" / "{date.iso}.json")),
    )
    g.artifacts.spend = aggregate_transactions(transactions=g.artifacts.vendor.transactions)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    def print_partition_info(artifact: Artifact, annotation) -> None:
        with g.backend.connect() as backend:
            for partition in backend.read_artifact_partitions(artifact):
                contents = g.read(artifact, annotation=annotation, storage_partitions=(partition,))
                logging.info(f"\t{partition.path}: {contents}")

    logging.info("Writing mock Transactions data:")
    g.write(
        [{"id": 1, "amount": 9.95}, {"id": 2, "amount": 7.5}],
        artifact=g.artifacts.vendor.transactions,
        keys={"date": DateKey(key=date(2021, 10, 1))},
    )
    g.write(
        [{"id": 3, "amount": 5.0}, {"id": 4, "amount": 12.0}, {"id": 4, "amount": 7.55}],
        artifact=g.artifacts.vendor.transactions,
        keys={"date": DateKey(key=date(2021, 10, 2))},
    )
    print_partition_info(g.artifacts.vendor.transactions, annotation=list)

    g.build()

    logging.info("Final Spend data:")
    print_partition_info(g.artifacts.spend, annotation=float)
