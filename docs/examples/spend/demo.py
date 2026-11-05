import logging
from pathlib import Path
from typing import Annotated, TypedDict, cast

from arti import (
    Annotation,
    Artifact,
    Format,
    Graph,
    Statistic,
    Storage,
    Threshold,
    Type,
    producer,
)
from arti.formats.json import JSON
from arti.partitions import DateField
from arti.statistics import Max, Min, NullCount, Ratio, RowCount
from arti.storage.local import LocalFile
from arti.thresholds import Range
from arti.types import Collection, Date, Float64, Int64, Struct
from arti.versions import SemVer

DIR = Path(__file__).parent


class Vendor(Annotation):
    name: str


class Transactions(Artifact):
    """Transactions partitioned by day."""

    type: Type = Collection(
        element=Struct(fields={"id": Int64(), "date": Date(), "amount": Float64()}),
        partition_by=("date",),
    )
    # Statistics are facts recorded for every partition...
    statistics: tuple[Statistic, ...] = (Max(field="amount"),)
    # ...and Thresholds are judgements about them, checked on every build. A Threshold's Statistic
    # is recorded too, including the inputs of derived Statistics (like this Ratio).
    thresholds: tuple[Threshold, ...] = (
        Range(statistic=RowCount(), min=1),
        Range(statistic=Ratio(numerator=NullCount(field="amount"), denominator=RowCount()), max=0),
        Range(statistic=Min(field="amount"), min=0),
    )


class TotalSpend(Artifact):
    """Aggregate spend over all time."""

    type: Type = Float64()
    format: Format = JSON()
    storage: Storage = LocalFile()


class Transaction(TypedDict):
    id: int
    amount: float


@producer(version=SemVer(major=1, minor=0, patch=0))
def aggregate_transactions(
    transactions: Annotated[list[Transaction], Transactions],
) -> Annotated[float, TotalSpend]:
    return float(sum(txn["amount"] for txn in transactions))


with Graph(name="test-graph") as g:
    g.artifacts.vendor.transactions = Transactions(
        annotations=(Vendor(name="Acme"),),
        format=JSON(),
        storage=LocalFile(path=str(DIR / "transactions" / "{date.iso}.json")),
    )
    g.artifacts.spend = aggregate_transactions(transactions=g.artifacts.vendor.transactions)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    snapshot = g.build()

    logging.info(
        f"Transactions: {snapshot.read(snapshot.artifacts.vendor.transactions, annotation=list)}"
    )
    transaction_statistics = snapshot.read_statistics(snapshot.artifacts.vendor.transactions)
    for partition, statistics in transaction_statistics.items():
        day = cast(DateField, partition.partition_key["date"]).iso
        values = ", ".join(f"{statistic!r}={value!r}" for statistic, value in statistics.items())
        logging.info(f"Statistics for {day}: {values}")
    logging.info(f"Total Spend: {snapshot.read(snapshot.artifacts.spend, annotation=float)}")
