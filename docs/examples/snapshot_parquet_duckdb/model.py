from __future__ import annotations

from typing import Annotated

import pandas as pd

from arti import Artifact, Format, Statistic, Threshold, Type, one_to_one, producer
from arti.formats.parquet import Parquet
from arti.statistics import DistinctCount, DistinctValues, Max, Min, NullCount, Ratio, RowCount
from arti.thresholds import Range, Subset
from arti.types import Collection, Date, Float64, Int64, String, Struct
from arti.versions import SemVer


class RawTransactions(Artifact):
    type: Type = Collection(
        element=Struct(
            fields={
                "transaction_id": String(),
                "transaction_date": Date(),
                "vendor": String(),
                "category": String(),
                "amount_cents": Int64(),
            }
        ),
        partition_by=("transaction_date",),
    )
    format: Format = Parquet()
    # Raw partitions written with `graph.write` compute Statistics from the written DataFrame.
    statistics: tuple[Statistic, ...] = (RowCount(),)


class CleanTransactions(Artifact):
    type: Type = Collection(
        element=Struct(
            fields={
                "transaction_id": String(),
                "transaction_date": Date(),
                "vendor": String(),
                "category": String(),
                "amount_usd": Float64(),
            }
        ),
        partition_by=("transaction_date",),
    )
    format: Format = Parquet()
    # Statistics are facts recorded for every partition. Produced partitions compute them from the
    # Producer's in-memory DataFrame.
    statistics: tuple[Statistic, ...] = (
        DistinctCount(field="transaction_id"),
        Max(field="amount_usd"),
    )
    # Thresholds are judgements over facts, checked on every build. Their Statistics are recorded
    # too. A failing partition stops the build before anything downstream reads it.
    thresholds: tuple[Threshold, ...] = (
        Range(statistic=RowCount(), min=1),
        # A compound judgement checks a derived Statistic: here, at most 5% of vendors are null.
        Range(
            statistic=Ratio(numerator=NullCount(field="vendor"), denominator=RowCount()), max=0.05
        ),
        Subset(
            statistic=DistinctValues(field="category"),
            of=frozenset({"meals", "office", "software"}),
        ),
        Range(statistic=Min(field="amount_usd"), min=0),
    )


@producer(version=SemVer(major=1, minor=0, patch=0), map=one_to_one("raw"))
def clean_transactions(
    raw: Annotated[pd.DataFrame, RawTransactions],
) -> Annotated[pd.DataFrame, CleanTransactions]:
    clean = raw.copy()
    clean["transaction_id"] = clean["transaction_id"].str.strip()
    clean["vendor"] = clean["vendor"].str.strip()
    clean["category"] = clean["category"].str.strip().str.lower()
    clean["amount_usd"] = (clean["amount_cents"].astype("float64") / 100).round(2)
    return clean[
        ["transaction_id", "transaction_date", "vendor", "category", "amount_usd"]
    ].sort_values(["transaction_date", "transaction_id"], ignore_index=True)


__all__ = [
    "CleanTransactions",
    "RawTransactions",
    "clean_transactions",
]
