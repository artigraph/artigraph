from __future__ import annotations

from typing import Annotated

import pandas as pd

from arti import Artifact, Format, Type, one_to_one, producer
from arti.formats.parquet import Parquet
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
