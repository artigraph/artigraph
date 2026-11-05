from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from arti import Artifact, Graph, PartitionKey, Type, io
from arti.formats.parquet import Parquet
from arti.partitions import DateField
from arti.storage.local import LocalFile
from arti.types import Collection, Date, Float64, Int64, String, Struct
from arti.views.pandas import DataFrame
from arti.views.python import List

TYPE = Collection(
    element=Struct(
        fields={
            "transaction_id": String(),
            "transaction_date": Date(),
            "amount_cents": Int64(),
            "amount_usd": Float64(),
        }
    ),
    partition_by=("transaction_date",),
)


class Transactions(Artifact):
    type: Type = TYPE


def _rows(day: date) -> list[dict[str, Any]]:
    return [
        {"transaction_id": "txn-1", "transaction_date": day, "amount_cents": 1, "amount_usd": 0.01},
        {
            "transaction_id": "txn-2",
            "transaction_date": day,
            "amount_cents": 50,
            "amount_usd": 0.5,
        },
    ]


def test_parquet_localfile_python_round_trip(tmp_path: Path) -> None:
    with Graph(name="test") as g:
        g.artifacts.transactions = Transactions(
            format=Parquet(),
            storage=LocalFile(path=str(tmp_path / "{transaction_date.iso}.parquet")),
        )
    artifact = g.artifacts.transactions
    days = [date(2026, 1, 1), date(2026, 1, 2)]
    partitions = [
        g.write(
            _rows(day),
            artifact=artifact,
            partition_key=PartitionKey(transaction_date=DateField(value=day)),
        )
        for day in days
    ]
    view = List(type=TYPE, mode="READ")
    rows = g.read(artifact, storage_partition_snapshots=tuple(partitions), view=view)
    assert rows == _rows(days[0]) + _rows(days[1])
    assert type(rows[0]["transaction_date"]) is date
    # The files are ordinary Parquet, readable with other Views.
    frame = g.read(
        artifact,
        storage_partition_snapshots=(partitions[0],),
        view=DataFrame(type=TYPE, mode="READ"),
    )
    assert isinstance(frame, pd.DataFrame)
    assert frame["transaction_id"].tolist() == ["txn-1", "txn-2"]


def test_parquet_localfile_python_rejects_extra_fields(tmp_path: Path) -> None:
    artifact = Transactions(
        format=Parquet(), storage=LocalFile(path=str(tmp_path / "{transaction_date.iso}.parquet"))
    )
    key = PartitionKey(transaction_date=DateField(value=date(2026, 1, 1)))
    with pytest.raises(ValueError, match=r"Rows have fields not in the Artifact type: \['extra'\]"):
        io.write(
            [{**_rows(date(2026, 1, 1))[0], "extra": 1}],
            type_=TYPE,
            format=artifact.format,
            storage_partition=artifact.storage.generate_partition(partition_key=key),
            view=List(type=TYPE, mode="WRITE"),
        )
    assert not list(tmp_path.iterdir())


def test_parquet_localfile_python_checks_nullability(tmp_path: Path) -> None:
    artifact = Transactions(
        format=Parquet(), storage=LocalFile(path=str(tmp_path / "{transaction_date.iso}.parquet"))
    )
    key = PartitionKey(transaction_date=DateField(value=date(2026, 1, 1)))
    with pytest.raises(ValueError, match="declared non-nullable but contains nulls"):
        io.write(
            [{**_rows(date(2026, 1, 1))[0], "amount_cents": None}],
            type_=TYPE,
            format=artifact.format,
            storage_partition=artifact.storage.generate_partition(partition_key=key),
            view=List(type=TYPE, mode="WRITE"),
        )
