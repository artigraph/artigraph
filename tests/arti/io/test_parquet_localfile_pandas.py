from datetime import date
from pathlib import Path
from typing import Annotated, Protocol, cast

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from arti import Artifact, PartitionKey, Type, View, io
from arti.formats.parquet import Parquet
from arti.partitions import DateField
from arti.storage.local import LocalFile, LocalFilePartition
from arti.types import Collection, Date, Float64, Int64, String, Struct


class Transactions(Artifact):
    type: Type = Collection(
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


class _ParquetSchemaReader(Protocol):
    @property
    def schema_arrow(self) -> pa.Schema: ...


def _partition_key(value: date) -> PartitionKey:
    return PartitionKey(transaction_date=DateField(value=value))


def test_parquet_localfile_pandas_round_trip(tmp_path: Path) -> None:
    artifact = Transactions(
        format=Parquet(),
        storage=LocalFile(path=str(tmp_path / "{transaction_date.iso}.parquet")),
    )
    view = View.from_annotation(
        Annotated[pd.DataFrame, Transactions],
        mode="READWRITE",
    )
    frames = [
        pd.DataFrame(
            {
                "transaction_id": [f"txn-{index}"],
                "transaction_date": [transaction_date],
                "amount_cents": [amount_cents],
                "amount_usd": [amount_cents / 100],
            }
        )
        for index, (transaction_date, amount_cents) in enumerate(
            [(date(2026, 1, 1), 1_250), (date(2026, 1, 2), 975)],
            start=1,
        )
    ]
    snapshots = tuple(
        io.write(
            frame,
            artifact.type,
            artifact.format,
            artifact.storage.generate_partition(
                partition_key=_partition_key(cast(date, frame["transaction_date"].iloc[0]))
            ),
            view,
        )
        for frame in frames
    )

    single = io.read(artifact.type, artifact.format, snapshots[:1], view)
    pd.testing.assert_frame_equal(single, frames[0])

    combined = io.read(artifact.type, artifact.format, snapshots, view)
    pd.testing.assert_frame_equal(combined, pd.concat(frames, ignore_index=True))

    schema = cast(
        _ParquetSchemaReader,
        pq.ParquetFile(cast(LocalFilePartition, snapshots[0].storage_partition).path),
    ).schema_arrow
    assert schema.names == [
        "transaction_id",
        "transaction_date",
        "amount_cents",
        "amount_usd",
    ]
    assert schema.types[1] == pa.date32()
    assert schema.metadata is not None
    assert b"artigraph" in schema.metadata


def _write(tmp_path: Path, frame: pd.DataFrame) -> None:
    artifact = Transactions(
        format=Parquet(), storage=LocalFile(path=str(tmp_path / "data.parquet"))
    )
    view = View.from_annotation(Annotated[pd.DataFrame, Transactions], mode="WRITE")
    io.write(
        frame,
        artifact.type,
        artifact.format,
        artifact.storage.generate_partition(partition_key=_partition_key(date(2026, 1, 1))),
        view,
    )


def _frame(**overrides: object) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "transaction_id": ["txn-1"],
            "transaction_date": [date(2026, 1, 1)],
            "amount_cents": [1_250],
            "amount_usd": [12.5],
        }
        | {name: [value] for name, value in overrides.items()}
    )


def test_parquet_localfile_pandas_write_rejects_non_dataframes(tmp_path: Path) -> None:
    with pytest.raises(TypeError, match="Expected a pandas DataFrame, got <class 'list'>"):
        _write(tmp_path, cast(pd.DataFrame, [{"transaction_id": "txn-1"}]))


def test_parquet_localfile_pandas_write_rejects_extra_columns(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match=r"columns not in the Artifact type: \['note'\]"):
        _write(tmp_path, _frame(note="dropped"))
    assert not (tmp_path / "data.parquet").exists()


@pytest.mark.parametrize(
    "value", [pd.Timestamp("2026-01-01"), pd.Timestamp("2026-01-01", tz="UTC")]
)
def test_parquet_localfile_pandas_write_accepts_midnight_datetimes(
    tmp_path: Path, value: pd.Timestamp
) -> None:
    _write(tmp_path, _frame(transaction_date=value))


def test_parquet_localfile_pandas_write_reports_nat_as_null(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="null"):  # Not as a dropped time of day
        _write(tmp_path, _frame(transaction_date=pd.NaT))


@pytest.mark.parametrize(
    "value",
    [
        pd.Timestamp("2026-01-01 12:30"),
        # Arrow writes the UTC date, which is 2025-12-31 here.
        pd.Timestamp("2026-01-01", tz="Asia/Tokyo"),
    ],
)
def test_parquet_localfile_pandas_write_rejects_dropped_times(
    tmp_path: Path, value: pd.Timestamp
) -> None:
    with pytest.raises(ValueError, match="'transaction_date' has times of day"):
        _write(tmp_path, _frame(transaction_date=value))
