import hashlib
import re
from datetime import date
from itertools import repeat
from pathlib import Path
from typing import cast

import pytest

from arti import Fingerprint, InputFingerprints, PartitionKey
from arti.partitions import DateField
from arti.storage.local import LocalFile, LocalFilePartition
from arti.types import Collection, Date, Struct
from tests.arti.dummies import DummyFormat


@pytest.fixture
def date_keys() -> list[PartitionKey]:
    return [
        PartitionKey(date=date_key)
        for date_key in (
            DateField(value=date(1970, 1, 1)),
            DateField(value=date(1970, 1, 2)),
            DateField(value=date(1970, 1, 3)),
            DateField(value=date(2021, 1, 1)),
        )
    ]


def generate_partition_files(storage: LocalFile, input_fingerprints: InputFingerprints) -> None:
    for pk, input_fingerprint in input_fingerprints.items():
        partition = storage.generate_partition(
            input_fingerprint=input_fingerprint, partition_key=pk
        )
        partition_path = Path(partition.path)
        partition_path.parent.mkdir(parents=True)
        partition_path.touch()


def test_local_partitioning(tmp_path: Path, date_keys: list[PartitionKey]) -> None:
    storage = (
        LocalFile(path=str(tmp_path / "{date.Y}" / "{date.m}" / "{date.d}" / "test"))
        .visit_type(Collection(element=Struct(fields={"date": Date()}), partition_by=("date",)))
        .visit_format(DummyFormat())
    )
    generate_partition_files(storage, InputFingerprints(zip(date_keys, repeat(None))))
    snapshots = storage.discover_partitions()
    assert len(snapshots) > 0
    for snapshot in snapshots:
        partition = snapshot.storage_partition
        assert isinstance(partition, LocalFilePartition)
        assert set(partition.partition_key) == {"date"}
        assert partition.partition_key in date_keys
        assert isinstance(partition.partition_key["date"], DateField)


def test_local_partitioning_default_template(tmp_path: Path, date_keys: list[PartitionKey]) -> None:
    # The default `partition_key_spec` zero pads the `DateField` month and day.
    storage = (
        LocalFile(path=str(tmp_path / "{partition_key_spec}" / "test"))
        .visit_type(Collection(element=Struct(fields={"date": Date()}), partition_by=("date",)))
        .visit_format(DummyFormat())
    )
    assert storage.path == str(
        tmp_path / "date_Y={date.Y}/date_m={date.m:02}/date_d={date.d:02}/test"
    )
    generate_partition_files(storage, InputFingerprints(zip(date_keys, repeat(None))))
    assert {snapshot.partition_key for snapshot in storage.discover_partitions()} == set(date_keys)


def test_local_partitioning_filtered(tmp_path: Path, date_keys: list[PartitionKey]) -> None:
    for year in {cast(DateField, k["date"]).Y for k in date_keys}:
        storage = (
            LocalFile(
                path=str(
                    tmp_path
                    / str(year)
                    / ("{date.Y[" + str(year) + "]}")
                    / "{date.m}"
                    / "{date.d}"
                    / "test"
                )
            )
            .visit_type(Collection(element=Struct(fields={"date": Date()}), partition_by=("date",)))
            .visit_format(DummyFormat())
        )
        # Generate files for *all* years - we want discover_partitions to do the filtering.
        generate_partition_files(storage, InputFingerprints(zip(date_keys, repeat(None))))
        snapshots = storage.discover_partitions()
        assert len(snapshots) > 0
        for snapshot in snapshots:
            partition = snapshot.storage_partition
            assert isinstance(partition, LocalFilePartition)
            assert set(partition.partition_key) == {"date"}
            assert partition.partition_key in date_keys
            assert isinstance(partition.partition_key["date"], DateField)
            assert year == cast(DateField, partition.partition_key["date"]).Y


def test_local_partitioning_with_input_fingerprints(
    tmp_path: Path, date_keys: list[PartitionKey]
) -> None:
    storage = (
        LocalFile(
            path=str(
                tmp_path / "{date.Y}" / "{date.m}" / "{date.d}" / "{input_fingerprint}" / "test"
            ),
        )
        .visit_type(Collection(element=Struct(fields={"date": Date()}), partition_by=("date",)))
        .visit_format(DummyFormat())
    )
    input_fingerprint = Fingerprint.from_int(42)
    input_fingerprints = InputFingerprints(zip(date_keys, repeat(input_fingerprint)))
    generate_partition_files(storage, input_fingerprints)
    snapshots = storage.discover_partitions(input_fingerprints)
    assert len(snapshots) > 0
    for snapshot in snapshots:
        partition = snapshot.storage_partition
        assert isinstance(partition, LocalFilePartition)
        assert set(partition.partition_key) == {"date"}
        assert partition.partition_key in date_keys
        assert isinstance(partition.partition_key["date"], DateField)
        assert partition.input_fingerprint == input_fingerprint


def test_local_partitioning_errors(tmp_path: Path) -> None:
    storage = LocalFile(
        path=str(tmp_path / "{date.Y}" / "{date.m}" / "{date.d}" / "test")
    ).visit_type(
        Collection(element=Struct(fields={"data_date": Date()}), partition_by=("data_date",))
    )
    with pytest.raises(
        ValueError,
        match=re.escape("No 'date' partition key found, expected one of ('data_date',)"),
    ):
        storage.discover_partitions()


def test_local_file_partition_fingerprint(tmp_path: Path) -> None:
    text = "hello world"
    path = tmp_path / "test.txt"
    with path.open("w") as f:
        f.write("hello world")
    partition = LocalFilePartition(
        partition_key=PartitionKey(), path=str(path), storage=LocalFile(path=str(path))
    ).snapshot()
    assert partition.content_fingerprint == Fingerprint.from_string(
        hashlib.sha256(text.encode()).hexdigest()
    )
