import hashlib
import re
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Iterator

import pytest

from arti.fingerprints import Fingerprint
from arti.partitions import CompositeKeyTypes, DateKey
from arti.storage.local import LocalFile, LocalFilePartition


@pytest.fixture()
def date_keys() -> frozenset[DateKey]:
    return frozenset(
        {
            DateKey(key=date(1970, 1, 1)),
            DateKey(key=date(1970, 1, 2)),
            DateKey(key=date(1970, 1, 3)),
            DateKey(key=date(2021, 1, 1)),
        }
    )


@contextmanager
def tmp_dir() -> Iterator[Path]:
    with TemporaryDirectory() as _tmpdir:
        yield Path(_tmpdir)


@contextmanager
def tmp_date_files(date_partitions: frozenset[DateKey], filename: str) -> Iterator[Path]:
    with tmp_dir() as tmpdir:
        for pk in date_partitions:
            path = tmpdir / str(pk.Y) / f"{pk.m:02}" / f"{pk.d:02}"
            path.mkdir(parents=True)
            (path / filename).touch()
        yield tmpdir


def test_local_partitioning(date_keys: frozenset[DateKey]) -> None:
    with tmp_date_files(date_keys, "test") as tmpdir:
        partitions = LocalFile(
            path=str(tmpdir / "{data_date.Y}" / "{data_date.m}" / "{data_date.d}" / "test")
        ).discover_partitions(CompositeKeyTypes(data_date=DateKey))
        for partition in partitions:
            assert isinstance(partition, LocalFilePartition)
            assert set(partition.keys) == {"data_date"}
            assert partition.keys["data_date"] in date_keys
            assert isinstance(partition.keys["data_date"], DateKey)


def test_local_partitioning_filtered(date_keys: frozenset[DateKey]) -> None:
    with tmp_date_files(date_keys, "test") as tmpdir:
        for year in {k.Y for k in date_keys}:
            partitions = LocalFile(
                path=str(
                    tmpdir
                    / ("{data_date.Y[" + str(year) + "]}")
                    / "{data_date.m}"
                    / "{data_date.d}"
                    / "test"
                )
            ).discover_partitions(CompositeKeyTypes(data_date=DateKey))
            for partition in partitions:
                assert isinstance(partition, LocalFilePartition)
                assert set(partition.keys) == {"data_date"}
                assert partition.keys["data_date"] in date_keys
                assert isinstance(partition.keys["data_date"], DateKey)
                assert partition.keys["data_date"].Y == year


def test_local_partitioning_errors(date_keys: frozenset[DateKey]) -> None:
    with tmp_date_files(date_keys, "test") as tmpdir:
        with pytest.raises(
            ValueError,
            match=re.escape("No 'data_date' partition key found, expected one of ('date',)"),
        ):
            LocalFile(
                path=str(
                    tmpdir / "{data_date.Y[2021]}" / "{data_date.m}" / "{data_date.d}" / "test"
                )
            ).discover_partitions(CompositeKeyTypes(date=DateKey))


def test_local_file_partition_fingerprint() -> None:
    text = "hello world"
    with tmp_dir() as tmpdir:
        path = tmpdir / "test.txt"
        with path.open("w") as f:
            f.write("hello world")

        partition = LocalFilePartition(keys={}, path=str(path)).with_fingerprint()
        assert partition.fingerprint == Fingerprint.from_string(
            hashlib.sha1(text.encode()).hexdigest()
        )
