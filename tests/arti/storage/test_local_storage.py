import hashlib
import re
from contextlib import contextmanager
from datetime import date
from itertools import repeat
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Iterator

import pytest

from arti.fingerprints import Fingerprint
from arti.partitions import CompositeKey, CompositeKeyTypes, DateKey
from arti.storage import InputFingerprints, Storage
from arti.storage.local import LocalFile, LocalFilePartition


@pytest.fixture()
def date_keys() -> list[CompositeKey]:
    return [
        CompositeKey(date=date_key)
        for date_key in (
            DateKey(key=date(1970, 1, 1)),
            DateKey(key=date(1970, 1, 2)),
            DateKey(key=date(1970, 1, 3)),
            DateKey(key=date(2021, 1, 1)),
        )
    ]


@contextmanager
def tmp_dir() -> Iterator[Path]:
    with TemporaryDirectory() as _tmpdir:
        yield Path(_tmpdir)


def generate_partition_files(storage: Storage[Any], input_fingerprints: InputFingerprints) -> None:
    for pk, input_fingerprint in input_fingerprints.items():
        partition = storage.generate_partition(
            pk, input_fingerprint, with_content_fingerprint=False
        )
        partition_path = Path(partition.path)
        partition_path.parent.mkdir(parents=True)
        partition_path.touch()


def test_local_partitioning(date_keys: list[CompositeKey]) -> None:
    with tmp_dir() as tmpdir:
        storage = LocalFile(path=str(tmpdir / "{date.Y}" / "{date.m}" / "{date.d}" / "test"))
        generate_partition_files(
            storage, InputFingerprints(zip(date_keys, repeat(Fingerprint.empty())))
        )
        partitions = storage.discover_partitions(CompositeKeyTypes(date=DateKey))
        assert len(partitions) > 0
        for partition in partitions:
            assert isinstance(partition, LocalFilePartition)
            assert set(partition.keys) == {"date"}
            assert partition.keys in date_keys
            assert isinstance(partition.keys["date"], DateKey)


def test_local_partitioning_filtered(date_keys: list[CompositeKey]) -> None:
    for year in {k["date"].Y for k in date_keys}:  # type: ignore
        with tmp_dir() as tmpdir:
            storage = LocalFile(path=str(tmpdir / "{date.Y}" / "{date.m}" / "{date.d}" / "test"))
            storage = LocalFile(
                path=str(
                    tmpdir / ("{date.Y[" + str(year) + "]}") / "{date.m}" / "{date.d}" / "test"
                )
            )
            # Generate files for *all* years - we want discover_partitions to do the filtering.
            generate_partition_files(
                storage, InputFingerprints(zip(date_keys, repeat(Fingerprint.empty())))
            )
            partitions = storage.discover_partitions(CompositeKeyTypes(date=DateKey))
            assert len(partitions) > 0
            for partition in partitions:
                assert isinstance(partition, LocalFilePartition)
                assert set(partition.keys) == {"date"}
                assert partition.keys in date_keys
                assert isinstance(partition.keys["date"], DateKey)
                assert partition.keys["date"].Y == year


def test_local_partitioning_with_input_fingerprints(date_keys: list[CompositeKey]) -> None:
    with tmp_dir() as tmpdir:
        storage = LocalFile(
            path=str(tmpdir / "{date.Y}" / "{date.m}" / "{date.d}" / "{input_fingerprint}" / "test")
        )
        input_fingerprint = Fingerprint.from_int(42)
        input_fingerprints = InputFingerprints(zip(date_keys, repeat(input_fingerprint)))
        generate_partition_files(storage, input_fingerprints)
        partitions = storage.discover_partitions(
            CompositeKeyTypes(date=DateKey), input_fingerprints
        )
        assert len(partitions) > 0
        for partition in partitions:
            assert isinstance(partition, LocalFilePartition)
            assert set(partition.keys) == {"date"}
            assert partition.keys in date_keys
            assert isinstance(partition.keys["date"], DateKey)
            assert partition.input_fingerprint == input_fingerprint


def test_local_partitioning_errors() -> None:
    with tmp_dir() as tmpdir:
        storage = LocalFile(path=str(tmpdir / "{date.Y}" / "{date.m}" / "{date.d}" / "test"))
        with pytest.raises(
            ValueError,
            match=re.escape("No 'date' partition key found, expected one of ('data_date',)"),
        ):
            storage.discover_partitions(CompositeKeyTypes(data_date=DateKey))


def test_local_file_partition_fingerprint() -> None:
    text = "hello world"
    with tmp_dir() as tmpdir:
        path = tmpdir / "test.txt"
        with path.open("w") as f:
            f.write("hello world")

        partition = LocalFilePartition(keys={}, path=str(path)).with_content_fingerprint()
        assert partition.content_fingerprint == Fingerprint.from_string(
            hashlib.sha1(text.encode()).hexdigest()
        )
