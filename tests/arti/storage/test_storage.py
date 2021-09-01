import re
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Iterator

import pytest

from arti.partitions import DateKey
from arti.storage.local import LocalFile


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
def tmp_date_dirs(date_partitions: frozenset[DateKey]) -> Iterator[Path]:
    with TemporaryDirectory() as _tmpdir:
        tmpdir = Path(_tmpdir)
        for pk in date_partitions:
            (tmpdir / str(pk.Y) / f"{pk.m:02}" / f"{pk.d:02}").mkdir(parents=True)
        yield tmpdir


def test_local_partitioning(date_keys: frozenset[DateKey]) -> None:
    with tmp_date_dirs(date_keys) as tmpdir:
        partitions = LocalFile(
            path=str(tmpdir / "{data_date.Y}" / "{data_date.m}" / "{data_date.d}")
        ).discover_partitions(data_date=DateKey)
        for partition in partitions:
            assert set(partition) == {"data_date"}
            assert partition["data_date"] in date_keys
            assert isinstance(partition["data_date"], DateKey)


def test_local_partitioning_filtered(date_keys: frozenset[DateKey]) -> None:
    with tmp_date_dirs(date_keys) as tmpdir:
        for year in [1970, 1971]:
            partitions = LocalFile(
                path=str(
                    tmpdir
                    / ("{data_date.Y[" + str(year) + "]}")
                    / "{data_date.m}"
                    / "{data_date.d}"
                )
            ).discover_partitions(data_date=DateKey)
            for partition in partitions:
                assert set(partition) == {"data_date"}
                assert partition["data_date"] in date_keys
                assert isinstance(partition["data_date"], DateKey)
                assert partition["data_date"].Y == year


def test_local_partitioning_errors(date_keys: frozenset[DateKey]) -> None:
    with tmp_date_dirs(date_keys) as tmpdir:
        with pytest.raises(
            ValueError,
            match=re.escape("No 'data_date' partition key found, expected one of ('date',)"),
        ):
            LocalFile(
                path=str(tmpdir / "{data_date.Y[2021]}" / "{data_date.m}" / "{data_date.d}")
            ).discover_partitions(date=DateKey)
