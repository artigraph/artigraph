from pathlib import Path

import pytest

from arti import CompositeKey, Fingerprint, Format, StoragePartition, StoragePartitions, View, io
from arti.formats.json import JSON
from arti.formats.pickle import Pickle
from arti.internal.utils import frozendict
from arti.partitions import Int64Key
from arti.storage.local import LocalFile
from arti.types import Collection, Int64, Struct
from tests.arti.dummies import Num


class PartitionedNum(Num):
    type = Collection(element=Struct(fields={"i": Int64()}), partition_by=("i",))


@pytest.mark.parametrize(
    ["format"],
    [
        (JSON(),),
        (Pickle(),),
    ],
)
def test_localfile_io(tmp_path: Path, format: Format) -> None:
    a, n = Num(format=format, storage=LocalFile(path=str(tmp_path / "a"))), 5

    io.write(
        n,
        a.type,
        a.format,
        a.storage.generate_partition(with_content_fingerprint=False),
        view=View.get_class_for(int, validation_type=a.type)(),
    )
    partitions = a.discover_storage_partitions()
    assert len(partitions) == 1
    assert (
        io.read(
            a.type,
            a.format,
            partitions,
            view=View.get_class_for(int, validation_type=a.type)(),
        )
        == n
    )
    with pytest.raises(FileNotFoundError, match="No data"):
        io.read(
            a.type,
            a.format,
            StoragePartitions(),
            view=View.get_class_for(int, validation_type=a.type)(),
        )
    with pytest.raises(
        ValueError, match="Multiple partitions can only be read into a partitioned Collection, not"
    ):
        io.read(
            a.type,
            a.format,
            partitions * 2,
            view=View.get_class_for(int, validation_type=a.type)(),
        )


@pytest.mark.parametrize(
    ["format"],
    [
        (JSON(),),
        (Pickle(),),
    ],
)
def test_localfile_io_partitioned(tmp_path: Path, format: Format) -> None:
    a = PartitionedNum(format=format, storage=LocalFile(path=str(tmp_path / "{i.key}")))
    data: dict[frozendict[str, int], StoragePartition] = {
        frozendict(i=i): a.storage.generate_partition(
            keys=CompositeKey(i=Int64Key(key=i)),
            input_fingerprint=Fingerprint.empty(),
            with_content_fingerprint=False,
        )
        for i in [1, 2]
    }
    for record, partition in data.items():
        io.write(
            [record],
            a.type,
            a.format,
            partition,
            view=View.get_class_for(list, validation_type=a.type)(),
        )
    assert set(p.with_content_fingerprint() for p in data.values()) == set(
        a.discover_storage_partitions()
    )
    for record, partition in data.items():
        assert (
            io.read(
                a.type,
                a.format,
                [partition],
                view=View.get_class_for(list, validation_type=a.type)(),
            )
            == [record]
        )
