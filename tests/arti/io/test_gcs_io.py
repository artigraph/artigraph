from typing import Annotated

import pytest

from arti import (
    Format,
    PartitionKey,
    StoragePartition,
    StoragePartitionSnapshots,
    Type,
    View,
    io,
)
from arti.formats.json import JSON
from arti.formats.pickle import Pickle
from arti.partitions import Int64Field
from arti.storage.google.cloud.storage import GCSFile
from arti.types import Collection, Int64, Struct
from tests.arti.dummies import Num


class PartitionedNum(Num):
    type: Type = Collection(element=Struct(fields={"i": Int64()}), partition_by=("i",))


@pytest.mark.parametrize("format", [JSON(), Pickle()])
def test_gcsfile_io(gcs_bucket: str, format: Format) -> None:
    a, n = Num(format=format, storage=GCSFile(bucket=gcs_bucket, path="file")), 5
    view = View.from_annotation(Annotated[int, a.type], mode="READWRITE")

    io.write(n, a.type, a.format, a.storage.generate_partition(), view=view)
    partitions = a.storage.discover_partitions()
    assert len(partitions) == 1
    assert io.read(a.type, a.format, partitions, view=view) == n
    with pytest.raises(FileNotFoundError, match="No data"):
        io.read(a.type, a.format, StoragePartitionSnapshots(), view=view)
    with pytest.raises(
        ValueError, match="Multiple partitions can only be read into a partitioned Collection, not"
    ):
        io.read(a.type, a.format, partitions * 2, view=view)


@pytest.mark.parametrize("format", [JSON(), Pickle()])
def test_gcsfile_io_partitioned(gcs_bucket: str, format: Format) -> None:
    a = PartitionedNum(format=format, storage=GCSFile(bucket=gcs_bucket, path="{i.value}"))
    data: dict[StoragePartition, dict[str, int]] = {
        a.storage.generate_partition(
            input_fingerprint=None, partition_key=PartitionKey(i=Int64Field(value=i))
        ): {"i": i}
        for i in [1, 2]
    }
    view = View.from_annotation(Annotated[list, a.type], mode="READWRITE")
    for partition, record in data.items():
        io.write([record], a.type, a.format, partition, view=view)
    assert {p.snapshot() for p in data} == set(a.storage.discover_partitions())
    for partition, record in data.items():
        assert io.read(a.type, a.format, (partition.snapshot(),), view=view) == [record]
