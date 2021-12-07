import json

import pytest

from arti import io
from arti.formats.json import JSON
from arti.partitions import CompositeKey
from arti.storage.literal import StringLiteral, StringLiteralPartition
from arti.types import Collection, Int64, Struct
from arti.views.python import Int as IntView
from tests.arti.dummies import Num as _Num


class Num(_Num):
    storage: StringLiteral


def test_stringliteral_io() -> None:
    n = 5
    a = Num(format=JSON(), storage=StringLiteral(value=json.dumps(n)))

    partitions = a.discover_storage_partitions()
    assert len(partitions) == 1
    assert io.read(a.type, a.format, partitions, view=IntView()) == n
    with pytest.raises(ValueError, match="Literal storage cannot be partitioned"):
        io.read(
            Collection(element=Struct(fields={"a": Int64()}), partition_by=("a",)),
            a.format,
            partitions * 2,  # Pretend more partitions
            view=IntView(),
        )
    with pytest.raises(FileNotFoundError, match="Literal has not been written yet"):
        io.read(a.type, a.format, [StringLiteralPartition(keys=CompositeKey())], view=IntView())

    # Test write
    partition = partitions[0]
    assert isinstance(partition, StringLiteralPartition)
    assert partition.value == json.dumps(n)
    new = io.write(10, a.type, a.format, partition, view=IntView())
    assert isinstance(new, StringLiteralPartition)
    assert new.value == json.dumps(10)
    assert partition.value == json.dumps(n)  # Confirm no mutation
