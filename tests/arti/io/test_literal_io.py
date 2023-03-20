import json

import pytest

from arti import View, io
from arti.formats.json import JSON
from arti.storage.literal import StringLiteral, StringLiteralPartition
from arti.types import Collection, Int64, Struct
from tests.arti.dummies import Num as _Num


class Num(_Num):
    storage: StringLiteral


def test_stringliteral_io() -> None:
    n = 5
    a = Num(format=JSON(), storage=StringLiteral(id="test", value=json.dumps(n)))
    view = View.from_annotation(int, mode="READWRITE")

    partitions = a.storage.discover_partitions()
    assert len(partitions) == 1
    partition = partitions[0]
    assert isinstance(partition, StringLiteralPartition)
    assert partition.value == json.dumps(n)

    assert io.read(a.type, a.format, partitions, view=view) == n
    # Read "partitioned" literal
    assert io.read(
        Collection(element=Struct(fields={"a": Int64()}), partition_by=("a",)),
        a.format,
        [
            partition.copy(update={"value": '[{"a": 1}]'}),
            partition.copy(update={"value": '[{"a": 2}]'}),
        ],
        view=view,
    ) == [{"a": 1}, {"a": 2}]
    # Check that read with value=None fails
    with pytest.raises(FileNotFoundError, match="Literal has not been written yet"):
        io.read(a.type, a.format, [StringLiteralPartition(id="junk")], view=view)

    # Test write
    unwritten = StringLiteralPartition(id="junk")
    new = io.write(10, a.type, a.format, unwritten, view=view)
    assert isinstance(new, StringLiteralPartition)
    assert new.value == json.dumps(10)
    assert partition.value == json.dumps(n)  # Confirm no mutation
    with pytest.raises(ValueError, match="Literals with a value already set cannot be written"):
        io.write(10, a.type, a.format, new, view=view)
