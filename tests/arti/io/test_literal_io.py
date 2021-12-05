import json

import pytest

from arti import io
from arti.formats.json import JSON
from arti.storage.literal import StringLiteral
from arti.types import Collection, Int64, Struct
from arti.views import View
from tests.arti.dummies import Num as _Num


class Num(_Num):
    storage: StringLiteral


def test_localfile_io() -> None:
    n = 5
    a = Num(format=JSON(), storage=StringLiteral(value=json.dumps(n)))

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
    with pytest.raises(ValueError, match="Literals cannot be partitioned"):
        io.read(
            Collection(element=Struct(fields={"a": Int64()}), partition_by=("a",)),
            a.format,
            partitions * 2,  # Pretend more partitions
            view=View.get_class_for(int, validation_type=a.type)(),
        )
