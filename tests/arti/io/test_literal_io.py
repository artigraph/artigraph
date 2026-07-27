import json
import pickle
import subprocess
import sys
from base64 import b64encode
from collections.abc import Callable
from datetime import date
from typing import Any

import pytest

from arti import Format, View, io
from arti.formats.json import JSON
from arti.formats.pickle import Pickle
from arti.storage.literal import StringLiteral, StringLiteralPartition
from arti.types import Collection, Date, Int64, Struct
from tests.arti.dummies import Num as _Num


class Num(_Num):
    storage: StringLiteral = StringLiteral()


def _pickle_dumps(value: Any) -> str:
    return b64encode(pickle.dumps(value)).decode()


@pytest.mark.parametrize(("format", "dumps"), [(JSON(), json.dumps), (Pickle(), _pickle_dumps)])
def test_stringliteral_io(format: Format, dumps: Callable[[Any], str]) -> None:
    n = 5
    a = Num(format=format, storage=StringLiteral(id="test", value=dumps(n)))
    view = View.from_annotation(int, mode="READWRITE")

    snapshots = a.storage.discover_partitions()
    assert len(snapshots) == 1
    partition = snapshots[0].storage_partition
    assert isinstance(partition, StringLiteralPartition)
    assert partition.value == dumps(n)

    assert io.read(a.type, a.format, snapshots, view=view) == n
    # Read "partitioned" literal
    assert io.read(
        Collection(element=Struct(fields={"a": Int64()}), partition_by=("a",)),
        a.format,
        tuple(
            p.snapshot()
            for p in [
                partition.model_copy(update={"value": dumps([{"a": 1}])}),
                partition.model_copy(update={"value": dumps([{"a": 2}])}),
            ]
        ),
        view=view,
    ) == [{"a": 1}, {"a": 2}]

    # Test write
    unwritten = StringLiteralPartition(id="junk", storage=a.storage)
    new_snapshot = io.write(10, a.type, a.format, unwritten, view=view)
    new_partition = new_snapshot.storage_partition
    assert isinstance(new_partition, StringLiteralPartition)
    assert new_partition.value == dumps(10)
    assert partition.value == dumps(n)  # Confirm no mutation
    with pytest.raises(ValueError, match="Literals with a value already set cannot be written"):
        io.write(10, a.type, a.format, new_partition, view=view)


def test_stringliteral_read_discovers_io_in_fresh_interpreter() -> None:
    # Run in a fresh interpreter so other tests' `arti.io.*` imports can't mask missing discovery.
    code = """
from arti import Artifact, View, io
from arti.formats.json import JSON
from arti.storage.literal import StringLiteral
from arti.types import Int64

a = Artifact(type=Int64(), format=JSON(), storage=StringLiteral(id="test", value="5"))
view = View.from_annotation(int, mode="READ")
assert io.read(a.type, a.format, a.storage.discover_partitions(), view=view) == 5
"""
    subprocess.run([sys.executable, "-c", code], check=True)  # noqa: S603


def test_pickle_stringliteral_io_non_json_value() -> None:
    view = View.from_annotation(date, mode="READWRITE")
    unwritten = StringLiteralPartition(id="junk", storage=StringLiteral())
    snapshot = io.write(date(1970, 1, 1), Date(), Pickle(), unwritten, view=view)
    assert io.read(Date(), Pickle(), (snapshot,), view=view) == date(1970, 1, 1)
