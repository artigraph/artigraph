import re
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest

from arti import Format, StoragePartition, Type, View, io
from arti.formats.json import JSON
from arti.storage.literal import StringLiteral, StringLiteralPartition
from arti.storage.local import LocalFile, LocalFilePartition
from arti.types import Collection, Int64, Struct
from arti.views.python import Int, PythonBuiltin


class F(Format):
    pass


class F1(F):
    pass


def test_register_reader_resolves_most_specific() -> None:
    @io.register_reader
    def read_f(
        type_: Type, format: F, storage_partitions: Sequence[StoragePartition], view: View
    ) -> Any:
        return "f"

    @io.register_reader
    def read_f1(
        type_: Type, format: F1, storage_partitions: Sequence[StoragePartition], view: View
    ) -> Any:
        return "f1"

    assert io.lookup_reader(io.IOKey(Int64, F, StringLiteralPartition, Int)) is read_f
    assert io.lookup_reader(io.IOKey(Int64, F1, StringLiteralPartition, Int)) is read_f1
    with pytest.raises(
        ValueError,
        match=re.escape(
            "No `io.read` implementation found for: type_=Int64, format=Format, storage_partition=StringLiteralPartition, view=Int"
        ),
    ):
        io.lookup_reader(io.IOKey(Int64, Format, StringLiteralPartition, Int))
    with pytest.raises(ValueError, match=re.escape("is already registered with")):
        io.register_reader(read_f)


def test_register_writer_rejects_ambiguous_matches() -> None:
    class F2(F):
        pass

    @io.register_writer
    def write_any_partition(
        data: Any, type_: Type, format: F2, storage_partition: StoragePartition, view: PythonBuiltin
    ) -> None:
        pass

    @io.register_writer
    def write_any_format(
        data: Any,
        type_: Type,
        format: F,
        storage_partition: StringLiteralPartition,
        view: PythonBuiltin,
    ) -> None:
        pass

    assert callable(write_any_partition)
    assert callable(write_any_format)
    with pytest.raises(ValueError, match=r"\['write_any_format', 'write_any_partition'\]"):
        io.lookup_writer(io.IOKey(Int64, F2, StringLiteralPartition, Int))


def test_register_validates_signature() -> None:
    def bad_names(type_: Type, format: F, storage_partitions: Sequence[StoragePartition]) -> Any:
        pass

    with pytest.raises(
        TypeError,
        match=re.escape(
            "Expected `bad_names` to have positional ['type_', 'format', 'storage_partitions', 'view'] parameters"
        ),
    ):
        io.register_reader(bad_names)

    def bad_kind(
        type_: Type, format: F, storage_partitions: Sequence[StoragePartition], *, view: View
    ) -> Any:
        pass

    with pytest.raises(TypeError, match="Expected `bad_kind` to have positional"):
        io.register_reader(bad_kind)

    def bad_sequence(
        type_: Type, format: F, storage_partitions: StoragePartition, view: View
    ) -> Any:
        pass

    with pytest.raises(
        TypeError,
        match=re.escape("`bad_sequence.storage_partitions` parameter to be a Sequence[...]"),
    ):
        io.register_reader(bad_sequence)

    def bad_list(
        type_: Type, format: F, storage_partitions: list[StoragePartition], view: View
    ) -> Any:
        pass

    with pytest.raises(
        TypeError,
        match=re.escape("`bad_list.storage_partitions` parameter to be a Sequence[...]"),
    ):
        io.register_reader(bad_list)

    def bad_type(
        data: Any, type_: Type, format: int, storage_partition: StoragePartition, view: View
    ) -> None:
        pass

    with pytest.raises(
        TypeError,
        match=re.escape(
            "Expected the `bad_type.format` parameter to be a subclass of <class 'arti.formats.Format'>, got <class 'int'>"
        ),
    ):
        io.register_writer(bad_type)


def test_read_rejects_mixed_partition_types(tmp_path: Path) -> None:
    path = tmp_path / "x.json"
    path.write_text("[]")
    snapshots = (
        StringLiteralPartition(id="x", value="[]", storage=StringLiteral()).snapshot(),
        LocalFilePartition(path=str(path), storage=LocalFile()).snapshot(),
    )
    with pytest.raises(
        ValueError,
        match=re.escape(
            "All partitions must share one StoragePartition type, got: ['LocalFilePartition', 'StringLiteralPartition']"
        ),
    ):
        io.read(
            type_=Collection(element=Struct(fields={"x": Int64()}), partition_by=("x",)),
            format=JSON(),
            storage_partition_snapshots=snapshots,
            view=View.from_annotation(list[dict[str, int]], mode="READ"),
        )
