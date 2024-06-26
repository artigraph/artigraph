from __future__ import annotations

import re
from typing import ClassVar, cast

import pytest

from arti import (
    Fingerprint,
    Graph,
    InputFingerprints,
    PartitionKey,
    PartitionKeyTypes,
    Storage,
    StoragePartition,
    StoragePartitionSnapshots,
    Type,
)
from arti.formats.json import JSON
from arti.partitions import Int8Field
from arti.types import Collection, Date, Int8, Struct
from tests.arti.dummies import DummyFormat


class MockStoragePartition(StoragePartition):
    path: str

    def compute_content_fingerprint(self) -> Fingerprint:
        return Fingerprint.from_string(self.path)


class MockStorage(Storage[MockStoragePartition]):
    path: str

    def discover_partitions(
        self, input_fingerprints: InputFingerprints = InputFingerprints()
    ) -> StoragePartitionSnapshots:
        assert all(v is Int8Field for v in self.key_types.values())  # Simplifies logic here...
        return tuple(
            self.generate_partition(partition_key=partition_key).snapshot()
            for partition_key in (
                PartitionKey({k: Int8Field(value=i) for k in self.key_types}) for i in range(3)
            )
        )


def test_StoragePartition_snapshot() -> None:
    storage = MockStorage(path="/tmp/test")
    partition = MockStoragePartition(path="/tmp/test", partition_key={}, storage=storage)
    snapshot = partition.snapshot()
    assert snapshot.content_fingerprint == partition.compute_content_fingerprint()
    assert snapshot.storage == storage
    assert snapshot.storage_partition == partition


def test_Storage_init_subclass() -> None:
    class Abstract(Storage):  # type: ignore[type-arg]
        _abstract_ = True

    assert not hasattr(Abstract, "storage_partition_type")

    with pytest.raises(TypeError, match="Bad fields must match MockStoragePartition"):

        class Bad(Storage[MockStoragePartition]):
            pass

    class S(Storage[MockStoragePartition]):
        path: str

    assert not hasattr(Storage, "storage_partition_type")
    assert S.storage_partition_type is MockStoragePartition  # type: ignore[misc]


def test_Storage_resolve_empty_error() -> None:
    s = MockStorage(path="/{name}")
    with pytest.raises(ValueError, match=".path was empty after removing unused templates"):
        s._visit_names(())


def test_Storage_visit_format() -> None:
    p = "somefile{extension}"
    assert MockStorage(path=p)._visit_format(JSON()).path == "somefile.json"
    assert MockStorage(path=p)._visit_format(JSON(extension="")).path == "somefile"


def test_Storage_visit_graph() -> None:
    s = MockStorage(path="/{graph_name}/{path_tags}/junk")
    assert s._visit_graph(Graph(name="test")) == MockStorage(path="/test/junk")
    assert s._visit_graph(Graph(name="test", path_tags={"a": "b"})) == MockStorage(
        path="/test/a=b/junk"
    )


def test_Storage_visit_input_fingerprint() -> None:
    s = MockStorage(path="/{input_fingerprint}/junk")
    assert s._visit_input_fingerprint(Fingerprint.from_int(10)) == MockStorage(path="/10/junk")
    assert s._visit_input_fingerprint(None) == MockStorage(path="/junk")


def test_Storage_visit_names() -> None:
    s = MockStorage(path="/{names}")
    assert s._visit_names(("a", "b")) == MockStorage(path="/a/b")

    s = MockStorage(path="/{names}/junk/{name}")
    assert s._visit_names(("a", "b")) == MockStorage(path="/a/b/junk/b")
    assert s._visit_names(()) == MockStorage(path="/junk")


@pytest.mark.parametrize(
    ("spec", "expected", "type"),
    [
        (
            "/tmp/test/{partition_key_spec}",
            "/tmp/test",
            Collection(element=Struct(fields={"a": Int8()})),
        ),
        (
            "/tmp/test/{partition_key_spec}",
            "/tmp/test/date_Y={date.Y}/date_m={date.m:02}/date_d={date.d:02}",
            Collection(element=Struct(fields={"date": Date()}), partition_by=("date",)),
        ),
        (
            "/tmp/test/{partition_key_spec}",
            "/tmp/test/a_value={a.value}/b_value={b.value}",
            Collection(element=Struct(fields={"a": Int8(), "b": Int8()}), partition_by=("a", "b")),
        ),
        (
            "/tmp/test/{tag}/{partition_key_spec}",
            "/tmp/test/{tag}/a_value={a.value}",
            Collection(element=Struct(fields={"a": Int8()}), partition_by=("a",)),
        ),
    ],
)
def test_Storage_visit_type(spec: str, expected: str, type: Type) -> None:
    assert MockStorage(path=spec)._visit_type(type).path == expected


def test_Storage_key_types() -> None:
    assert MockStorage(path="test")._visit_type(
        Collection(element=Struct(fields={"a": Int8()}), partition_by=("a",))
    ).key_types == PartitionKeyTypes(a=Int8Field)
    assert MockStorage(path="test")._visit_type(Int8()).key_types == PartitionKeyTypes()
    with pytest.raises(ValueError, match="`key_types` have not been set yet."):
        assert MockStorage(path="test").key_types


def test_Storage_vist_type_extra() -> None:
    class TablePartition(StoragePartition):
        dataset: str
        name: str

        def compute_content_fingerprint(self) -> Fingerprint:
            return Fingerprint.from_string("test")

    class Table(Storage[TablePartition]):
        # NOTE: The type hints are needed to fix https://github.com/pydantic/pydantic/issues/1777#issuecomment-1465026331
        key_value_sep: ClassVar[str] = "_"
        partition_name_component_sep: ClassVar[str] = "_"
        segment_sep: ClassVar[str] = "__"

        dataset: str = "s_{tag}"
        name: str = "{partition_key_spec}"

        def discover_partitions(
            self, input_fingerprints: InputFingerprints = InputFingerprints()
        ) -> StoragePartitionSnapshots:
            return ()

    type = Collection(element=Struct(fields={"a": Int8(), "b": Int8()}), partition_by=("a", "b"))
    t = Table()._visit_type(type)
    assert t.dataset == "s_{tag}"
    assert t.name == "a_value_{a.value}__b_value_{b.value}"


def test_Storage_discover_partitions() -> None:
    s = (
        MockStorage(path="/test/{i.value}/file")
        ._visit_type(Collection(element=Struct(fields={"i": Int8()}), partition_by=("i",)))
        ._visit_format(DummyFormat())
    )
    partitions = s.discover_partitions()
    for i, sps in enumerate(
        sorted(partitions, key=lambda x: cast(MockStoragePartition, x.storage_partition).path)
    ):
        sp = sps.storage_partition
        assert isinstance(sp, MockStoragePartition)
        assert sp.path == f"/test/{i}/file"
        assert isinstance(sp.partition_key["i"], Int8Field)
        assert sp.partition_key["i"].value == i


def test_Storage_generate_partition() -> None:
    partition_key = PartitionKey(i=Int8Field(value=5))
    input_fingerprint = Fingerprint.from_int(10)
    s = (
        MockStorage(path="{i.value:02}/{input_fingerprint}")
        ._visit_type(Collection(element=Struct(fields={"i": Int8()}), partition_by=("i",)))
        ._visit_format(DummyFormat())
    )
    expected_partition = MockStoragePartition(
        input_fingerprint=input_fingerprint, partition_key=partition_key, path="05/10", storage=s
    )

    output = s.generate_partition(partition_key=partition_key, input_fingerprint=input_fingerprint)
    assert output == expected_partition

    output_snapshot = s.generate_partition(
        input_fingerprint=input_fingerprint, partition_key=partition_key
    ).snapshot()
    assert output_snapshot == expected_partition.snapshot()

    output = s.generate_partition(input_fingerprint=input_fingerprint, partition_key=partition_key)
    assert output == expected_partition

    # Check behavior when the Storage spec doesn't align with the passed in key/fingerprint. We'll
    # probably want nicer error messages for these eventually.
    with pytest.raises(KeyError, match="i"):
        s.generate_partition(
            partition_key=PartitionKey(j=Int8Field(value=5)), input_fingerprint=input_fingerprint
        )
    with pytest.raises(ValueError, match="requires an input_fingerprint, but none was provided"):
        s.generate_partition(partition_key=partition_key, input_fingerprint=None)

    with pytest.raises(ValueError, match="Expected no partition key but got:"):
        MockStorage(path="hard coded")._visit_type(Int8()).generate_partition(
            partition_key=partition_key
        )
    with pytest.raises(
        ValueError, match=re.escape("Expected partition key with ('i',) but none were passed")
    ):
        MockStorage(path="hard coded")._visit_type(
            Collection(element=Struct(fields={"i": Int8()}), partition_by=("i",))
        ).generate_partition()
    with pytest.raises(ValueError, match="does not specify a {input_fingerprint} template"):
        MockStorage(path="hard coded")._visit_type(Int8()).generate_partition(
            input_fingerprint=Fingerprint.from_string("fake")
        )
