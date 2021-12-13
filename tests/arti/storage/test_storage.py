from __future__ import annotations

import pytest

from arti import (
    CompositeKey,
    CompositeKeyTypes,
    Fingerprint,
    InputFingerprints,
    Storage,
    StoragePartition,
    Type,
)
from arti.formats.json import JSON
from arti.internal.utils import frozendict
from arti.partitions import Int8Key
from arti.types import Collection, Date, Int8, Struct


class MockStoragePartition(StoragePartition):
    path: str

    def compute_content_fingerprint(self) -> Fingerprint:
        return Fingerprint.from_string(self.path)


class MockStorage(Storage[MockStoragePartition]):
    path: str

    def discover_partitions(
        self, input_fingerprints: InputFingerprints = InputFingerprints()
    ) -> tuple[MockStoragePartition, ...]:
        assert all(v is Int8Key for v in self.key_types.values())  # Simplifies logic here...
        return tuple(
            self.generate_partition(keys=keys)
            for keys in (
                CompositeKey({k: Int8Key(key=i) for k in self.key_types}) for i in range(3)
            )
        )


def test_StoragePartition_content_fingerprint() -> None:
    sp = MockStoragePartition(path="/tmp/test", keys={})
    assert sp.content_fingerprint == Fingerprint.empty()
    populated = sp.with_content_fingerprint()
    assert sp.content_fingerprint == Fingerprint.empty()
    assert populated.content_fingerprint == Fingerprint.from_string(sp.path)
    assert populated.with_content_fingerprint(keep_existing=True) is populated
    assert populated.with_content_fingerprint(keep_existing=False) == populated
    assert populated.with_content_fingerprint(keep_existing=False) is not populated


def test_Storage_init_subclass() -> None:
    class Abstract(Storage):  # type: ignore
        _abstract_ = True

    assert not hasattr(Abstract, "storage_partition_type")

    with pytest.raises(TypeError, match="NoSubscript must subclass a subscripted Generic"):

        class NoSubscript(Storage):  # type: ignore
            pass

    with pytest.raises(TypeError, match="Bad fields must match MockStoragePartition"):

        class Bad(Storage[MockStoragePartition]):
            pass

    class S(Storage[MockStoragePartition]):
        path: str

    assert not hasattr(Storage, "storage_partition_type")
    assert S.storage_partition_type is MockStoragePartition  # type: ignore


def test_Storage_resolve_empty_error() -> None:
    s = MockStorage(path="/{name}")
    with pytest.raises(ValueError, match=".path was empty after removing unused templates"):
        s.resolve_templates(names=())


def test_Storage_resolve_extension() -> None:
    p = "somefile{extension}"
    assert MockStorage(path=p, format=JSON()).resolve_templates().path == "somefile.json"
    assert MockStorage(path=p, format=JSON(extension="")).resolve_templates().path == "somefile"


def test_Storage_resolve_graph_name() -> None:
    s = MockStorage(path="/{graph_name}/junk")
    assert s.resolve_templates(graph_name="test") == MockStorage(path="/test/junk")
    # Confirm we strip trailing (ie: dup) separators when resolving an empty placeholder.
    assert s.resolve_templates(graph_name="") == MockStorage(path="/junk")


def test_Storage_resolve_input_fingerprint() -> None:
    s = MockStorage(path="/{input_fingerprint}/junk")
    assert s.resolve_templates(input_fingerprint=Fingerprint.from_int(10)) == MockStorage(
        path="/10/junk"
    )
    assert s.resolve_templates(input_fingerprint=Fingerprint.empty()) == MockStorage(path="/junk")


def test_Storage_resolve_names() -> None:
    s = MockStorage(path="/{names}")
    assert s.resolve_templates(names=("a", "b")) == MockStorage(path="/a/b")

    s = MockStorage(path="/{names}/junk/{name}")
    assert s.resolve_templates(names=("a", "b")) == MockStorage(path="/a/b/junk/b")
    assert s.resolve_templates(names=()) == MockStorage(path="/junk")


@pytest.mark.parametrize(
    ("spec", "expected", "type"),
    (
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
            "/tmp/test/a_key={a.key}/b_key={b.key}",
            Collection(element=Struct(fields={"a": Int8(), "b": Int8()}), partition_by=("a", "b")),
        ),
        (
            "/tmp/test/{tag}/{partition_key_spec}",
            "/tmp/test/{tag}/a_key={a.key}",
            Collection(element=Struct(fields={"a": Int8()}), partition_by=("a",)),
        ),
    ),
)
def test_Storage_resolve_partition_key_spec(spec: str, expected: str, type: Type) -> None:
    assert MockStorage(path=spec, type=type).resolve_templates().path == expected


def test_Storage_key_types() -> None:
    assert MockStorage(
        path="test", type=Collection(element=Struct(fields={"a": Int8()}), partition_by=("a",))
    ).key_types == CompositeKeyTypes(a=Int8Key)
    assert MockStorage(path="test", type=Int8()).key_types == CompositeKeyTypes()
    with pytest.raises(ValueError, match=".type is not set"):
        assert MockStorage(path="test").key_types


def test_Storage_resolve_partition_key_spec_extra() -> None:
    class TablePartition(StoragePartition):
        dataset: str
        name: str

        def compute_content_fingerprint(self) -> Fingerprint:
            return Fingerprint.empty()

    class Table(Storage[TablePartition]):
        key_value_sep = "_"
        partition_name_component_sep = "_"
        segment_sep = "__"

        dataset: str = "s_{tag}"
        name: str = "{partition_key_spec}"

        def discover_partitions(
            self, input_fingerprints: InputFingerprints = InputFingerprints()
        ) -> tuple[TablePartition, ...]:
            return ()

    type = Collection(element=Struct(fields={"a": Int8(), "b": Int8()}), partition_by=("a", "b"))
    t = Table(type=type).resolve_templates()
    assert t.dataset == "s_{tag}"
    assert t.name == "a_key_{a.key}__b_key_{b.key}"


def test_Storage_resolve_path_tags() -> None:
    s = MockStorage(path="{path_tags}")
    assert s.resolve_templates(path_tags=frozendict(a="b")) == MockStorage(path="a=b")


def test_Storage_discover_partitions() -> None:
    s = MockStorage(
        path="/test/{i.key}/file",
        type=Collection(element=Struct(fields={"i": Int8()}), partition_by=("i",)),
    )
    partitions = s.discover_partitions()
    for i, sp in enumerate(sorted(partitions, key=lambda x: x.path)):
        assert sp.path == f"/test/{i}/file"
        assert isinstance(sp.keys["i"], Int8Key)
        assert sp.keys["i"].key == i


def test_Storage_generate_partition() -> None:
    keys = CompositeKey(i=Int8Key(key=5))
    input_fingerprint = Fingerprint.from_int(10)
    s = MockStorage(
        path="{i.key:02}/{input_fingerprint}",
        type=Collection(element=Struct(fields={"i": Int8()}), partition_by=("i",)),
    )
    expected_partition = MockStoragePartition(
        input_fingerprint=input_fingerprint,
        keys=keys,
        path="05/10",
    )

    output = s.generate_partition(keys=keys, input_fingerprint=input_fingerprint)
    assert output == expected_partition.with_content_fingerprint()

    output = s.generate_partition(
        keys=keys, input_fingerprint=input_fingerprint, with_content_fingerprint=True
    )
    assert output == expected_partition.with_content_fingerprint()

    output = s.generate_partition(
        keys=keys, input_fingerprint=input_fingerprint, with_content_fingerprint=False
    )
    assert output == expected_partition

    # Check behavior when the Storage spec doesn't align with the passed in keys/fingerprint. We'll
    # probably want nicer error messages for these eventually.
    with pytest.raises(KeyError, match="i"):
        s.generate_partition(
            keys=CompositeKey(j=Int8Key(key=5)), input_fingerprint=input_fingerprint
        )
    with pytest.raises(ValueError, match="requires an input_fingerprint, but none was provided"):
        s.generate_partition(keys=keys, input_fingerprint=Fingerprint.empty())

    with pytest.raises(ValueError, match="is not partitioned but keys were passed:"):
        MockStorage(path="hard coded", type=Int8()).generate_partition(keys=keys)
    with pytest.raises(ValueError, match="is partitioned but no keys were passed"):
        MockStorage(
            path="hard coded",
            type=Collection(element=Struct(fields={"i": Int8()}), partition_by=("i",)),
        ).generate_partition()
    with pytest.raises(ValueError, match="does not specify a {input_fingerprint} template"):
        MockStorage(path="hard coded", type=Int8()).generate_partition(
            input_fingerprint=Fingerprint.from_string("fake")
        )
