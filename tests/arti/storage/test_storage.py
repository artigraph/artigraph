from __future__ import annotations

import re
from typing import ClassVar

import pytest

from arti import (
    CompositeKey,
    CompositeKeyTypes,
    Fingerprint,
    Graph,
    InputFingerprints,
    Storage,
    StoragePartition,
    Type,
)
from arti.formats.json import JSON
from arti.internal.utils import frozendict
from arti.partitions import Int8Key
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
    class Abstract(Storage):  # type: ignore[type-arg]
        _abstract_ = True

    assert not hasattr(Abstract, "storage_partition_type")

    with pytest.raises(TypeError, match="NoSubscript must subclass a subscripted Generic"):

        class NoSubscript(Storage):  # type: ignore[type-arg]
            pass

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
    assert s._visit_graph(Graph(name="test", path_tags=frozendict(a="b"))) == MockStorage(
        path="/test/a=b/junk"
    )


def test_Storage_visit_input_fingerprint() -> None:
    s = MockStorage(path="/{input_fingerprint}/junk")
    assert s._visit_input_fingerprint(Fingerprint.from_int(10)) == MockStorage(path="/10/junk")
    assert s._visit_input_fingerprint(Fingerprint.empty()) == MockStorage(path="/junk")


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
            "/tmp/test/a_key={a.key}/b_key={b.key}",
            Collection(element=Struct(fields={"a": Int8(), "b": Int8()}), partition_by=("a", "b")),
        ),
        (
            "/tmp/test/{tag}/{partition_key_spec}",
            "/tmp/test/{tag}/a_key={a.key}",
            Collection(element=Struct(fields={"a": Int8()}), partition_by=("a",)),
        ),
    ],
)
def test_Storage_visit_type(spec: str, expected: str, type: Type) -> None:
    assert MockStorage(path=spec)._visit_type(type).path == expected


def test_Storage_key_types() -> None:
    assert MockStorage(path="test")._visit_type(
        Collection(element=Struct(fields={"a": Int8()}), partition_by=("a",))
    ).key_types == CompositeKeyTypes(a=Int8Key)
    assert MockStorage(path="test")._visit_type(Int8()).key_types == CompositeKeyTypes()
    with pytest.raises(ValueError, match="`key_types` have not been set yet."):
        assert MockStorage(path="test").key_types


def test_Storage_vist_type_extra() -> None:
    class TablePartition(StoragePartition):
        dataset: str
        name: str

        def compute_content_fingerprint(self) -> Fingerprint:
            return Fingerprint.empty()

    class Table(Storage[TablePartition]):
        # NOTE: The type hints are needed to fix https://github.com/pydantic/pydantic/issues/1777#issuecomment-1465026331
        key_value_sep: ClassVar[str] = "_"
        partition_name_component_sep: ClassVar[str] = "_"
        segment_sep: ClassVar[str] = "__"

        dataset: str = "s_{tag}"
        name: str = "{partition_key_spec}"

        def discover_partitions(
            self, input_fingerprints: InputFingerprints = InputFingerprints()
        ) -> tuple[TablePartition, ...]:
            return ()

    type_ = Collection(element=Struct(fields={"a": Int8(), "b": Int8()}), partition_by=("a", "b"))
    t = Table()._visit_type(type_)
    assert t.dataset == "s_{tag}"
    assert t.name == "a_key_{a.key}__b_key_{b.key}"


def test_Storage_discover_partitions() -> None:
    s = (
        MockStorage(path="/test/{i.key}/file")
        ._visit_type(Collection(element=Struct(fields={"i": Int8()}), partition_by=("i",)))
        ._visit_format(DummyFormat())
    )
    partitions = s.discover_partitions()
    for i, sp in enumerate(sorted(partitions, key=lambda x: x.path)):
        assert sp.path == f"/test/{i}/file"
        assert isinstance(sp.keys["i"], Int8Key)
        assert sp.keys["i"].key == i


def test_Storage_generate_partition() -> None:
    keys = CompositeKey(i=Int8Key(key=5))
    input_fingerprint = Fingerprint.from_int(10)
    s = (
        MockStorage(path="{i.key:02}/{input_fingerprint}")
        ._visit_type(Collection(element=Struct(fields={"i": Int8()}), partition_by=("i",)))
        ._visit_format(DummyFormat())
    )
    expected_partition = MockStoragePartition(
        input_fingerprint=input_fingerprint, keys=keys, path="05/10"
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

    with pytest.raises(ValueError, match="Expected no partition keys but got:"):
        MockStorage(path="hard coded")._visit_type(Int8()).generate_partition(keys=keys)
    with pytest.raises(
        ValueError, match=re.escape("Expected partition keys ('i',) but none were passed")
    ):
        MockStorage(path="hard coded")._visit_type(
            Collection(element=Struct(fields={"i": Int8()}), partition_by=("i",))
        ).generate_partition()
    with pytest.raises(ValueError, match="does not specify a {input_fingerprint} template"):
        MockStorage(path="hard coded")._visit_type(Int8()).generate_partition(
            input_fingerprint=Fingerprint.from_string("fake")
        )
