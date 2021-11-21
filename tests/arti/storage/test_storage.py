from __future__ import annotations

from typing import Optional

import pytest

from arti.fingerprints import Fingerprint
from arti.internal.utils import frozendict
from arti.partitions import CompositeKey, CompositeKeyTypes, DateKey, Int8Key
from arti.storage import InputFingerprints, Storage, StoragePartition


class MockStoragePartition(StoragePartition):
    path: str

    def compute_content_fingerprint(self) -> Fingerprint:
        return Fingerprint.from_string(self.path)


class MockStorage(Storage[MockStoragePartition]):
    path: str

    def discover_partitions(
        self, key_types: CompositeKeyTypes, input_fingerprints: Optional[InputFingerprints] = None
    ) -> tuple[MockStoragePartition, ...]:
        assert all(v is Int8Key for v in key_types.values())  # Simplifies logic here...
        return tuple(
            self.storage_partition_type(path=self.path.format(**keys), keys=keys)
            for keys in ({k: Int8Key(key=i) for k in key_types} for i in range(3))
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


def test_Storage_resolve_extension() -> None:
    s = MockStorage(path="{extension}")
    assert s.resolve_extension("test") == MockStorage(path="test")
    assert s.resolve_extension(None) == MockStorage(path="")


def test_Storage_resolve_graph_name() -> None:
    s = MockStorage(path="/{graph_name}/junk")
    assert s.resolve_graph_name("test") == MockStorage(path="/test/junk")
    # Confirm we strip trailing (ie: dup) separators when resolving an empty placeholder.
    assert s.resolve_graph_name("") == MockStorage(path="/junk")


def test_Storage_resolve_names() -> None:
    s = MockStorage(path="/{names}")
    assert s.resolve_names(("a", "b")) == MockStorage(path="/a/b")
    assert s.resolve_names(()) == MockStorage(path="/")

    s = MockStorage(path="/{names}/junk/{name}")
    assert s.resolve_names(("a", "b")) == MockStorage(path="/a/b/junk/b")
    assert s.resolve_names(()) == MockStorage(path="/junk/")


@pytest.mark.parametrize(
    ("spec", "expected", "key_types"),
    (
        (
            "/tmp/test/{partition_key_spec}",
            "/tmp/test/",
            {},
        ),
        (
            "/tmp/test/{partition_key_spec}",
            "/tmp/test/date_Y={date.Y}/date_m={date.m:02}/date_d={date.d:02}",
            {"date": DateKey},
        ),
        (
            "/tmp/test/{partition_key_spec}",
            "/tmp/test/a_key={a.key}/b_key={b.key}",
            {"a": Int8Key, "b": Int8Key},
        ),
        (
            "/tmp/test/{tag}/{partition_key_spec}",
            "/tmp/test/{tag}/a_key={a.key}",
            {"a": Int8Key},
        ),
    ),
)
def test_Storage_resolve_partition_key_spec(
    spec: str, expected: str, key_types: CompositeKeyTypes
) -> None:
    assert MockStorage(path=spec).resolve_partition_key_spec(key_types).path == expected


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
            self,
            key_types: CompositeKeyTypes,
            input_fingerprints: Optional[InputFingerprints] = None,
        ) -> tuple[TablePartition, ...]:
            return ()

    t = Table().resolve_partition_key_spec(CompositeKeyTypes(a=Int8Key, b=Int8Key))
    assert t.dataset == "s_{tag}"
    assert t.name == "a_key_{a.key}__b_key_{b.key}"


def test_Storage_resolve_path_tags() -> None:
    s = MockStorage(path="{path_tags}")
    assert s.resolve_path_tags(frozendict(a="b")) == MockStorage(path="a=b")
    assert s.resolve_path_tags(frozendict()) == MockStorage(path="")


def test_Storage_discover_partitions() -> None:
    s = MockStorage(path="/test/{i.key}/file")
    partitions = s.discover_partitions(CompositeKeyTypes(i=Int8Key))
    for i, sp in enumerate(sorted(partitions, key=lambda x: x.path)):
        assert sp.path == f"/test/{i}/file"
        assert isinstance(sp.keys["i"], Int8Key)
        assert sp.keys["i"].key == i


def test_Storage_generate_partition() -> None:
    keys = CompositeKey(i=Int8Key(key=5))
    input_fingerprint = Fingerprint.from_int(10)
    s = MockStorage(path="{i.key:02}/{input_fingerprint}")
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
    with pytest.raises(KeyError, match="input_fingerprint"):  # empty fingerprints aren't filled in
        s.generate_partition(keys=keys, input_fingerprint=Fingerprint.empty())
