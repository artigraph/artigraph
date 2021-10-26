from __future__ import annotations

from typing import Optional

import pytest

from arti.fingerprints import Fingerprint
from arti.partitions import CompositeKeyTypes, DateKey, Int8Key
from arti.storage import InputFingerprints, Storage, StoragePartition


class MockStoragePartition(StoragePartition):
    path: str

    def compute_fingerprint(self) -> Fingerprint:
        return Fingerprint.from_string(self.path)


class MockStorage(Storage[MockStoragePartition]):
    # TODO: Add default
    path: str

    def discover_partitions(
        self, key_types: CompositeKeyTypes, input_fingerprints: Optional[InputFingerprints] = None
    ) -> tuple[MockStoragePartition, ...]:
        assert all(v is Int8Key for v in key_types.values())  # Simplifies logic here...
        return tuple(
            self.storage_partition_type(path=self.path.format(**keys), keys=keys)
            for keys in ({k: Int8Key(key=i) for k in key_types} for i in range(3))
        )


def test_StoragePartition_fingerprint() -> None:
    sp = MockStoragePartition(path="/tmp/test", keys={"key": Int8Key(key=5)})
    assert sp.fingerprint == Fingerprint.empty()
    populated = sp.with_fingerprint()
    assert sp.fingerprint == Fingerprint.empty()
    assert populated.fingerprint == Fingerprint.from_string(sp.path)
    assert populated.with_fingerprint(keep_existing=True) is populated
    assert populated.with_fingerprint(keep_existing=False) == populated
    assert populated.with_fingerprint(keep_existing=False) is not populated


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
            "/tmp/test/date_Y={date.Y}/date_m={date.m}/date_d={date.d}",
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

        def compute_fingerprint(self) -> Fingerprint:
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


def test_Storage_discover_partitions() -> None:
    s = MockStorage(path="/test/{i.key}/file")
    partitions = s.discover_partitions(CompositeKeyTypes(i=Int8Key))
    for i, sp in enumerate(sorted(partitions, key=lambda x: x.path)):
        assert sp.path == f"/test/{i}/file"
        assert isinstance(sp.keys["i"], Int8Key)
        assert sp.keys["i"].key == i
