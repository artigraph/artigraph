from collections.abc import Mapping
from typing import Any, get_args

import pytest

from arti.fingerprints import Fingerprint
from arti.internal.type_hints import lenient_issubclass
from arti.partitions import CompositeKey, IntKey, PartitionKey
from arti.storage import Storage, StoragePartition


class MockStoragePartition(StoragePartition):
    path: str

    def compute_fingerprint(self) -> Fingerprint:
        return Fingerprint.from_string(self.path)


class MockStorage(Storage[MockStoragePartition]):
    path: str

    def discover_partition_keys(
        self, **key_types: type[PartitionKey]
    ) -> Mapping[str, CompositeKey]:
        assert all(v is IntKey for v in key_types.values())  # Simplifies logic here...
        return {
            self.path.format(**keys): keys
            for keys in ({k: IntKey(key=i) for k in key_types} for i in range(3))
        }


def test_StoragePartition_fingerprint() -> None:
    sp = MockStoragePartition(path="/tmp/test", partition_key={"key": IntKey(key=5)})
    assert sp.fingerprint is None
    modified = sp.with_fingerprint()
    assert sp.fingerprint is None
    assert modified.fingerprint == Fingerprint.from_string(sp.path)


def test_Storage_class_getitem() -> None:
    assert Storage[Any] is Storage

    s = Storage[MockStoragePartition]
    assert lenient_issubclass(s, Storage)
    assert s._abstract_
    assert s.storage_partition_type is MockStoragePartition  # type: ignore
    assert get_args(s) == (MockStoragePartition,)


def test_Storage_init_subclass() -> None:
    with pytest.raises(TypeError, match="Bad fields must match MockStoragePartition"):

        class Bad(Storage[MockStoragePartition]):
            pass

    class S(Storage[MockStoragePartition]):
        path: str

    assert S.storage_partition_type is MockStoragePartition  # type: ignore


def test_Storage_discover_partitions() -> None:
    s = MockStorage(path="/test/{i.key}/file")
    partitions = s.discover_partitions(i=IntKey)
    for i, sp in enumerate(sorted(partitions, key=lambda x: x.path)):
        assert sp.path == f"/test/{i}/file"
        assert isinstance(sp.partition_key["i"], IntKey)
        assert sp.partition_key["i"].key == i
