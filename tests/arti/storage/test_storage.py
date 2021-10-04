import pytest

from arti.fingerprints import Fingerprint
from arti.partitions import Int8Key, PartitionKey
from arti.storage import Storage, StoragePartition


class MockStoragePartition(StoragePartition):
    path: str

    def compute_fingerprint(self) -> Fingerprint:
        return Fingerprint.from_string(self.path)


class MockStorage(Storage[MockStoragePartition]):
    path: str

    def discover_partitions(
        self, **key_types: type[PartitionKey]
    ) -> tuple[MockStoragePartition, ...]:
        assert all(v is Int8Key for v in key_types.values())  # Simplifies logic here...
        return tuple(
            self.storage_partition_type(path=self.path.format(**keys), keys=keys)
            for keys in ({k: Int8Key(key=i) for k in key_types} for i in range(3))
        )


def test_StoragePartition_fingerprint() -> None:
    sp = MockStoragePartition(path="/tmp/test", keys={"key": Int8Key(key=5)})
    assert sp.fingerprint is None
    modified = sp.with_fingerprint()
    assert sp.fingerprint is None
    assert modified.fingerprint == Fingerprint.from_string(sp.path)


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


def test_Storage_discover_partitions() -> None:
    s = MockStorage(path="/test/{i.key}/file")
    partitions = s.discover_partitions(i=Int8Key)
    for i, sp in enumerate(sorted(partitions, key=lambda x: x.path)):
        assert sp.path == f"/test/{i}/file"
        assert isinstance(sp.keys["i"], Int8Key)
        assert sp.keys["i"].key == i