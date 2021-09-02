import hashlib
from collections.abc import Mapping
from glob import glob

from arti.fingerprints import Fingerprint
from arti.partitions import CompositeKey, PartitionKey
from arti.storage import Storage, StoragePartition
from arti.storage._internal import parse_partition_keys, spec_to_wildcard


class LocalFilePartition(StoragePartition):
    path: str

    def compute_fingerprint(self, buffer_size: int = 1024 * 1024) -> Fingerprint:
        with open(self.path, mode="rb") as f:
            sha = hashlib.sha1()
            data = f.read(buffer_size)
            while len(data) > 0:
                sha.update(data)
                data = f.read(buffer_size)
        return Fingerprint.from_string(sha.hexdigest())


class LocalFile(Storage[LocalFilePartition]):
    path: str

    def discover_partition_keys(
        self, **key_types: type[PartitionKey]
    ) -> Mapping[str, CompositeKey]:
        wildcard = spec_to_wildcard(self.path, key_types)
        paths = set(glob(wildcard))
        return parse_partition_keys(paths, spec=self.path, key_types=key_types)
