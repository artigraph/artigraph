from __future__ import annotations

import hashlib
import os
import tempfile
from glob import glob

from arti.fingerprints import Fingerprint
from arti.partitions import PartitionKey
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
    path: str = os.sep.join([tempfile.gettempdir(), "{partition_key_spec}"])

    def discover_partitions(
        self, **key_types: type[PartitionKey]
    ) -> tuple[LocalFilePartition, ...]:
        wildcard = spec_to_wildcard(self.path, key_types)
        paths = set(glob(wildcard))
        path_keys = parse_partition_keys(paths, spec=self.path, key_types=key_types)
        return tuple(
            self.storage_partition_type(
                path=path,
                keys=keys,
            )
            for path, keys in path_keys.items()
        )
