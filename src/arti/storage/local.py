from __future__ import annotations

import hashlib
import tempfile
from glob import glob
from pathlib import Path

from arti.fingerprints import Fingerprint
from arti.partitions import CompositeKeyTypes
from arti.storage import InputFingerprints, Storage, StoragePartition
from arti.storage._internal import parse_spec, spec_to_wildcard


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
    path: str = str(
        Path(tempfile.gettempdir())
        / "{graph_name}"
        / "{path_tags}"
        / "{names}"
        / "{partition_key_spec}"
        / "{input_fingerprint}"
        / "{name}.{extension}"
    )

    def discover_partitions(
        self,
        key_types: CompositeKeyTypes,
        input_fingerprints: InputFingerprints = InputFingerprints(),
    ) -> tuple[LocalFilePartition, ...]:
        wildcard = spec_to_wildcard(self.path, key_types)
        paths = set(glob(wildcard))
        path_metadata = parse_spec(
            paths, spec=self.path, key_types=key_types, input_fingerprints=input_fingerprints
        )
        return tuple(
            self.storage_partition_type(
                path=path,
                keys=keys,
            )
            for path, (input_fingerprint, keys) in path_metadata.items()
        )
