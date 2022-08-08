from __future__ import annotations

import hashlib
import tempfile
from glob import glob
from pathlib import Path
from typing import Optional, Union

from arti.fingerprints import Fingerprint
from arti.partitions import InputFingerprints
from arti.storage import Storage, StoragePartition
from arti.storage._internal import parse_spec, spec_to_wildcard


class LocalFilePartition(StoragePartition):
    path: str

    def compute_content_fingerprint(self, buffer_size: int = 1024 * 1024) -> Fingerprint:
        with open(self.path, mode="rb") as f:
            sha = hashlib.sha256()
            data = f.read(buffer_size)
            while len(data) > 0:
                sha.update(data)
                data = f.read(buffer_size)
        return Fingerprint.from_string(sha.hexdigest())


class LocalFile(Storage[LocalFilePartition]):
    # `_DEFAULT_PATH_TEMPLATE` and `rooted_at` ease testing, where we often want to just override
    # the tempdir, but keep the rest of the template. Eventually, we should introduce Resources and
    # implement a MockFS (to be used in `io.*`).
    _DEFAULT_PATH_TEMPLATE = str(
        Path("{graph_name}")
        / "{path_tags}"
        / "{names}"
        / "{partition_key_spec}"
        / "{input_fingerprint}"
        / "{name}{extension}"
    )

    path: str = str(Path(tempfile.gettempdir()) / _DEFAULT_PATH_TEMPLATE)

    def discover_partitions(
        self, input_fingerprints: InputFingerprints = InputFingerprints()
    ) -> tuple[LocalFilePartition, ...]:
        wildcard = spec_to_wildcard(self.path, self.key_types)
        paths = set(glob(wildcard))
        path_metadata = parse_spec(
            paths, spec=self.path, key_types=self.key_types, input_fingerprints=input_fingerprints
        )
        return tuple(
            self.generate_partition(input_fingerprint=input_fingerprint, keys=keys)
            for path, (input_fingerprint, keys) in path_metadata.items()
        )

    @classmethod
    def rooted_at(cls, root: Union[str, Path], path: Optional[str] = None) -> LocalFile:
        path = path if path is not None else cls._DEFAULT_PATH_TEMPLATE
        return cls(path=str(Path(root) / path))
