from __future__ import annotations

from pathlib import Path

from gcsfs import GCSFileSystem

from arti import Fingerprint, InputFingerprints, Storage, StoragePartition
from arti.internal.models import Model
from arti.storage._internal import parse_spec, spec_to_wildcard


class _GCSMixin(Model):
    bucket: str
    path: str

    @property
    def qualified_path(self) -> str:
        return f"{self.bucket}/{self.path}"


class GCSFilePartition(_GCSMixin, StoragePartition):
    def compute_content_fingerprint(self) -> Fingerprint:
        # TODO: GCSFileSystem needs to be injected somehow
        info = GCSFileSystem().info(f"{self.bucket}/{self.path}")
        # Prefer md5Hash if available
        return Fingerprint.from_string(info["md5Hash"] if "md5Hash" in info else info["crc32c"])


class GCSFile(_GCSMixin, Storage[GCSFilePartition]):
    path = str(
        Path("{graph_name}")
        / "{path_tags}"
        / "{names}"
        / "{partition_key_spec}"
        / "{input_fingerprint}"
        / "{name}.{extension}"
    )

    def discover_partitions(
        self, input_fingerprints: InputFingerprints = InputFingerprints()
    ) -> tuple[GCSFilePartition, ...]:
        # NOTE: The bucket/path must *already* have any graph tags resolved, otherwise they will be try to be parsed as
        # partition keys.
        spec = f"{self.bucket}/{self.path}"  # type: ignore[operator] # likely some pydantic.mypy bug
        wildcard = spec_to_wildcard(spec, self.key_types)
        # TODO: GCSFileSystem needs to be injected somehow
        paths = GCSFileSystem().glob(wildcard)
        path_metadata = parse_spec(
            paths, spec=spec, key_types=self.key_types, input_fingerprints=input_fingerprints
        )
        return tuple(
            self.generate_partition(input_fingerprint=input_fingerprint, keys=keys)
            for (bucket, path), (input_fingerprint, keys) in {
                tuple(path.split("/", 1)): metadata for path, metadata in path_metadata.items()
            }.items()
        )
