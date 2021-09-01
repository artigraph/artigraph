from glob import glob

from arti.partitions import PartitionKey
from arti.storage import Storage
from arti.storage._internal import parse_partition_keys, spec_to_wildcard


class LocalFile(Storage):
    path: str

    def discover_partitions(
        self, **key_types: type[PartitionKey]
    ) -> tuple[dict[str, PartitionKey], ...]:
        wildcard = spec_to_wildcard(self.path, key_types)
        paths = glob(wildcard)
        return parse_partition_keys(paths, spec=self.path, key_types=key_types)
