from __future__ import annotations

import abc

__path__ = __import__("pkgutil").extend_path(__path__, __name__)  # type: ignore

from arti.formats import Format
from arti.internal.models import Model
from arti.partitions import PartitionKey
from arti.types import Type


class Storage(Model):
    _abstract_ = True

    @abc.abstractmethod
    def discover_partitions(
        self, **key_types: type[PartitionKey]
    ) -> tuple[dict[str, PartitionKey], ...]:
        raise NotImplementedError()

    def supports(self, type_: Type, format: Format) -> None:
        # TODO: Ensure the storage supports all of the specified types and partitioning on the
        # specified field(s).
        pass
