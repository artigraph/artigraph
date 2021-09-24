from __future__ import annotations

__path__ = __import__("pkgutil").extend_path(__path__, __name__)  # type: ignore

import abc
from typing import Any, ClassVar, Generic, Optional, TypeVar

from arti.fingerprints import Fingerprint
from arti.formats import Format
from arti.internal.models import Model
from arti.internal.type_hints import get_class_type_vars
from arti.partitions import CompositeKey, PartitionKey
from arti.types import Type


class StoragePartition(Model):
    keys: CompositeKey
    fingerprint: Optional[Fingerprint] = None

    def with_fingerprint(self) -> StoragePartition:
        return self.copy(update={"fingerprint": self.compute_fingerprint()})

    @abc.abstractmethod
    def compute_fingerprint(self) -> Fingerprint:
        raise NotImplementedError("{type(self).__name__}.compute_fingerprint is not implemented!")


_StoragePartition = TypeVar("_StoragePartition", bound=StoragePartition)


class Storage(Model, Generic[_StoragePartition]):
    _abstract_ = True
    storage_partition_type: ClassVar[type[_StoragePartition]]

    @classmethod
    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if cls._abstract_:
            return
        cls.storage_partition_type = get_class_type_vars(cls)[0]
        expected_field_types = {
            name: info.outer_type_
            for name, info in cls.storage_partition_type.__fields__.items()
            if name not in StoragePartition.__fields__
        }
        fields = {
            name: info.outer_type_
            for name, info in cls.__fields__.items()
            if name not in Storage.__fields__
        }
        if fields != expected_field_types:
            raise TypeError(
                f"{cls.__name__} fields must match {cls.storage_partition_type.__name__} ({expected_field_types}), got: {fields}"
            )

    def supports(self, type_: Type, format: Format) -> None:
        # TODO: Ensure the storage supports all of the specified types and partitioning on the
        # specified field(s).
        pass

    @abc.abstractmethod
    def discover_partitions(self, **key_types: type[PartitionKey]) -> tuple[_StoragePartition, ...]:
        raise NotImplementedError()
