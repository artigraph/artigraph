from __future__ import annotations

__path__ = __import__("pkgutil").extend_path(__path__, __name__)  # type: ignore

import abc
from collections.abc import Mapping
from types import GenericAlias
from typing import Any, ClassVar, Generic, Optional, TypeVar

from arti.fingerprints import Fingerprint
from arti.formats import Format
from arti.internal.models import Model
from arti.partitions import CompositeKey, PartitionKey
from arti.types import Type


class StoragePartition(Model):
    partition_key: CompositeKey
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

    def __class_getitem__(cls, item: type[_StoragePartition]) -> GenericAlias:
        # Artifact.storage is marked as Storage[Any] for simplicity
        if item is Any:
            return cls  # type: ignore
        return GenericAlias(
            type(
                f"{item.__name__}Owner",
                (cls,),
                # Mark _abstract_ to require subclassing the subscripted type in order to set the
                # appropriate fields matching the StoragePartition (which __init_subclass__ will
                # then check).
                {"_abstract_": True, "storage_partition_type": item},
            ),
            item,
        )

    @classmethod
    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if cls._abstract_:
            return
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

    def discover_partitions(self, **key_types: type[PartitionKey]) -> tuple[_StoragePartition, ...]:
        return tuple(
            self.storage_partition_type(
                path=path,
                partition_key=partition_key,
            )
            for path, partition_key in self.discover_partition_keys(**key_types).items()
        )

    @abc.abstractmethod
    def discover_partition_keys(
        self, **key_types: type[PartitionKey]
    ) -> Mapping[str, CompositeKey]:
        raise NotImplementedError()
