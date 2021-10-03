__path__ = __import__("pkgutil").extend_path(__path__, __name__)  # type: ignore

import abc
import os
from typing import Any, ClassVar, Generic, Optional, TypeVar

from arti.fingerprints import Fingerprint
from arti.formats import Format
from arti.internal.models import Model
from arti.internal.type_hints import get_class_type_vars, lenient_issubclass
from arti.partitions import CompositeKey, PartitionKey
from arti.storage._internal import partial_format
from arti.types import Type


class StoragePartition(Model):
    keys: CompositeKey
    fingerprint: Optional[Fingerprint] = None

    def with_fingerprint(self) -> "StoragePartition":
        return self.copy(update={"fingerprint": self.compute_fingerprint()})

    @abc.abstractmethod
    def compute_fingerprint(self) -> Fingerprint:
        raise NotImplementedError("{type(self).__name__}.compute_fingerprint is not implemented!")


_StoragePartition = TypeVar("_StoragePartition", bound=StoragePartition)
_Storage = TypeVar("_Storage", bound="Storage[Any]")


class Storage(Model, Generic[_StoragePartition]):
    """Storage is a data reference identifying 1 or more partitions of data.

    Storage fields should have defaults set with placeholders for tags and partition
    keys. This allows automatic injection of the tags and partition keys for simple
    cases.
    """

    _abstract_ = True

    # These separators are used in the default resolve_* helpers to format metadata into
    # the storage fields.
    #
    # The defaults are tailored for "path"-like fields.
    key_value_sep: ClassVar[str] = "="
    partition_name_component_sep: ClassVar[str] = "_"
    segment_sep: ClassVar[str] = os.sep

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

    def resolve_partition_key_spec(self: _Storage, **key_types: type[PartitionKey]) -> _Storage:
        key_component_specs = {
            f"{name}{self.partition_name_component_sep}{key_component}": (
                "{" + f"{name}.{key_component}" + "}"
            )
            for name, pk in key_types.items()
            for key_component in pk.default_key_components
        }
        return self.copy(
            update={
                name: partial_format(
                    getattr(self, name),
                    partition_key_spec=self.segment_sep.join(
                        f"{name}{self.key_value_sep}{spec}"
                        for name, spec in key_component_specs.items()
                    ),
                )
                for name, field in self.__fields__.items()
                if lenient_issubclass(field.outer_type_, str)
            }
        )

    @abc.abstractmethod
    def discover_partitions(self, **key_types: type[PartitionKey]) -> tuple[_StoragePartition, ...]:
        raise NotImplementedError()
