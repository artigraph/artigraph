__path__ = __import__("pkgutil").extend_path(__path__, __name__)  # type: ignore

import abc
import os
from typing import Any, ClassVar, Generic, Optional, TypeVar

from arti.fingerprints import Fingerprint
from arti.formats import Format
from arti.internal.models import Model
from arti.internal.type_hints import get_class_type_vars, lenient_issubclass
from arti.internal.utils import frozendict
from arti.partitions import CompositeKey, CompositeKeyTypes
from arti.storage._internal import InputFingerprints as InputFingerprints
from arti.storage._internal import partial_format, strip_partition_indexes
from arti.types import Type

_StoragePartition = TypeVar("_StoragePartition", bound="StoragePartition")


class StoragePartition(Model):
    keys: CompositeKey
    fingerprint: Fingerprint = Fingerprint.empty()

    def with_fingerprint(self: _StoragePartition, keep_existing: bool = True) -> _StoragePartition:
        if keep_existing and not self.fingerprint.is_empty:
            return self
        return self.copy(update={"fingerprint": self.compute_fingerprint()})

    @abc.abstractmethod
    def compute_fingerprint(self) -> Fingerprint:
        raise NotImplementedError("{type(self).__name__}.compute_fingerprint is not implemented!")


StoragePartitions = tuple[StoragePartition, ...]  # type: ignore

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

    @abc.abstractmethod
    def discover_partitions(
        self,
        key_types: CompositeKeyTypes,
        input_fingerprints: InputFingerprints = InputFingerprints(),
    ) -> tuple[_StoragePartition, ...]:
        raise NotImplementedError()

    def generate_partition(
        self,
        keys: CompositeKey,
        input_fingerprint: Fingerprint,
        with_fingerprint: bool = True,
    ) -> _StoragePartition:
        kwargs = dict[Any, Any](keys)
        if not input_fingerprint.is_empty:
            kwargs["input_fingerprint"] = str(input_fingerprint.key)
        field_values = {
            name: (
                strip_partition_indexes(original).format(**kwargs)
                if lenient_issubclass(type(original := getattr(self, name)), str)
                else original
            )
            for name in self.__fields__
            if name in self.storage_partition_type.__fields__
        }
        partition = self.storage_partition_type(keys=keys, **field_values)
        if with_fingerprint:
            partition = partition.with_fingerprint()
        return partition

    def _resolve_field(self, spec: str, placeholder_values: dict[str, str]) -> str:
        for placeholder, value in placeholder_values.items():
            if not value:
                # Strip placeholder *and* any trailing self.segment_sep
                trim = "{" + placeholder + "}"
                if f"{trim}{self.segment_sep}" in spec:
                    trim = f"{trim}{self.segment_sep}"
                spec = spec.replace(trim, "")
        return partial_format(spec, **placeholder_values)

    def resolve(self: _Storage, **placeholder_values: str) -> _Storage:
        return self.copy(
            update={
                name: updated_value
                for name in self.__fields__
                if lenient_issubclass(type(original := getattr(self, name)), str)
                # Avoid "setting" the value if not updated to reduce pydantic repr verbosity (which
                # only shows "set" fields by default).
                and (updated_value := self._resolve_field(original, placeholder_values)) != original
            }
        )

    def resolve_extension(self: _Storage, extension: Optional[str]) -> _Storage:
        if extension is None:
            return self
        return self.resolve(extension=extension)

    def resolve_graph_name(self: _Storage, graph_name: str) -> _Storage:
        return self.resolve(graph_name=graph_name)

    def resolve_names(self: _Storage, names: tuple[str, ...]) -> _Storage:
        return self.resolve(names=self.segment_sep.join(names), name=names[-1])

    def resolve_partition_key_spec(self: _Storage, key_types: CompositeKeyTypes) -> _Storage:
        key_component_specs = {
            f"{name}{self.partition_name_component_sep}{key_component}": (
                "{" + f"{name}.{key_component}" + "}"
            )
            for name, pk in key_types.items()
            for key_component in pk.default_key_components
        }
        return self.resolve(
            partition_key_spec=self.segment_sep.join(
                f"{name}{self.key_value_sep}{spec}" for name, spec in key_component_specs.items()
            )
        )

    def resolve_path_tags(self: _Storage, path_tags: frozendict[str, str]) -> _Storage:
        return self.resolve(
            path_tags=self.segment_sep.join(
                f"{tag}{self.key_value_sep}{value}" for tag, value in path_tags.items()
            )
        )

    def supports(self: _Storage, type_: Type, format: Format) -> None:
        # TODO: Ensure the storage supports all of the specified types and partitioning on the
        # specified field(s).
        pass
