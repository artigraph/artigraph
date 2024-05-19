from __future__ import annotations

__path__ = __import__("pkgutil").extend_path(__path__, __name__)

import abc
import os
from typing import TYPE_CHECKING, Any, ClassVar, Generic, Self, TypeVar, cast

from pydantic import Field, PrivateAttr

from arti.fingerprints import Fingerprint
from arti.formats import Format
from arti.internal.models import Model
from arti.internal.type_hints import get_class_type_vars, lenient_issubclass
from arti.internal.utils import frozendict
from arti.partitions import InputFingerprints, PartitionKey, PartitionKeyTypes
from arti.storage._internal import partial_format, strip_partition_indexes
from arti.types import Type

if TYPE_CHECKING:
    from arti.graphs import Graph


class StoragePartition(Model):
    input_fingerprint: Fingerprint | None = None
    partition_key: PartitionKey = PartitionKey()
    storage: Storage[StoragePartition] = Field(repr=False)

    @abc.abstractmethod
    def compute_content_fingerprint(self) -> Fingerprint:
        raise NotImplementedError(
            "{type(self).__name__}.compute_content_fingerprint is not implemented!"
        )

    def snapshot(self) -> StoragePartitionSnapshot:
        return StoragePartitionSnapshot(
            content_fingerprint=self.compute_content_fingerprint(), storage_partition=self
        )


StoragePartitions = tuple[StoragePartition, ...]
StoragePartitionVar = TypeVar("StoragePartitionVar", bound=StoragePartition)
StoragePartitionVar_co = TypeVar("StoragePartitionVar_co", bound=StoragePartition, covariant=True)


class StoragePartitionSnapshot(Model):
    content_fingerprint: Fingerprint
    storage_partition: StoragePartition

    @property
    def input_fingerprint(self) -> Fingerprint | None:
        return self.storage_partition.input_fingerprint

    @property
    def partition_key(self) -> PartitionKey:
        return self.storage_partition.partition_key

    @property
    def storage(self) -> Storage[StoragePartition]:
        return self.storage_partition.storage


StoragePartitionSnapshots = tuple[StoragePartitionSnapshot, ...]


class Storage(Model, Generic[StoragePartitionVar_co]):
    """Storage is a data reference identifying 1 or more partitions of data.

    Storage fields should have defaults set with placeholders for tags and partition
    keys. This allows automatic injection of the tags and partition keys for simple
    cases.
    """

    _abstract_ = True
    storage_partition_type: ClassVar[type[StoragePartitionVar_co]]  # type: ignore[misc]

    # These separators are used in the default resolve_* helpers to format metadata into
    # the storage fields.
    #
    # The defaults are tailored for "path"-like fields.
    key_value_sep: ClassVar[str] = "="
    partition_name_component_sep: ClassVar[str] = "_"
    segment_sep: ClassVar[str] = os.sep

    _key_types: PartitionKeyTypes | None = PrivateAttr(None)

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

    @classmethod
    def get_default(cls) -> Storage[StoragePartition]:
        from arti.storage.literal import StringLiteral

        # TODO: Support some sort of configurable defaults.
        return cast(Storage[StoragePartition], StringLiteral())

    def _visit_type(self, type_: Type) -> Self:
        # TODO: Check support for the types and partitioning on the specified field(s).
        copy = self.copy()
        copy._key_types = PartitionKey.types_from(type_)
        assert copy.key_types is not None
        field_component_specs = {
            f"{name}{self.partition_name_component_sep}{component_name}": f"{{{name}.{component_spec}}}"
            for name, field in copy.key_types.items()
            for component_name, component_spec in field.default_components.items()
        }
        return copy.resolve(
            partition_key_spec=self.segment_sep.join(
                f"{name}{self.key_value_sep}{spec}" for name, spec in field_component_specs.items()
            )
        )

    def _visit_format(self, format_: Format) -> Self:
        return self.resolve(extension=format_.extension)

    def _visit_graph(self, graph: Graph) -> Self:
        return self.resolve(
            graph_name=graph.name,
            path_tags=self.segment_sep.join(
                f"{tag}{self.key_value_sep}{value}" for tag, value in graph.path_tags.items()
            ),
        )

    def _visit_input_fingerprint(self, input_fingerprint: Fingerprint | None) -> Self:
        return self.resolve(
            input_fingerprint="" if input_fingerprint is None else str(input_fingerprint)
        )

    def _visit_names(self, names: tuple[str, ...]) -> Self:
        return self.resolve(name=names[-1] if names else "", names=self.segment_sep.join(names))

    @property
    def includes_input_fingerprint_template(self) -> bool:
        return any("{input_fingerprint}" in val for val in self._format_fields.values())

    @property
    def key_types(self) -> PartitionKeyTypes:
        if self._key_types is None:
            raise ValueError("`key_types` have not been set yet.")
        return self._key_types

    @property
    def _format_fields(self) -> frozendict[str, str]:
        return frozendict(
            {
                name: value
                for name in self.__fields__
                if lenient_issubclass(type(value := getattr(self, name)), str)
            }
        )

    @classmethod
    def _check_key(cls, key_types: PartitionKeyTypes, key: PartitionKey) -> None:
        # TODO: Confirm the key names and types align
        if key_types and not key:
            raise ValueError(f"Expected partition key with {tuple(key_types)} but none were passed")
        if key and not key_types:
            raise ValueError(f"Expected no partition key but got: {key}")

    @abc.abstractmethod
    def discover_partitions(
        self, input_fingerprints: InputFingerprints = InputFingerprints()
    ) -> StoragePartitionSnapshots:
        raise NotImplementedError()

    def generate_partition(
        self,
        *,
        input_fingerprint: Fingerprint | None = None,
        partition_key: PartitionKey = PartitionKey(),
    ) -> StoragePartitionVar_co:
        self._check_key(self.key_types, partition_key)
        format_kwargs = dict[Any, Any](partition_key)
        if input_fingerprint is None:
            if self.includes_input_fingerprint_template:
                raise ValueError(f"{self} requires an input_fingerprint, but none was provided")
        else:
            if not self.includes_input_fingerprint_template:
                raise ValueError(f"{self} does not specify a {{input_fingerprint}} template")
            format_kwargs["input_fingerprint"] = str(input_fingerprint)
        field_values = {
            name: (
                strip_partition_indexes(original).format(**format_kwargs)
                if lenient_issubclass(type(original := getattr(self, name)), str)
                else original
            )
            for name in self.__fields__
            if name in self.storage_partition_type.__fields__
        }
        return self.storage_partition_type(
            input_fingerprint=input_fingerprint,
            partition_key=partition_key,
            storage=self,
            **field_values,
        )

    def _resolve_field(self, name: str, spec: str, placeholder_values: dict[str, str]) -> str:
        for placeholder, value in placeholder_values.items():
            if not value:
                # Strip placeholder *and* any trailing self.segment_sep.
                trim = "{" + placeholder + "}"
                if f"{trim}{self.segment_sep}" in spec:
                    trim = f"{trim}{self.segment_sep}"
                # Also strip any trailing separators, eg: if the placeholder was at the end.
                spec = spec.replace(trim, "").rstrip(self.segment_sep)
                if not spec:
                    raise ValueError(f"{self}.{name} was empty after removing unused templates")
        return partial_format(spec, **placeholder_values)

    def resolve(self, **values: str) -> Self:
        return self.copy(
            update={
                name: new
                for name, original in self._format_fields.items()
                # Avoid "setting" the value if not updated to reduce pydantic repr verbosity (which
                # only shows "set" fields by default).
                if (new := self._resolve_field(name, original, values)) != original
            }
        )


StoragePartition.update_forward_refs()
