from __future__ import annotations

__path__ = __import__("pkgutil").extend_path(__path__, __name__)

import abc
import os
from typing import Any, ClassVar, Generic, Optional, Self, TypeVar

from pydantic import Field, validator

from arti.fingerprints import Fingerprint
from arti.formats import Format
from arti.internal.models import Model
from arti.internal.type_hints import get_class_type_vars, lenient_issubclass
from arti.internal.utils import frozendict
from arti.partitions import CompositeKey, CompositeKeyTypes, InputFingerprints, PartitionKey
from arti.storage._internal import partial_format, strip_partition_indexes
from arti.types import Type


class _StorageMixin(Model):
    @property
    def key_types(self) -> CompositeKeyTypes:
        if self.type is None:  # type: ignore[attr-defined]
            raise ValueError(f"{self}.type is not set")
        return PartitionKey.types_from(self.type)  # type: ignore[attr-defined]

    @classmethod
    def _check_keys(cls, key_types: CompositeKeyTypes, keys: CompositeKey) -> None:
        # TODO: Confirm the key names and types align
        if key_types and not keys:
            raise ValueError(f"Expected partition keys {tuple(key_types)} but none were passed")
        if keys and not key_types:
            raise ValueError(f"Expected no partition keys but got: {keys}")


class StoragePartition(_StorageMixin, Model):
    type: Type = Field(repr=False)
    format: Format = Field(repr=False)
    keys: CompositeKey = CompositeKey()
    input_fingerprint: Fingerprint = Fingerprint.empty()
    content_fingerprint: Fingerprint = Fingerprint.empty()

    @validator("keys")
    @classmethod
    def validate_keys(cls, keys: CompositeKey, values: dict[str, Any]) -> CompositeKey:
        if "type" in values:
            cls._check_keys(PartitionKey.types_from(values["type"]), keys)
        return keys

    def with_content_fingerprint(self, keep_existing: bool = True) -> Self:
        if keep_existing and not self.content_fingerprint.is_empty:
            return self
        return self.copy(update={"content_fingerprint": self.compute_content_fingerprint()})

    @abc.abstractmethod
    def compute_content_fingerprint(self) -> Fingerprint:
        raise NotImplementedError(
            "{type(self).__name__}.compute_content_fingerprint is not implemented!"
        )


StoragePartitionVar = TypeVar("StoragePartitionVar", bound=StoragePartition)
StoragePartitionVar_co = TypeVar("StoragePartitionVar_co", bound=StoragePartition, covariant=True)
StoragePartitions = tuple[StoragePartition, ...]


class Storage(_StorageMixin, Model, Generic[StoragePartitionVar_co]):
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

    storage_partition_type: ClassVar[type[StoragePartitionVar_co]]  # type: ignore[misc]

    type: Optional[Type] = Field(None, repr=False)
    format: Optional[Format] = Field(None, repr=False)

    @validator("type")
    @classmethod
    def validate_type(cls, type_: Type) -> Type:
        # TODO: Check support for the types and partitioning on the specified field(s).
        return type_

    @validator("format")
    @classmethod
    def validate_format(cls, format: Format) -> Format:
        return format

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
    def get_default(cls) -> Storage[StoragePartition]:
        from arti.storage.literal import StringLiteral

        return StringLiteral()  # TODO: Support some sort of configurable defaults.

    @abc.abstractmethod
    def discover_partitions(
        self, input_fingerprints: InputFingerprints = InputFingerprints()
    ) -> tuple[StoragePartitionVar_co, ...]:
        raise NotImplementedError()

    def generate_partition(
        self,
        keys: CompositeKey = CompositeKey(),
        input_fingerprint: Fingerprint = Fingerprint.empty(),
        with_content_fingerprint: bool = True,
    ) -> StoragePartitionVar_co:
        self._check_keys(self.key_types, keys)
        format_kwargs = dict[Any, Any](keys)
        if input_fingerprint.is_empty:
            if self.includes_input_fingerprint_template:
                raise ValueError(f"{self} requires an input_fingerprint, but none was provided")
        else:
            if not self.includes_input_fingerprint_template:
                raise ValueError(f"{self} does not specify a {{input_fingerprint}} template")
            format_kwargs["input_fingerprint"] = str(input_fingerprint.key)
        field_values = {
            name: (
                strip_partition_indexes(original).format(**format_kwargs)
                if lenient_issubclass(type(original := getattr(self, name)), str)
                else original
            )
            for name in self.__fields__
            if name in self.storage_partition_type.__fields__
        }
        partition = self.storage_partition_type(
            input_fingerprint=input_fingerprint, keys=keys, **field_values
        )
        if with_content_fingerprint:
            partition = partition.with_content_fingerprint()
        return partition

    @property
    def includes_input_fingerprint_template(self) -> bool:
        return any("{input_fingerprint}" in val for val in self._format_fields.values())

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

    def resolve_templates(
        self,
        graph_name: Optional[str] = None,
        input_fingerprint: Optional[Fingerprint] = None,
        names: Optional[tuple[str, ...]] = None,
        path_tags: Optional[frozendict[str, str]] = None,
    ) -> Self:
        values = {}
        if graph_name is not None:
            values["graph_name"] = graph_name
        if input_fingerprint is not None:
            input_fingerprint_key = str(input_fingerprint.key)
            if input_fingerprint.is_empty:
                input_fingerprint_key = ""
            values["input_fingerprint"] = input_fingerprint_key
        if names is not None:
            values["name"] = names[-1] if names else ""
            values["names"] = self.segment_sep.join(names)
        if path_tags is not None:
            values["path_tags"] = self.segment_sep.join(
                f"{tag}{self.key_value_sep}{value}" for tag, value in path_tags.items()
            )
        if self.format is not None:
            values["extension"] = self.format.extension
        if self.type is not None:
            key_component_specs = {
                f"{name}{self.partition_name_component_sep}{component_name}": f"{{{name}.{component_spec}}}"
                for name, pk in self.key_types.items()
                for component_name, component_spec in pk.default_key_components.items()
            }
            values["partition_key_spec"] = self.segment_sep.join(
                f"{name}{self.key_value_sep}{spec}" for name, spec in key_component_specs.items()
            )
        return self.copy(
            update={
                name: new
                for name, original in self._format_fields.items()
                # Avoid "setting" the value if not updated to reduce pydantic repr verbosity (which
                # only shows "set" fields by default).
                if (new := self._resolve_field(name, original, values)) != original
            }
        )
