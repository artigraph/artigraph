from __future__ import annotations

__path__ = __import__("pkgutil").extend_path(__path__, __name__)

import abc
import os
from typing import TYPE_CHECKING, Any, ClassVar, Generic, Optional, TypeVar

from pydantic import PrivateAttr

from arti.fingerprints import Fingerprint
from arti.formats import Format
from arti.internal.models import Model
from arti.internal.type_hints import Self, get_class_type_vars, lenient_issubclass
from arti.internal.utils import frozendict
from arti.partitions import CompositeKey, CompositeKeyTypes, InputFingerprints, PartitionKey
from arti.storage._internal import partial_format, strip_partition_indexes
from arti.types import Type

if TYPE_CHECKING:
    from arti.graphs import Graph


class StoragePartition(Model):
    keys: CompositeKey = CompositeKey()
    input_fingerprint: Fingerprint = Fingerprint.empty()
    content_fingerprint: Fingerprint = Fingerprint.empty()

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

    _key_types: Optional[CompositeKeyTypes] = PrivateAttr(None)

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

        return StringLiteral()  # TODO: Support some sort of configurable defaults.

    def _visit_type(self, type_: Type) -> Self:
        # TODO: Check support for the types and partitioning on the specified field(s).
        copy = self.copy()
        copy._key_types = PartitionKey.types_from(type_)
        assert copy.key_types is not None
        key_component_specs = {
            f"{name}{self.partition_name_component_sep}{component_name}": f"{{{name}.{component_spec}}}"
            for name, pk in copy.key_types.items()
            for component_name, component_spec in pk.default_key_components.items()
        }
        return copy.resolve(
            partition_key_spec=self.segment_sep.join(
                f"{name}{self.key_value_sep}{spec}" for name, spec in key_component_specs.items()
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

    def _visit_input_fingerprint(self, input_fingerprint: Fingerprint) -> Self:
        input_fingerprint_key = str(input_fingerprint.key)
        if input_fingerprint.is_empty:
            input_fingerprint_key = ""
        return self.resolve(input_fingerprint=input_fingerprint_key)

    def _visit_names(self, names: tuple[str, ...]) -> Self:
        return self.resolve(name=names[-1] if names else "", names=self.segment_sep.join(names))

    @property
    def includes_input_fingerprint_template(self) -> bool:
        return any("{input_fingerprint}" in val for val in self._format_fields.values())

    @property
    def key_types(self) -> CompositeKeyTypes:
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
    def _check_keys(cls, key_types: CompositeKeyTypes, keys: CompositeKey) -> None:
        # TODO: Confirm the key names and types align
        if key_types and not keys:
            raise ValueError(f"Expected partition keys {tuple(key_types)} but none were passed")
        if keys and not key_types:
            raise ValueError(f"Expected no partition keys but got: {keys}")

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
        elif self.includes_input_fingerprint_template:
            format_kwargs["input_fingerprint"] = str(input_fingerprint.key)
        else:
            raise ValueError(f"{self} does not specify a {{input_fingerprint}} template")
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
