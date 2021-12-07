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


class StoragePartition(Model):
    keys: CompositeKey
    input_fingerprint: Fingerprint = Fingerprint.empty()
    content_fingerprint: Fingerprint = Fingerprint.empty()

    def with_content_fingerprint(
        self: "_StoragePartition", keep_existing: bool = True
    ) -> "_StoragePartition":
        if keep_existing and not self.content_fingerprint.is_empty:
            return self
        return self.copy(update={"content_fingerprint": self.compute_content_fingerprint()})

    @abc.abstractmethod
    def compute_content_fingerprint(self) -> Fingerprint:
        raise NotImplementedError(
            "{type(self).__name__}.compute_content_fingerprint is not implemented!"
        )


_StoragePartition = TypeVar("_StoragePartition", bound=StoragePartition)


StoragePartitions = tuple[StoragePartition, ...]  # type: ignore


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

    @property
    def _format_fields(self) -> frozendict[str, str]:
        return frozendict(
            {
                name: value
                for name in self.__fields__
                if lenient_issubclass(type(value := getattr(self, name)), str)
            }
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
        with_content_fingerprint: bool = True,
    ) -> _StoragePartition:
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

    # TODO: Reconsider the `resolve_*` interfaces to reduce the number of copies if possible.
    def resolve(self: "_Storage", **placeholder_values: str) -> "_Storage":
        return self.copy(
            update={
                name: new
                for name, original in self._format_fields.items()
                # Avoid "setting" the value if not updated to reduce pydantic repr verbosity (which
                # only shows "set" fields by default).
                if (new := self._resolve_field(name, original, placeholder_values)) != original
            }
        )

    def resolve_extension(self: "_Storage", extension: Optional[str]) -> "_Storage":
        if extension is None:
            return self.resolve(extension="")
        return self.resolve(extension=extension)

    def resolve_graph_name(self: "_Storage", graph_name: str) -> "_Storage":
        return self.resolve(graph_name=graph_name)

    def resolve_input_fingerprint(self: "_Storage", input_fingerprint: Fingerprint) -> "_Storage":
        val = str(input_fingerprint.key)
        if input_fingerprint.is_empty:
            val = ""
        return self.resolve(input_fingerprint=val)

    def resolve_names(self: "_Storage", names: tuple[str, ...]) -> "_Storage":
        name = names[-1] if names else ""
        return self.resolve(names=self.segment_sep.join(names), name=name)

    def resolve_partition_key_spec(self: "_Storage", key_types: CompositeKeyTypes) -> "_Storage":
        key_component_specs = {
            f"{name}{self.partition_name_component_sep}{component_name}": f"{{{name}.{component_spec}}}"
            for name, pk in key_types.items()
            for component_name, component_spec in pk.default_key_components.items()
        }
        return self.resolve(
            partition_key_spec=self.segment_sep.join(
                f"{name}{self.key_value_sep}{spec}" for name, spec in key_component_specs.items()
            )
        )

    def resolve_path_tags(self: "_Storage", path_tags: frozendict[str, str]) -> "_Storage":
        return self.resolve(
            path_tags=self.segment_sep.join(
                f"{tag}{self.key_value_sep}{value}" for tag, value in path_tags.items()
            )
        )

    def supports(self: "_Storage", type_: Type, format: Format) -> None:
        # TODO: Ensure the storage supports all of the specified types and partitioning on the
        # specified field(s).
        pass


# mypy doesn't (yet?) support nested TypeVars[1], so mark internal as Any.
#
# 1: https://github.com/python/mypy/issues/2756
_Storage = TypeVar("_Storage", bound=Storage[Any])
