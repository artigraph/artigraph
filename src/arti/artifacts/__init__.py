__path__ = __import__("pkgutil").extend_path(__path__, __name__)  # type: ignore

from itertools import chain
from typing import TYPE_CHECKING, Any, ClassVar, Optional

from pydantic import Field, validator
from pydantic.fields import ModelField

from arti.annotations import Annotation
from arti.formats import Format
from arti.internal.models import Model
from arti.internal.utils import classproperty
from arti.partitions import CompositeKeyTypes, PartitionKey
from arti.storage import Storage
from arti.types import Type

if TYPE_CHECKING:
    from arti.producers import Producer


class BaseArtifact(Model):
    """A BaseArtifact is the most basic data structure describing data in the Artigraph ecosystem.

    A BaseArtifact is comprised of three key elements:
    - type: spec of the data's structure, such as data types, nullable, etc.
    - format: the data's serialized format, such as CSV, Parquet, database native, etc.
    - storage: the data's persistent storage system, such as blob storage, database native, etc.
    """

    # Type *must* be set on the class and be rather static - small additions may be necessary at
    # Graph level (eg: dynamic column additions), but these should be minor. We might allow Struct
    # Types to be "open" (partial type) or "closed".
    #
    # Format and storage *should* be set with defaults on Artifact subclasses to ease most Graph
    # definitions, but will often need to be overridden at the Graph level.
    #
    # In order to override on the instance, avoid ClassVars lest mypy complains when/if we override.
    type: Type
    format: Format
    storage: Storage[Any]

    # Hide the producer to prevent showing the entire upstream graph
    producer: Optional["Producer"] = Field(None, repr=False)

    # Class level alias for `type`, which must be set on (non-abstract) subclasses.
    #
    # Pydantic removes class defaults and stashes them in cls.__fields__. To ease access, we
    # automatically populate this from `type` in `__init_subclass__`.
    _type: ClassVar[Type]

    @classmethod
    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if not cls._abstract_ and cls.__fields__["type"].default is None:
            raise ValueError(f"{cls.__name__} must set `type`")
        cls._type = cls.__fields__["type"].default

    @validator("type", always=True)
    @classmethod
    def _validate_type(cls, type_: Type) -> Type:
        if type_ != cls._type:
            # NOTE: We do a lot of class level validation (particularly in Producer) that relies on
            # the *class* type, such as partition key validation. It's possible we could loosen this
            # a bit by allowing *new* Struct fields, but still requiring an exact match for other
            # Types (including existing Struct fields).
            raise ValueError("overriding `type` is not supported")
        return type_

    @validator("format", always=True)
    @classmethod
    def _validate_format(cls, format: Format, values: dict[str, Any]) -> Format:
        if "type" in values:
            format.supports(type_=values["type"])
        return format

    @validator("storage", always=True)
    @classmethod
    def _validate_storage(cls, storage: Storage[Any], values: dict[str, Any]) -> Storage[Any]:
        if "type" in values and "format" in values:
            storage.supports(type_=values["type"], format=values["format"])
        return storage

    @classproperty
    @classmethod
    def partition_key_types(cls) -> CompositeKeyTypes:
        return PartitionKey.types_from(cls._type)

    @classproperty
    @classmethod
    def is_partitioned(cls) -> bool:
        return bool(cls.partition_key_types)


class Statistic(BaseArtifact):
    """A Statistic is a piece of data derived from an Artifact that can be tracked over time."""

    # TODO: Set format/storage to some "system default" that can be used across backends?

    _abstract_ = True


class Artifact(BaseArtifact):
    """An Artifact is the base structure describing an existing or generated dataset.

    An Artifact is comprised of three key elements:
    - `type`: spec of the data's structure, such as data types, nullable, etc.
    - `format`: the data's serialized format, such as CSV, Parquet, database native, etc.
    - `storage`: the data's persistent storage system, such as blob storage, database native, etc.

    In addition to the core elements, an Artifact can be tagged with additional `annotations`
    (to associate it with human knowledge) and `statistics` (to track derived characteristics
    over time).
    """

    _abstract_ = True

    annotations: tuple[Annotation, ...] = ()
    statistics: tuple[Statistic, ...] = ()

    @validator("annotations", "statistics", always=True, pre=True)
    @classmethod
    def _merge_class_defaults(cls, value: tuple[Any, ...], field: ModelField) -> tuple[Any, ...]:
        return tuple(chain(cls.__fields__[field.name].default, value))

    @classmethod
    def cast(cls, value: Any) -> "Artifact":
        """Attempt to convert an arbitrary value to an appropriate Artifact instance.

        `Artifact.cast` is used to convert values assigned to an `Artifact.box` (such as
        `Graph.artifacts`) into an Artifact. When called with:
        - an Artifact instance, it is returned
        - a Producer instance with a single output Artifact, the output Artifact is returned
        - a Producer instance with a multiple output Artifacts, an error is raised
        - other types, an error is raised
        """
        from arti.producers import Producer

        # TODO: Leverage a TypeSystem("python") to cast to Artifact classes with "backend native"
        # storage to support builtin assignment and custom type registration.
        if isinstance(value, Artifact):
            return value
        if isinstance(value, Producer):
            output_artifacts = value.out()
            if isinstance(output_artifacts, Artifact):
                return output_artifacts
            n_outputs = len(output_artifacts)
            if n_outputs == 0:  # pragma: no cover
                # TODO: "side effect" Producers: https://github.com/replicahq/artigraph/issues/11
                raise ValueError(f"{type(value).__name__} doesn't produce any Artifacts!")
            assert n_outputs > 1
            raise ValueError(
                f"{type(value).__name__} produces {len(output_artifacts)} Artifacts. Try assigning each to a new name in the Graph!"
            )

        raise NotImplementedError("Casting python objects to Artifacts is not implemented yet!")
