from __future__ import annotations

from itertools import chain
from typing import Any, ClassVar, Optional

from pydantic import validator
from pydantic.fields import ModelField

from arti.annotations.core import Annotation
from arti.formats.core import Format
from arti.internal.models import Model
from arti.storage.core import Storage
from arti.types.core import Type


class BaseArtifact(Model):
    """A BaseArtifact is the most basic data structure describing data in the Artigraph ecosystem.

    A BaseArtifact is comprised of three key elements:
    - type: spec of the data's structure, such as data types, nullable, etc.
    - format: the data's serialized format, such as CSV, Parquet, database native, etc.
    - storage: the data's persistent storage system, such as blob storage, database native, etc.
    """

    # is_scalar denotes whether this Artifacts represents a *single* value of the specified type or
    # a *collection*. Namely, even if the type is a Struct(...), but there is only one, it will be
    # scalar for our purposes.
    is_scalar: ClassVar[bool]

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
    storage: Storage

    producer: Optional[Producer] = None

    # TODO: Allow smarter type/format/storage merging w/ the default?

    @validator("format", always=True)
    @classmethod
    def _validate_format(cls, format: Format, values: dict[str, Any]) -> Format:
        format.supports(type_=values["type"])
        return format

    @validator("storage", always=True)
    @classmethod
    def _validate_storage(cls, storage: Storage, values: dict[str, Any]) -> Storage:
        # format won't be available if it doesn't pass validation.
        if "format" in values:
            storage.supports(type_=values["type"], format=values["format"])
        return storage

    # TODO: Remove after making Producers models
    class Config:
        arbitrary_types_allowed = True


from arti.statistics.core import Statistic  # noqa: E402 # # pylint: disable=wrong-import-position


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

    # Artifacts are collections by default (think database tables, etc), but may be overridden.
    is_scalar: ClassVar[bool] = False

    annotations: tuple[Annotation, ...] = ()
    statistics: tuple[Statistic, ...] = ()

    @validator("annotations", "statistics", always=True, pre=True)
    @classmethod
    def _validate_annotations(cls, value: tuple[Any, ...], field: ModelField) -> tuple[Any, ...]:
        return tuple(chain(cls.__fields__[field.name].default, value))

    @classmethod
    def cast(cls, value: Any) -> Artifact:
        """Attempt to convert an arbitrary value to an appropriate Artifact instance.

        `Artifact.cast` is used to convert values assigned to an `Artifact.box` (such as
        `Graph.artifacts`) into an Artifact. When called with:
        - an Artifact instance, it is returned
        - a Producer instance with a single output Artifact, the output Artifact is returned
        - a Producer instance with a multiple output Artifacts, an error is raised
        - other types, an error is raised
        """
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


from arti.producers.core import Producer  # noqa: E402 # # pylint: disable=wrong-import-position

BaseArtifact.update_forward_refs()
Artifact.update_forward_refs()
