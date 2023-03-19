__path__ = __import__("pkgutil").extend_path(__path__, __name__)

import json
from itertools import chain
from typing import TYPE_CHECKING, Any, Optional

from pydantic import Field, validator
from pydantic.fields import ModelField

from arti.annotations import Annotation
from arti.formats import Format
from arti.internal.models import Model, get_field_default
from arti.internal.type_hints import get_annotation_from_value
from arti.statistics import Statistic
from arti.storage import Storage, StoragePartition
from arti.types import Type

if TYPE_CHECKING:
    from arti.producers import ProducerOutput


class Artifact(Model):
    """An Artifact is the base structure describing an existing or generated dataset.

    An Artifact is comprised of three key elements:
    - `type`: spec of the data's structure, such as data types, nullable, etc.
    - `format`: the data's serialized format, such as CSV, Parquet, database native, etc.
    - `storage`: the data's persistent storage system, such as blob storage, database native, etc.

    In addition to the core elements, an Artifact can be tagged with additional `annotations` (to
    associate it with human knowledge) and `statistics` (to track derived characteristics over
    time).
    """

    type: Type
    format: Format = Field(default_factory=Format.get_default)
    storage: Storage[StoragePartition] = Field(default_factory=Storage.get_default)

    annotations: tuple[Annotation, ...] = ()
    statistics: tuple[Statistic, ...] = ()

    # Hide `producer_output` in repr to prevent showing the entire upstream graph.
    #
    # ProducerOutput is a ForwardRef/cyclic import. Quote the entire hint to force full resolution
    # during `.update_forward_refs`, rather than `Optional[ForwardRef("ProducerOutput")]`.
    producer_output: "Optional[ProducerOutput]" = Field(None, repr=False)

    # NOTE: Narrow the fields that affect the fingerprint to minimize changes (which trigger
    # recompute). Importantly, avoid fingerprinting the `.producer_output` (ie: the *upstream*
    # producer) to prevent cascading fingerprint changes (Producer.fingerprint accesses the *input*
    # Artifact.fingerprints). Even so, this may still be quite sensitive.
    _fingerprint_includes_ = frozenset(["type", "format", "storage"])

    @validator("format", always=True)
    @classmethod
    def validate_format(cls, format: Format, values: dict[str, Any]) -> Format:
        if "type" in values:
            return format.copy(update={"type": values["type"]})
        return format

    @validator("storage", always=True)
    @classmethod
    def validate_storage(cls, storage: Storage[Any], values: dict[str, Any]) -> Storage[Any]:
        return storage.copy(
            update={name: values[name] for name in ["type", "format"] if name in values}
        ).resolve_templates()

    @validator("annotations", "statistics", always=True, pre=True)
    @classmethod
    def _merge_class_defaults(cls, value: tuple[Any, ...], field: ModelField) -> tuple[Any, ...]:
        return tuple(chain(get_field_default(cls, field.name) or (), value))

    @classmethod
    def cast(cls, value: Any) -> "Artifact":
        """Attempt to convert an arbitrary value to an appropriate Artifact instance.

        `Artifact.cast` is used to convert values assigned to an `ArtifactBox` (such as
        `Graph.artifacts`) into an Artifact. When called with:
        - an Artifact instance, it is returned
        - a Producer instance with a single output Artifact, the output Artifact is returned
        - a Producer instance with a multiple output Artifacts, an error is raised
        - other types, we attempt to map to a `Type` and return an Artifact instance with defaulted Format and Storage
        """
        from arti.formats.json import JSON
        from arti.producers import Producer
        from arti.storage.literal import StringLiteral
        from arti.types.python import python_type_system

        if isinstance(value, Artifact):
            return value
        if isinstance(value, Producer):
            output_artifacts = value.out()
            if isinstance(output_artifacts, Artifact):
                return output_artifacts
            n_outputs = len(output_artifacts)
            if n_outputs == 0:  # pragma: no cover
                # TODO: "side effect" Producers: https://github.com/artigraph/artigraph/issues/11
                raise ValueError(f"{type(value).__name__} doesn't produce any Artifacts!")
            assert n_outputs > 1
            raise ValueError(
                f"{type(value).__name__} produces {len(output_artifacts)} Artifacts. Try assigning each to a new name in the Graph!"
            )

        annotation = get_annotation_from_value(value)
        return cls(
            type=python_type_system.to_artigraph(annotation, hints={}),
            format=JSON(),
            storage=StringLiteral(value=json.dumps(value)),
        )
