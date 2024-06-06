from __future__ import annotations

__path__ = __import__("pkgutil").extend_path(__path__, __name__)

import json
from itertools import chain
from typing import TYPE_CHECKING, Annotated, Any

from pydantic import Field, ValidationInfo, field_validator

from arti.annotations import Annotation
from arti.fingerprints import SkipFingerprint
from arti.formats import Format
from arti.internal.models import Model, get_field_default
from arti.internal.type_hints import get_annotation_from_value
from arti.statistics import Statistic
from arti.storage import Storage
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
    format: Format = Field(default_factory=Format.get_default, validate_default=True)
    storage: Storage = Field(default_factory=Storage.get_default, validate_default=True)

    annotations: Annotated[tuple[Annotation, ...], Field(validate_default=True)] = ()
    statistics: Annotated[tuple[Statistic, ...], Field(validate_default=True)] = ()

    # Omit the `producer_output` (ie: this Artifact's Producer) from the:
    # - `fingerprint` to prevent upstream changes from triggering cascading fingerprint changes.
    #     - Artifacts represent a *template* for data, independent of the Producer. We don't want
    #       Producer changes (including changes to their input Artifacts) to affect this Artifact's
    #       fingerprint. This isolates the overall fingerprint changes within a Graph to just the
    #       changed Artifact(s) and Producer(s), instead of cascading. Once we're actually building,
    #       changes to the Producer or input StoragePartitionSnapshots will be reflected in a new
    #       `input_fingerprint` that is formatted into the Storage, creating the appropriate
    #       output StoragePartitions.
    # - `repr` to prevent showing the entire upstream graph.
    #
    # NOTE : ProducerOutput is a ForwardRef/cyclic import.
    producer_output: Annotated[ProducerOutput | None, Field(repr=False), SkipFingerprint()] = None

    @field_validator("format")
    @classmethod
    def _validate_format(cls, format: Format, info: ValidationInfo) -> Format:
        if (type_ := info.data.get("type")) is not None:
            return format._visit_type(type_)
        return format

    @field_validator("storage")
    @classmethod
    def _validate_storage(cls, storage: Storage, info: ValidationInfo) -> Storage:
        if (type_ := info.data.get("type")) is not None:
            storage = storage._visit_type(type_)
        if (format_ := info.data.get("format")) is not None:
            storage = storage._visit_format(format_)
        return storage

    @field_validator("annotations", "statistics", mode="before")
    @classmethod
    def _merge_class_defaults(cls, value: tuple[Any, ...], info: ValidationInfo) -> tuple[Any, ...]:
        assert info.field_name is not None
        return tuple(chain(get_field_default(cls, info.field_name, fallback=()), value))

    @classmethod
    def cast(cls, value: Any) -> Artifact:
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
