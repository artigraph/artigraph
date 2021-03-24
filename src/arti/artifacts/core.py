from __future__ import annotations

from collections.abc import Iterable
from itertools import chain
from typing import TYPE_CHECKING, Any, Optional

from arti.formats.core import Format
from arti.storage.core import Storage
from arti.types.core import Type

if TYPE_CHECKING:
    from arti.annotations.core import Annotation
    from arti.producers.core import Producer
    from arti.statistics.core import Statistic


class BaseArtifact:
    """ A BaseArtifact is the most basic data structure describing data in the Artigraph ecosystem.

        A BaseArtifact is comprised of three key elements:
        - schema: spec of the data's structure, such as data types, nullable, etc.
        - format: the data's serialized format, such as CSV, Parquet, database native, etc.
        - storage: the data's persistent storage system, such as blob storage, database native, etc.
    """

    # Schema *must* be set on the class and be rather static - small additions may be necessary at
    # Graph level (eg: dynamic column additions), but these should be minor. We might allow Struct
    # Types to be "open" (partial schema) or "closed".
    #
    # Format and storage *should* be set with defaults on Artifact subclasses to ease most Graph
    # definitions, but will often need to be overridden at the Graph level.
    #
    # In order to override on the instance, avoid ClassVars lest mypy complains when/if we override.
    schema: Optional[Type] = None
    format: Optional[Format] = None
    storage: Optional[Storage] = None

    # is_scalar denotes whether this Artifacts represents a *single* value of the specified schema
    # or a *collection*. Namely, even if the schema is a Struct(...), but there is only one, it will
    # be scalar for our purposes.
    is_scalar: bool

    @classmethod
    def __init_subclass__(cls, **kwargs: Any) -> None:
        if cls.format is not None:
            cls.format.validate(schema=cls.schema)
        if cls.storage is not None:
            cls.storage.validate(schema=cls.schema, format=cls.format)
        super().__init_subclass__()

    def __init__(self) -> None:
        # TODO: Allow storage/format override and re-validate them.
        self.producer: Optional[Producer] = None
        super().__init__()


class Artifact(BaseArtifact):
    """ An Artifact is the base structure describing an existing or generated dataset.

        An Artifact is comprised of three key elements:
        - `schema`: spec of the data's structure, such as data types, nullable, etc.
        - `format`: the data's serialized format, such as CSV, Parquet, database native, etc.
        - `storage`: the data's persistent storage system, such as blob storage, database native, etc.

        In addition to the core elements, an Artifact can be tagged with additional `annotations`
        (to associate it with human knowledge) and `statistics` (to track derived characteristics
        over time).
    """

    annotations: tuple[Annotation, ...] = ()
    statistics: tuple[Statistic, ...] = ()

    # Artifacts are collections by default (think database tables, etc), but may be overridden.
    is_scalar = False

    @classmethod
    def cast(cls, value: Any) -> Artifact:
        """ Attempt to convert an arbitrary value to an appropriate Artifact instance.

            `Artifact.cast` is used to convert values assigned to an `Artifact.box` (such as
            `Graph.artifacts`) into an Artifact. When called with:
            - an Artifact instance, it is returned
            - a Producer instance with a single output Artifact, the output Artifact is returned
            - a Producer instance with a multiple output Artifacts, an error is raised
            - other types, an error is raised
        """
        # TODO: Leverage a TypeSystem("python") to cast to Artifact classes with "backend native"
        # storage to support builtin assignment and custom type registration.
        from arti.producers.core import Producer

        if isinstance(value, Artifact):
            return value
        if isinstance(value, Producer):
            n_outputs = len(value.output_artifacts)
            if n_outputs == 0:  # pragma: no cover
                # TODO: "side effect" Producers: https://github.com/replicahq/artigraph/issues/11
                raise ValueError(f"{type(value).__name__} doesn't produce any Artifacts!")
            if n_outputs > 1:
                raise ValueError(
                    f"{type(value).__name__} produces {len(value.output_artifacts)} Artifacts. Try assigning each to a new name in the Graph!"
                )
            return value.output_artifacts[0]

        raise NotImplementedError("Casting python objects to Artifacts is not implemented yet!")

    def __init__(
        self, *, annotations: Iterable[Annotation] = (), statistics: Iterable[Statistic] = (),
    ) -> None:
        # Add the instance metadata to the class default.
        self.annotations = tuple(chain(self.annotations, annotations))
        self.statistics = tuple(chain(self.statistics, statistics))
        super().__init__()
