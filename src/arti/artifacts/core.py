from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

from arti.formats.core import Format
from arti.internal.pointers import Pointer
from arti.storage.core import Storage
from arti.types.core import Type

if TYPE_CHECKING:
    from arti.annotations.core import Annotation
    from arti.graphs.core import Graph
    from arti.producers.core import Producer
    from arti.statistics.core import Statistic


class BaseArtifact:
    """ A BaseArtifact represents a piece data.

        A BaseArtifact is comprised of three key elements:
        - schema: spec of the data's structure, such as `int`, "struct", etc.
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

    # Graph should not be set directly, but will be populated when added to one.
    graph: Optional[Graph] = None

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
        self.producer: Optional[Producer] = None
        self.consumers: set[Producer] = set()
        super().__init__()


class Artifact(BaseArtifact, Pointer):
    """ An Artifact represents a piece data.

        An Artifact is comprised of three key elements:
        - `schema`: spec of the data's structure, such as `int`, "struct", etc.
        - `format`: the data's serialized format, such as CSV, Parquet, database native, etc.
        - `storage`: the data's persistent storage system, such as blob storage, database native, etc.

        In addition to the core elements, an Artifact can be tagged with additional `annotations`
        (to associate it with human knowledge) and `statistics` (to track derived characteristics
        over time).
    """

    annotations: Optional[tuple[Annotation, ...]] = ()
    statistics: Optional[tuple[Statistic, ...]] = ()

    # Artifacts are collections by default (think database tables, etc), but may be overridden.
    is_scalar = False

    @classmethod
    def cast(cls, value: Any) -> Artifact:
        """ Attempt to convert any value to an appropriate Artifact instance.
        """
        from arti.producers.core import Producer

        if isinstance(value, Artifact):
            return value
        if isinstance(value, Producer):
            n_outputs = len(value.output_artifacts)
            if n_outputs == 0:  # pragma: no cover
                # NOTE: These shouldn't exist yet, but might be useful for "side effect only" sorts
                # of things - we might return a randomized checkpoint artifact instead of error
                # here. Or, the Producer might want to determine how to handle it (run once vs run
                # many) and return an appropriate Artifact. Either way, the Producer must be
                # assigned to the Graph rather than simply initialized (though we could check the
                # graph for artifacts with downstream producers that are terminal).
                raise ValueError(f"{type(value).__name__} doesn't produce any Artifacts!")
            if n_outputs > 1:
                raise ValueError(
                    f"{type(value).__name__} produces {len(value.output_artifacts)} Artifacts. Try assigning each to a new name in the Graph!"
                )
            return value.output_artifacts[0]

        # TODO: Leverage a TypeSystem("python") to cast to per-type Artifact classes with "backend
        # native" storage.
        raise NotImplementedError("Casting python objects to Artifacts is not implemented yet!")

    def with_annotations(self, *args: Annotation) -> Artifact:
        self.annotations = (self.annotations or ()) + args
        return self

    def with_statistics(self, *args: Statistic) -> Artifact:
        self.statistics = (self.statistics or ()) + args
        return self
