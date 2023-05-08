from __future__ import annotations

__path__ = __import__("pkgutil").extend_path(__path__, __name__)

import builtins
from typing import Any, ClassVar, Literal, Optional, get_origin

from pydantic import validator

from arti import io
from arti.artifacts import Artifact
from arti.internal.models import Model, get_field_default
from arti.internal.type_hints import discard_Annotated, get_item_from_annotated, lenient_issubclass
from arti.internal.utils import import_submodules, register
from arti.types import Type, TypeSystem

MODE = Literal["READ", "WRITE", "READWRITE"]


class View(Model):
    """View represents the in-memory representation of the artifact.

    Examples include pandas.DataFrame, dask.DataFrame, a BigQuery table.
    """

    _abstract_ = True
    _by_python_type_: ClassVar[dict[Optional[type], type[View]]] = {}

    priority: ClassVar[int] = 0  # Set priority of this view for its python_type. Higher is better.
    python_type: ClassVar[Optional[type]]
    type_system: ClassVar[TypeSystem]

    mode: MODE
    artifact_class: type[Artifact] = Artifact
    type: Type

    @classmethod
    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if not cls._abstract_:
            register(cls._by_python_type_, cls.python_type, cls, lambda x: x.priority)

    @classmethod
    def _check_type_compatibility(cls, view_type: Type, artifact_type: Type) -> None:
        # TODO: Consider supporting some form of "reader schema" where the View's Type is a subset
        # of the Artifact's Type (and we filter the columns on read). We could also consider
        # allowing the Producer's Type to be a superset of the Artifact's Type and we'd filter the
        # columns on write.
        #
        # If implementing, we can leverage the `mode` to determine which should be the "superset".
        if view_type != artifact_type:
            raise ValueError(
                f"the specified Type (`{view_type}`) is not compatible with the Artifact's Type (`{artifact_type}`)."
            )

    @validator("type")
    @classmethod
    def _validate_type(cls, type_: Type, values: dict[str, Any]) -> Type:
        artifact_class: Optional[type[Artifact]] = values.get("artifact_class")
        if artifact_class is None:
            return type_  # pragma: no cover
        artifact_type: Optional[Type] = get_field_default(artifact_class, "type")
        if artifact_type is not None:
            cls._check_type_compatibility(view_type=type_, artifact_type=artifact_type)
        return type_

    @classmethod
    def _get_kwargs_from_annotation(cls, annotation: Any) -> dict[str, Any]:
        artifact_class = get_item_from_annotated(
            annotation, Artifact, is_subclass=True
        ) or get_field_default(cls, "artifact_class")
        assert artifact_class is not None
        assert issubclass(artifact_class, Artifact)
        # Try to extract or infer the Type. We prefer: an explicit Type in the annotation, followed
        # by an Artifact's default type, falling back to inferring a Type from the type hint.
        type_ = get_item_from_annotated(annotation, Type, is_subclass=False)
        if type_ is None:
            artifact_type: Optional[Type] = get_field_default(artifact_class, "type")
            if artifact_type is None:
                from arti.types.python import python_type_system

                type_ = python_type_system.to_artigraph(discard_Annotated(annotation), hints={})
            else:
                type_ = artifact_type
        # NOTE: We validate that type_ and artifact_type (if set) are compatible in _validate_type,
        # which will run for *any* instance, not just those created with `.from_annotation`.
        return {"artifact_class": artifact_class, "type": type_}

    @classmethod  # TODO: Use typing.Self for return, pending mypy support
    def get_class_for(cls, annotation: Any) -> builtins.type[View]:
        view_class = get_item_from_annotated(annotation, cls, is_subclass=True)
        if view_class is None:
            # We've already searched for a View instance in the original Annotated args, so just
            # extract the root annotation.
            annotation = discard_Annotated(annotation)
            # Import the View submodules to trigger registration.
            import_submodules(__path__, __name__)
            view_class = cls._by_python_type_.get(annotation)
        # If no match and the type is a subscripted Generic (eg: `list[int]`), try to unwrap any
        # extra type variables.
        if view_class is None and (origin := get_origin(annotation)) is not None:
            view_class = cls._by_python_type_.get(origin)
        if view_class is None:
            raise ValueError(
                f"{annotation} cannot be matched to a View, try setting one explicitly (eg: `Annotated[int, arti.views.python.Int]`)"
            )
        return view_class

    @classmethod  # TODO: Use typing.Self for return, pending mypy support
    def from_annotation(cls, annotation: Any, *, mode: MODE) -> View:
        view_class = cls.get_class_for(annotation)
        view = view_class(mode=mode, **cls._get_kwargs_from_annotation(annotation))
        view.check_annotation_compatibility(annotation)
        return view

    def check_annotation_compatibility(self, annotation: Any) -> None:
        # We're only checking the root annotation (lenient_issubclass ignores Annotated anyway), so
        # tidy up the value to improve error messages.
        annotation = discard_Annotated(annotation)
        system_type = self.type_system.to_system(self.type, hints={})
        if not (
            lenient_issubclass(system_type, annotation)
            or lenient_issubclass(type(system_type), annotation)
        ):
            raise ValueError(f"{annotation} cannot be used to represent {self.type}")

    def check_artifact_compatibility(self, artifact: Artifact) -> None:
        if not isinstance(artifact, self.artifact_class):
            raise ValueError(f"expected an instance of {self.artifact_class}, got {type(artifact)}")
        self._check_type_compatibility(view_type=self.type, artifact_type=artifact.type)
        if self.mode in {"READ", "READWRITE"}:
            io._read.lookup(
                type(artifact.type),
                type(artifact.format),
                list[artifact.storage.storage_partition_type],  # type: ignore[name-defined]
                type(self),
            )
        if self.mode in {"WRITE", "READWRITE"}:
            io._write.lookup(
                self.python_type,
                type(artifact.type),
                type(artifact.format),
                artifact.storage.storage_partition_type,
                type(self),
            )
