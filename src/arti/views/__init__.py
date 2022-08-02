__path__ = __import__("pkgutil").extend_path(__path__, __name__)
from typing import Any, ClassVar, Optional, get_args, get_origin

from arti.internal.models import Model
from arti.internal.type_hints import get_item_from_annotated, is_Annotated, lenient_issubclass
from arti.internal.utils import import_submodules, register
from arti.types import Type, TypeSystem


class View(Model):
    """View represents the in-memory representation of the artifact.

    Examples include pandas.DataFrame, dask.DataFrame, a BigQuery table.
    """

    _abstract_ = True
    _by_python_type_: "ClassVar[dict[type, type[View]]]" = {}

    priority: ClassVar[int] = 0  # Set priority of this view for its python_type. Higher is better.
    python_type: ClassVar[type]
    type_system: ClassVar[TypeSystem]

    @classmethod
    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if not cls._abstract_:
            register(cls._by_python_type_, cls.python_type, cls, lambda x: x.priority)

    @classmethod
    def from_annotation(cls, annotation: Any, *, validation_type: Optional[Type] = None) -> "View":
        import_submodules(__path__, __name__)

        original = annotation
        view = get_item_from_annotated(annotation, cls, is_subclass=False)
        if view is None:
            if is_Annotated(annotation):
                # We've already searched for a View instance in the original Annotated
                # args, so just extract the root annotation.
                annotation, *_ = get_args(annotation)
            # If the type is Generic, we want to unwrap any extra type variables - the view registry
            # (currently) only matches on the base.
            view_class = cls._by_python_type_.get(get_origin(annotation) or annotation)
            if view_class is None:
                raise ValueError(
                    f"{original} cannot be matched to a View, try setting one explicitly (eg: `Annotated[int, arti.views.python.Int()]`)"
                )
            view = view_class()
        if validation_type is not None:
            view.check_type_similarity(arti=validation_type, python_type=annotation)
        return view

    def check_type_similarity(self, *, arti: Type, python_type: type) -> None:
        system_type = self.type_system.to_system(arti, hints={})
        if not (
            lenient_issubclass(system_type, python_type)
            or lenient_issubclass(type(system_type), python_type)
        ):
            raise ValueError(f"{python_type} cannot be used to represent {arti}")
