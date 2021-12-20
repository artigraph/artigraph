__path__ = __import__("pkgutil").extend_path(__path__, __name__)
from typing import Annotated, Any, ClassVar, Optional, get_args, get_origin

from arti.internal.models import Model
from arti.internal.type_hints import lenient_issubclass
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
    def get_class_for(
        cls, annotation: Any, *, validation_type: Optional[Type] = None
    ) -> type["View"]:
        import_submodules(__path__, __name__)

        view = None

        origin, args = get_origin(annotation), get_args(annotation)
        if origin is Annotated:
            annotation, *hints = args
            views: list[type[View]] = [hint for hint in hints if lenient_issubclass(hint, View)]
            if len(views) == 0:
                return cls.get_class_for(annotation, validation_type=validation_type)
            if len(views) == 1:
                view = views[0]
            else:
                raise ValueError("multiple Views set")
        if view is None:
            if origin is None:
                origin = annotation
            if origin not in cls._by_python_type_:
                raise ValueError(
                    f"{annotation} cannot be matched to a View, try setting one explicitly (eg: `Annotated[pd.DataFrame, MyArtifact, PandasDataFrame]`)"
                )
            view = cls._by_python_type_[origin]
        if validation_type is not None:
            view.check_type_similarity(arti=validation_type, python_type=annotation)
        return view

    @classmethod
    def check_type_similarity(cls, *, arti: Type, python_type: type) -> None:
        system_type = cls.type_system.to_system(arti, hints={})
        if not (
            lenient_issubclass(system_type, python_type)
            or lenient_issubclass(type(system_type), python_type)
        ):
            raise ValueError(f"{python_type} cannot be used to represent {arti}")
