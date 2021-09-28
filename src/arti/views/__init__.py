__path__ = __import__("pkgutil").extend_path(__path__, __name__)  # type: ignore

from typing import Annotated, Any, ClassVar, get_args, get_origin

from arti.internal.models import Model
from arti.internal.type_hints import lenient_issubclass
from arti.internal.utils import import_submodules, register
from arti.types import TypeSystem


class View(Model):
    """View represents the in-memory representation of the artifact.

    Examples include pandas.DataFrame, dask.DataFrame, a BigQuery table.
    """

    _abstract_ = True
    _by_python_type_: ClassVar[dict[type, type["View"]]] = {}

    priority: ClassVar[int] = 0  # Set priority of this view for its python_type. Higher is better.
    python_type: ClassVar[type]
    type_system: ClassVar[TypeSystem]

    @classmethod
    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if not cls._abstract_:
            register(cls._by_python_type_, cls.python_type, cls, lambda x: x.priority)

    @classmethod
    def get_class_for(cls, annotation: Any) -> type[View]:
        import_submodules(__path__, __name__)

        origin, args = get_origin(annotation), get_args(annotation)
        if origin is Annotated:
            annotation, *hints = args
            views: list[type[View]] = [hint for hint in hints if lenient_issubclass(hint, View)]
            if len(views) == 0:
                return cls.get_class_for(annotation)
            if len(views) == 1:
                return views[0]
            raise ValueError("multiple Views set")
        if origin is None:
            origin = annotation
        if origin not in cls._by_python_type_:
            raise ValueError(
                f"{annotation} cannot be matched to a View, try setting one explicitly (eg: `Annotated[pd.DataFrame, MyArtifact, PandasDataFrame]`)"
            )
        return cls._by_python_type_[origin]
