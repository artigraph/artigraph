__path__ = __import__("pkgutil").extend_path(__path__, __name__)  # type: ignore

from typing import Any, ClassVar

from arti.internal.models import Model
from arti.internal.utils import register
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
