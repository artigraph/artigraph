from __future__ import annotations

from typing import Any, ClassVar

from arti.internal.models import Model
from arti.internal.utils import register
from arti.types.core import TypeSystem


class View(Model):
    """View represents the in-memory representation of the artifact.

    Examples include pandas.DataFrame, dask.DataFrame, a BigQuery table.
    """

    _abstract_ = True
    _registry_: ClassVar[dict[type, type[View]]] = dict()

    priority: ClassVar[int] = 0  # Set priority of this view for its python_type. Higher is better.
    python_type: ClassVar[type]
    type_system: ClassVar[TypeSystem]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if not cls._abstract_:
            register(cls._registry_, cls.python_type, cls, lambda x: x.priority)
