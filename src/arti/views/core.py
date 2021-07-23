from __future__ import annotations

from typing import Any, ClassVar

from arti.internal.models import Model
from arti.internal.utils import register
from arti.types.core import TypeSystem

view_registry: dict[str, type[View]] = dict()


class View(Model):
    """View represents the in-memory representation of the artifact.

    Examples include pandas.DataFrame, dask.DataFrame, a BigQuery table.
    """

    __abstract__ = True

    type_system: ClassVar[TypeSystem]

    build_type: ClassVar[type]

    priority: ClassVar[int] = 0  # Set priority of this view for its build_type. Higher is better.

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if not cls.__abstract__:
            register(view_registry, cls.__name__, cls)

    @classmethod
    def match_build_type(cls, type_: Any) -> bool:
        return type_ is cls.build_type
