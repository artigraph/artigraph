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

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if not cls.__abstract__:
            register(view_registry, cls.__name__, cls)
