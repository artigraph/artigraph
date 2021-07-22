from __future__ import annotations

from typing import ClassVar

from arti.internal.models import Model
from arti.types.core import TypeSystem


class View(Model):
    """View represents the in-memory representation of the artifact.

    Examples include pandas.DataFrame, dask.DataFrame, a BigQuery table.
    """

    __abstract__ = True

    type_system: ClassVar[TypeSystem]
