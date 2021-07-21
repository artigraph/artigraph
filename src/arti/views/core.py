from __future__ import annotations

from typing import Any, ClassVar

from multimethod import multidispatch

from arti.formats.core import Format
from arti.internal.models import Model
from arti.storage.core import Storage
from arti.types.core import TypeSystem


class View(Model):
    """View represents the in-memory representation of the artifact.

    Examples include pandas.DataFrame, dask.DataFrame, a BigQuery table.
    """

    __abstract__ = True

    type_system: ClassVar[TypeSystem]


# TODO write/read partitioned data, column subset


@multidispatch
def read(format: Format, storage: Storage, view: View) -> View:
    raise NotImplementedError(
        f"Read into {view} view from {format} format in {storage} storage not implemented."
    )


@multidispatch
def write(data: Any, format: Format, storage: Storage, view: View) -> None:
    raise NotImplementedError(
        f"Write from {view} view into {format} format in {storage} storage not implemented."
    )
