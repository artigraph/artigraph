from __future__ import annotations

from typing import Any, ClassVar, Optional

from multimethod import multidispatch

from arti.artifacts.core import Artifact
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

    data: Optional[Any] = None

    # TODO write/read partitioned data, column subset

    @classmethod
    def read(cls, artifact: Artifact) -> View:
        return read(artifact.format, artifact.storage, cls())

    def write(self, artifact: Artifact) -> None:
        write(artifact.format, artifact.storage, self)


@multidispatch
def read(format: Format, storage: Storage, view: View) -> View:
    raise NotImplementedError(
        f"Read into {view} view from {format} format in {storage} storage not implemented."
    )


@multidispatch
def write(format: Format, storage: Storage, view: View) -> None:
    raise NotImplementedError(
        f"Write from {view} view into {format} format in {storage} storage not implemented."
    )
