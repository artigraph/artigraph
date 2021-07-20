from __future__ import annotations

from typing import Any, ClassVar, Optional

from multimethod import multimethod

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
    type_system: ClassVar[TypeSystem] = None
    data: Optional[Any] = None

    # TODO write/read partitioned data, column subset

    @classmethod
    def read(cls, artifact: Artifact) -> View:
        return read(cls(), artifact.format, artifact.storage)

    def write(self, artifact: Artifact) -> None:
        write(self, artifact.format, artifact.storage)


@multimethod
def read(view: View, format: Format, storage: Storage) -> View:
    ...  # pragma: no cover


@multimethod
def write(view: View, format: Format, storage: Storage) -> None:
    ...  # pragma: no cover
