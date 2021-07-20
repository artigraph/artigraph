from __future__ import annotations

from typing import ClassVar, Optional

from multimethod import multimethod

from arti.artifacts.core import Artifact
from arti.formats.core import Format
from arti.internal.models import Model
from arti.storage.core import Storage


class View(Model):
    """View represents the in-memory representation of the artifact.

    Examples include pandas.DataFrame, dask.DataFrame, a BigQuery table.
    """

    __abstract__ = True
    type_system: ClassVar[TypeSystem]

    data: Optional[Any] = None

    # TODO write/read partitioned data, column subset

    @classmethod
    def read(cls, artifact: Artifact):
        return read(cls(), artifact.format, artifact.storage)

    def write(self, artifact: Artifact):
        return write(self, artifact.format, artifact.storage)

    def validate(self, schema: Optional[Type]) -> None:
        # TODO: eventually producer will validate
        pass


@multimethod
def read(view: View, format: Format, storage: Storage):
    ...


@multimethod
def write(view: View, format: Format, storage: Storage):
    ...
