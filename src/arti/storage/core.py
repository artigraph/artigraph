from __future__ import annotations

from typing import Optional

from arti.formats.core import Format
from arti.internal.models import Model
from arti.types.core import Type


class Storage(Model):
    _abstract_ = True

    def validate_artifact(self, schema: Optional[Type], format: Optional[Format]) -> None:
        # TODO: Ensure the storage supports all of the specified schema types and partitioning on
        # the specified field(s).
        pass
