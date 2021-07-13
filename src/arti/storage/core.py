from __future__ import annotations

from typing import ClassVar, Optional

from arti.formats.core import Format
from arti.internal.models import Model, requires_subclass
from arti.types.core import Type, TypeSystem


@requires_subclass
class Storage(Model):
    type_system: ClassVar[Optional[TypeSystem]]

    def validate_artifact(self, schema: Optional[Type], format: Optional[Format]) -> None:
        # TODO: Ensure the storage supports all of the specified schema types and partitioning on
        # the specified field(s).
        pass
