from __future__ import annotations

from typing import ClassVar, Optional, Union

from arti.formats.core import Format
from arti.types.core import Type, TypeSystem


class Storage:
    type_system: ClassVar[Optional[TypeSystem]]

    def validate(self, schema: Optional[Type], format: Optional[Format]) -> None:
        # TODO: Ensure the storage supports all of the specified schema types and partitioning on
        # the specified field(s).
        pass

    def __init__(self, storage_type: Optional[str] = None, path: Optional[str] = None) -> None:
        self.type = storage_type
        self.path = path

    @classmethod
    def from_dict(cls, storage_dict: dict[str, str]) -> Storage:
        if "type" not in storage_dict or "path" not in storage_dict:
            raise ValueError(
                f'Missing a required "type" and/or "path" key in the Storage dict. Available keys: {storage_dict.keys()}'
            )
        return cls(storage_dict["type"], storage_dict["path"])

    def to_dict(self) -> dict[str, Union[str, None]]:
        return {"type": self.type, "path": self.path}
