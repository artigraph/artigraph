from __future__ import annotations

from arti.formats import Format
from arti.types.python import python_type_system


class JSON(Format):
    extension: str = ".json"
    # Perhaps we narrow down a json_type_system with the subset of supported types + a way to hook
    # into json.JSON{De,En}coders.
    #
    # Eg: Binary isn't supported out of the box.
    type_system = python_type_system
