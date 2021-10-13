from arti.formats import Format
from arti.types.python import python_type_system


class JSON(Format):
    extension: str = "json"
    # Perhaps we narrow down a json_type_system with the subset of supported types + a way to hook
    # into json.JSON{De,En}coders.
    type_system = python_type_system
