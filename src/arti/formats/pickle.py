from arti.formats import Format
from arti.types.python import python_type_system


class Pickle(Format):
    type_system = python_type_system
