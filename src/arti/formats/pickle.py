from __future__ import annotations

from arti.formats import Format
from arti.types.python import python_type_system


class Pickle(Format):
    extension = ".pickle"
    type_system = python_type_system
