from __future__ import annotations

from datetime import date, datetime

from arti.types.python import python_type_system
from arti.views import View


class PythonBuiltin(View):
    _abstract_ = True

    type_system = python_type_system


class Date(PythonBuiltin):
    python_type = date


class Datetime(PythonBuiltin):
    python_type = datetime


class Dict(PythonBuiltin):
    python_type = dict


class Float(PythonBuiltin):
    python_type = float


class Int(PythonBuiltin):
    python_type = int


class List(PythonBuiltin):
    python_type = list


class Null(PythonBuiltin):
    python_type = None


class Str(PythonBuiltin):
    python_type = str
