from datetime import date, datetime

from arti.types.core import (
    Date,
    Float16,
    Float32,
    Float64,
    Int32,
    Int64,
    Null,
    String,
    Struct,
    Timestamp,
)
from arti.types.python import python


def test_python_numerics() -> None:
    assert isinstance(python.to_artigraph(int), Int64)
    for int_type in (Int64, Int32):
        assert python.to_system(int_type()) is int

    assert isinstance(python.to_artigraph(float), Float64)
    for float_type in (Float64, Float32, Float16):
        assert python.to_system(float_type()) is float


def test_python_str() -> None:
    assert isinstance(python.to_artigraph(str), String)
    assert python.to_system(String()) is str


def test_python_datetime() -> None:
    assert isinstance(python.to_artigraph(datetime), Timestamp)
    assert python.to_system(Timestamp(precision="microsecond")) is datetime

    assert isinstance(python.to_artigraph(date), Date)
    assert python.to_system(Date()) is date


def test_python_null() -> None:
    assert isinstance(python.to_artigraph(type(None)), Null)
    assert python.to_system(Null()) is type(None)  # noqa: E721


def test_python_struct() -> None:
    s = Struct(fields={"x": Int64()})
    p = {"x": int}

    assert python.to_system(s) == p
    assert python.to_artigraph(p) == s
