from datetime import date, datetime
from typing import Any, Optional, TypedDict, get_args, get_type_hints

import pytest

from arti.internal.type_hints import NoneType
from arti.types import (
    Date,
    Float16,
    Float32,
    Float64,
    Int8,
    Int16,
    Int32,
    Int64,
    List,
    Map,
    Null,
    String,
    Struct,
    Timestamp,
    Type,
)
from arti.types.python import PyOptional, python_type_system


def test_python_numerics() -> None:
    assert isinstance(python_type_system.to_artigraph(int), Int64)
    for int_type in (Int64, Int32, Int16, Int8):
        assert python_type_system.to_system(int_type()) is int

    assert isinstance(python_type_system.to_artigraph(float), Float64)
    for float_type in (Float64, Float32, Float16):
        assert python_type_system.to_system(float_type()) is float


def test_python_str() -> None:
    assert isinstance(python_type_system.to_artigraph(str), String)
    assert python_type_system.to_system(String()) is str


def test_python_datetime() -> None:
    assert isinstance(python_type_system.to_artigraph(datetime), Timestamp)
    assert python_type_system.to_system(Timestamp(precision="microsecond")) is datetime
    assert python_type_system.to_system(Timestamp(precision="millisecond")) is datetime
    assert python_type_system.to_system(Timestamp(precision="second")) is datetime

    assert isinstance(python_type_system.to_artigraph(date), Date)
    assert python_type_system.to_system(Date()) is date


def test_python_list() -> None:
    a = List(value_type=Int64())
    p = list[int]

    assert python_type_system.to_system(a) == p
    assert python_type_system.to_artigraph(p) == a


def test_python_map() -> None:
    a = Map(key_type=String(), value_type=Int64())
    p = dict[str, int]

    assert python_type_system.to_system(a) == p
    assert python_type_system.to_artigraph(p) == a


def test_python_null() -> None:
    assert isinstance(python_type_system.to_artigraph(NoneType), Null)
    assert python_type_system.to_system(Null()) is NoneType


@pytest.mark.parametrize(
    ["arti", "py"],
    (
        (Int64(nullable=True), Optional[int]),
        (Float64(nullable=True), Optional[float]),
    ),
)
def test_python_optional(arti: Type, py: Any) -> None:
    for (a, p) in [
        (arti, py),
        # Confirm non-null too
        (arti.copy(update={"nullable": False}), get_args(py)[0]),
    ]:
        assert python_type_system.to_system(a) == p
        assert python_type_system.to_artigraph(p) == a


def test_python_optional_priority() -> None:
    # Confirm PyOptional is first since we need to wrap all other types if applicable.
    assert tuple(python_type_system._priority_sorted_adapters)[0] is PyOptional


def test_python_struct() -> None:
    a = Struct(name="P", fields={"x": Int64()})

    class P(TypedDict):
        x: int

    # TypedDicts don't support equality (they must check on id or similar).
    P1 = python_type_system.to_system(a)
    assert P1.__name__ == P.__name__
    assert get_type_hints(P1) == {"x": int}
    assert python_type_system.to_artigraph(P) == a
