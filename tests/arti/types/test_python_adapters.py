import re
from datetime import date, datetime
from typing import Any, Literal, Optional, TypedDict, Union, get_args, get_type_hints

import pytest

from arti import Type
from arti.internal.type_hints import NoneType
from arti.types import (
    Boolean,
    Collection,
    Date,
    Enum,
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
    Set,
    String,
    Struct,
    Timestamp,
)
from arti.types.python import PyLiteral, PyOptional, PyTuple, python_type_system


def test_python_bool() -> None:
    assert isinstance(python_type_system.to_artigraph(bool, hints={}), Boolean)
    assert python_type_system.to_system(Boolean(), hints={}) is bool


def test_python_numerics() -> None:
    assert isinstance(python_type_system.to_artigraph(int, hints={}), Int64)
    for int_type in (Int64, Int32, Int16, Int8):
        assert python_type_system.to_system(int_type(), hints={}) is int

    assert isinstance(python_type_system.to_artigraph(float, hints={}), Float64)
    for float_type in (Float64, Float32, Float16):
        assert python_type_system.to_system(float_type(), hints={}) is float


def test_python_str() -> None:
    assert isinstance(python_type_system.to_artigraph(str, hints={}), String)
    assert python_type_system.to_system(String(), hints={}) is str


def test_python_datetime() -> None:
    assert isinstance(python_type_system.to_artigraph(datetime, hints={}), Timestamp)
    assert python_type_system.to_system(Timestamp(precision="microsecond"), hints={}) is datetime
    assert python_type_system.to_system(Timestamp(precision="millisecond"), hints={}) is datetime
    assert python_type_system.to_system(Timestamp(precision="second"), hints={}) is datetime

    assert isinstance(python_type_system.to_artigraph(date, hints={}), Date)
    assert python_type_system.to_system(Date(), hints={}) is date


def test_python_frozenset() -> None:
    a = Set(element=Int64())
    p = frozenset[int]

    assert python_type_system.to_system(a, hints={}) == set[int]  # set has higher priority
    assert python_type_system.to_artigraph(p, hints={}) == a


def test_python_list() -> None:
    a = List(element=Int64())
    p = list[int]

    assert python_type_system.to_system(a, hints={}) == p
    assert python_type_system.to_artigraph(p, hints={}) == a
    # Confirm we can convert Collections to a list (NOTE: round trip still goes to List)
    assert python_type_system.to_system(Collection(element=Int64()), hints={}) == p


def test_python_literal() -> None:
    # Order shouldn't matter
    a = Enum(type=Int64(), items=(1, 2, 3))
    p = Literal[3, 2, 1]

    assert python_type_system.to_system(a, hints={}) == p
    assert python_type_system.to_artigraph(p, hints={}) == a
    assert python_type_system.to_artigraph(Literal[1.0, 2.0], hints={}) == Enum(
        type=Float64(), items=(1.0, 2.0)
    )
    # Check for Union+Literal combos
    assert python_type_system.to_artigraph(Union[Literal[1], Literal[2, 3]], hints={}) == a
    # Optional uses a Union as well, so add a few extra checks
    nullable_a = a.copy(update={"nullable": True})
    assert python_type_system.to_artigraph(Optional[Literal[1, 2, 3]], hints={}) == nullable_a
    assert python_type_system.to_artigraph(Union[Literal[1, 2, 3], None], hints={}) == nullable_a


def test_python_literal_errors() -> None:
    with pytest.raises(ValueError, match="All Literals must be the same type"):
        assert python_type_system.to_artigraph(Literal[1, 1.0], hints={})
    with pytest.raises(NotImplementedError, match="Invalid Literal with no values"):
        PyLiteral.to_artigraph(Literal[()], hints={}, type_system=python_type_system)
    # Confirm other Unions aren't handled
    for invalid_hint in (Union[int, str], Union[int, Literal[1]]):
        with pytest.raises(NotImplementedError, match="No TypeSystem.* adapter for system type"):
            assert python_type_system.to_artigraph(invalid_hint, hints={})
    # This path shouldn't normally be accessible (ie: `match_system` should guard against it, as
    # above), but are there for extra safety.
    with pytest.raises(
        NotImplementedError,
        match=re.escape("Only Union[Literal[...], ...] (enums) are currently supported"),
    ):
        PyLiteral.to_artigraph(Union[int, str], hints={}, type_system=python_type_system)


def test_python_map() -> None:
    a = Map(key=String(), value=Int64())
    p = dict[str, int]

    assert python_type_system.to_system(a, hints={}) == p
    assert python_type_system.to_artigraph(p, hints={}) == a


def test_python_null() -> None:
    assert isinstance(python_type_system.to_artigraph(NoneType, hints={}), Null)
    assert python_type_system.to_system(Null(), hints={}) is NoneType


@pytest.mark.parametrize(
    ["arti", "py"],
    (
        (Int64(nullable=True), Optional[int]),
        (Float64(nullable=True), Optional[float]),
        (Enum(type=Int64(), items=(1, 2, 3), nullable=True), Optional[Literal[1, 2, 3]]),
    ),
)
def test_python_optional(arti: Type, py: Any) -> None:
    for a, p in [
        (arti, py),
        # Confirm non-null too
        (arti.copy(update={"nullable": False}), get_args(py)[0]),
    ]:
        assert python_type_system.to_system(a, hints={}) == p
        assert python_type_system.to_artigraph(p, hints={}) == a


def test_python_optional_priority() -> None:
    # Confirm PyOptional is first since we need to wrap all other types if applicable.
    assert tuple(python_type_system._priority_sorted_adapters)[0] is PyOptional


def test_python_set() -> None:
    a = Set(element=Int64())
    p = set[int]

    assert python_type_system.to_system(a, hints={}) == p
    assert python_type_system.to_artigraph(p, hints={}) == a


def test_python_struct() -> None:
    a = Struct(name="P", fields={"x": Int64()})

    class P(TypedDict):
        x: int

    # TypedDicts don't support equality (they must check on id or similar).
    P1 = python_type_system.to_system(a, hints={})
    assert P1.__name__ == P.__name__
    assert get_type_hints(P1) == {"x": int}
    assert python_type_system.to_artigraph(P, hints={}) == a


def test_python_tuple() -> None:
    a = List(element=Int64())
    p = tuple[int, ...]

    assert python_type_system.to_system(a, hints={}) == list[int]  # list has higher priority
    assert PyTuple.to_system(List(element=Int64()), hints={}, type_system=python_type_system) == p
    assert python_type_system.to_artigraph(p, hints={}) == a

    assert (
        PyTuple.to_system(Collection(element=Int64()), hints={}, type_system=python_type_system)
        == p
    )

    # We don't (currently) support structure based tuples
    with pytest.raises(NotImplementedError):
        python_type_system.to_artigraph(tuple[str, int], hints={})  # type: ignore[misc]
