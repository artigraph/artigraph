from __future__ import annotations

import math
import operator as op
from collections.abc import Callable
from functools import partial
from typing import Any, Union, cast

import pytest

from arti.internal.utils import (
    TypedBox,
    _int,
    class_name,
    classproperty,
    int64,
    named_temporary_file,
    ordinal,
    qname,
    register,
    uint64,
)


def test_class_name() -> None:
    class Test:
        key = class_name()

    assert Test.key == "Test"
    assert Test().key == "Test"


def test_classproperty() -> None:
    class Test:
        @classproperty
        def a(cls) -> str:
            return cls.__name__  # type: ignore

        @classproperty
        @classmethod
        def b(cls) -> str:
            return cls.__name__

    assert Test.a == "Test"
    assert Test().a == "Test"
    assert Test.b == "Test"
    assert Test().b == "Test"


class _I(_int):
    pass


@pytest.mark.parametrize(
    ["op"],
    (
        (op.add,),
        (op.and_,),
        (op.floordiv,),
        (op.lshift,),
        (op.mod,),
        (op.mul,),
        (op.or_,),
        (op.rshift,),
        (op.sub,),
        (op.xor,),
    ),
)
def test__int_binary(op: Callable[..., Any]) -> None:
    i, _i = 123, _I(123)
    left = op(_i, i)
    right = op(i, _i)
    assert left == right
    assert isinstance(left, _I)
    assert isinstance(right, _I)


@pytest.mark.parametrize(
    ["op"],
    (
        (math.ceil,),
        (math.floor,),
        (math.trunc,),
        (op.invert,),
        (op.neg,),
        (op.pos,),
        (partial(round, ndigits=-1),),
    ),
)
def test__int_unary(op: Callable[..., Any]) -> None:
    output, expected = op(_I(123)), op(123)
    assert output == expected
    assert isinstance(output, _I)


def test__int_repr() -> None:
    assert repr(_I(5)) == "_I(5)"


@pytest.mark.parametrize(
    ["typ"],
    (
        (int64,),
        (uint64,),
    ),
)
def test_sizedint(typ: type[Union[int64, uint64]]) -> None:
    low, high = typ(typ._min), typ(typ._max)

    assert low == typ._min
    assert high == typ._max

    with pytest.raises(ValueError):
        low - 1

    with pytest.raises(ValueError):
        high + 1


def test_sizedint_cast() -> None:
    assert int64(uint64(18446744073709551611)) == int64(-5)
    assert int64(uint64(5)) == int64(5)
    assert uint64(int64(-5)) == uint64(18446744073709551611)
    assert uint64(int64(5)) == uint64(5)


@pytest.mark.parametrize(
    ["mode"],
    (
        ("w+",),
        ("wb",),
    ),
)
def test_named_temporary_file(mode: str) -> None:
    with named_temporary_file(mode=mode) as f:
        assert f.mode == mode
        with open(f.name, mode=f.mode) as f1:
            assert f1


def test_ordinal() -> None:
    assert ordinal(0) == "0th"
    assert ordinal(3) == "3rd"
    assert ordinal(122) == "122nd"
    assert ordinal(213) == "213th"


def test_register() -> None:
    reg: dict[str, int] = {}

    register(reg, "x", 5)
    register(reg, "y", 5)
    assert reg == {"x": 5, "y": 5}
    with pytest.raises(ValueError, match="is already registered"):
        register(reg, "x", 10)


def test_qname() -> None:
    assert qname(TypedBox) == "TypedBox"
    assert qname(TypedBox()) == "TypedBox"


class BaseCoord:
    def __init__(self, x: int, y: int):
        self.x, self.y = (x, y)


def test_TypedBox() -> None:
    class Coord(BaseCoord):
        pass

    CoordBox = TypedBox[Coord]

    box = CoordBox({"home": Coord(1, 1)})
    assert (box.home.x, box.home.y) == (1, 1)
    with pytest.raises(ValueError, match="home is already set!"):
        box.home = Coord(0, 0)
    box.work = Coord(2, 2)
    with pytest.raises(TypeError, match="Expected an instance of Coord"):
        box.school = (3, 3)


def test_TypedBox_cast() -> None:
    class Coord(BaseCoord):
        @classmethod
        def cast(cls, value: tuple[int, int]) -> Coord:
            return cls(*value)

    CoordBox = TypedBox[Coord]

    home = Coord(1, 1)
    box = CoordBox(
        {
            "home": home,  # Existing values will be used directly
            "work": (2, 2),  # (2, 2) -> Coord.cast((2, 2))
        }
    )
    box.school = cast(Coord, (3, 3))
    assert box.home is home
    assert (box.home.x, box.home.y) == (1, 1)
    assert (box.work.x, box.work.y) == (2, 2)
    assert (box.school.x, box.school.y) == (3, 3)


def test_TypedBox_bad_cast() -> None:
    class Coord(BaseCoord):
        @classmethod
        def cast(cls, value: tuple[int, int]) -> tuple[int, int]:  # Should return a Coord
            return value

    CoordBox = TypedBox[Coord]

    box = CoordBox()
    box.home = Coord(1, 1)
    with pytest.raises(TypeError, match=r"Expected .*\.cast.* to return"):
        box.work = (2, 2)
