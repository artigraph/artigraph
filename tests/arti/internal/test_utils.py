from __future__ import annotations

from typing import cast

import pytest

from arti.internal.utils import TypedBox, class_name, classproperty, ordinal, register


def test_class_name() -> None:
    class Test:
        key = class_name()

    assert Test.key == "Test"
    assert Test().key == "Test"


def test_classproperty() -> None:
    class Test:
        @classproperty
        def hi(cls) -> str:
            return "hi"

    assert Test.hi == "hi"
    assert Test().hi == "hi"


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
