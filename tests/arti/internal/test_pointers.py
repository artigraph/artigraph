from __future__ import annotations

from typing import Optional, no_type_check

import pytest

from arti.internal.pointers import Pointer, TypedProxy


class BaseCoord:
    def __init__(self, x: int, y: int):
        self.x, self.y = (x, y)


class BaseCoordProxy(TypedProxy[BaseCoord]):  # type: ignore
    __target_type__ = BaseCoord


class Coord(Pointer, BaseCoord):
    @classmethod
    def cast(cls, value: tuple[int, int]) -> Coord:
        return cls(*value)


def check_proxy(
    coord: BaseCoord, pts: tuple[int, int], base: type[BaseCoord] = Coord, id_: Optional[int] = None
) -> None:
    assert (coord.x, coord.y) == pts
    assert isinstance(coord, base)
    assert isinstance(coord, TypedProxy)
    assert isinstance(coord.__wrapped__, base)
    assert not isinstance(coord.__wrapped__, TypedProxy)
    if id_:
        assert id(coord) == id_


@no_type_check
def test_Pointer() -> None:
    assert Coord.__proxy_type__.__target_type__ is Coord
    assert Coord.box.__proxy_type__ is Coord.__proxy_type__
    assert Coord.__proxy_type__.__name__ == "CoordProxy"

    # Test the automatic instance -> proxy conversion
    coord = Coord(5, 5)
    check_proxy(coord, (5, 5))
    # Ensure __wrapped__ can accept a direct Coord
    coord.__wrapped__ = Coord(10, 10).__wrapped__
    check_proxy(coord, (10, 10))
    # Ensure __wrapped__ can accept an existing proxy
    coord.__wrapped__ = Coord(10, 10)
    check_proxy(coord, (10, 10))
    # Ensure __wrapped__ can Coord.cast other values
    coord.__wrapped__ = (15, 15)  # Uses Coord.cast
    check_proxy(coord, (15, 15))


def test_Pointer_no_cast() -> None:
    class StrictCoord(Pointer, BaseCoord):
        pass

    coord = StrictCoord(5, 5)
    check_proxy(coord, (5, 5), base=StrictCoord)

    with pytest.raises(TypeError, match="Expected an instance of"):
        coord.__wrapped__ = (10, 10)  # type: ignore


def test_Pointer_bad_cast() -> None:
    class BadCastCoord(Pointer, BaseCoord):
        @staticmethod
        def cast(value: tuple[int, int]) -> tuple[int, int]:  # Should return a Coord
            return value

    coord = BadCastCoord(5, 5)
    check_proxy(coord, (5, 5), BadCastCoord)

    with pytest.raises(TypeError, match=r"Expected .*\.cast.* to return"):
        coord.__wrapped__ = (5, 5)  # type: ignore


def test_Pointer_subclassing() -> None:
    assert type(Coord(5, 5)).__name__ == "CoordProxy"

    # By default, grandchildren+ Pointer subclasses use the immediate subclass's proxy.
    class Coord1(Coord):
        pass

    assert type(Coord1(5, 5)).__name__ == "CoordProxy"

    class Coord2(Coord):
        __configure_subclass_proxy__ = True

    assert type(Coord2(5, 5)).__name__ == "Coord2Proxy"


def test_PointerBox() -> None:
    # Converts (5, 5) -> Coord.cast((5, 5)) -> Coord(5, 5) -> CoordProxy(Coord(5, 5))
    box = Coord.box({"home": (5, 5)})
    home_id = id(box.home)
    check_proxy(box.home, (5, 5))

    box.home = Coord(10, 10)
    check_proxy(box.home, (10, 10), id_=home_id)

    box.home = (15, 15)
    check_proxy(box.home, (15, 15), id_=home_id)  # type: ignore

    box = Coord.box(box)
    check_proxy(box.home, (15, 15), id_=home_id)
