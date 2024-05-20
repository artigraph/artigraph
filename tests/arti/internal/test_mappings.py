from __future__ import annotations

import copy
import pickle
from typing import Any, get_args, get_origin

import pytest

from arti.internal.mappings import InvalidKeyError, TypedBox, frozendict


def test_frozendict() -> None:
    # Test input variations
    assert frozendict([("a", 5)])
    assert frozendict([("a", 5)], b=10)
    assert frozendict(a=5)
    assert frozendict(a=5, b=10)
    assert frozendict(a=5, b=frozendict(b=10))
    assert frozendict({"a": 5})
    assert frozendict({"a": 5}, b=10)
    with pytest.raises(TypeError, match="unhashable type: 'dict'"):
        frozendict(a=5, b={"b": 10})


def test_frozendict_immutability() -> None:
    val = frozendict({"x": 5})
    with pytest.raises(TypeError, match="does not support item assignment"):
        val["x"] = 10  # type: ignore[index]
    with pytest.raises(TypeError, match="does not support item assignment"):
        val["y"] = 10  # type: ignore[index]
    with pytest.raises(TypeError, match="does not support item deletion"):
        del val["x"]  # type: ignore[attr-defined]


def test_frozendict_hash() -> None:
    assert hash(frozendict(a=5)) == hash(frozenset((("a", 5),)))


def test_frozendict_union() -> None:
    assert {"a": 5} | frozendict(b=10) == frozendict(a=5, b=10)
    assert frozendict(a=5) | {"b": 10} == frozendict(a=5, b=10)
    assert frozendict(a=5) | frozendict(b=10) == frozendict(a=5, b=10)


def test_frozendict_typing() -> None:
    assert get_origin(frozendict[str, int]) is frozendict
    assert get_args(frozendict[str, int]) == (str, int)
    for klass in (frozendict, frozendict[str, int]):
        # Confirm deepcopying the class works:
        #     https://bugs.python.org/issue45167
        assert copy.deepcopy(klass) == klass


class Coord:
    def __init__(self, x: int, y: int):
        self.x, self.y = (x, y)

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, type(self)) and (self.x, self.y) == (other.x, other.y)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.x}, {self.y})"

    def __hash__(self) -> int:
        return hash((self.x, self.y))


coord = Coord(1, 1)


# The class must:
# - be defined globally to allow `pickle`ing.
# - subclass the subscripted TypedBox to allow `isinstance` checks.
class CoordBox(TypedBox[Coord]):
    pass


def _validate_box_copy(box: TypedBox) -> None:
    dup = copy.copy(box)
    assert dup == box
    assert dup is not box
    assert dup._status is box._status

    dup = copy.deepcopy(box)
    assert dup == box
    assert dup is not box
    assert dup._status is not box._status
    assert dup._status == box._status

    dup = pickle.loads(pickle.dumps(box))
    assert dup == box
    assert dup is not box
    assert dup._status is not box._status
    assert dup._status == box._status


def test_TypedBox() -> None:
    with pytest.raises(TypeError, match="TypedBox expects a single value type"):
        TypedBox[str, str]  # type: ignore[misc]

    assert CoordBox.__target_type__ is Coord

    box = CoordBox({"home": coord}, work=coord)
    assert box.home == coord
    assert box.work == coord
    assert len(box) == 2
    assert "home" in box
    assert "work" in box

    box.school = coord
    assert box.school == coord
    assert len(box) == 3

    with pytest.raises(ValueError, match="home is already set"):
        box.home = coord

    with pytest.raises(TypeError, match="Expected an instance of Coord"):
        box.bad = (3, 3)

    with pytest.raises(ValueError, match="CoordBox is still open and cannot be hashed"):
        hash(box)

    box._status.root = "closed"

    assert hash(box)

    with pytest.raises(ValueError, match="CoordBox is frozen."):
        box.shop = coord

    assert len(box) == 3  # Ensure we didn't add any keys
    _validate_box_copy(box)


def test_TypedBox_contains() -> None:
    box = CoordBox()
    assert "a" not in box
    assert "a.b.c" not in box
    assert "a" not in box  # Confirm check nested keys don't add
    assert len(box) == 0

    box.a = Coord(1, 1)
    box.x.y.z = Coord(1, 1)
    assert "a" in box
    assert "x" in box
    assert "x.y" in box
    assert "x.y.z" in box


def test_TypedBox_InvalidKeyError() -> None:
    box = CoordBox()
    with pytest.raises(InvalidKeyError):
        box["_dne"]
    with pytest.raises(InvalidKeyError):
        box["x._dne"]
    with pytest.raises(InvalidKeyError):
        box["x._dne"] = coord
    with pytest.raises(InvalidKeyError):
        box._dne
    with pytest.raises(InvalidKeyError):
        box.x._dne
    with pytest.raises(InvalidKeyError):
        box.x._dne = coord


def test_TypedBox_nesting() -> None:
    box = CoordBox(
        {
            "init": {"dict": {"child": {"coord": coord}}},
            "init.key.child.coord": coord,
        }
    )
    box.post.attribute.child.coord = coord  # pyright: ignore[reportAttributeAccessIssue]
    box["post"] = {"dict": {"child": {"coord": coord}}}
    box["post.key.child.coord"] = coord

    for namespace in (
        "init.dict.child",
        "init.key.child",
        "post.attribute.child",
        "post.dict.child",
        "post.key.child",
    ):
        box._status.root = "closed"  # Close to prevent accidental additions

        # Verify `["a.b.c"]`
        dot_key = box[f"{namespace}.coord"]
        assert dot_key == coord

        # Verify `.a.b.c` and `["a"]["b"]["c"]`
        attr_branch, key_branch = box, box
        for part in namespace.split("."):
            attr_branch = getattr(attr_branch, part)
            assert isinstance(attr_branch, CoordBox)

            key_branch = key_branch[part]
            assert isinstance(key_branch, CoordBox)

        assert attr_branch is key_branch
        assert attr_branch.__target_type__ == Coord
        box._status.root = "open"
        assert attr_branch._status == "open"
        box._status.root = "closed"
        assert attr_branch._status == "closed"
        _validate_box_copy(attr_branch)

        chain_attr, chain_key = attr_branch.coord, key_branch["coord"]
        assert isinstance(chain_attr, Coord)
        assert isinstance(chain_key, Coord)
        assert chain_attr == chain_key == coord


class CastCoord(Coord):
    @classmethod
    def cast(cls, value: Any) -> Any:
        if isinstance(value, tuple):
            return cls(*value)
        return value  # Don't handle unknown values and let the box handle it.


cast_coord = CastCoord(2, 2)


class CastCoordBox(TypedBox[CastCoord]):
    pass


def test_TypedBox_cast() -> None:
    box = CastCoordBox(
        {  # pyright: ignore[reportArgumentType]
            "home": cast_coord,  # Existing values will be used directly
            "work": (cast_coord.x, cast_coord.y),  # (2, 2) -> Coord.cast((2, 2))
        }
    )
    box.school = (cast_coord.x, cast_coord.y)

    assert box.home == cast_coord
    assert box.work == cast_coord
    assert box.school == cast_coord

    with pytest.raises(TypeError, match=r"Expected .*\.cast.* to return"):
        box.junk = "junk"
