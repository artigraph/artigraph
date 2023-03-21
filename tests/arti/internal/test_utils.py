import math
import operator as op
import os
from collections.abc import Callable
from copy import deepcopy
from functools import partial
from pathlib import Path
from types import ModuleType
from typing import Any, Union, cast, get_args, get_origin

import pytest

from arti.internal.utils import (
    TypedBox,
    _int,
    class_name,
    classproperty,
    frozendict,
    get_module_name,
    import_submodules,
    int64,
    named_temporary_file,
    one_or_none,
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
            return cls.__name__  # type: ignore[attr-defined,no-any-return]

    assert Test.a == "Test"
    assert Test().a == "Test"


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
    assert {"a": 5} | frozendict(b=10) == frozendict(a=5, b=10)  # type: ignore[comparison-overlap]
    assert frozendict(a=5) | {"b": 10} == frozendict(a=5, b=10)  # type: ignore[comparison-overlap]
    assert frozendict(a=5) | frozendict(b=10) == frozendict(a=5, b=10)


def test_frozendict_typing() -> None:
    assert get_origin(frozendict[str, int]) is frozendict
    assert get_args(frozendict[str, int]) == (str, int)
    for klass in (frozendict, frozendict[str, int]):
        # Confirm deepcopying the class works:
        #     https://bugs.python.org/issue45167
        assert deepcopy(klass) == klass


def test_get_module_name() -> None:
    assert get_module_name(depth=0) == get_module_name.__module__
    assert get_module_name(depth=1) == get_module_name() == __name__
    assert get_module_name(depth=100) is None


# NOTE: We don't test the thread safety here
def test_import_submodules() -> None:
    from tests.arti.internal import import_submodules_test_modules

    basedir = Path(import_submodules_test_modules.__file__).parent
    modules = {
        "a",
        "sub.b",
        "sub.folder.c",
    }

    assert import_submodules_test_modules.entries == set()
    output = import_submodules(
        import_submodules_test_modules.__path__, import_submodules_test_modules.__name__
    )
    assert {
        name.replace(f"{import_submodules_test_modules.__name__}.", "") for name in output
    } == modules
    assert all(isinstance(mod, ModuleType) for mod in output.values())
    assert {
        str(Path(path).relative_to(basedir)).replace(".py", "")
        for path in import_submodules_test_modules.entries
    } == {name.replace(".", os.sep) for name in modules}


class _I(_int):
    pass


@pytest.mark.parametrize(
    "op",
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
    "op",
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
    "typ",
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
    "mode",
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


def test_one_or_none() -> None:
    assert one_or_none([1], item_name="num") == 1
    assert one_or_none([], item_name="num") is None
    with pytest.raises(ValueError, match="multiple num values found"):
        one_or_none([1, 2], item_name="num")


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


def test_register_priority() -> None:
    class Val:
        def __init__(self, priority: int):
            self.priority = priority

    get_priority = op.attrgetter("priority")

    reg: dict[str, Val] = {}
    x, y = Val(1), Val(1)
    register(reg, "x", x, get_priority)
    register(reg, "y", y, get_priority)
    y2 = Val(y.priority + 1)
    register(reg, "y", y2, get_priority)  # Override existing lower priority value
    register(reg, "y", y, get_priority)  # Ensure lower priority value doesn't override
    with pytest.raises(ValueError, match="is already registered"):
        register(reg, "y", Val(y2.priority), get_priority)
    assert reg == {"x": x, "y": y2}


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


def test_TypedBox_introspection() -> None:
    # We subclass the original TypedBox to set .__target_type__
    hint = TypedBox[str]
    origin, args = get_origin(hint), get_args(hint)
    assert origin is not None
    assert issubclass(origin, TypedBox)
    assert args == (str,)


def test_TypedBox_bad_hint() -> None:
    with pytest.raises(TypeError, match="TypedBox expects a single value type"):
        TypedBox[str, str]  # type: ignore[misc]


def test_TypedBox_cast() -> None:
    class Coord(BaseCoord):
        @classmethod
        def cast(cls, value: tuple[int, int]) -> "Coord":
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
