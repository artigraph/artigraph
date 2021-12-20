import math
import operator as op
import os
import re
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
    dispatch,
    frozendict,
    import_submodules,
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


class A:
    pass


class A1(A):
    pass


class B:
    pass


class B1(B):
    pass


def test_dispatch() -> None:
    @dispatch
    def dispatch_test(a: A, b: B) -> Any:
        return "good_a_b"

    @dispatch_test.register
    def good_a_b1(a: A, b: B1) -> Any:
        return "good_a_b1"

    @dispatch_test.register
    def good_a1_b(a: A1, b: B) -> Any:
        return "good_a1_b"

    # Check that the non-annotated registration works
    @dispatch_test.register(A1, B1)
    def good_a1_b1(a, b) -> Any:  # type: ignore
        return "good_a1_b1"

    with pytest.raises(
        TypeError,
        match=re.escape("Expected `bad_name` to have ['a', 'b'] parameters, got ['a']"),
    ):

        @dispatch_test.register
        def bad_name(a: int) -> Any:
            return a

    with pytest.raises(
        TypeError,
        match="Expected the `bad_param_kind.a` parameter to be POSITIONAL_OR_KEYWORD, got KEYWORD_ONLY",
    ):

        @dispatch_test.register
        def bad_param_kind(*, a: A, b: B) -> Any:
            return a, b

    with pytest.raises(
        TypeError,
        match="Expected the `bad_type.a` parameter to be a subclass of <class 'test_utils.A'>, got <class 'int'>",
    ):

        @dispatch_test.register
        def bad_type(a: int, b: str) -> Any:
            return a, b

    with pytest.raises(
        TypeError,
        match=re.escape("Expected the `bad` return to match"),
    ):

        @dispatch
        def ok(a: A) -> str:
            return "good_a_b"

        @ok.register
        def bad(a: A1) -> int:
            return 5

    assert dispatch_test(A(), B()) == "good_a_b"
    assert dispatch_test(A(), B1()) == "good_a_b1"
    assert dispatch_test(A1(), B()) == "good_a1_b"
    assert dispatch_test(A1(), B1()) == "good_a1_b1"
    # Check that a bad one didn't get registered
    with pytest.raises(TypeError):
        assert dispatch_test(5, "")


def test_frozendict() -> None:
    for klass in (
        frozendict,
        frozendict[str, int],
    ):
        # Confirm deepcopying the class works:
        #     https://bugs.python.org/issue45167
        assert deepcopy(klass) == klass
        val = klass(x=5)
        assert isinstance(val, frozendict)
        assert val == {"x": 5}  # type: ignore
        with pytest.raises(TypeError, match="doesn't support item assignment"):
            val["x"] = 10  # type: ignore
        with pytest.raises(TypeError, match="doesn't support item assignment"):
            val["y"] = 10  # type: ignore

    assert get_origin(frozendict[str, int]) is frozendict
    assert get_args(frozendict[str, int]) == (str, int)


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

    CoordBox = TypedBox[str, Coord]

    box = CoordBox({"home": Coord(1, 1)})
    assert (box.home.x, box.home.y) == (1, 1)
    with pytest.raises(ValueError, match="home is already set!"):
        box.home = Coord(0, 0)
    box.work = Coord(2, 2)
    with pytest.raises(TypeError, match="Expected an instance of Coord"):
        box.school = (3, 3)


def test_TypedBox_introspection() -> None:
    # We subclass the original TypedBox to set .__target_type__
    hint = TypedBox[str, str]
    origin, args = get_origin(hint), get_args(hint)
    assert origin is not None
    assert issubclass(origin, TypedBox)
    assert args == (str, str)


def test_TypedBox_bad_hint() -> None:
    with pytest.raises(TypeError, match="TypedBox expects a key and value type"):
        TypedBox[str]  # type: ignore
    with pytest.raises(TypeError, match="TypedBox expects a key and value type"):
        TypedBox[str, str, str]  # type: ignore
    with pytest.raises(TypeError, match="TypedBox key must be `str`"):
        TypedBox[int, str]


def test_TypedBox_cast() -> None:
    class Coord(BaseCoord):
        @classmethod
        def cast(cls, value: tuple[int, int]) -> "Coord":
            return cls(*value)

    CoordBox = TypedBox[str, Coord]

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

    CoordBox = TypedBox[str, Coord]

    box = CoordBox()
    box.home = Coord(1, 1)
    with pytest.raises(TypeError, match=r"Expected .*\.cast.* to return"):
        box.work = (2, 2)
