from __future__ import annotations

import math
import operator
import os
from collections.abc import Callable
from functools import partial
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from arti.internal.utils import (
    class_name,
    classproperty,
    get_module_name,
    import_submodules,
    int64,
    named_temporary_file,
    one_or_none,
    ordinal,
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


@pytest.mark.parametrize("type_", [int64, uint64])
def test_sizedint(type_: type[int64 | uint64]) -> None:
    assert repr(type_(5)) == f"{type_.__name__}(5)"

    low, high = type_(type_._min), type_(type_._max)

    assert low == type_._min
    assert high == type_._max

    with pytest.raises(ValueError):  # noqa: PT011
        assert low - 1

    with pytest.raises(ValueError):  # noqa: PT011
        assert high + 1


def test_sizedint_cast() -> None:
    assert int64(uint64(18446744073709551611)) == int64(-5)
    assert int64(uint64(5)) == int64(5)
    assert uint64(int64(-5)) == uint64(18446744073709551611)
    assert uint64(int64(5)) == uint64(5)


@pytest.mark.parametrize(
    "op",
    [
        math.ceil,
        math.floor,
        math.trunc,
        operator.invert,
        operator.neg,
        operator.pos,
        partial(round, ndigits=-1),
    ],
)
@pytest.mark.parametrize("type_", [int64, uint64])
def test_sizedint_unary(type_: type[int], op: Callable[..., Any]) -> None:
    if type_ is uint64 and op in (operator.invert, operator.neg):
        pytest.skip("invalid for unsigned ints")

    output, expected = op(type_(123)), op(123)
    assert output == expected
    assert isinstance(output, type_)


@pytest.mark.parametrize(
    "op",
    [
        operator.add,
        operator.and_,
        operator.floordiv,
        operator.lshift,
        operator.mod,
        operator.mul,
        operator.or_,
        operator.rshift,
        operator.sub,
        operator.xor,
    ],
)
@pytest.mark.parametrize("type_", [int64, uint64])
def test_sizedint_binary(type_: type[int], op: Callable[..., Any]) -> None:
    i, _i = 10, type_(10)
    left = op(_i, i)
    right = op(i, _i)
    assert left == right
    assert isinstance(left, type_)
    assert isinstance(right, type_)


@pytest.mark.parametrize("mode", ["w+", "wb"])
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

    get_priority = operator.attrgetter("priority")

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
