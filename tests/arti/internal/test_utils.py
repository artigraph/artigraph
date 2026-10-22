# pyright: reportPrivateUsage=false
from __future__ import annotations

import operator
import os
from pathlib import Path
from types import ModuleType
from typing import Any, cast

import pytest

from arti.internal.utils import (
    ClassProperty,
    class_name,
    classproperty,
    get_module_name,
    import_submodules,
    one_or_none,
    ordinal,
    register,
)


def test_class_name() -> None:
    class Test:
        key = class_name()

    assert Test.key == "Test"
    assert Test().key == "Test"


def test_classproperty() -> None:
    class Test:
        @classproperty
        def a(cls: Any) -> str:
            return cast(type[Test], cls).__name__

    assert Test.a == "Test"
    assert Test().a == "Test"

    descriptor = vars(Test)["a"]
    assert isinstance(descriptor, ClassProperty)
    assert descriptor.__get__(Test()) == "Test"


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
