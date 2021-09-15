from contextlib import nullcontext
from typing import Annotated, Any, ClassVar, Literal, Optional, Union

import pytest
from pydantic import ValidationError

from arti.internal.models import Model
from arti.internal.utils import frozendict


class Abstract(Model):
    _abstract_: ClassVar[bool] = True


class Concrete(Model):
    pass


def test_Model() -> None:
    obj = Concrete()
    assert str(obj) == repr(obj)
    for model in (Model, Abstract):
        with pytest.raises(ValidationError, match="cannot be instantiated directly"):
            model()


def test_Model_unknown_kwargs() -> None:
    with pytest.raises(ValidationError, match="extra fields not permitted"):
        Concrete(junk=1)


def test_Model_static_types() -> None:
    class M(Model):
        a: Any
        b: Literal["b"]
        c: int
        d: frozendict[str, int]

    # frozendict is special cased in the type conversions to automatically convert dicts.
    m = M(a=5, b="b", c=0, d={"a": 1})
    assert isinstance(m.d, frozendict)
    with pytest.raises(ValidationError, match="Expected an instance of <class 'str'>, got"):
        M(a=5, b=5, c=0)
    with pytest.raises(ValidationError, match=r"Expected an instance of <class 'int'>, got"):
        M(a=5, b="b", c=0.0)


class Sub(Model):
    x: int


@pytest.mark.parametrize(
    ("hint", "value", "error_type"),
    [
        # NOTE: mypy wrongly errors on `tuple[typ, ...]` hints with "Type application has too many
        # types (1 expected)", even when that form is accepted as a var/attr hint.
        (Annotated[int, "blah"], 5, None),
        (Literal[5], 5, None),
        (Optional[int], None, None),
        (Sub, Sub(x=5), None),
        (Union[Literal[5], None], 5, None),
        (Union[int, str], 5, None),
        (dict[int, dict[int, Sub]], {5: {"5": Sub(x=5)}}, ValueError),
        (dict[int, str], {5: "hi"}, None),
        (int, 5, None),
        (str, "hi", None),
        (tuple, ("hi", "bye"), None),
        (tuple[Optional[int], ...], (5, None), None),  # type: ignore
        (tuple[int, ...], (1, 2), None),  # type: ignore
        (tuple[int], (5,), None),
        (tuple[str, int], ("test", 5), None),  # type: ignore
        # Detected bad input:
        (Literal[5], 6, ValueError),
        (Optional[int], "hi", ValueError),
        (Union[int, str], 5.0, ValueError),
        (dict[int, dict[int, Sub]], {5: {"5": Sub(x=5)}}, ValueError),
        (dict[int, str], {"5": "hi"}, ValueError),
        (dict[str, int], {"hi": "5"}, ValueError),
        (int, None, ValueError),
        (tuple[str, int], ("test",), ValueError),  # type: ignore
        # Known Union edge cases (more general type in front causes casting/parsing):
        (Union[str, int], 5, AssertionError),
    ],
)
def test_Model_static_types_complex(
    hint: Any, value: Any, error_type: Optional[type[Exception]]
) -> None:
    M = type(str(hint), (Model,), {"__annotations__": {"x": hint}})
    ctx = nullcontext() if error_type is None else pytest.raises(error_type)
    with ctx:  # type: ignore
        data = {"x": value}
        # Ensure data can be round-tripped to (at the least) confirm dict keys are checked.
        assert dict(M(**data)) == data


def test_Model_equality() -> None:
    class Animal(Model):
        name: str = ""

    class Dog(Animal):
        pass

    class Cat(Animal):
        pass

    class Owner(Model):
        name: str = ""
        pets: list[Animal]

    assert Dog() != 5
    assert Dog() == Dog()
    assert Dog() != Cat()
    assert Owner(pets=[Dog()]) != Owner(pets=[Cat()])
