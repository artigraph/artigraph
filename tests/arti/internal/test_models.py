import re
from collections import defaultdict
from contextlib import nullcontext
from typing import Annotated, Any, ClassVar, Literal, Optional, Union

import pytest
from pydantic import Field, PrivateAttr, ValidationError

from arti import Fingerprint
from arti.internal.models import Model
from arti.internal.utils import frozendict


class Abstract(Model):
    _abstract_: ClassVar[bool] = True


class Concrete(Model):
    pass


class Sub(Model):
    x: int


def test_Model() -> None:
    obj = Concrete()
    assert str(obj) == repr(obj)
    for model in (Model, Abstract):
        with pytest.raises(ValidationError, match="cannot be instantiated directly"):
            model()


def test_Model_repr() -> None:
    class M(Model):
        i: int
        j: int = Field(repr=False)

    assert repr(M(i=1, j=1)) == "M(i=1)"


class Sneaky(Model):
    x: int
    _stuff = PrivateAttr(default_factory=lambda: defaultdict(list))
    _unset = PrivateAttr()


def test_Model_copy_private_attributes() -> None:
    orig = Sneaky(x=1)
    orig._stuff["a"].append("value")
    assert orig._stuff == {"a": ["value"]}


@pytest.mark.parametrize(
    ["validate"],
    [
        (False,),
        (True,),
    ],
)
def test_Model_copy_private_attributes_validation(validate: bool) -> None:
    orig = Sneaky(x=1)
    orig._stuff["a"].append("value")

    copy = orig.copy(validate=validate)
    # Confirm private attributes are pulled over
    assert orig._stuff == copy._stuff
    # as shared refs.
    assert orig._stuff is copy._stuff
    assert orig._stuff["a"] is copy._stuff["a"]

    copy = orig.copy(deep=True, validate=validate)
    # Confirm private attributes are pulled over
    assert orig._stuff == copy._stuff
    # but as deepcopies, not shared refs.
    assert orig._stuff is not copy._stuff
    assert orig._stuff["a"] is not copy._stuff["a"]


def test_Model_copy_validation() -> None:
    v = Sub(x=5)
    v1 = v.copy(update={"x": 10})
    assert v.x == 5
    assert v1.x == 10
    with pytest.raises(ValueError, match="expected an instance"):
        v.copy(update={"x": "junk"})
    v2 = v.copy(update={"x": "junk"}, validate=False)  # Skip validation for "trusted" data.
    assert v2.x == "junk"  # type: ignore


def test_Model_fingerprint() -> None:
    class A(Model):
        a: int
        b: str

    a = A(a=1, b="b")
    assert a.fingerprint == Fingerprint.from_string('A:{"a": 1, "b": "b"}')

    class B(Model):
        a: A
        b: set[str]

    assert B(a=a, b={"b"}).fingerprint == Fingerprint.from_string(
        f'B:{{"a": {a.fingerprint.key}, "b": ["b"]}}'
    )

    class Excludes(A):
        _fingerprint_excludes_ = frozenset(["a"])

    assert Excludes(a=1, b="b").fingerprint == Fingerprint.from_string('Excludes:{"b": "b"}')

    with pytest.raises(ValueError, match=re.escape("Unknown `_fingerprint_excludes_` field(s)")):

        class BadExcludes(A):
            _fingerprint_excludes_ = frozenset(["z"])

    class Includes(A):
        _fingerprint_includes_ = frozenset(["a"])

    assert Includes(a=1, b="b").fingerprint == Fingerprint.from_string('Includes:{"a": 1}')

    with pytest.raises(ValueError, match=re.escape("Unknown `_fingerprint_includes_` field(s)")):

        class BadIncludes(A):
            _fingerprint_includes_ = frozenset(["z"])


def test_Model__iter() -> None:
    class A(Model):
        a: int

    a = A(a=5)
    assert dict(a._iter()) == {"a": 5}
    a.__dict__["b"] = "junk"  # Shouldn't show up!
    assert dict(a._iter()) == {"a": 5}


def test_Model_unknown_kwargs() -> None:
    with pytest.raises(ValidationError, match="extra fields not permitted"):
        Concrete(junk=1)


def test_Model_static_types() -> None:
    class M(Model):
        a: Any
        b: Literal["b"]
        c: int
        d: frozendict[str, int]
        e: type[int]

    class MyInt(int):
        pass

    # frozendict is special cased in the type conversions to automatically convert dicts.
    m = M(a=5, b="b", c=0, d={"a": 1}, e=MyInt)
    assert isinstance(m.d, frozendict)
    with pytest.raises(ValidationError, match="expected an instance of <class 'str'>, got"):
        M(a=5, b=5, c=0)
    with pytest.raises(ValidationError, match=r"expected an instance of <class 'int'>, got"):
        M(a=5, b="b", c=0.0)
    with pytest.raises(ValidationError, match=r"expected a subclass of <class 'int'>, got"):
        M(a=5, b="b", c=0, d={"a": 1}, e=str)


@pytest.mark.parametrize(
    ("hint", "value", "error_type"),
    [
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
        (tuple[Optional[int], ...], (5, None), None),
        (tuple[int, ...], (1, 2), None),
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
