from collections import defaultdict
from typing import Annotated, Any, ClassVar

import pytest
from pydantic import Field, PrivateAttr, ValidationError

from arti import Fingerprint
from arti.fingerprints import SkipFingerprint
from arti.internal.mappings import FrozenMapping, frozendict
from arti.internal.models import Model


class Abstract(Model):
    _abstract_: ClassVar[bool] = True


class Nested(Model):
    x: Abstract


class Concrete(Abstract):
    i: int = 1


def test_Model() -> None:
    obj = Concrete()
    assert str(obj) == repr(obj) == "Concrete()"
    obj = Concrete(i=5)
    assert str(obj) == repr(obj) == "Concrete(i=5)"

    for model in (Model, Abstract):
        with pytest.raises(TypeError, match="cannot be instantiated directly"):
            model()

    assert Nested(x=Concrete())
    with pytest.raises(ValidationError, match="Field required"):
        assert Nested()  # pyright: ignore[reportCallIssue]
    with pytest.raises(ValidationError, match="Input should be a valid"):
        assert Nested(x="junk")  # pyright: ignore[reportArgumentType]


def test_Model_fingerprint() -> None:
    class A(Model):
        a: int
        b: int
        c: Annotated[int, SkipFingerprint()]
        d: Annotated[int, Field(exclude=True)]

    a, a_repr = A(a=1, b=2, c=3, d=4), '{"_arti_type_":"A","a":1,"b":2}'
    assert a._arti_fingerprint_fields_ == ("_arti_type_", "a", "b")
    assert a.fingerprint == Fingerprint.from_string(a_repr)

    # Test wrapped models omit fields with SkipFingerprint.
    class Wrapper(Model):
        a: A

    w, w_repr = (
        Wrapper(a=a),
        '{"_arti_type_":"Wrapper","a":{"_arti_type_":"A","a":1,"b":2}}',
    )
    assert w._arti_fingerprint_fields_ == ("_arti_type_", "a")
    assert w.fingerprint == Fingerprint.from_string(w_repr)

    # Test we can fingerprint subclasses with additional fields.
    class B(A):
        z: int

    w, w_repr = (
        Wrapper(a=B(a=1, b=2, c=3, d=4, z=26)),
        '{"_arti_type_":"Wrapper","a":{"_arti_type_":"B","a":1,"b":2,"z":26}}',
    )
    assert w.fingerprint == Fingerprint.from_string(w_repr)


# The rest more or less test the default Model's configuration of base pydantic functionality.


def test_Model_no_extras() -> None:
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        Concrete(junk=1)  # type: ignore[call-arg]


def test_Model_strict_types() -> None:
    class M(Model):
        a: int
        b: tuple[int, ...]
        c: FrozenMapping[str, int]
        d: Any

    class MyInt(int):
        pass

    a, b, c, d = 1, (1,), {"a": 1}, object()

    m = M(a=a, b=b, c=c, d=d)  # pyright: ignore[reportArgumentType]
    assert isinstance(m.c, frozendict)  # FrozenModel has a validator to convert

    m = M(a=MyInt(a), b=b, c=c, d=d)  # pyright: ignore[reportArgumentType]
    assert not isinstance(m.a, MyInt)  # Subclasses of primitive types aren't preserved...

    with pytest.raises(ValidationError, match="Input should be a valid tuple"):
        M(a=a, b=[1], c=c, d=d)  # pyright: ignore[reportArgumentType]

    with pytest.raises(ValidationError, match="Input should be a valid integer"):
        M(a=a, b=("a",), c=c, d=d)  # pyright: ignore[reportArgumentType]


class Sneaky(Model):
    x: int
    _stuff: dict[str, list] = PrivateAttr(default_factory=lambda: defaultdict(list))


def test_Model_private_attributes() -> None:
    orig = Sneaky(x=1)
    orig._stuff["a"].append("value")
    assert orig._stuff == {"a": ["value"]}

    copy = orig.model_copy()
    # Confirm private attributes are pulled over
    assert orig._stuff == copy._stuff
    # as shared refs.
    assert orig._stuff is copy._stuff
    assert orig._stuff["a"] is copy._stuff["a"]

    copy = orig.model_copy(deep=True)
    # Confirm private attributes are pulled over
    assert orig._stuff == copy._stuff
    # but as deepcopies, not shared refs.
    assert orig._stuff is not copy._stuff
    assert orig._stuff["a"] is not copy._stuff["a"]
