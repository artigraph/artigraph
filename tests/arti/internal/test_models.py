# pyright: reportPrivateUsage=false
from collections import defaultdict
from typing import Annotated, Any, ClassVar, cast

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
        assert cast(Any, Nested)()
    with pytest.raises(ValidationError, match="Input should be a valid"):
        assert cast(Any, Nested)(x="junk")


def test_Model_fingerprint() -> None:
    skip = SkipFingerprint()
    assert skip
    assert skip == 1

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


def test_Model_copy_fingerprint() -> None:
    obj = Concrete(i=1)
    assert obj.fingerprint == Concrete(i=1).fingerprint  # Populate the cache

    # The cached fingerprint must not survive an update to the fields it was computed from.
    assert obj.model_copy(update={"i": 2}).fingerprint == Concrete(i=2).fingerprint
    assert obj.model_copy(update={"i": 2}, deep=True).fingerprint == Concrete(i=2).fingerprint
    assert obj.model_copy().fingerprint == obj.fingerprint


def test_Model_copy_checks_hashability() -> None:
    class M(Model):
        value: Any = None

    # Like construction, updated copies must be hashable.
    with pytest.raises(TypeError, match="unhashable"):
        M().model_copy(update={"value": []})


# The rest more or less test the default Model's configuration of base pydantic functionality.


def test_Model_no_extras() -> None:
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        cast(Any, Concrete)(junk=1)


def test_Model_strict_types() -> None:
    class M(Model):
        a: int
        b: tuple[int, ...]
        c: FrozenMapping[str, int]
        d: Any

    class MyInt(int):
        pass

    a, b, c, d = 1, (1,), {"a": 1}, object()

    m = M(a=a, b=b, c=c, d=d)
    assert isinstance(m.c, frozendict)  # FrozenModel has a validator to convert

    m = M(a=MyInt(a), b=b, c=c, d=d)
    assert not isinstance(m.a, MyInt)  # Subclasses of primitive types aren't preserved...

    with pytest.raises(ValidationError, match="Input should be a valid tuple"):
        cast(Any, M)(a=a, b=[1], c=c, d=d)

    with pytest.raises(ValidationError, match="Input should be a valid integer"):
        cast(Any, M)(a=a, b=("a",), c=c, d=d)


def _new_stuff() -> dict[str, list[Any]]:
    return defaultdict(list)


class Sneaky(Model):
    x: int
    _stuff: dict[str, list[Any]] = PrivateAttr(default_factory=_new_stuff)

    @property
    def stuff(self) -> dict[str, list[Any]]:
        return self._stuff


def test_Model_private_attributes() -> None:
    orig = Sneaky(x=1)
    orig.stuff["a"].append("value")
    assert orig.stuff == {"a": ["value"]}

    copy = orig.model_copy()
    # Confirm private attributes are pulled over
    assert orig.stuff == copy.stuff
    # as shared refs.
    assert orig.stuff is copy.stuff
    assert orig.stuff["a"] is copy.stuff["a"]

    copy = orig.model_copy(deep=True)
    # Confirm private attributes are pulled over
    assert orig.stuff == copy.stuff
    # but as deepcopies, not shared refs.
    assert orig.stuff is not copy.stuff
    assert orig.stuff["a"] is not copy.stuff["a"]
