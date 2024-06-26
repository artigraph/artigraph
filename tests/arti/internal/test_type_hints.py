from collections.abc import Mapping, Sequence
from typing import Annotated, Any, Generic, Literal, Optional, TypedDict, TypeVar, Union, get_origin

import pytest

from arti.internal.type_hints import (
    NoneType,
    assert_all_instances,
    get_class_type_vars,
    get_item_from_annotated,
    is_optional_hint,
    is_union,
    is_union_hint,
    lenient_issubclass,
)


class MyTuple(tuple):  # type: ignore[type-arg]
    pass


class MyTypedDict(TypedDict):
    a: int


MyTupleVar = TypeVar("MyTupleVar", bound=MyTuple)
TupleVar = TypeVar("TupleVar", bound=tuple)  # type: ignore[type-arg]
UnboundVar = TypeVar("UnboundVar")


def test_assert_all_instances() -> None:
    assert_all_instances([1, 2, 3], type=int)
    with pytest.raises(TypeError, match="Expected int instances"):
        assert_all_instances([1, 2, 3, "4"], type=int)


def test_get_class_type_vars() -> None:
    T1 = TypeVar("T1")
    T2 = TypeVar("T2")

    class Base(Generic[T1, T2]):
        pass

    # Test it works directly with GenericAlias
    assert get_class_type_vars(Base[int, int]) == (int, int)

    # Test it works with fully subscripted subclasses
    class Sub(Base[int, int]):
        pass

    assert get_class_type_vars(Sub) == (int, int)

    # Test it works with fully subscripted subclasses up the MRO
    class Mixin:
        pass

    class SubWithMixin(Mixin, Sub):
        pass

    assert get_class_type_vars(SubWithMixin) == (int, int)

    # And finally, check error cases
    with pytest.raises(TypeError, match="Base must subclass a subscripted Generic"):
        get_class_type_vars(Base)


@pytest.mark.parametrize(
    ("annotation", "klass", "kind", "expected"),
    [
        (int, int, "class", None),
        (int, int, "object", None),
        (Annotated[int, 5], int, "object", 5),
        (Annotated[int, 5], int, "class", None),
        (Annotated[int, int], int, "object", None),
        (Annotated[int, int], int, "class", int),
    ],
)
def test_get_item_from_annotated(
    annotation: Any, klass: type, kind: Literal["class", "object"], expected: Any
) -> None:
    assert get_item_from_annotated(annotation, klass, kind=kind) == expected


@pytest.mark.parametrize(
    ("klass", "class_or_tuple"),
    [
        (Annotated[int, 5], Annotated[int, 5]),
        (Annotated[int, 5], int),
        (Annotated[list[int], 5], Annotated[list, 5]),
        (Annotated[list[int], 5], Annotated[list[int], 5]),
        (Annotated[list[int], 5], list),
        (Annotated[list[int], 5], list[int]),
        (Any, Any),
        (Any, UnboundVar),
        (MyTuple, TupleVar),
        (MyTuple, tuple),
        (MyTupleVar, TupleVar),
        (MyTypedDict, dict),
        (NoneType, (int, NoneType)),
        (NoneType, Optional[int]),  # noqa: UP007
        (NoneType, int | None),
        (bool, Any),
        (dict[str, int], Any),
        (dict[str, int], Mapping),
        (dict[str, int], Mapping[str, int]),
        (dict[str, int], dict),
        (dict[str, int], dict[str, int]),
        (int, (int, str)),
        (int, Annotated[int, 5]),
        (int, Optional[int]),  # noqa: UP007
        (int, Union[int, str]),  # noqa: UP007
        (int, int | None),
        (int, int | str),
        (list[int], Annotated[list, 5]),
        (list[int], Annotated[list[int], 5]),
        (str, Any),
        (str, Union[int, str]),  # noqa: UP007
        (str, int | str),
        (tuple, TupleVar),
        (tuple, UnboundVar),
        (tuple, tuple),
        (tuple[MyTuple[int]], tuple),  # type: ignore[type-arg]
        (tuple[MyTuple[int]], tuple[tuple[int]]),  # type: ignore[type-arg]
        (tuple[int], Sequence),
        (tuple[int], Sequence[int]),
        (tuple[int], tuple),
        (tuple[int], tuple[int]),
        (tuple[tuple[int]], tuple),
        (tuple[tuple[int]], tuple[tuple[int]]),
    ],
)
def test_lenient_issubclass_true(klass: type, class_or_tuple: type | tuple[type, ...]) -> None:
    assert lenient_issubclass(klass, class_or_tuple)


@pytest.mark.parametrize(
    ("klass", "class_or_tuple"),
    [
        (Any, bool),
        (MyTypedDict, dict[str, int]),  # Might implement in the future
        (MyTypedDict, dict[str, str]),
        (TupleVar, MyTupleVar),
        (UnboundVar, bool),
        (dict, Mapping[str, int]),
        (dict, dict[str, int]),
        (dict[str, str], Mapping[str, int]),
        (list, tuple),
        (str, (int, float)),
        (str, Union[int, float]),  # noqa: UP007
        (str, int | None),
        (str, int | float),
        (tuple, MyTupleVar),
        (tuple, Sequence[int]),
        (tuple, tuple[MyTuple[int]]),  # type: ignore[type-arg]
        (tuple, tuple[int]),
        (tuple[str], Sequence[int]),
        (tuple[tuple[int]], tuple[MyTuple[int]]),  # type: ignore[type-arg]
        (tuple[tuple[str]], tuple[tuple[int]]),
        # Stock `issubclass` raises a `TypeError: issubclass() arg 1 must be a class` for
        # these.
        (int | None, int),
    ],
)
def test_lenient_issubclass_false(klass: type, class_or_tuple: type | tuple[type, ...]) -> None:
    assert not lenient_issubclass(klass, class_or_tuple)


def test_is_optional_hint() -> None:
    assert is_optional_hint(Optional[int])  # noqa: UP007
    assert is_optional_hint(Union[int, None])  # noqa: UP007
    assert is_optional_hint(int | None)
    assert is_optional_hint(int | str | None)
    assert not is_optional_hint(Union[int, str])  # noqa: UP007
    assert not is_optional_hint(int | str)
    assert not is_optional_hint(int)


@pytest.mark.parametrize(
    ("hint", "should_match"),
    [
        (Literal[5], False),
        (Union[int, str], True),  # noqa: UP007
        (int | None, True),
    ],
)
def test_is_union(hint: Any, should_match: bool) -> None:
    if should_match:
        assert is_union(get_origin(hint)) is is_union_hint(hint) is True
    else:
        assert is_union(get_origin(hint)) is is_union_hint(hint) is False
