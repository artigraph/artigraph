import typing
from collections.abc import Mapping, Sequence
from typing import Annotated, Any, Generic, Literal, Optional, TypedDict, TypeVar, Union, get_origin

import pytest

from arti.internal.type_hints import (
    NoneType,
    get_class_type_vars,
    get_item_from_annotated,
    is_optional_hint,
    is_union,
    is_union_hint,
    lenient_issubclass,
)

Tuple = getattr(typing, "Tuple")  # Work around https://github.com/asottile/pyupgrade/issues/574


class MyTuple(tuple):  # type: ignore[type-arg]
    pass


class MyTypedDict(TypedDict):
    a: int


MyTupleVar = TypeVar("MyTupleVar", bound=MyTuple)
TupleVar = TypeVar("TupleVar", bound=tuple)  # type: ignore[type-arg]
UnboundVar = TypeVar("UnboundVar")


def test_get_class_type_vars() -> None:
    T1 = TypeVar("T1")
    T2 = TypeVar("T2")

    class Base(Generic[T1, T2]):
        pass

    # Test it works directly with GenericAlias
    get_class_type_vars(Base[int, int]) == (int, int)

    # Test it works with fully subscripted subclasses
    class Sub(Base[int, int]):
        pass

    get_class_type_vars(Sub) == (int, int)

    # Test it works with fully subscripted subclasses up the MRO
    class Mixin:
        pass

    class SubWithMixin(Mixin, Sub):
        pass

    get_class_type_vars(SubWithMixin) == (int, int)

    # And finally, check error cases
    with pytest.raises(TypeError, match="Base must subclass a subscripted Generic"):
        get_class_type_vars(Base)


@pytest.mark.parametrize(
    ("annotation", "klass", "is_subclass", "expected"),
    (
        (int, int, True, None),
        (int, int, False, None),
        (Annotated[int, 5], int, False, 5),
        (Annotated[int, 5], int, True, None),
        (Annotated[int, int], int, False, None),
        (Annotated[int, int], int, True, int),
    ),
)
def test_get_item_from_annotated(
    annotation: Any, klass: type, is_subclass: bool, expected: Any
) -> None:
    assert get_item_from_annotated(annotation, klass, is_subclass=is_subclass) == expected


@pytest.mark.parametrize(
    ("klass", "class_or_tuple"),
    (
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
        (NoneType, Optional[int]),
        (bool, Any),
        (dict[str, int], Any),
        (dict[str, int], Mapping),
        (dict[str, int], Mapping[str, int]),
        (dict[str, int], dict),
        (dict[str, int], dict[str, int]),
        (int, (int, str)),
        (int, Annotated[int, 5]),
        (int, Optional[int]),
        (int, Union[int, str]),
        (list[int], Annotated[list, 5]),
        (list[int], Annotated[list[int], 5]),
        (str, Any),
        (str, Union[int, str]),
        (tuple, Tuple),
        (tuple, TupleVar),
        (tuple, UnboundVar),
        (tuple, tuple),
        (tuple[MyTuple[int]], tuple),  # type: ignore[type-arg]
        (tuple[MyTuple[int]], tuple[tuple[int]]),  # type: ignore[type-arg]
        (tuple[int], Sequence),
        (tuple[int], Sequence[int]),
        (tuple[int], Tuple),
        (tuple[int], tuple),
        (tuple[int], tuple[int]),
        (tuple[tuple[int]], tuple),
        (tuple[tuple[int]], tuple[tuple[int]]),
    ),
)
def test_lenient_issubclass_true(
    klass: type, class_or_tuple: Union[type, tuple[type, ...]]
) -> None:
    assert lenient_issubclass(klass, class_or_tuple)


@pytest.mark.parametrize(
    ("klass", "class_or_tuple"),
    (
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
        (str, Optional[int]),
        (str, Union[int, float]),
        (tuple, MyTupleVar),
        (tuple, Sequence[int]),
        (tuple, tuple[MyTuple[int]]),  # type: ignore[type-arg]
        (tuple, tuple[int]),
        (tuple[str], Sequence[int]),
        (tuple[tuple[int]], tuple[MyTuple[int]]),  # type: ignore[type-arg]
        (tuple[tuple[str]], tuple[tuple[int]]),
        # Stock `issubclass` raises a `TypeError: issubclass() arg 1 must be a class` for these (but
        # oddly, `issubclass(tuple[int], tuple)` does not).
        (Optional[int], int),
        (Tuple[int], tuple),
    ),
)
def test_lenient_issubclass_false(
    klass: type, class_or_tuple: Union[type, tuple[type, ...]]
) -> None:
    assert not lenient_issubclass(klass, class_or_tuple)


def test_lenient_issubclass_error_cases() -> None:
    assert not lenient_issubclass(5, 5)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="arg 2 must be a class"):
        lenient_issubclass(tuple, 5)  # type: ignore[arg-type]
    # Might support this in the future
    with pytest.raises(TypeError, match="TypedDict does not support instance and class checks"):
        lenient_issubclass(dict, MyTypedDict)


def test_is_optional_hint() -> None:
    assert is_optional_hint(Optional[int])
    assert is_optional_hint(Union[int, None])
    assert is_optional_hint(Union[int, str, NoneType])
    assert is_optional_hint(Union[int, NoneType])
    assert not is_optional_hint(Union[int, str])
    assert not is_optional_hint(int)


@pytest.mark.parametrize(
    ["hint", "should_match"],
    (
        (Literal[5], False),
        (Optional[int], True),
        (Union[int, str], True),
    ),
)
def test_is_union(hint: Any, should_match: bool) -> None:
    if should_match:
        assert is_union(get_origin(hint)) is is_union_hint(hint) is True
    else:
        assert is_union(get_origin(hint)) is is_union_hint(hint) is False
