from collections.abc import Mapping, Sequence
from typing import Any, Literal, Optional, Tuple, Union, get_origin

import pytest

from arti.internal.type_hints import (
    NoneType,
    is_optional_hint,
    is_union,
    is_union_hint,
    lenient_issubclass,
)


class MyTuple(tuple):  # type: ignore
    pass


@pytest.mark.parametrize(
    ("klass", "class_or_tuple"),
    (
        (MyTuple, tuple),
        (NoneType, (int, NoneType)),
        (NoneType, Optional[int]),
        (dict[str, int], Mapping),
        (dict[str, int], Mapping[str, int]),
        (dict[str, int], dict),
        (dict[str, int], dict[str, int]),
        (int, (int, str)),
        (int, Optional[int]),
        (int, Union[int, str]),
        (str, Union[int, str]),
        (tuple, Tuple),
        (tuple, tuple),
        (tuple[MyTuple[int]], tuple),  # type: ignore
        (tuple[MyTuple[int]], tuple[tuple[int]]),  # type: ignore
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
        (dict, Mapping[str, int]),
        (dict, dict[str, int]),
        (dict[str, str], Mapping[str, int]),
        (list, tuple),
        (str, (int, float)),
        (str, Optional[int]),
        (str, Union[int, float]),
        (tuple, Sequence[int]),
        (tuple, tuple[MyTuple[int]]),  # type: ignore
        (tuple, tuple[int]),
        (tuple[str], Sequence[int]),
        (tuple[tuple[int]], tuple[MyTuple[int]]),  # type: ignore
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
    assert not lenient_issubclass(5, 5)  # type: ignore
    with pytest.raises(TypeError, match="arg 2 must be a class"):
        lenient_issubclass(tuple, 5)  # type: ignore


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
