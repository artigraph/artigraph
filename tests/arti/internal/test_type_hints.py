from collections.abc import Mapping, Sequence
from typing import Literal, Optional, Tuple, Union, get_origin

import pytest

from arti.internal.type_hints import is_union, lenient_issubclass


class MyTuple(tuple):  # type: ignore
    pass


@pytest.mark.parametrize(
    ("klass", "class_or_tuple"),
    (
        (MyTuple, tuple),
        (int, (int, str)),
        (int, Optional[int]),
        (int, Union[int, str]),
        (str, Union[int, str]),
        (tuple, Tuple),
        (tuple, tuple),
        (type(None), (int, type(None))),
        (type(None), Optional[int]),
    ),
)
def test_lenient_issubclass_true(
    klass: type, class_or_tuple: Union[type, tuple[type, ...]]
) -> None:
    assert lenient_issubclass(klass, class_or_tuple)


@pytest.mark.parametrize(
    ("klass", "class_or_tuple"),
    (
        (list, tuple),
        (str, Optional[int]),
        (str, Union[int, float]),
        (str, (int, float)),
        (tuple[int], tuple),
        # Stock `issubclass` raises a `TypeError: issubclass() arg 1 must be a class` for these (but
        # oddly, `issubclass(tuple[int], tuple)` does not).
        #
        # It might be nice for some of them (eg: Mapping, Sequence) to return True, but we will likely
        # need to handle Generics more specifically anyway.
        (Optional[int], int),
        (Tuple[int], tuple),
        (dict[str, int], Mapping),
        (tuple[int], Sequence),
    ),
)
def test_lenient_issubclass_false(
    klass: type, class_or_tuple: Union[type, tuple[type, ...]]
) -> None:
    assert not lenient_issubclass(klass, class_or_tuple)


def test_lenient_issubclass_error_cases() -> None:
    assert not lenient_issubclass(5, 5)  # type: ignore
    with pytest.raises(TypeError, match="arg 2 must be a class or tuple of classes"):
        lenient_issubclass(tuple, 5)  # type: ignore
    with pytest.raises(TypeError, match="argument 2 cannot be a parameterized generic"):
        lenient_issubclass(tuple, tuple[int])


def test_is_union() -> None:
    assert is_union(Union)
    assert not is_union(Literal)

    assert is_union(get_origin(Union[int, str]))
    assert not is_union(Union[int, str])

    assert not is_union(Optional[int])
    assert is_union(get_origin(Optional[int]))
