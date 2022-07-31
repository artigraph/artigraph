import re
from typing import Any

import pytest

from arti.internal.dispatch import multipledispatch


class A:
    pass


class A1(A):
    pass


class B:
    pass


class B1(B):
    pass


def test_multipledispatch() -> None:
    @multipledispatch
    def test(a: A, b: B) -> Any:
        return "good_a_b"

    @test.register
    def good_a_b1(a: A, b: B1) -> Any:
        return "good_a_b1"

    @test.register
    def good_a1_b(a: A1, b: B) -> Any:
        return "good_a1_b"

    # Check that the non-annotated registration works
    @test.register(A1, B1)
    def good_a1_b1(a, b) -> Any:  # type: ignore
        return "good_a1_b1"

    with pytest.raises(
        TypeError,
        match=re.escape("Expected `bad_name` to have ['a', 'b'] parameters, got ['a']"),
    ):

        @test.register
        def bad_name(a: int) -> Any:
            return a

    with pytest.raises(
        TypeError,
        match="Expected the `bad_param_kind.a` parameter to be POSITIONAL_OR_KEYWORD, got KEYWORD_ONLY",
    ):

        @test.register
        def bad_param_kind(*, a: A, b: B) -> Any:
            return a, b

    with pytest.raises(
        TypeError,
        match="Expected the `bad_type.a` parameter to be a subclass of <class 'test_dispatch.A'>, got <class 'int'>",
    ):

        @test.register
        def bad_type(a: int, b: str) -> Any:
            return a, b

    with pytest.raises(
        TypeError,
        match=re.escape("Expected the `bad` return to match"),
    ):

        @multipledispatch
        def ok(a: A) -> str:
            return "good_a_b"

        @ok.register
        def bad(a: A1) -> int:
            return 5

    assert test(A(), B()) == "good_a_b"
    assert test(A(), B1()) == "good_a_b1"
    assert test(A1(), B()) == "good_a1_b"
    assert test(A1(), B1()) == "good_a1_b1"
    # Check that a bad one didn't get registered
    with pytest.raises(TypeError):
        assert test(5, "")
