# NOTE: These stubs are partial

import re
from typing import Literal, overload

_PARSED_VALUE = str | dict[str, str]
_EXTRA_TYPES = dict[str, type] | None

class Parser:
    def __init__(
        self,
        format: str,
        extra_types: _EXTRA_TYPES = None,
        case_sensitive: bool = False,
    ) -> None: ...
    @overload
    def parse(self, string: str, evaluate_result: Literal[True] = True) -> Result | None: ...
    @overload
    def parse(self, string: str, evaluate_result: Literal[False]) -> Match | None: ...

class Match:
    def __init__(self, parser: Parser, match: re.Match[str]) -> None:
        self.parser = parser
        self.match = match

    def evaluate_result(self) -> Result: ...

class Result:
    def __init__(
        self,
        fixed: tuple[_PARSED_VALUE, ...],
        named: dict[str, _PARSED_VALUE],
        spans: dict[str, tuple[int, int]],
    ) -> None:
        self.fixed = fixed
        self.named = named
        self.spans = spans

    def __getitem__(self, item: str | int | slice) -> _PARSED_VALUE: ...
    def __contains__(self, name: str) -> bool: ...

# parse
@overload
def parse(
    format: str,
    string: str,
    extra_types: _EXTRA_TYPES = None,
    evaluate_result: Literal[True] = True,
    case_sensitive: bool = False,
) -> Result | None: ...
@overload
def parse(
    format: str,
    string: str,
    evaluate_result: Literal[False],
    extra_types: _EXTRA_TYPES = None,
    case_sensitive: bool = False,
) -> Match | None: ...

# compile
def compile(
    format: str, extra_types: _EXTRA_TYPES = None, case_sensitive: bool = False
) -> Parser: ...
