# NOTE: These stubs are partial

import re
from typing import Literal, Optional, Union, overload

_PARSED_VALUE = Union[str, dict[str, str]]
_EXTRA_TYPES = Optional[dict[str, type]]

class Parser:
    def __init__(
        self,
        format: str,
        extra_types: _EXTRA_TYPES = None,
        case_sensitive: bool = False,
    ) -> None: ...
    @overload
    def parse(self, string: str, evaluate_result: Literal[True] = True) -> Optional[Result]: ...
    @overload
    def parse(self, string: str, evaluate_result: Literal[False]) -> Optional[Match]: ...

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
    def __getitem__(self, item: Union[str, int, slice]) -> _PARSED_VALUE: ...
    def __contains__(self, name: str) -> bool: ...

# parse
@overload
def parse(
    format: str,
    string: str,
    extra_types: _EXTRA_TYPES = None,
    evaluate_result: Literal[True] = True,
    case_sensitive: bool = False,
) -> Optional[Result]: ...
@overload
def parse(
    format: str,
    string: str,
    evaluate_result: Literal[False],
    extra_types: _EXTRA_TYPES = None,
    case_sensitive: bool = False,
) -> Optional[Match]: ...

# compile
def compile(
    format: str, extra_types: _EXTRA_TYPES = None, case_sensitive: bool = False
) -> Parser: ...
