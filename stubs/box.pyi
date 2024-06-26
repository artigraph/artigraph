# Pending https://github.com/python/typeshed/pull/5140

from collections.abc import Iterator, MutableMapping
from typing import Any

class BoxError(Exception): ...

class Box(MutableMapping[str, Any | MutableMapping[str, Any]]):
    def __delitem__(self, a: str) -> None: ...
    def __getattr__(self, a: str) -> Any: ...
    def __getitem__(self, a: str) -> Any: ...
    def __init__(self, *args: Any, **kwargs: Any) -> None: ...
    def __iter__(self) -> Iterator[str]: ...
    def __len__(self) -> int: ...
    def __setattr__(self, a: str, b: Any) -> None: ...
    def __setitem__(self, a: str, b: Any) -> None: ...
    # Box uses name mangling - it looks like we need to resolve these to support calls/overrides?
    def _Box__convert_and_store(self, a: str, b: Any) -> None: ...
