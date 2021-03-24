from __future__ import annotations

from typing import Any, ClassVar, Generic, TypeVar

from arti.types.core import Type


class Partitioner:
    schema: ClassVar[Type]


_Upstream = TypeVar("_Upstream", bound=Any)
_Downstream = TypeVar("_Downstream", bound=Any)


class PartitionKey(Generic[_Downstream]):
    __slots__ = ("key", "dependencies")

    def __init__(
        self, key: _Downstream, dependencies: dict[type[Artifact], list[PartitionKey[_Upstream]]]
    ) -> None:
        self.key = key
        self.dependencies = dependencies


# Import type annotation references so we can resolve ForwardRefs at runtime with get_type_hints.
from arti.artifacts.core import Artifact  # noqa: E402 # # pylint: disable=wrong-import-position
