# pylint: skip-file
# flake8: noqa

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Iterator
from typing import Any, Callable, Literal, Optional, TypeVar
from typing import Union as _Union
from typing import cast

_KT = TypeVar("_KT")
_VT = TypeVar("_VT")

class ODict(OrderedDict[_KT, _VT]):
    def __getattr__(self, name: str) -> _VT: ...

class Schema:
    def __init__(self, base_schema: Optional[Schema] = None) -> None: ...
    def __contains__(self, key: str) -> bool: ...
    def __getitem__(self, key: str) -> BaseMeta: ...
    def __getattr__(self, key: str) -> BaseMeta: ...
    def __iter__(self) -> Iterator[BaseMeta]: ...
    def __iadd__(self, typ: BaseMeta) -> None: ...
    def __isub__(self, typ: BaseMeta) -> None: ...

global_schema = Schema()

class BaseMeta(type): ...

class BaseType(metaclass=BaseMeta):
    __schema__: Schema = global_schema
    __kind__: str

class BaseMetaWithTypename(BaseMeta): ...
class BaseTypeWithTypename(BaseType, metaclass=BaseMetaWithTypename): ...

def non_null(t: type) -> BaseMeta: ...
def list_of(t: type) -> BaseMeta: ...

class Scalar(BaseType):
    __kind__ = "scalar"
    converter = Callable[[Any], Any]

class EnumMeta(BaseMeta):
    def __contains__(cls, v: str) -> bool: ...
    def __iter__(cls) -> Iterator[str]: ...
    def __len__(cls) -> int: ...

class Enum(BaseType, metaclass=EnumMeta):
    __kind__ = "enum"
    __choices__: tuple[str, ...] = ()

class UnionMeta(BaseMetaWithTypename):
    def __contains__(cls, name_or_type: _Union[str, BaseMeta]) -> bool: ...
    def __iter__(cls) -> Iterator[BaseMeta]: ...
    def __len__(cls) -> int: ...

class Union(BaseTypeWithTypename, metaclass=UnionMeta):
    __kind__ = "union"
    __types__: tuple[BaseMeta, ...] = ()

class ContainerTypeMeta(BaseMetaWithTypename):
    _ContainerTypeMeta__fields: OrderedDict[str, Field]
    __interfaces__: tuple[ContainerTypeMeta, ...]
    def __getitem__(cls, key: str) -> Field: ...
    def __getattr__(cls, key: str) -> Field: ...
    def __iter__(cls) -> Iterator[Field]: ...
    def __contains__(cls, field_name: str) -> bool: ...

class ContainerType(BaseTypeWithTypename, metaclass=ContainerTypeMeta):
    def __contains__(self, name: str) -> bool: ...
    def __getitem__(self, name: str) -> Any: ...
    def __iter__(self) -> Iterator[Any]: ...
    def __len__(self) -> int: ...
    def __setattr__(self, name: str, value: Any) -> None: ...
    def __setitem__(self, name: str, value: Any) -> None: ...

class BaseItem:
    graphql_name: Optional[str]
    name: str
    schema: Optional[Schema]
    container: Optional[type["ContainerType"]]
    def __init__(self, typ: BaseMeta, graphql_name: Optional[str] = None) -> None: ...
    @property
    def type(self) -> BaseMeta: ...

class Variable:
    name: str
    graphql_name: str

class Arg(BaseItem):
    default: Optional[Any]
    def __init__(
        self, typ: BaseMeta, graphql_name: Optional[str] = None, default: Optional[Any] = None
    ): ...

class ArgDict(OrderedDict[str, Arg]): ...

class Field(BaseItem):
    args: Optional[ArgDict]
    name: str
    def __init__(
        self, typ: BaseMeta, graphql_name: Optional[str] = None, args: Optional[ArgDict] = None
    ) -> None: ...

class Type(ContainerType):
    __kind__ = "type"

class Interface(ContainerType):
    __kind__ = "interface"

class Input(ContainerType):
    __kind__ = "input"

class Int(Scalar):
    converter = int

class Float(Scalar):
    converter = float

class String(Scalar):
    converter = str

class Boolean(Scalar):
    converter = bool

class ID(Scalar):
    converter = str

class UnknownType(Type): ...

map_python_to_graphql: dict[type, BaseMeta]
