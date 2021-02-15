from __future__ import annotations

from typing import Literal, Optional, Union


class Type:
    """ Type represents a data type.
    """

    def __init__(self, *, desc: Optional[str] = None) -> None:
        self.desc = desc


# TODO: Expand the core types (and/or describe how to customize).


class Struct(Type):
    def __init__(self, fields: dict[str, Type], *, desc: Optional[str] = None) -> None:
        self.fields = fields
        super().__init__(desc=desc)


class Null(Type):
    pass


class String(Type):
    pass


class _Numeric(Type):
    pass


class _Float(_Numeric):
    pass


class Float16(_Float):
    pass


class Float32(_Float):
    pass


class Float64(_Float):
    pass


class _Int(_Numeric):
    pass


class Int32(_Int):
    pass


class Int64(_Int):
    pass


class Date(Type):
    pass


class Timestamp(Type):
    """ UTC timestamp with configurable precision.
    """

    def __init__(
        self,
        precision: Union[Literal["second"], Literal["millisecond"]],
        *,
        desc: Optional[str] = None,
    ) -> None:
        self.precision = precision
        super().__init__(desc=desc)


# TODO: Fiddle - code runs, but mypy doesn't like `TypeSystem(...).Adapter`:
#     python = TypeSystem("python")
#
#
#     class Int64(python.Adapter):  # Name 'python.Adapter' is not defined
#         system = python
#         internal = core.Int64
#         external = int
#
# class _TypeAdapter:
#     """ _TypeAdapter maps between Artigraph types and a foreign type system.
#     """
#
#     external: ClassVar[Optional[Any]] = None  # If available, the external type.
#     internal: ClassVar[type[Type]]  # Mark which Artigraph Type this maps to.
#     priority: ClassVar[int] = 0  # Set the priority of this mapping. Higher is better.
#
#     key: ClassVar[str] = class_name()
#
#     def to_external(self, type_: Type) -> Any:
#         raise NotImplementedError()
#
#     def to_internal(self, type_: Any) -> Type:
#         raise NotImplementedError()
#
#     # Internal machinery - the TypeSystem will register when initialized.
#
#     system: ClassVar[TypeSystem]
#     __skip_subclass_registration__ = True
#
#     @classmethod
#     def __init_subclass__(cls, **kwargs) -> None:
#         if cls.__skip_subclass_registration__:
#             cls.__skip_subclass_registration__ = False
#         else:
#             register(cls.system.adapter_by_key, cls.key, cls)
#         super().__init_subclass__()
#
#
# class TypeSystem:
#     def __init__(self, key: str) -> None:
#         self.key = key
#         self.adapter_by_key: dict[str, type[_TypeAdapter]] = {}
#         self._Adapter: type[_TypeAdapter] = type(
#             f"{key.title()}Adapter", (_TypeAdapter,), {"system": self},
#         )
#         super().__init__()
#
#     @property
#     def Adaptor(self) -> _TypeAdapter:  # type: ignore
#         return cast(_TypeAdapter, self._Adapter)
#
#     @property
#     def sorted_adapters(self) -> Iterator[type[_TypeAdapter]]:
#         return reversed(sorted(self.adapter_by_key.values(), key=attrgetter("priority")))
#
#     @property
#     def adapter_by_internal_priority(self) -> Iterator[tuple[str, Iterator[type[_TypeAdapter]]]]:
#         return groupby(self.sorted_adapters, key=attrgetter("internal"))
#
#     @property
#     def adapter_by_external_priority(self) -> Iterator[tuple[str, Iterator[type[_TypeAdapter]]]]:
#         return groupby(self.sorted_adapters, key=attrgetter("external"))
#
#     # TODO: Implement using registered types+priority (sort by priority, take first isinstance).
#
#     def from_core(self, type_: Type) -> Any:
#         ...
#
#     def to_core(self, type_: Any) -> Type:
#         ...


class TypeSystem:
    def __init__(self, key: str) -> None:
        self.key = key
