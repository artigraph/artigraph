from __future__ import annotations

from typing import Any, ClassVar, Literal, Optional, Union, cast

from arti.internal.utils import class_name, classproperty, register


class Type:
    """Type represents a data type."""

    key = class_name()

    def __init__(self, *, description: Optional[str] = None) -> None:
        if type(self) is Type:
            raise ValueError(
                "Type cannot be instantiated directly, please use the appropriate subclass!"
            )
        self.description = description

    @classproperty
    def type_registry(cls) -> dict[str, type[Type]]:
        return {t.key: t for t in cls.__subclasses__()}

    @classmethod
    def from_dict(cls, type_dict: dict[str, Any]) -> Type:
        """
        Instantiates the appropriate subclass of Type based on type_dict.

        type_dict should look like: {
            "type": string name of the Type subclass
            "params": optional dict of additional parameters needed to instantiate the subclass
        }
        If no params are passed, the subclass is instantiated directly. If params are passed,
        a `from_dict` method is expected to exist on the subclass, which will be instantiated
        through a call to <Subclass>.from_dict(params)
        """

        if "type" not in type_dict:
            raise ValueError(
                f'Missing a required "type" key in the Type dict. Available keys: {type_dict.keys()}'
            )
        type_ = type_dict["type"]
        type_cls = cast(Optional[type[Type]], Type.type_registry.get(type_))
        if type_cls is None:
            raise ValueError(f"{type_} is not an Artigraph Type.")
        try:
            if "params" in type_dict and len(type_dict["params"]):
                return type_cls.from_dict(type_dict["params"])
            return type_cls()
        except Exception as e:
            raise ValueError(f"Error instantiating {type_}: {e}")

    def to_dict(self) -> dict[str, str]:
        return {"type": self.key}


# TODO: Expand the core types (and/or describe how to customize).


class Struct(Type):
    def __init__(self, fields: dict[str, Type], *, description: Optional[str] = None) -> None:
        self.fields = fields
        super().__init__(description=description)

    @classmethod
    def from_dict(cls, type_dict: dict[str, Any]) -> Struct:
        try:
            fields = {
                field_key: Type.from_dict(field_type)
                for field_key, field_type in type_dict["fields"].items()
            }
            return cls(fields)
        except Exception as e:
            raise ValueError(f"Error instantiating Struct from params {type_dict}: {e}")

    def to_dict(self, include_type: Optional[bool] = False) -> dict[str, Any]:
        return {
            "type": type(self).__name__,
            "params": {
                "fields": {
                    field_key: field_type.to_dict() for field_key, field_type in self.fields.items()
                }
            },
        }


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
    """UTC timestamp with configurable precision."""

    def __init__(
        self,
        precision: Union[Literal["second"], Literal["millisecond"]],
        *,
        description: Optional[str] = None,
    ) -> None:
        self.precision = precision
        super().__init__(description=description)

    @classmethod
    def from_dict(
        cls, params: dict[str, Union[Literal["second"], Literal["millisecond"]]]
    ) -> Timestamp:
        try:
            return cls(precision=params["precision"])
        except Exception as e:
            raise ValueError(f"Unable to instantiate Timestamp from params {params}: {e}")

    def to_dict(self) -> dict[str, Any]:
        return {"type": type(self).__name__, "params": {"precision": self.precision}}


class TypeAdapter:
    """TypeAdapter maps between Artigraph types and a foreign type system."""

    external: ClassVar[Optional[Any]] = None  # If available, the external type.
    internal: ClassVar[type[Type]]  # Mark which Artigraph Type this maps to.
    priority: ClassVar[int] = 0  # Set the priority of this mapping. Higher is better.

    key: ClassVar[str] = class_name()

    def to_external(self, type_: Type) -> Any:
        raise NotImplementedError()

    def to_internal(self, type_: Any) -> Type:
        raise NotImplementedError()


class TypeSystem:
    def __init__(self, key: str) -> None:
        self.key = key
        self.adapter_by_key: dict[str, type[TypeAdapter]] = {}
        super().__init__()

    def register_adapter(self, adapter: type[TypeAdapter]) -> type[TypeAdapter]:
        return register(self.adapter_by_key, adapter.key, adapter)

    def from_core(self, type_: Type) -> Any:
        raise NotImplementedError()

    def to_core(self, type_: Any) -> Type:
        raise NotImplementedError()
