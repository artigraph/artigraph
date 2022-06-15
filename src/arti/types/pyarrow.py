from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any, cast

import pyarrow as pa

from arti import types
from arti.internal.utils import classproperty

pyarrow_type_system = types.TypeSystem(key="pyarrow")

# Not implemented:
#     decimal128(int precision, int scale=0),
#     dictionary(index_type, value_type, …),
#     large_binary(),
#     large_list(value_type),
#     large_string(),


class _PyarrowTypeAdapter(types.TypeAdapter):
    @classproperty
    def _is_system(cls) -> Callable[[pa.DataType], bool]:
        return getattr(pa.types, f"is_{cls.system.__name__}")  # type: ignore

    @classmethod
    def to_artigraph(cls, type_: Any, *, hints: dict[str, Any]) -> types.Type:
        return cls.artigraph()

    @classmethod
    def matches_system(cls, type_: Any, *, hints: dict[str, Any]) -> bool:
        return isinstance(type_, pa.DataType) and cls._is_system(type_)

    @classmethod
    def to_system(cls, type_: types.Type, *, hints: dict[str, Any]) -> Any:
        return cls.system()


def _gen_adapter(
    *, artigraph: type[types.Type], system: Any, priority: int = 0
) -> type[types.TypeAdapter]:
    return pyarrow_type_system.register_adapter(
        type(
            f"Pyarrow{system.__name__}",
            (_PyarrowTypeAdapter,),
            {"artigraph": artigraph, "system": system, "priority": priority},
        )
    )


_gen_adapter(artigraph=types.String, system=pa.string)
_gen_adapter(artigraph=types.Null, system=pa.null)
# Date matching requires `priority=_precision` since it is not 1:1, but the float/int ones are.
for _precision in (32, 64):
    _gen_adapter(
        artigraph=types.Date,
        system=getattr(pa, f"date{_precision}"),
        priority=_precision,
    )
for _precision in (16, 32, 64):
    _gen_adapter(
        artigraph=getattr(types, f"Float{_precision}"),
        system=getattr(pa, f"float{_precision}"),
    )
for _precision in (8, 16, 32, 64):
    _gen_adapter(
        artigraph=getattr(types, f"Int{_precision}"),
        system=getattr(pa, f"int{_precision}"),
    )
    _gen_adapter(
        artigraph=getattr(types, f"UInt{_precision}"),
        system=getattr(pa, f"uint{_precision}"),
    )


@pyarrow_type_system.register_adapter
class BinaryTypeAdapter(_PyarrowTypeAdapter):
    artigraph = types.Binary
    system = pa.binary

    @classmethod
    def to_artigraph(cls, type_: Any, *, hints: dict[str, Any]) -> types.Type:
        if isinstance(type_, pa.FixedSizeBinaryType):
            return cls.artigraph(byte_size=type_.byte_width)
        return cls.artigraph()

    @classmethod
    def matches_system(cls, type_: Any, *, hints: dict[str, Any]) -> bool:
        # pa.binary returns a DataType(binary) when length=-1, otherwise a FixedSizeBinaryType...
        # but pa.types.is_binary only checks for DataType(binary).
        return super().matches_system(type_, hints=hints) or pa.types.is_fixed_size_binary(type_)

    @classmethod
    def to_system(cls, type_: types.Type, *, hints: dict[str, Any]) -> Any:
        assert isinstance(type_, cls.artigraph)
        return cls.system(length=-1 if type_.byte_size is None else type_.byte_size)


# The pyarrow bool constructor and checker have different names
@pyarrow_type_system.register_adapter
class BoolTypeAdapter(_PyarrowTypeAdapter):
    artigraph = types.Boolean
    system = pa.bool_

    @classmethod
    def matches_system(cls, type_: Any, *, hints: dict[str, Any]) -> bool:
        return cast(bool, pa.types.is_boolean(type_))


@pyarrow_type_system.register_adapter
class GeographyTypeAdapter(_PyarrowTypeAdapter):
    # TODO: Can we do something with pa.field metadata to round trip (eg: format, srid, etc) or
    # infer GeoParquet?
    artigraph = types.Geography
    system = pa.string  # or pa.binary if geography.format == "WKB"

    @classmethod
    def matches_system(cls, type_: Any, *, hints: dict[str, Any]) -> bool:
        # We don't have any metadata to differentiate normal strings from geographies, so avoid
        # matching. This will prevent round tripping.
        return False

    @classmethod
    def to_system(cls, type_: types.Type, *, hints: dict[str, Any]) -> Any:
        assert isinstance(type_, cls.artigraph)
        return pa.binary() if type_.format == "WKB" else pa.string()


@pyarrow_type_system.register_adapter
class ListTypeAdapter(_PyarrowTypeAdapter):
    artigraph = types.List
    system = pa.list_

    @classmethod
    def to_artigraph(cls, type_: Any, *, hints: dict[str, Any]) -> types.Type:
        return cls.artigraph(
            element=pyarrow_type_system.to_artigraph(type_.value_type, hints=hints),
        )

    @classmethod
    def matches_system(cls, type_: Any, *, hints: dict[str, Any]) -> bool:
        return cast(bool, pa.types.is_list(type_))

    @classmethod
    def to_system(cls, type_: types.Type, *, hints: dict[str, Any]) -> Any:
        assert isinstance(type_, cls.artigraph)
        return cls.system(value_type=pyarrow_type_system.to_system(type_.element, hints=hints))


@pyarrow_type_system.register_adapter
class MapTypeAdapter(_PyarrowTypeAdapter):
    artigraph = types.Map
    system = pa.map_

    @classmethod
    def to_artigraph(cls, type_: Any, *, hints: dict[str, Any]) -> types.Type:
        return cls.artigraph(
            key=pyarrow_type_system.to_artigraph(type_.key_type, hints=hints),
            value=pyarrow_type_system.to_artigraph(type_.item_type, hints=hints),
        )

    @classmethod
    def matches_system(cls, type_: Any, *, hints: dict[str, Any]) -> bool:
        return cast(bool, pa.types.is_map(type_))

    @classmethod
    def to_system(cls, type_: types.Type, *, hints: dict[str, Any]) -> Any:
        assert isinstance(type_, cls.artigraph)
        return cls.system(
            key_type=pyarrow_type_system.to_system(type_.key, hints=hints),
            item_type=pyarrow_type_system.to_system(type_.value, hints=hints),
        )


@pyarrow_type_system.register_adapter
class StructTypeAdapter(_PyarrowTypeAdapter):
    artigraph = types.Struct
    system = pa.struct

    @classmethod
    def _field_to_artigraph(cls, type_: Any, *, hints: dict[str, Any]) -> types.Type:
        ret = pyarrow_type_system.to_artigraph(type_.type, hints=hints)
        if type_.nullable != ret.nullable:  # Avoid setting nullable if matching to minimize repr
            ret = ret.copy(update={"nullable": type_.nullable})
        return ret

    @classmethod
    def to_artigraph(cls, type_: Any, *, hints: dict[str, Any]) -> types.Type:
        return cls.artigraph(
            fields={field.name: cls._field_to_artigraph(field, hints=hints) for field in type_}
        )

    @classmethod
    def _field_to_system(cls, name: str, type_: types.Type, *, hints: dict[str, Any]) -> Any:
        return pa.field(
            name, pyarrow_type_system.to_system(type_, hints=hints), nullable=type_.nullable
        )

    @classmethod
    def to_system(cls, type_: types.Type, *, hints: dict[str, Any]) -> Any:
        assert isinstance(type_, cls.artigraph)
        return cls.system(
            [
                cls._field_to_system(name, subtype, hints=hints)
                for name, subtype in type_.fields.items()
            ]
        )


# NOTE: pa.schema and pa.struct are structurally similar, but pa.schema has additional attributes
# (eg: .metadata) and cannot be nested (like Collection).
@pyarrow_type_system.register_adapter
class SchemaTypeAdapter(_PyarrowTypeAdapter):
    artigraph = types.Collection
    system = pa.schema

    @classmethod
    def matches_artigraph(cls, type_: types.Type, *, hints: dict[str, Any]) -> bool:
        # Collection can hold arbitrary types, but `pa.schema` is only a struct (but with arbitrary
        # metadata)
        return super().matches_artigraph(type_=type_, hints=hints) and isinstance(
            type_.element, types.Struct  # type: ignore
        )

    @classmethod
    def to_artigraph(cls, type_: Any, *, hints: dict[str, Any]) -> types.Type:
        kwargs = {}
        # NOTE: pyarrow converts all metadata keys/values to bytes
        if type_.metadata and b"artigraph" in type_.metadata:
            kwargs = json.loads(type_.metadata[b"artigraph"].decode())
            for key in ["partition_by", "cluster_by"]:
                if key in kwargs:  # pragma: no cover
                    kwargs[key] = tuple(kwargs[key])
        return cls.artigraph(element=StructTypeAdapter.to_artigraph(type_, hints=hints), **kwargs)

    @classmethod
    def matches_system(cls, type_: Any, *, hints: dict[str, Any]) -> bool:
        return isinstance(type_, pa.lib.Schema)

    @classmethod
    def to_system(cls, type_: types.Type, *, hints: dict[str, Any]) -> Any:
        assert isinstance(type_, cls.artigraph)
        assert isinstance(type_.element, types.Struct)
        return cls.system(
            StructTypeAdapter.to_system(type_.element, hints=hints),
            metadata={
                "artigraph": json.dumps(
                    {
                        "name": type_.name,
                        "partition_by": type_.partition_by,
                        "cluster_by": type_.cluster_by,
                    }
                )
            },
        )


class _BaseTimeTypeAdapter(_PyarrowTypeAdapter):
    precision_to_unit = {
        "second": "s",
        "millisecond": "ms",
        "microsecond": "us",
        "nanosecond": "ns",
    }

    @classproperty
    def unit_to_precision(cls) -> dict[str, str]:
        return {v: k for k, v in cls.precision_to_unit.items()}

    @classmethod
    def to_artigraph(cls, type_: Any, *, hints: dict[str, Any]) -> types.Type:
        if (precision := cls.unit_to_precision.get(type_.unit)) is None:  # pragma: no cover
            raise ValueError(
                f"{type_}.unit must be one of {tuple(cls.unit_to_precision)}, got {type_.unit}"
            )
        return cls.artigraph(precision=precision)

    @classmethod
    def to_system(cls, type_: types.Type, *, hints: dict[str, Any]) -> Any:
        precision = type_.precision  # type: ignore
        if (unit := cls.precision_to_unit.get(precision)) is None:  # pragma: no cover
            raise ValueError(
                f"{type_}.precision must be one of {tuple(cls.precision_to_unit)}, got {precision}"
            )
        return cls.system(unit=unit)


@pyarrow_type_system.register_adapter
class DateTimeTypeAdapter(_BaseTimeTypeAdapter):
    artigraph = types.DateTime
    system = pa.timestamp

    @classmethod
    def matches_system(cls, type_: Any, *, hints: dict[str, Any]) -> bool:
        return super().matches_system(type_, hints=hints) and type_.tz is None


@pyarrow_type_system.register_adapter
class TimestampTypeAdapter(_BaseTimeTypeAdapter):
    artigraph = types.Timestamp
    system = pa.timestamp

    @classmethod
    def to_artigraph(cls, type_: Any, *, hints: dict[str, Any]) -> types.Type:
        tz = type_.tz.upper()
        if tz != "UTC":
            raise ValueError(f"Timestamp {type_}.tz must be in UTC, got {tz}")
        return super().to_artigraph(type_, hints=hints)

    @classmethod
    def matches_system(cls, type_: Any, *, hints: dict[str, Any]) -> bool:
        return super().matches_system(type_, hints=hints) and type_.tz is not None

    @classmethod
    def to_system(cls, type_: types.Type, *, hints: dict[str, Any]) -> Any:
        ts = super().to_system(type_, hints=hints)
        return cls.system(ts.unit, "UTC")


class _BaseSizedTimeTypeAdapter(_BaseTimeTypeAdapter):
    artigraph = types.Time

    @classmethod
    def matches_artigraph(cls, type_: types.Type, *, hints: dict[str, Any]) -> bool:
        return (
            super().matches_artigraph(type_=type_, hints=hints)
            and type_.precision in cls.precision_to_unit  # type: ignore
        )

    @classmethod
    def matches_system(cls, type_: Any, *, hints: dict[str, Any]) -> bool:
        return (
            super().matches_system(type_=type_, hints=hints) and type_.unit in cls.unit_to_precision
        )


@pyarrow_type_system.register_adapter
class Time32TypeAdapter(_BaseSizedTimeTypeAdapter):
    precision_to_unit = {
        "second": "s",
        "millisecond": "ms",
    }
    system = pa.time32


@pyarrow_type_system.register_adapter
class Time64TypeAdapter(_BaseSizedTimeTypeAdapter):
    precision_to_unit = {
        "second": "us",
        "millisecond": "ns",
    }
    system = pa.time64
