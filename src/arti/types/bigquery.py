from __future__ import annotations

import warnings
from copy import deepcopy
from typing import Any

from google.cloud import bigquery
from google.cloud.bigquery.enums import SqlTypeNames

from arti import Type, TypeAdapter, TypeSystem, types

# The BigQuery types are enumerated in [1], but a few are not (yet) implemented:
# - BIGNUMERIC
# - INTERVAL
# - JSON
# - NUMERIC
#
# 1: https://github.com/googleapis/python-bigquery/blob/76d88fbb1316317a61fa1a63c101bc6f42f23af8/google/cloud/bigquery/enums.py#L252-L274
bigquery_type_system = TypeSystem(key="bigquery")


class BIGQUERY_MODE:
    REQUIRED = "REQUIRED"
    NULLABLE = "NULLABLE"
    REPEATED = "REPEATED"


# BigQuery Structs contain list[SchemaField], each with an embedded name. Artigraph Structs contain
# dict[name, Type]. Therefore, converting a Type to a SchemaField requires the field name to be
# passed in from higher up, which is handled via this key in the `hints`.
BIGQUERY_HINT_FIELD_NAME = f"{bigquery_type_system.key}.field_name"


def _create_schema_field(
    field_type: str, type_: Type, hints: dict[str, Any], **kwargs: Any
) -> bigquery.SchemaField:
    # TODO: Support default values (which would need support in arti.Type)
    if type_.description is not None:
        kwargs.setdefault("description", type_.description)
    return bigquery.SchemaField(
        name=hints.get(BIGQUERY_HINT_FIELD_NAME, types.DEFAULT_ANONYMOUS_NAME),
        field_type=field_type,
        mode=BIGQUERY_MODE.NULLABLE if type_.nullable else BIGQUERY_MODE.REQUIRED,
        **kwargs,
    )


class _BigQueryTypeAdapter(TypeAdapter):
    @classmethod
    def to_artigraph(
        cls, type_: bigquery.SchemaField, *, hints: dict[str, Any], type_system: TypeSystem
    ) -> Type:
        return cls.artigraph(description=type_.description, nullable=type_.is_nullable)

    @classmethod
    def matches_system(cls, type_: bigquery.SchemaField, *, hints: dict[str, Any]) -> bool:
        return isinstance(type_, bigquery.SchemaField) and type_.field_type.upper() == cls.system  # type: ignore[no-any-return]

    @classmethod
    def to_system(cls, type_: Type, *, hints: dict[str, Any], type_system: TypeSystem) -> Any:
        return _create_schema_field(cls.system, type_, hints)


def _gen_adapter(*, artigraph: type[Type], system: Any, priority: int = 0) -> type[TypeAdapter]:
    return bigquery_type_system.register_adapter(
        type(
            f"BigQuery{system}{artigraph}",
            (_BigQueryTypeAdapter,),
            {"artigraph": artigraph, "system": system, "priority": priority},
        )
    )


_gen_adapter(artigraph=types.Binary, system=SqlTypeNames.BYTES)
_gen_adapter(artigraph=types.Boolean, system=SqlTypeNames.BOOL)
_gen_adapter(artigraph=types.Date, system=SqlTypeNames.DATE)
_gen_adapter(artigraph=types.Geography, system=SqlTypeNames.GEOGRAPHY)
_gen_adapter(artigraph=types.String, system=SqlTypeNames.STRING)

# BQ only supports 64-bit ints and floats (aside from numerics), so round tripping results in eg:
#     arti Float16 -> bq FLOAT64 -> arti Float64
for _precision in (16, 32, 64):
    _gen_adapter(
        artigraph=getattr(types, f"Float{_precision}"),
        system=SqlTypeNames.FLOAT64,
        priority=_precision,
    )
for _precision in (8, 16, 32, 64):
    _gen_adapter(
        artigraph=getattr(types, f"Int{_precision}"), system=SqlTypeNames.INT64, priority=_precision
    )


class _BaseTimeTypeAdapter(_BigQueryTypeAdapter):
    # BQ time precision is microsecond (https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#timestamp_type)
    precision = "microsecond"

    @classmethod
    def to_artigraph(cls, type_: Any, *, hints: dict[str, Any], type_system: TypeSystem) -> Type:
        assert issubclass(cls.artigraph, types._TimeMixin)
        return cls.artigraph(
            description=type_.description, nullable=type_.is_nullable, precision=cls.precision
        )


@bigquery_type_system.register_adapter
class DateTimeTypeAdapter(_BaseTimeTypeAdapter):
    artigraph = types.DateTime
    system = SqlTypeNames.DATETIME


@bigquery_type_system.register_adapter
class TimeTypeAdapter(_BaseTimeTypeAdapter):
    artigraph = types.Time
    system = SqlTypeNames.TIME


@bigquery_type_system.register_adapter
class TimestampTypeAdapter(_BaseTimeTypeAdapter):
    artigraph = types.Timestamp
    system = SqlTypeNames.TIMESTAMP


@bigquery_type_system.register_adapter
class StructTypeAdapter(_BigQueryTypeAdapter):
    # See https://cloud.google.com/bigquery/docs/nested-repeated

    artigraph = types.Struct
    system = SqlTypeNames.STRUCT

    @classmethod
    def to_artigraph(
        cls, type_: bigquery.SchemaField, *, hints: dict[str, Any], type_system: TypeSystem
    ) -> Type:
        return cls.artigraph(
            description=type_.description,
            fields={
                field.name: type_system.to_artigraph(field, hints=hints) for field in type_.fields
            },
            nullable=type_.is_nullable,
        )

    @classmethod
    def to_system(cls, type_: Type, *, hints: dict[str, Any], type_system: TypeSystem) -> Any:
        assert isinstance(type_, cls.artigraph)
        return _create_schema_field(
            cls.system,
            type_,
            hints,
            fields=[
                type_system.to_system(subtype, hints=hints | {BIGQUERY_HINT_FIELD_NAME: name})
                for name, subtype in type_.fields.items()
            ],
        )


@bigquery_type_system.register_adapter
class ListFieldTypeAdapter(TypeAdapter):
    # See https://cloud.google.com/bigquery/docs/nested-repeated

    artigraph = types.List
    system = bigquery.SchemaField
    priority = int(1e9)  # Bump the priority so we can catch all with `mode == "REPEATED"`

    @classmethod
    def to_artigraph(
        cls, type_: bigquery.SchemaField, *, hints: dict[str, Any], type_system: TypeSystem
    ) -> Type:
        # Convert the REPEATED field to REQUIRED (BigQuery only supports non-nullable array
        # elements) for subsequent conversion by other TypeAdapters.
        element_type = deepcopy(type_)
        element_type._properties["mode"] = BIGQUERY_MODE.REQUIRED
        return types.List(
            description=type_.description,
            element=type_system.to_artigraph(element_type, hints=hints),
            nullable=False,  # Cannot be nullable
        )

    @classmethod
    def matches_system(cls, type_: bigquery.SchemaField, *, hints: dict[str, Any]) -> bool:
        return isinstance(type_, bigquery.SchemaField) and type_.mode == BIGQUERY_MODE.REPEATED  # type: ignore[no-any-return]

    @classmethod
    def matches_artigraph(cls, type_: Type, *, hints: dict[str, Any]) -> bool:
        # Collection is a subclass of List - but handled by a separate TypeAdapter
        return super().matches_artigraph(type_, hints=hints) and not isinstance(
            type_, types.Collection
        )

    @classmethod
    def to_system(cls, type_: Type, *, hints: dict[str, Any], type_system: TypeSystem) -> Any:
        assert isinstance(type_, cls.artigraph)
        if type_.nullable:
            warnings.warn("BigQuery doesn't support nullable arrays", stacklevel=2)
        if type_.element.nullable:
            warnings.warn("BigQuery doesn't support nullable array elements", stacklevel=2)
            type_ = type_.copy(update={"element": type_.element.copy(update={"nullable": False})})
        if isinstance(type_.element, types.List):
            raise ValueError("BigQuery doesn't support nested arrays")
        field = type_system.to_system(type_.element, hints=hints)
        assert field.mode == BIGQUERY_MODE.REQUIRED
        field._properties["mode"] = BIGQUERY_MODE.REPEATED
        return field


@bigquery_type_system.register_adapter
class TableTypeAdapter(TypeAdapter):
    artigraph = types.Collection
    system = bigquery.Table
    priority = ListFieldTypeAdapter.priority + 1

    @classmethod
    def to_artigraph(
        cls, type_: bigquery.Table, *, hints: dict[str, Any], type_system: TypeSystem
    ) -> Type:
        kwargs: dict[str, Any] = {}
        if type_.time_partitioning:
            if type_.time_partitioning.type_ != bigquery.TimePartitioningType.DAY:
                raise NotImplementedError(
                    f"BigQuery time partitioning other than 'DAY' is not implemented (got '{type_.time_partitioning.type_}')"
                )
            kwargs["partition_by"] = (type_.time_partitioning.field,)
        if type_.range_partitioning:
            raise NotImplementedError("BigQuery integer range partitioning is not implemented")
        if type_.clustering_fields:
            kwargs["cluster_by"] = tuple(type_.clustering_fields)
        return cls.artigraph(
            name=f"{type_.project}.{type_.dataset_id}.{type_.table_id}",
            element=type_system.to_artigraph(
                bigquery.SchemaField(
                    types.DEFAULT_ANONYMOUS_NAME,
                    SqlTypeNames.STRUCT,
                    fields=type_.schema,
                    mode="REQUIRED",
                ),
                hints=hints,
            ),
            nullable=False,
            **kwargs,
        )

    @classmethod
    def matches_system(cls, type_: bigquery.SchemaField, *, hints: dict[str, Any]) -> bool:
        return isinstance(type_, cls.system)

    @classmethod
    def to_system(cls, type_: Type, *, hints: dict[str, Any], type_system: TypeSystem) -> Any:
        assert isinstance(type_, cls.artigraph)
        assert isinstance(type_.element, types.Struct)
        name = type_.name
        # Override invalid default (must be project/dataset qualified)
        if name == types.DEFAULT_ANONYMOUS_NAME:
            name = "project.dataset.table"
        table = cls.system(name, schema=type_system.to_system(type_.element, hints=hints).fields)
        partition, cluster = type_.partition_by, type_.cluster_by
        if partition:
            # BigQuery only supports a single partitioning field. We'll move the rest to the
            # beginning of the cluster_by. This shouldn't matter much anyway since, depending on the
            # Storage, we'll have separate tables for each unique composite key.
            head, *tail = partition
            if tail:
                cluster = (*tail, *cluster)
            if isinstance(
                type_.element.fields[head], (types.Date, types.DateTime, types.Timestamp)
            ):
                # TODO: Support other granularities than DAY
                table.time_partitioning = bigquery.TimePartitioning(
                    field=head, type_=bigquery.TimePartitioningType.DAY
                )
                table.require_partition_filter = True
            elif isinstance(type_.element.fields[head], types._Int):
                raise NotImplementedError("BigQuery integer range partitioning is not implemented")
            else:
                raise ValueError("BigQuery only supports integer range or time partitioning")
        if cluster:
            table.clustering_fields = cluster
        return table
