import re
from collections.abc import Callable
from copy import deepcopy
from typing import Any

import pytest
from google.cloud import bigquery
from google.cloud.bigquery.enums import SqlTypeNames

from arti.types import (
    DEFAULT_ANONYMOUS_NAME,
    Binary,
    Boolean,
    Collection,
    Date,
    DateTime,
    Float16,
    Float32,
    Float64,
    Geography,
    Int8,
    Int16,
    Int32,
    Int64,
    List,
    String,
    Struct,
    Time,
    Timestamp,
    Type,
)
from arti.types.bigquery import BIGQUERY_HINT_FIELD_NAME, bigquery_type_system


@pytest.fixture()
def bigquery_table() -> bigquery.Table:
    table = bigquery.Table(
        "project.dataset.table",
        schema=[
            *[
                bigquery.SchemaField(name, type, mode="REQUIRED")
                for name, type in [
                    ("binary", SqlTypeNames.BYTES),
                    ("boolean", SqlTypeNames.BOOL),
                    ("date", SqlTypeNames.DATE),
                    ("datetime", SqlTypeNames.DATETIME),
                    ("float64", SqlTypeNames.FLOAT64),
                    ("geography", SqlTypeNames.GEOGRAPHY),
                    ("int64", SqlTypeNames.INT64),
                    ("string", SqlTypeNames.STRING),
                    ("time", SqlTypeNames.TIME),
                    ("timestamp", SqlTypeNames.TIMESTAMP),
                ]
            ],
            bigquery.SchemaField(
                "records",
                SqlTypeNames.RECORD,
                mode="REPEATED",
                fields=[
                    bigquery.SchemaField(
                        "id",
                        SqlTypeNames.INTEGER,
                        mode="REQUIRED",
                    ),
                    bigquery.SchemaField(
                        "data",
                        SqlTypeNames.RECORD,
                        mode="REQUIRED",
                        fields=[
                            bigquery.SchemaField(
                                "values",
                                SqlTypeNames.INTEGER,
                                mode="REPEATED",
                            )
                        ],
                    ),
                ],
            ),
        ],
    )
    table.time_partitioning = bigquery.TimePartitioning(field="date")
    table.clustering_fields = ["geography"]
    return table


@pytest.fixture()
def arti_collection() -> Collection:
    return Collection(
        name="project.dataset.table",
        element=Struct(
            fields={
                "binary": Binary(),
                "boolean": Boolean(),
                "date": Date(),
                "datetime": DateTime(precision="microsecond"),
                "float64": Float64(),
                "geography": Geography(),
                "int64": Int64(),
                "string": String(),
                "time": Time(precision="microsecond"),
                "timestamp": Timestamp(precision="microsecond"),
                "records": List(
                    element=Struct(
                        fields={
                            "id": Int64(),
                            "data": Struct(fields={"values": List(element=Int64())}),
                        },
                    )
                ),
            }
        ),
        partition_by=("date",),
        cluster_by=("geography",),
    )


def test_bigquery_type_system_comprehensive(
    arti_collection: Collection, bigquery_table: bigquery.Table
) -> None:
    assert arti_collection == bigquery_type_system.to_artigraph(bigquery_table, hints={})
    assert bigquery_table == bigquery_type_system.to_system(arti_collection, hints={})


@pytest.mark.parametrize(
    ("mutate", "msg"),
    [
        (
            lambda tbl: setattr(
                tbl, "time_partitioning", bigquery.TimePartitioning(field="date", type_="HOUR")
            ),
            "BigQuery time partitioning other than 'DAY' is not implemented (got 'HOUR')",
        ),
        (
            lambda tbl: setattr(
                tbl, "time_partitioning", bigquery.TimePartitioning(field="date", type_="MONTH")
            ),
            "BigQuery time partitioning other than 'DAY' is not implemented (got 'MONTH')",
        ),
        (
            lambda tbl: setattr(
                tbl, "time_partitioning", bigquery.TimePartitioning(field="date", type_="YEAR")
            ),
            "BigQuery time partitioning other than 'DAY' is not implemented (got 'YEAR')",
        ),
        (
            lambda tbl: setattr(
                tbl, "range_partitioning", bigquery.RangePartitioning(field="int64")
            ),
            "BigQuery integer range partitioning is not implemented",
        ),
    ],
)
def test_bigquery_type_system_to_system_not_implemented_errors(
    bigquery_table: bigquery.Table, mutate: Callable[[bigquery.Table], None], msg: str
) -> None:
    mutate(bigquery_table)
    with pytest.raises(NotImplementedError, match=re.escape(msg)):
        bigquery_type_system.to_artigraph(bigquery_table, hints={})


@pytest.mark.parametrize(
    ("update", "error_type", "msg"),
    [
        (
            {"partition_by": ("int64",)},
            NotImplementedError,
            "BigQuery integer range partitioning is not implemented",
        ),
        (
            {"partition_by": ("string",)},
            ValueError,
            "BigQuery only supports integer range or time partitioning",
        ),
    ],
)
def test_bigquery_type_system_to_artigraph_not_implemented_errors(
    arti_collection: Collection, update: dict[str, Any], error_type: type[Exception], msg: str
) -> None:
    arti_collection = arti_collection.copy(update=update)
    with pytest.raises(error_type, match=re.escape(msg)):
        bigquery_type_system.to_system(arti_collection, hints={})


def test_bigquery_type_system_partition_and_cluster(
    bigquery_table: bigquery.Table, arti_collection: Collection
) -> None:
    bq = deepcopy(bigquery_table)
    bq.time_partitioning = None
    arti = arti_collection.copy(update={"partition_by": ()})
    assert bigquery_type_system.to_artigraph(bq, hints={}) == arti
    assert bigquery_type_system.to_system(arti, hints={}) == bq

    bq = deepcopy(bigquery_table)
    bq.clustering_fields = None
    arti = arti_collection.copy(update={"cluster_by": ()})
    assert bigquery_type_system.to_artigraph(bq, hints={}) == arti
    assert bigquery_type_system.to_system(arti, hints={}) == bq

    # BigQuery only supports a single partition field, but Artigraph Types don't have the same
    # limit. Instead, additional partitioning fields are moved to the cluster (ahead of existing
    # clustering fields).
    bq = deepcopy(bigquery_table)
    bq.clustering_fields = ["int64", "float64"]
    assert bigquery_type_system.to_artigraph(bq, hints={}) == arti_collection.copy(
        update={"partition_by": ("date",), "cluster_by": ("int64", "float64")}
    )
    assert (
        bigquery_type_system.to_system(
            arti_collection.copy(
                update={"partition_by": ("date", "int64"), "cluster_by": ("float64",)}
            ),
            hints={},
        )
        == bq
    )


def test_bigquery_type_system_table_name(arti_collection: Collection) -> None:
    # Confirm the default name maps to fake values (BQ requires fully qualified names)
    table = bigquery_type_system.to_system(arti_collection.copy(exclude={"name"}), hints={})
    assert table.project == "project"
    assert table.dataset_id == "dataset"
    assert table.table_id == "table"

    table = bigquery_type_system.to_system(arti_collection.copy(update={"name": "p.d.t"}), hints={})
    assert table.project == "p"
    assert table.dataset_id == "d"
    assert table.table_id == "t"


@pytest.mark.parametrize(
    ("arti_type", "bq_field_type", "reverse_arti_type"),
    [
        (Int8, "INTEGER", Int64),
        (Int16, "INTEGER", Int64),
        (Int32, "INTEGER", Int64),
        (Int64, "INTEGER", Int64),
        (Float16, "FLOAT", Float64),
        (Float32, "FLOAT", Float64),
        (Float64, "FLOAT", Float64),
    ],
)
def test_bigquery_type_system_numerics(
    arti_type: type[Type], bq_field_type: str, reverse_arti_type: type[Type]
) -> None:
    intermediate = bigquery_type_system.to_system(arti_type(), hints={})
    assert intermediate.field_type == bq_field_type
    assert isinstance(bigquery_type_system.to_artigraph(intermediate, hints={}), reverse_arti_type)


def test_bigquery_type_system_description() -> None:
    arti_type = String(description="abc", nullable=True)
    assert arti_type.description is not None
    bigquery_type = bigquery.SchemaField(
        "should_have_description", SqlTypeNames.STRING, description=arti_type.description
    )

    assert arti_type == bigquery_type_system.to_artigraph(bigquery_type, hints={})
    assert bigquery_type == bigquery_type_system.to_system(
        arti_type, hints={BIGQUERY_HINT_FIELD_NAME: bigquery_type.name}
    )


@pytest.mark.parametrize(
    ("arti_nullable", "bq_mode"),
    (
        [False, "REQUIRED"],
        [True, "NULLABLE"],
    ),
)
def test_bigquery_type_system_nullable(arti_nullable: bool, bq_mode: str) -> None:
    arti_type = String(nullable=arti_nullable)
    bigquery_type = bigquery.SchemaField(bq_mode, SqlTypeNames.STRING, mode=bq_mode)

    assert arti_type == bigquery_type_system.to_artigraph(bigquery_type, hints={})
    assert bigquery_type == bigquery_type_system.to_system(
        arti_type, hints={BIGQUERY_HINT_FIELD_NAME: bigquery_type.name}
    )


def test_bigquery_type_system_struct() -> None:
    arti_type = Struct(fields={"int": Int64()})
    bigquery_type = bigquery.SchemaField(
        "nested",
        SqlTypeNames.STRUCT,
        fields=[
            bigquery.SchemaField(
                "int",
                "INTEGER",
                mode="REQUIRED",
            )
        ],
        mode="REQUIRED",
    )

    assert arti_type == bigquery_type_system.to_artigraph(bigquery_type, hints={})
    assert bigquery_type == bigquery_type_system.to_system(
        arti_type, hints={BIGQUERY_HINT_FIELD_NAME: "nested"}
    )


def test_bigquery_type_system_list_ints() -> None:
    arti_type = List(element=Int64(description="int_array"))
    system_type = bigquery_type_system.to_system(arti_type, hints={})
    assert system_type == bigquery.SchemaField(
        DEFAULT_ANONYMOUS_NAME, "INTEGER", mode="REPEATED", description="int_array"
    )


def test_bigquery_type_system_list_error() -> None:
    arti_type = Struct(fields={"list_of_lists": List(element=List(element=Int64(nullable=True)))})
    with pytest.raises(ValueError, match="BigQuery doesn't support nested arrays"):
        bigquery_type_system.to_system(arti_type, hints={})
    with pytest.warns(Warning, match="BigQuery doesn't support nullable arrays"):
        bigquery_type_system.to_system(List(element=Int64(), nullable=True), hints={})
    with pytest.warns(Warning, match="BigQuery doesn't support nullable array elements"):
        bigquery_type_system.to_system(List(element=Int64(nullable=True)), hints={})
