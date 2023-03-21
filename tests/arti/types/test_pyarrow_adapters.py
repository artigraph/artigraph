import pyarrow as pa
import pytest

from arti.types import (
    Binary,
    Boolean,
    Collection,
    DateTime,
    Geography,
    Int8,
    Int32,
    Int64,
    List,
    Map,
    String,
    Struct,
    Timestamp,
    Type,
)
from arti.types.pyarrow import pyarrow_type_system


@pytest.mark.parametrize(
    ("arti_type", "pa_type"),
    [
        pytest.param(Binary(), pa.binary(), id="binary"),
        pytest.param(Binary(byte_size=5), pa.binary(5), id="binary[5]"),
        pytest.param(Boolean(), pa.bool_(), id="bool"),
        pytest.param(  # NOTE: pa fields default to nullable
            Collection(element=Struct(fields={"a": Int8(nullable=True), "b": Int8()})),
            pa.schema([pa.field("a", pa.int8()), pa.field("b", pa.int8(), nullable=False)]),
            id="collection",
        ),
        pytest.param(DateTime(precision="second"), pa.timestamp("s", tz=None), id="datetime[s]"),
        pytest.param(Int32(), pa.int32(), id="int32"),
        pytest.param(Int64(), pa.int64(), id="int64"),
        pytest.param(List(element=Int8()), pa.list_(pa.int8()), id="list[int8]"),
        pytest.param(
            Map(key=String(), value=Int8()), pa.map_(pa.string(), pa.int8()), id="map[string, int8]"
        ),
        pytest.param(String(), pa.string(), id="string"),
        pytest.param(  # NOTE: pa fields default to nullable
            Struct(fields={"a": Int8(nullable=True), "b": Int8()}),
            pa.struct([pa.field("a", pa.int8()), pa.field("b", pa.int8(), nullable=False)]),
            id="struct",
        ),
        # tz must be set to UTC for Timestamp
        pytest.param(
            Timestamp(precision="microsecond"), pa.timestamp("us", tz="UTC"), id="timestamp[us]"
        ),
        pytest.param(
            Timestamp(precision="millisecond"), pa.timestamp("ms", tz="UTC"), id="timestamp[ms]"
        ),
        pytest.param(
            Timestamp(precision="nanosecond"), pa.timestamp("ns", tz="UTC"), id="timestamp[ns]"
        ),
        pytest.param(Timestamp(precision="second"), pa.timestamp("s", tz="UTC"), id="timestamp[s]"),
    ],
)
def test_pyarrow_type_system(arti_type: Type, pa_type: pa.DataType) -> None:
    output_pa_type = pyarrow_type_system.to_system(arti_type, hints={})
    assert output_pa_type == pa_type
    output_arti_type = pyarrow_type_system.to_artigraph(pa_type, hints={})
    assert output_arti_type == arti_type


def test_pyarrow_collection_edge_cases() -> None:
    # Check that partition/cluster can be roundtripped
    arti_type = Collection(
        name="test",
        element=Struct(fields={"a": Int8(nullable=True), "b": Int8()}),
        partition_by=("a",),
        cluster_by=("b",),
    )
    roundtripped = pyarrow_type_system.to_artigraph(
        pyarrow_type_system.to_system(arti_type, hints={}), hints={}
    )
    assert roundtripped == arti_type


def test_pyarrow_timestamp_timezone_error() -> None:
    with pytest.raises(ValueError, match="must be in UTC"):
        pyarrow_type_system.to_artigraph(pa.timestamp("s", tz="fake"), hints={})


# NOTE: Geography cannot be round-tripped, it's downgraded to a plain pa.string().
def test_pyarrow_geography() -> None:
    output_pa_type = pyarrow_type_system.to_system(Geography(), hints={})
    assert pa.types.is_string(output_pa_type)
