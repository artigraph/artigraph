import numpy as np
import pytest

from arti.types import (
    Binary,
    Boolean,
    Float16,
    Float32,
    Float64,
    Int8,
    Int16,
    Int32,
    Int64,
    List,
    String,
    Type,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
)
from arti.types.numpy import numpy_type_system


@pytest.mark.parametrize(
    ["arti_type", "np_type"],
    [
        pytest.param(Binary(), np.bytes_, id="bytes_"),
        pytest.param(Boolean(), np.bool_, id="bool"),
        pytest.param(Float16(), np.float16, id="float16"),
        pytest.param(Float32(), np.float32, id="float32"),
        pytest.param(Float64(), np.float64, id="float64"),
        pytest.param(Int16(), np.int16, id="int16"),
        pytest.param(Int32(), np.int32, id="int32"),
        pytest.param(Int64(), np.int64, id="int64"),
        pytest.param(Int8(), np.int8, id="int8"),
        pytest.param(List(element=Int64()), np.array([0]), id="ndarray-1d-int64"),
        pytest.param(List(element=List(element=Int64())), np.array([[0]]), id="ndarray-2d-int64"),
        pytest.param(List(element=List(element=String())), np.array([[""]]), id="ndarray-2d-str"),
        pytest.param(String(), np.str_, id="str_"),
        pytest.param(UInt16(), np.uint16, id="uint16"),
        pytest.param(UInt32(), np.uint32, id="uint32"),
        pytest.param(UInt64(), np.uint64, id="uint64"),
        pytest.param(UInt8(), np.uint8, id="uint8"),
    ],
)
def test_numpy_type_system(arti_type: Type, np_type: np.generic) -> None:
    output_np_type = numpy_type_system.to_system(arti_type, hints={})
    assert output_np_type == np_type
    output_arti_type = numpy_type_system.to_artigraph(np_type, hints={})
    assert output_arti_type == arti_type
