from typing import Union

import pandas as pd
import pytest

from arti.types import Float64, Int64, List, Map, String, Struct, Type
from arti.types.pandas import pandas_type_system


@pytest.mark.parametrize(
    ("arti_type", "pd_type"),
    [
        pytest.param(
            List(element=Struct(fields={"float64": Float64(), "int64": Int64(), "str": String()})),
            pd.DataFrame({"float64": [0.0], "int64": [0], "str": [""]}),
            id="df",
        ),
        pytest.param(
            List(
                element=Struct(
                    fields={"dict": Map(key=String(), value=Int64()), "list": List(element=Int64())}
                )
            ),
            pd.DataFrame({"dict": [{"": 0}], "list": [[0]]}),
            id="df-complex-objects",
            marks=pytest.mark.xfail(reason="not implemented yet - see issue #258"),
        ),
        pytest.param(List(element=Float64()), pd.Series([0.0]), id="series[float64]"),
        pytest.param(List(element=Int64()), pd.Series([0]), id="series[int64]"),
        pytest.param(List(element=String()), pd.Series([""]), id="series[string]"),
    ],
)
def test_pandas_type_system(arti_type: Type, pd_type: Union[pd.DataFrame, pd.Series]) -> None:
    output_pd_type = pandas_type_system.to_system(arti_type, hints={})
    if isinstance(pd_type, pd.DataFrame):
        pd.testing.assert_frame_equal(output_pd_type, pd_type)
    elif isinstance(pd_type, pd.Series):
        pd.testing.assert_series_equal(output_pd_type, pd_type)
    else:
        raise NotImplementedError()

    output_arti_type = pandas_type_system.to_artigraph(pd_type, hints={})
    assert output_arti_type == arti_type
