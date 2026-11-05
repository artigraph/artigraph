from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any

import numpy as np
import pandas as pd
import pytest

from arti import Artifact, Statistic
from arti.formats.json import JSON
from arti.statistics import (
    DistinctCount,
    DistinctValues,
    ImplementationKey,
    Max,
    Min,
    NullCount,
    Ratio,
    RowCount,
    check_implementations,
    compute,
    compute_missing,
    decode_value,
    encode_value,
    expand,
    lookup_implementation,
    register_implementation,
    validate_value,
)
from arti.types import (
    Binary,
    Collection,
    Date,
    Float64,
    Int64,
    List,
    Set,
    String,
    Struct,
    Timestamp,
    Type,
    UInt8,
)
from arti.views.pandas import DataFrame
from arti.views.python import Int
from arti.views.python import List as PyList

TYPE = Collection(
    element=Struct(
        fields={
            "id": Int64(),
            "day": Date(),
            "amount": Float64(),
            "vendor": String(),
            "at": Timestamp(precision="microsecond"),
            "tags": List(element=String()),
        }
    )
)
ROWS: list[dict[str, Any]] = [
    {
        "id": 1,
        "day": date(2026, 1, 2),
        "amount": 1.5,
        "vendor": "b",
        "at": datetime(2026, 1, 2, 3),
        "tags": [],
    },
    {
        "id": 2,
        "day": date(2026, 1, 1),
        "amount": None,
        "vendor": "a",
        "at": datetime(2026, 1, 1, 3),
        "tags": [],
    },
    {
        "id": 3,
        "day": date(2026, 1, 3),
        "amount": 4.0,
        "vendor": "a",
        "at": datetime(2026, 1, 3, 3),
        "tags": [],
    },
]

EXPECTED: list[tuple[Statistic, Any]] = [
    (RowCount(), 3),
    (NullCount(field="amount"), 1),
    (NullCount(field="id"), 0),
    (DistinctCount(field="vendor"), 2),
    (DistinctCount(field="amount"), 2),
    (DistinctValues(field="vendor"), {"a", "b"}),
    (DistinctValues(field="day"), {date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3)}),
    (DistinctValues(field="id"), {1, 2, 3}),
    (Max(field="id"), 3),
    (Min(field="amount"), 1.5),
    (Max(field="day"), date(2026, 1, 3)),
    (Min(field="vendor"), "a"),
    (Max(field="at"), datetime(2026, 1, 3, 3)),
]


def _types(value: Any) -> set[type]:
    return {type(v) for v in value} if isinstance(value, set) else {type(value)}  # pyright: ignore[reportUnknownVariableType, reportUnknownArgumentType]


@pytest.mark.parametrize(("statistic", "expected"), EXPECTED)
def test_compute_python(statistic: Statistic, expected: Any) -> None:
    value = compute(statistic, ROWS, PyList(type=TYPE, mode="READ"))
    assert value == expected
    assert type(value) is type(expected)
    assert _types(value) == _types(expected)


@pytest.mark.parametrize(("statistic", "expected"), EXPECTED)
def test_compute_pandas(statistic: Statistic, expected: Any) -> None:
    value = compute(statistic, pd.DataFrame(ROWS), DataFrame(type=TYPE, mode="READ"))
    assert value == expected
    assert type(value) is type(expected)
    # eg: numpy scalars are converted to Python values.
    assert _types(value) == _types(expected)


@pytest.mark.parametrize("data", [[], pd.DataFrame(columns=list(TYPE.element.fields))])
def test_compute_empty(data: Any) -> None:
    view = (PyList if isinstance(data, list) else DataFrame)(type=TYPE, mode="READ")
    assert compute(RowCount(), data, view) == 0
    assert compute(DistinctCount(field="id"), data, view) == 0
    assert compute(DistinctValues(field="id"), data, view) == set()
    assert compute(Max(field="id"), data, view) is None
    assert compute(Min(field="day"), data, view) is None


def test_compute_pandas_conversions() -> None:
    view = DataFrame(type=TYPE, mode="READ")
    # pandas holds integers with nulls as floats and may hold dates as datetime64.
    data = pd.DataFrame({"id": [1.0, None, 3.0], "day": pd.to_datetime(["2026-01-01"] * 3)})
    assert (value := compute(Max(field="id"), data, view)) == 3
    assert type(value) is int
    assert compute(DistinctValues(field="id"), data, view) == {1, 3}
    assert (value := compute(Min(field="day"), data, view)) == date(2026, 1, 1)
    assert type(value) is date
    assert compute(DistinctValues(field="day"), data, view) == {date(2026, 1, 1)}


def test_value_type_validation() -> None:
    for statistic in (NullCount(field="missing"), Max(field="missing")):
        with pytest.raises(ValueError, match="references an unknown field"):
            statistic.value_type(TYPE)
    for statistic in (DistinctCount(field="tags"), DistinctValues(field="tags")):
        with pytest.raises(ValueError, match="requires a scalar field"):
            statistic.value_type(TYPE)
    with pytest.raises(ValueError, match="requires an ordered field"):
        Max(field="tags").value_type(TYPE)
    unsigned = Collection(element=Struct(fields={"u": UInt8()}))
    for statistic in (Max(field="u"), DistinctValues(field="u")):
        with pytest.raises(ValueError, match="must have a Python type"):
            statistic.value_type(unsigned)
    with pytest.raises(ValueError, match="requires numeric inputs"):
        Ratio(numerator=Max(field="day"), denominator=RowCount()).value_type(TYPE)
    with pytest.raises(ValueError, match="requires a List or Collection"):
        RowCount().value_type(Int64())
    with pytest.raises(ValueError, match="requires a Collection"):
        NullCount(field="x").value_type(List(element=Int64()))
    assert RowCount().value_type(List(element=Int64())) == Int64()
    assert Max(field="day").value_type(TYPE) == Date(nullable=True)
    assert DistinctValues(field="day").value_type(TYPE) == Set(element=Date())
    ratio = Ratio(numerator=NullCount(field="id"), denominator=RowCount())
    assert ratio.value_type(TYPE) == Float64(nullable=True)


def test_Artifact_validates_statistics() -> None:
    null_fraction = Ratio(numerator=NullCount(field="amount"), denominator=RowCount())
    artifact = Artifact(
        type=TYPE, format=JSON(), statistics=(null_fraction, Max(field="day"), RowCount())
    )
    # Derived Statistics' inputs are computed too, before the derived Statistic.
    assert artifact.all_statistics == (
        NullCount(field="amount"),
        RowCount(),
        null_fraction,
        Max(field="day"),
    )
    with pytest.raises(ValueError, match="references an unknown field"):
        Artifact(type=TYPE, format=JSON(), statistics=(Max(field="missing"),))
    with pytest.raises(ValueError, match="references an unknown field"):
        Artifact(
            type=TYPE,
            format=JSON(),
            statistics=(Ratio(numerator=NullCount(field="missing"), denominator=RowCount()),),
        )
    # Duplicates are dropped.
    duplicated = Artifact(type=TYPE, format=JSON(), statistics=(RowCount(), RowCount()))
    assert duplicated.statistics == (RowCount(),)


def test_expand() -> None:
    nulls, rows = NullCount(field="id"), RowCount()
    inner = Ratio(numerator=nulls, denominator=rows)
    outer = Ratio(numerator=inner, denominator=rows)
    assert expand([outer, nulls]) == (nulls, rows, inner, outer)


def test_Ratio() -> None:
    ratio = Ratio(numerator=NullCount(field="amount"), denominator=RowCount())
    assert ratio.derive({ratio.numerator: 1, ratio.denominator: 4}) == 0.25
    assert ratio.derive({ratio.numerator: 1, ratio.denominator: 0}) is None
    assert ratio.derive({ratio.numerator: None, ratio.denominator: 4}) is None
    with pytest.raises(TypeError, match="derived from other Statistics"):
        compute(ratio, ROWS, PyList(type=TYPE, mode="READ"))


def test_compute_missing() -> None:
    view = PyList(type=TYPE, mode="READ")
    loads = list[None]()

    def load() -> tuple[Any, PyList]:
        loads.append(None)
        return ROWS, view

    nulls, rows = NullCount(field="amount"), RowCount()
    ratio = Ratio(numerator=nulls, denominator=rows)
    # Only missing values are computed, loading the data at most once.
    missing = compute_missing([ratio], artifact_type=TYPE, known={}, load=load)
    assert missing == {nulls: 1, rows: 3, ratio: 1 / 3}
    assert len(loads) == 1
    missing = compute_missing([ratio], artifact_type=TYPE, known={rows: 3}, load=load)
    assert missing == {nulls: 1, ratio: 1 / 3}
    assert len(loads) == 2
    # Derived Statistics don't need the data when their inputs are known.
    derived = compute_missing([ratio], artifact_type=TYPE, known={nulls: 1, rows: 4}, load=load)
    assert derived == {ratio: 0.25}
    assert compute_missing([rows], artifact_type=TYPE, known={rows: 3}, load=load) == {}
    assert len(loads) == 2


def test_check_implementations() -> None:
    ratio = Ratio(numerator=NullCount(field="amount"), denominator=RowCount())
    check_implementations([ratio], PyList(type=TYPE, mode="READ"))
    with pytest.raises(ValueError, match="No `Statistic` implementation found"):
        check_implementations([ratio], Int(type=Int64(), mode="READ"))


def test_validate_value() -> None:
    assert validate_value(1, Int64()) == 1
    assert validate_value(None, Int64(nullable=True)) is None
    assert (value := validate_value(1, Float64())) == 1.0
    assert type(value) is float
    assert validate_value(datetime(2026, 1, 1), Timestamp(precision="microsecond"))
    assert validate_value({1}, Set(element=Int64())) == {1}
    for value, type_ in [
        (None, Int64()),
        (True, Int64()),
        (1.0, Int64()),
        (np.int64(1), Int64()),
        (datetime(2026, 1, 1), Date()),
        ([1], Set(element=Int64())),
        ({True}, Set(element=Int64())),
    ]:
        with pytest.raises(TypeError, match="invalid"):
            validate_value(value, type_)
    with pytest.raises(ValueError, match="must have a Python type"):
        validate_value(1, UInt8())


@pytest.mark.parametrize(
    ("value", "type_", "encoded"),
    [
        (1, Int64(), 1),
        (None, Int64(nullable=True), None),
        (1.5, Float64(), 1.5),
        (date(2026, 1, 1), Date(), "2026-01-01"),
        (datetime(2026, 1, 1, 3), Timestamp(precision="microsecond"), "2026-01-01T03:00:00"),
        (b"\x00", Binary(), "AA=="),
        ({date(2026, 1, 1)}, Set(element=Date()), ["2026-01-01"]),
    ],
)
def test_encode_decode_value(value: Any, type_: Type, encoded: Any) -> None:
    assert (actual := encode_value(value, type_)) == encoded or type(value) is bytes
    assert decode_value(actual, type_) == value


class BadRowCount(RowCount):
    pass


@register_implementation
def bad_row_count(statistic: BadRowCount, data: list[Any], view: PyList) -> Any:
    return float(len(data))


def test_compute_rejects_invalid_values() -> None:
    with pytest.raises(TypeError, match="bad_row_count returned an invalid value: invalid Int64"):
        compute(BadRowCount(), ROWS, PyList(type=TYPE, mode="READ"))


def test_lookup_implementation() -> None:
    # Subclasses use their parent's implementation unless they register a more specific one.
    class MyRowCount(RowCount):
        pass

    key = ImplementationKey(MyRowCount, PyList)
    assert str(key) == "statistic=MyRowCount, view=List"
    assert lookup_implementation(key) is lookup_implementation(ImplementationKey(RowCount, PyList))
    with pytest.raises(
        ValueError,
        match=re.escape("No `Statistic` implementation found for: statistic=RowCount, view=Int"),
    ):
        lookup_implementation(ImplementationKey(RowCount, Int))


def test_register_implementation_validation() -> None:
    with pytest.raises(TypeError, match="to have positional"):

        @register_implementation
        def wrong_params(statistic: RowCount, view: PyList) -> int:
            return 0

    with pytest.raises(TypeError, match="statistic` parameter to be a subclass of"):

        @register_implementation
        def wrong_statistic(statistic: int, data: Any, view: PyList) -> int:
            return 0

    with pytest.raises(TypeError, match="view` parameter to be a subclass of"):

        @register_implementation
        def wrong_view(statistic: RowCount, data: Any, view: int) -> int:
            return 0

    with pytest.raises(ValueError, match="is already registered"):

        @register_implementation
        def duplicate(statistic: RowCount, data: Any, view: PyList) -> int:
            return 0
