from __future__ import annotations

from datetime import date
from typing import Any, cast

import pytest

from arti import Artifact, Threshold
from arti.formats.json import JSON
from arti.statistics import DistinctValues, Max, NullCount, Ratio, RowCount
from arti.thresholds import Equal, Range, Subset
from arti.types import Collection, Date, Float64, Int64, String, Struct

TYPE = Collection(
    element=Struct(fields={"id": Int64(), "day": Date(), "amount": Float64(), "category": String()})
)


def test_Threshold_is_abstract() -> None:
    with pytest.raises(TypeError, match="cannot be instantiated directly"):
        Threshold(statistic=RowCount())  # pyright: ignore[reportAbstractUsage]


@pytest.mark.parametrize(
    ("threshold", "value", "expected"),
    [
        (Range(statistic=RowCount(), min=1), 1, True),
        (Range(statistic=RowCount(), min=1), 0, False),
        (Range(statistic=RowCount(), max=10), 10, True),
        (Range(statistic=RowCount(), max=10), 11, False),
        (Range(statistic=RowCount(), min=1, max=10), 5, True),
        (Range(statistic=Max(field="amount"), max=100), 99.5, True),
        (Range(statistic=Max(field="amount"), max=100), None, False),
        (Range(statistic=Max(field="day"), min=date(2026, 1, 1)), date(2025, 12, 31), False),
        (Equal(statistic=NullCount(field="id"), value=0), 0, True),
        (Equal(statistic=NullCount(field="id"), value=0), 1, False),
        (Equal(statistic=Max(field="day"), value=None), None, True),
        (Subset(statistic=DistinctValues(field="category"), of=frozenset({"a", "b"})), {"a"}, True),
        (Subset(statistic=DistinctValues(field="category"), of=frozenset({"a", "b"})), set[str](), True),
        (
            Subset(statistic=DistinctValues(field="category"), of=frozenset({"a", "b"})),
            {"a", "c"},
            False,
        ),
    ],
)
def test_check(threshold: Threshold, value: Any, expected: bool) -> None:
    assert threshold.check(value) is expected


def test_Range_validation() -> None:
    with pytest.raises(ValueError, match="at least one of `min` or `max` must be set"):
        Range(statistic=RowCount())
    with pytest.raises(ValueError, match="must not be greater than `max`"):
        Range(statistic=RowCount(), min=2, max=1)
    with pytest.raises(ValueError, match="must be comparable"):
        Range(statistic=RowCount(), min=date(2026, 1, 1), max=1)


def test_Subset_fingerprint_is_stable() -> None:
    # frozenset iteration order varies by process, so the values are serialized sorted.
    subset = Subset(statistic=DistinctValues(field="category"), of=cast(Any, ["b", "a"]))
    assert '"of":["a","b"]' in subset.model_dump_json()


def test_Artifact_validates_thresholds() -> None:
    def artifact(*thresholds: Any) -> Artifact:
        return Artifact(type=TYPE, format=JSON(), thresholds=thresholds)

    null_fraction = Ratio(numerator=NullCount(field="amount"), denominator=RowCount())
    valid = artifact(
        Range(statistic=RowCount(), min=1),
        # Ints are allowed for float Statistics.
        Range(statistic=Max(field="amount"), min=0, max=100.5),
        Range(statistic=Max(field="day"), min=date(2026, 1, 1)),
        Equal(statistic=Max(field="day"), value=None),
        Range(statistic=null_fraction, max=0.05),
        Subset(statistic=DistinctValues(field="category"), of=frozenset({"a", "b"})),
    )
    # Thresholds' Statistics (and derived inputs) are computed without listing them separately.
    assert valid.statistics == ()
    assert valid.all_statistics == (
        RowCount(),
        Max(field="amount"),
        Max(field="day"),
        NullCount(field="amount"),
        null_fraction,
        DistinctValues(field="category"),
    )
    with pytest.raises(ValueError, match="references an unknown field"):
        artifact(Equal(statistic=NullCount(field="missing"), value=0))
    with pytest.raises(ValueError, match="`min` is incompatible with the Statistic"):
        artifact(Range(statistic=Max(field="day"), min=1))
    with pytest.raises(ValueError, match="`max` is incompatible with the Statistic"):
        artifact(Range(statistic=RowCount(), max=1.5))
    with pytest.raises(ValueError, match="`value` is incompatible with the Statistic"):
        artifact(Equal(statistic=RowCount(), value=None))
    with pytest.raises(ValueError, match="requires a Set Statistic"):
        artifact(Subset(statistic=RowCount(), of=frozenset({1})))
    with pytest.raises(ValueError, match="`of` is incompatible with the Statistic"):
        artifact(Subset(statistic=DistinctValues(field="category"), of=frozenset({1})))
    # Duplicates are dropped.
    assert artifact(Range(statistic=RowCount(), min=1), Range(statistic=RowCount(), min=1)) == (
        artifact(Range(statistic=RowCount(), min=1))
    )


def test_Artifact_accumulates_class_thresholds() -> None:
    class Rows(Artifact):
        type: Any = TYPE
        format: Any = JSON()
        thresholds: Any = (Range(statistic=RowCount(), min=1),)

    class Amounts(Rows):
        thresholds: Any = (Range(statistic=Max(field="amount"), max=100),)

    amounts = Amounts(thresholds=(Range(statistic=RowCount(), max=10),))
    assert amounts.thresholds == (
        Range(statistic=RowCount(), min=1),
        Range(statistic=Max(field="amount"), max=100),
        Range(statistic=RowCount(), max=10),
    )
