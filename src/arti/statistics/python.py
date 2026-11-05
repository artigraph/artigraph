"""Compute Statistics of Python lists, such as a Collection read as a `list` of `dict` rows."""

from __future__ import annotations

from typing import Any

from arti.statistics import (
    DistinctCount,
    DistinctValues,
    Max,
    Min,
    NullCount,
    RowCount,
    register_implementation,
)
from arti.views.python import List


@register_implementation
def row_count(statistic: RowCount, data: list[Any], view: List) -> int:
    return len(data)


# NOTE: Field statistics index each row with `row[field]`, so a row missing a field raises rather
# than counting as null. For example, partition fields that are only stored in the path (rather
# than the file) aren't present in the rows.


@register_implementation
def null_count(statistic: NullCount, data: list[dict[str, Any]], view: List) -> int:
    return sum(row[statistic.field] is None for row in data)


@register_implementation
def distinct_count(statistic: DistinctCount, data: list[dict[str, Any]], view: List) -> int:
    return len({value for row in data if (value := row[statistic.field]) is not None})


@register_implementation
def distinct_values(statistic: DistinctValues, data: list[dict[str, Any]], view: List) -> set[Any]:
    return {value for row in data if (value := row[statistic.field]) is not None}


@register_implementation
def max_(statistic: Max, data: list[dict[str, Any]], view: List) -> Any:
    return max((v for row in data if (v := row[statistic.field]) is not None), default=None)


@register_implementation
def min_(statistic: Min, data: list[dict[str, Any]], view: List) -> Any:
    return min((v for row in data if (v := row[statistic.field]) is not None), default=None)
