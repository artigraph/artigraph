"""Compute Statistics of pandas DataFrames."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, cast

try:
    import numpy as np
    import pandas as pd

    from arti.views.pandas import DataFrame
except ModuleNotFoundError as error:  # pragma: no cover - dependency-free subprocess
    if (error.name or "").split(".", 1)[0] not in {"numpy", "pandas"}:
        raise
    # Statistic discovery imports every implementation. Optional pandas dependencies must not
    # prevent the Python implementations from working in a minimal installation.
else:
    from arti.statistics import (
        DistinctCount,
        DistinctValues,
        FieldStatistic,
        Max,
        Min,
        NullCount,
        RowCount,
        register_implementation,
    )
    from arti.types import Date, _Int  # pyright: ignore[reportPrivateUsage]

    def _to_python(value: Any, statistic: FieldStatistic, view: DataFrame) -> Any:
        """Convert pandas/numpy scalars into the plain Python value for the field's Type."""
        if pd.isna(value):
            return None
        field_type = statistic.field_type(view.type)
        if isinstance(value, pd.Timestamp):
            # Date fields may be held in a datetime64 column.
            return value.date() if isinstance(field_type, Date) else value.to_pydatetime()
        if isinstance(value, np.generic):
            value = cast(Any, value.item())
        # pandas holds integer columns with nulls as floats.
        if isinstance(field_type, _Int) and isinstance(value, float) and value.is_integer():
            return int(value)
        assert isinstance(value, bool | date | datetime | float | int | str)
        return value

    @register_implementation
    def row_count(statistic: RowCount, data: pd.DataFrame, view: DataFrame) -> int:
        return len(data)

    @register_implementation
    def null_count(statistic: NullCount, data: pd.DataFrame, view: DataFrame) -> int:
        return int(data[statistic.field].isna().sum())

    @register_implementation
    def distinct_count(statistic: DistinctCount, data: pd.DataFrame, view: DataFrame) -> int:
        return int(data[statistic.field].nunique(dropna=True))

    @register_implementation
    def distinct_values(statistic: DistinctValues, data: pd.DataFrame, view: DataFrame) -> set[Any]:
        return {
            _to_python(value, statistic, view) for value in data[statistic.field].dropna().unique()
        }

    @register_implementation
    def max_(statistic: Max, data: pd.DataFrame, view: DataFrame) -> Any:
        return _to_python(data[statistic.field].max(skipna=True), statistic, view)

    @register_implementation
    def min_(statistic: Min, data: pd.DataFrame, view: DataFrame) -> Any:
        return _to_python(data[statistic.field].min(skipna=True), statistic, view)
