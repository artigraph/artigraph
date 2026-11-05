# Statistics and thresholds

Artigraph splits data quality into two parts:

- A **statistic** is a computed *fact* about one partition's data, such as its row count or the
  minimum value of a column.
- A **threshold** is a *judgement* about a statistic, such as "at least one row" or "no negative
  amounts". It gates whether a partition is good enough to build on.

The split matters. A partition's facts never change, because a partition is identified by its
content fingerprint. But the bounds you accept can change over time. So Artigraph records statistics
once per partition and checks thresholds on every build. You can revise a threshold and rebuild
without recomputing statistics or rebuilding data.

```python
from arti import Artifact, Format, Statistic, Threshold, Type
from arti.formats.parquet import Parquet
from arti.statistics import DistinctValues, Max, Min, NullCount, Ratio, RowCount
from arti.thresholds import Range, Subset
from arti.types import Collection, Date, Float64, String, Struct


class CleanTransactions(Artifact):
    type: Type = Collection(
        element=Struct(
            fields={
                "day": Date(),
                "vendor": String(),
                "category": String(),
                "amount_usd": Float64(),
            }
        ),
        partition_by=("day",),
    )
    format: Format = Parquet()
    # Facts to record without a judgement.
    statistics: tuple[Statistic, ...] = (Max(field="amount_usd"),)
    # Judgements. Their statistics are recorded too.
    thresholds: tuple[Threshold, ...] = (
        Range(statistic=RowCount(), min=1),
        Range(statistic=Ratio(numerator=NullCount(field="vendor"), denominator=RowCount()), max=0.05),
        Subset(
            statistic=DistinctValues(field="category"),
            of=frozenset({"meals", "office", "software"}),
        ),
        Range(statistic=Min(field="amount_usd"), min=0),
    )
```

Each threshold checks one statistic, and that statistic is recorded without listing it again in
`statistics`. `artifact.all_statistics` lists everything that's recorded. Artifacts check both
lists when you define them: a statistic must fit the artifact's type (for example, the field must
exist), and a threshold's values must match the statistic's value type.

Like `annotations`, an artifact subclass adds to its parent's `statistics` and `thresholds`, and so
do instances. Duplicates are dropped.

## Built-in statistics

| Statistic | Value | Notes |
| --- | --- | --- |
| `RowCount()` | `Int64` | Works for any `List` or `Collection`. |
| `NullCount(field=...)` | `Int64` | Counts null values. |
| `DistinctCount(field=...)` | `Int64` | Counts distinct non-null values in a scalar field. |
| `DistinctValues(field=...)` | `Set` of the field's type | The distinct non-null values. The whole set is recorded, so prefer `DistinctCount` for fields with many values. |
| `Min(field=...)`, `Max(field=...)` | the field's type, nullable | Works for ordered fields (numbers, strings, dates, timestamps). The value is null when there are no non-null values. |
| `Ratio(numerator=..., denominator=...)` | `Float64`, nullable | Derived from two numeric statistics, such as the fraction of null values. The value is null if either input is null or the denominator is zero. |

A statistic's value can be any type that has a Python type, including containers like `Set`.
Artigraph strictly validates every value against the statistic's value type. For example, an
`Int64` value must be an `int`: not a `bool`, and not a `numpy.int64`.

## Built-in thresholds

| Threshold | Passes when |
| --- | --- |
| `Range(statistic=..., min=..., max=...)` | The value is within the inclusive bounds. Set at least one bound. A null value fails. |
| `Equal(statistic=..., value=...)` | The value equals `value`. |
| `Subset(statistic=..., of=...)` | A set value (such as `DistinctValues`) only has values in `of`. |

## Compound judgements

A threshold judges one statistic. To judge a combination of statistics, such as "at most 5% of
vendors are null", check a **derived statistic**. A derived statistic is computed from the values of
other statistics, not from the data:

```python
Range(statistic=Ratio(numerator=NullCount(field="vendor"), denominator=RowCount()), max=0.05)
```

Artigraph records the derived value and its inputs, so a dashboard can show the null fraction too.
If you add a derived statistic later and its inputs are already recorded, Artigraph computes it
without reading the data.

## When statistics are computed

Computing a statistic from data needs the data in memory. A
[view](storage-formats-and-views.md#views) is the in-memory form of the data, such as a `list` of
`dict` rows or a pandas `DataFrame`. Each view needs its own implementation of each statistic.
Artigraph computes statistics at two points:

1. **When a partition is written.** This covers `graph.write(...)` and every producer output.
   Artigraph computes the statistics with the view that already holds the data, so it doesn't read
   the data back. It records the partition *first*, so a failing statistic doesn't lose data that
   may have been expensive to produce. The error is raised, and the next build computes the missing
   values.
2. **When a build finds a partition without them.** For example, a statistic failed earlier, the
   files were written outside Artigraph, or you added a statistic after the data was written.
   Artigraph computes only the missing values. It reads the partition with
   `graph.statistics_view(artifact)` only if a missing statistic needs the data. For produced
   artifacts, that's the producer's output view. For raw artifacts, it's the Python view for the
   artifact's type.

Artigraph doesn't guess another view. If a statistic has no implementation for the view, Artigraph
raises an error before writing anything. For produced artifacts, it checks this when the graph is
defined.

**Note:** The Python view reads a `Collection` as a `list` of `dict` rows, exactly as stored in the
file. If a partition field is only stored in the path (like `{date.iso}.json`), the rows don't
contain it. A statistic on that field raises an error instead of counting it as null.

Read the recorded values from a snapshot:

```python
for partition, values in snapshot.read_statistics(snapshot.artifacts.clean).items():
    print(dict(partition.partition_key), values)
```

The metadata backend stores the values (see [Metadata backends](metadata-backends.md)), so they're
available for dashboards and comparisons over time.

## When thresholds are checked

`graph.build()` visits artifacts in dependency order. For each artifact, it computes any missing
statistics for the snapshot's partitions and then checks every threshold. This happens after the
artifact's producer runs and before any consumer reads it. So consumers are always blocked on both
the producer and the statistics, including in a concurrent build that finds the partition before its
statistics are recorded.

If any partition fails, the build raises a `ThresholdError` that lists every failure:

```text
`clean` failed 1 Threshold check(s):
- {'day': DateField(value=datetime.date(2026, 1, 3))}: Range(statistic=Min(field='amount_usd'), min=0) got -18.0
Fix the data, revise the Thresholds, or accept a partition with `Graph.override_threshold`.
```

Downstream producers don't run, and existing tagged snapshots are untouched. The failing partition
and its statistics stay recorded, because they're still facts about that data. From there you have
three options:

- **Fix the data.** Correct the input and build again.
- **Revise the threshold.** If the bound was wrong for everyone, change it and build again. The
  existing partition is reused, not rebuilt.
- **Override the threshold for this partition.** If this one partition is acceptable, record that
  decision with a reason:

```python
try:
    graph.build()
except ThresholdError as error:
    for failure in error.failures:
        graph.override_threshold(
            graph.artifacts.clean,
            failure.partition,
            failure.threshold,
            reason="Refunds are expected",
        )
graph.build()  # Accepts (and logs) the overridden failure.
```

An override applies to the partition's exact contents and the exact threshold. If either changes, it
no longer applies, and the build asks for a new decision. The metadata backend records overrides
with their reasons.

## Adding a statistic

A statistic is a `Statistic` subclass that declares its value type. You register an implementation
for each view that should support it:

```python
from typing import Any

import pandas as pd

from arti.statistics import FieldStatistic, register_implementation
from arti.types import Float64, Type
from arti.views.pandas import DataFrame


class Mean(FieldStatistic):
    def value_type(self, artifact_type: Type) -> Type:
        self.field_type(artifact_type)  # Raises if the field doesn't exist
        return Float64(nullable=True)


@register_implementation
def mean(statistic: Mean, data: pd.DataFrame, view: DataFrame) -> Any:
    value = data[statistic.field].mean()
    return None if pd.isna(value) else float(value)
```

Artigraph looks up implementations by the annotated `statistic` and `view` classes, the same way it
looks up IO readers and writers. The most specific match wins, and an ambiguous match raises an
error. An implementation must return a plain Python value that matches the value type.

A derived statistic subclasses `DerivedStatistic` instead. It lists its `inputs` and implements
`derive(values)`, so it needs no view implementations.

Custom thresholds subclass `Threshold` and implement `check(value) -> bool`. They can also override
`check_type(value_type)` to validate their settings when the artifact is defined.

## Future work

Views backed by a database (a SQL table or query, for example) aren't available yet. For those, the
"data in memory" would be a handle to the table or query, and a statistic's implementation would
run an aggregate query. Artigraph doesn't yet compare a partition with others, such as "within 20%
of yesterday's row count".
