# Producers

A **producer** is a versioned Python transformation that builds one or more artifacts from input
artifacts. This snippet uses the `Transactions` and `TotalSpend` artifacts from the first graph in
[Getting started](../getting-started.md).

```python
from typing import Annotated

from arti import producer
from arti.versions import SemVer


@producer(version=SemVer(major=1, minor=0, patch=0))
def aggregate_transactions(
    transactions: Annotated[list[dict], Transactions],
) -> Annotated[float, TotalSpend]:
    return float(sum(txn["amount"] for txn in transactions))
```

## How producers connect graphs

Inside a `Graph` definition, calling a producer with artifact inputs creates a dependency edge. The
producer is not just an ad-hoc task call: its code-facing signature, version, input artifacts, output
artifacts, and partition mapping contribute to graph identity.

```python
from arti import Graph

with Graph(name="spend") as graph:
    graph.artifacts.transactions = Transactions(...)
    graph.artifacts.total = aggregate_transactions(
        transactions=graph.artifacts.transactions,
    )
```

## Partition mapping

Partitioned producers decide which input partitions produce each output partition. The common case is
one output partition for each input partition. This example mirrors the partitioned Parquet demo:
`raw` is the producer parameter whose partitions drive the mapping, so each raw transaction partition
produces one clean transaction partition.

```python
from typing import Annotated

import pandas as pd

from arti import one_to_one, producer


@producer(map=one_to_one("raw"))
def clean_transactions(
    raw: Annotated[pd.DataFrame, RawTransactions],
) -> Annotated[pd.DataFrame, CleanTransactions]: ...
```

Mapping is part of the data contract. It lets Artigraph record exactly which input partition
fingerprints were used to build each output partition.
