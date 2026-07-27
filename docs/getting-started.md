# Getting started

The fastest way to understand Artigraph is to build a small graph that writes real data, records
fingerprints, and reads a snapshot back.

## Install

From a checkout:

```shell
uv sync --all-extras
```

From PyPI:

```shell
pip install arti
```

Add optional extras when you need them:

```shell
pip install 'arti[parquet]'  # Pandas/Parquet data files
pip install 'arti[sqlite]'   # durable local metadata
```

## Build your first graph

This example defines one raw artifact and one produced artifact, writes input data, builds a
snapshot, and reads the result.

```python
from typing import Annotated, TypedDict

from arti import Artifact, ArtifactStorage, Format, Graph, Type, producer
from arti.formats.json import JSON
from arti.storage.local import LocalFile
from arti.types import Collection, Float64, Int64, Struct
from arti.versions import SemVer


class Transactions(Artifact):
    type: Type = Collection(element=Struct(fields={"id": Int64(), "amount": Float64()}))
    format: Format = JSON()
    storage: ArtifactStorage = LocalFile(path="/tmp/artigraph-first-graph/transactions.json")


class TotalSpend(Artifact):
    type: Type = Float64()
    format: Format = JSON()
    storage: ArtifactStorage = LocalFile(
        path="/tmp/artigraph-first-graph/produced/{input_fingerprint}/total.json",
    )


class Transaction(TypedDict):
    id: int
    amount: float


@producer(version=SemVer(major=1, minor=0, patch=0))
def aggregate(
    transactions: Annotated[list[Transaction], Transactions],
) -> Annotated[float, TotalSpend]:
    return float(sum(transaction["amount"] for transaction in transactions))


with Graph(name="first-graph") as graph:
    graph.artifacts.transactions = Transactions()
    graph.artifacts.total = aggregate(transactions=graph.artifacts.transactions)


graph.write(
    [{"id": 1, "amount": 18.0}, {"id": 2, "amount": 24.0}],
    artifact=graph.artifacts.transactions,
)
snapshot = graph.build()
total = snapshot.read(snapshot.artifacts.total, annotation=float)
print(total)  # 42.0
```

The core steps are:

- **Artifact** — the contract for a data product: type, format, and storage.
- **Producer** — a versioned Python function that builds one artifact from another.
- **`graph.write(...)`** — records raw input data before a build.
- **`graph.build()`** — runs producers and returns a `GraphSnapshot`.
- **`snapshot.read(...)`** — reads an artifact value from that exact snapshot.

See [The graph lifecycle](concepts/lifecycle.md) for tagging and reloading a snapshot later.

## Run the small spend example

The smallest tested example computes total spend from JSON transactions:

```shell
uv run python docs/examples/spend/demo.py
```

It finishes with:

```text
INFO:root:Total Spend: 42.0
```

## Run the local data-product demo

The flagship local demo uses Pandas producers, partitioned Parquet outputs, durable SQLite metadata,
and a snapshot-aware readback step:

```shell
uv run --all-extras --with duckdb python docs/examples/snapshot_parquet_duckdb/demo.py
```

Expected output includes:

```text
Baseline snapshot total: 42.00
Corrected snapshot total: 39.50
Naive glob total: 51.50
```

DuckDB is just one ordinary tool that can consume the result. The user-facing benefit is that
Artigraph supplies the exact files in a snapshot, so downstream tools do not guess from path globs or
mix versions by accident.

## What to read next

- [Status and fit](status.md): what works today and where Artigraph belongs in your stack.
- [The graph lifecycle](concepts/lifecycle.md): `write → build → tag → read`.
- [Artifacts](concepts/artifacts.md): the contracts for data products.
- [Producers](concepts/producers.md): the versioned Python functions that build artifacts.
- [Snapshots and fingerprints](concepts/snapshots-and-fingerprints.md): how Artigraph names a
  coherent graph version.
- [Storage, formats, and views](concepts/storage-formats-and-views.md): how data is written and read.
