# Artigraph

[![pypi](https://img.shields.io/pypi/v/arti.svg)](https://pypi.python.org/pypi/arti)
[![changelog](https://img.shields.io/github/v/release/artigraph/artigraph?label=changelog)](https://github.com/artigraph/artigraph/releases)
[![downloads](https://pepy.tech/badge/arti/month)](https://pepy.tech/project/arti)
[![versions](https://img.shields.io/pypi/pyversions/arti.svg)](https://github.com/artigraph/artigraph)
[![license](https://img.shields.io/github/license/artigraph/artigraph.svg)](https://github.com/artigraph/artigraph/blob/main/LICENSE)
[![CI](https://github.com/artigraph/artigraph/actions/workflows/ci.yaml/badge.svg)](https://github.com/artigraph/artigraph/actions/workflows/ci.yaml)
[![codecov](https://codecov.io/gh/artigraph/artigraph/branch/main/graph/badge.svg?token=6LUCpjcGdN)](https://codecov.io/gh/artigraph/artigraph)
[![OpenSSF Best Practices](https://bestpractices.coreinfrastructure.org/projects/5561/badge)](https://bestpractices.coreinfrastructure.org/projects/5561)

**Declarative data production for Python.**

Artigraph is a Python library for building data as **versioned, fingerprinted artifacts** instead of
loose files scattered across a filesystem. You declare the artifacts your workflow should produce and
the producers that build them; Artigraph records a fingerprint for every partition and a snapshot of
every graph version, so you can reproduce, inspect, and hand off a coherent version of your data
later.

The problem it solves: once a pipeline has run a few times, "which files make up this dataset?"
becomes guesswork. A glob can pick up stale partitions; a rerun can overwrite an output you still
needed; a downstream tool can silently mix versions. Artigraph makes snapshot membership explicit, so
a version is something you can tag and load — not a directory you hope is clean.

Artigraph is not a scheduler or orchestrator. Bring your own runner — a script, cron, Airflow,
Dagster, or anything else. Artigraph owns the data contract: types, storage, fingerprints, and
snapshot lineage.

Read the docs at [artigraph.github.io/artigraph](https://artigraph.github.io/artigraph/) or try the
[browser playground](https://artigraph.github.io/artigraph/playground/).

Artigraph is an early-stage, pre-1.0 project hosted by the
[LF AI & Data Foundation](https://lfaidata.foundation) as a Sandbox project. See
[Status and fit](https://artigraph.github.io/artigraph/status/) for what works today and where it
belongs in a stack.

## Installation

Artigraph can be installed from PyPI with:

```shell
pip install arti
```

Install optional data format and metadata backend extras as needed:

```shell
pip install 'arti[parquet]'  # Pandas/Parquet data files
pip install 'arti[sqlite]'   # local durable metadata
pip install 'arti[postgres]' # Postgres durable metadata
```

## Build a first graph

This complete example defines one raw artifact and one produced artifact, writes input data, builds a
snapshot, and reads the result back.

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

The core loop is `write → build → read`: write raw inputs, build a fingerprinted snapshot, then read
an artifact from that exact version.

## Run the local demo

The best local starting point is the
[snapshot-aware Pandas/Parquet demo](https://github.com/artigraph/artigraph/blob/main/docs/examples/snapshot_parquet_duckdb/README.md).
It shows the core Artigraph workflow with partitioned Pandas data, Parquet files, SQLite metadata,
versioned outputs, tagged snapshots, and snapshot-aware readback.

Run it from a checkout with:

```shell
uv run --all-extras --with duckdb python docs/examples/snapshot_parquet_duckdb/demo.py
```

Expected output includes:

```text
Baseline snapshot total: 42.00
Corrected snapshot total: 39.50
Naive glob total: 51.50
```

The final step uses in-memory DuckDB only as an ecosystem interop check: DuckDB gets the exact
Parquet files recorded in Artigraph snapshot metadata instead of guessing from a path glob.

## Durable metadata backends

Graphs use an in-process memory backend by default. SQLite and Postgres backends persist graphs,
snapshots, tags, and storage-partition metadata with SQLAlchemy:

```python
from pathlib import Path

from arti import Graph
from arti.backends.sqlite import SQLiteBackend

backend = SQLiteBackend(path=Path("artigraph.sqlite"))
with Graph(name="example", backend=backend) as graph:
    ...
```

Use `path=":memory:"` for a shared in-memory SQLite database. Postgres accepts a SQLAlchemy
psycopg URL:

```python
from arti.backends.postgres import PostgresBackend

backend = PostgresBackend.from_url("postgresql+psycopg://artigraph:password@localhost/artigraph")
```

Both backends apply bundled Alembic migrations on first use. Pass `auto_upgrade=False` to require an
explicit `backend.upgrade()` instead.

Persisted metadata is structured JSON/JSONB containing module-qualified Python type paths. Artifact
and producer classes therefore need to be defined at module scope in an importable module; local
classes, local decorated functions, and definitions run only as `__main__` cannot be restored in a
future process. Treat the database as trusted: restoring metadata imports the recorded modules.

## Community

We're excited to hear from anyone interested in the project. Feel free to introduce yourself in the
[Intro Discussions](https://github.com/artigraph/artigraph/discussions/categories/intros). See our
[support page](https://github.com/artigraph/artigraph/blob/main/SUPPORT.md) for help or our
[contributing page](https://github.com/artigraph/artigraph/blob/main/CONTRIBUTING.md) for guidelines.
