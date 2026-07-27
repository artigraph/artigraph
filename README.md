# artigraph

[![pypi](https://img.shields.io/pypi/v/arti.svg)](https://pypi.python.org/pypi/arti)
[![changelog](https://img.shields.io/github/v/release/artigraph/artigraph?label=changelog)](https://github.com/artigraph/artigraph/releases)
[![downloads](https://pepy.tech/badge/arti/month)](https://pepy.tech/project/arti)
[![versions](https://img.shields.io/pypi/pyversions/arti.svg)](https://github.com/artigraph/artigraph)
[![license](https://img.shields.io/github/license/artigraph/artigraph.svg)](https://github.com/artigraph/artigraph/blob/main/LICENSE)
[![CI](https://github.com/artigraph/artigraph/actions/workflows/ci.yaml/badge.svg)](https://github.com/artigraph/artigraph/actions/workflows/ci.yaml)
[![codecov](https://codecov.io/gh/artigraph/artigraph/branch/main/graph/badge.svg?token=6LUCpjcGdN)](https://codecov.io/gh/artigraph/artigraph)
[![OpenSSF Best Practices](https://bestpractices.coreinfrastructure.org/projects/5561/badge)](https://bestpractices.coreinfrastructure.org/projects/5561)

Declarative Data Production

Artigraph is a tool to improve the authorship, management, and quality of data. It emphasizes that the core deliverable of a data pipeline or workflow is the data, not the tasks.

Artigraph is hosted by the [LF AI and Data Foundation](https://lfaidata.foundation) as a Sandbox project. See our [deck](https://docs.google.com/presentation/d/1KLM9r0L5sTbpb_UPR5nx4fil-7fO-UnmhTeatSiaN3Y) or [presentation](https://wiki.lfaidata.foundation/download/attachments/7733341/GMT20220127-140219_Recording_3840x2160.mp4?version=1&modificationDate=1643716019000&api=v2) (@6m35s) requesting Sandbox incubation.

## Community

We're excited to hear from anyone interested in the project - feel free to introduce yourself over in the [Intro Discussions](https://github.com/artigraph/artigraph/discussions/categories/intros)! See our [support page](SUPPORT.md) for help or our [contributing page](CONTRIBUTING.md) for guidelines.

## Installation

Artigraph can be installed from PyPI with `pip install arti`.

Install optional data format and metadata backend extras as needed:

```shell
pip install 'arti[parquet]'  # Parquet/Pandas data files
pip install 'arti[sqlite]'   # local durable metadata
pip install 'arti[postgres]' # Postgres durable metadata
```

## Flagship local demo

The best starting point is the
[snapshot-aware Pandas/Parquet demo](docs/examples/snapshot_parquet_duckdb/README.md). It shows the
core Artigraph workflow: define artifacts, produce partitioned data with Pandas, preserve output
versions with fingerprints, tag graph snapshots, and read a specific snapshot back through ordinary
Python APIs. The final step uses in-memory DuckDB only as an ecosystem interop check against the
exact Parquet files recorded in Artigraph metadata.

Run it from a checkout with:

```shell
uv run --all-extras --with duckdb python docs/examples/snapshot_parquet_duckdb/demo.py
```

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

Both backends apply bundled Alembic migrations on first use. Pass `auto_upgrade=False` to require
an explicit `backend.upgrade()` instead.

Persisted metadata is structured JSON/JSONB containing module-qualified Python type paths. Artifact
and Producer classes therefore need to be defined at module scope in an importable module; local
classes, local decorated functions, and definitions run only as `__main__` cannot be restored in a
future process. Treat the database as trusted: restoring metadata imports the recorded modules.

## Example

This sample from the [spend example](docs/examples/spend/demo.py) highlights computing the total amount spent from a series of purchase transactions:

```python
from pathlib import Path
from typing import Annotated, TypedDict

from arti import Annotation, Artifact, ArtifactStorage, Format, Graph, Type, producer
from arti.formats.json import JSON
from arti.storage.local import LocalFile
from arti.types import Collection, Date, Float64, Int64, Struct
from arti.versions import SemVer

DIR = Path(__file__).parent


class Vendor(Annotation):
    name: str


class Transactions(Artifact):
    """Transactions partitioned by day."""

    type: Type = Collection(
        element=Struct(fields={"id": Int64(), "date": Date(), "amount": Float64()}),
        partition_by=("date",),
    )


class TotalSpend(Artifact):
    """Aggregate spend over all time."""

    type: Type = Float64()
    format: Format = JSON()
    storage: ArtifactStorage = LocalFile()


class Transaction(TypedDict):
    id: int
    amount: float


@producer(version=SemVer(major=1, minor=0, patch=0))
def aggregate_transactions(
    transactions: Annotated[list[Transaction], Transactions],
) -> Annotated[float, TotalSpend]:
    return float(sum(txn["amount"] for txn in transactions))


with Graph(name="test-graph") as g:
    g.artifacts.vendor.transactions = Transactions(
        annotations=(Vendor(name="Acme"),),
        format=JSON(),
        storage=LocalFile(path=str(DIR / "transactions" / "{date.iso}.json")),
    )
    g.artifacts.spend = aggregate_transactions(transactions=g.artifacts.vendor.transactions)
```

The [full, tested example](docs/examples/spend/demo.py) can be run from a checkout:

```shell
uv run python docs/examples/spend/demo.py
```

Or build and run it in Docker:

```shell
docker build --file docs/examples/spend/Dockerfile --tag artigraph/example-spend .
docker run --rm artigraph/example-spend
```

Both commands finish with:

```
INFO:root:Total Spend: 42.0
```
