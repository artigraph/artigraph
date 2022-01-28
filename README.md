# artigraph

[![pypi](https://img.shields.io/pypi/v/arti.svg)](https://pypi.python.org/pypi/arti)
[![downloads](https://pepy.tech/badge/arti/month)](https://pepy.tech/project/arti)
[![versions](https://img.shields.io/pypi/pyversions/arti.svg)](https://github.com/artigraph/artigraph)
[![license](https://img.shields.io/github/license/artigraph/artigraph.svg)](https://github.com/artigraph/artigraph/blob/golden/LICENSE)
[![CI](https://github.com/artigraph/artigraph/actions/workflows/ci.yaml/badge.svg)](https://github.com/artigraph/artigraph/actions/workflows/ci.yaml)
[![codecov](https://codecov.io/gh/artigraph/artigraph/branch/golden/graph/badge.svg?token=6LUCpjcGdN)](https://codecov.io/gh/artigraph/artigraph)

Declarative Data Production

Artigraph is a tool to improve the authorship, management, and quality of data. It emphasizes that the core deliverable of a data pipeline or workflow is the data, not the tasks.

## Installation

Artigraph can be installed from PyPI on python 3.9+ with `pip install arti`.

## Example

This sample from the [spend example](docs/examples/spend/demo.py) highlights computing the total amount spent from a series of purchase transactions:

```python
from pathlib import Path
from typing import Annotated

from arti import Annotation, Artifact, Graph, producer
from arti.formats.json import JSON
from arti.storage.local import LocalFile
from arti.types import Collection, Date, Float64, Int64, Struct
from arti.versions import SemVer

DIR = Path(__file__).parent


class Vendor(Annotation):
    name: str


class Transactions(Artifact):
    """Transactions partitioned by day."""

    type = Collection(
        element=Struct(fields={"id": Int64(), "date": Date(), "amount": Float64()}),
        partition_by=("date",),
    )


class TotalSpend(Artifact):
    """Aggregate spend over all time."""

    type = Float64()
    format = JSON()
    storage = LocalFile()


@producer(version=SemVer(major=1, minor=0, patch=0))
def aggregate_transactions(
    transactions: Annotated[list[dict], Transactions]
) -> Annotated[float, TotalSpend]:
    return sum(txn["amount"] for txn in transactions)


with Graph(name="test-graph") as g:
    g.artifacts.vendor.transactions = Transactions(
        annotations=[Vendor(name="Acme")],
        format=JSON(),
        storage=LocalFile(path=str(DIR / "transactions" / "{date.iso}.json")),
    )
    g.artifacts.spend = aggregate_transactions(
        transactions=g.artifacts.vendor.transactions
    )
```

The full example can be run easily with `docker run --rm artigraph/example-spend`:
```
INFO:root:Writing mock Transactions data:
INFO:root:      /usr/src/app/transactions/2021-10-01.json: [{'id': 1, 'amount': 9.95}, {'id': 2, 'amount': 7.5}]
INFO:root:      /usr/src/app/transactions/2021-10-02.json: [{'id': 3, 'amount': 5.0}, {'id': 4, 'amount': 12.0}, {'id': 4, 'amount': 7.55}]
INFO:root:Building aggregate_transactions(transactions=Transactions(format=JSON(), storage=LocalFile(path='/usr/src/app/transactions/{date.iso}.json'), annotations=(Vendor(name='Acme'),)))...
INFO:root:Build finished.
INFO:root:Final Spend data:
INFO:root:      /tmp/test-graph/spend/7564053533177891797/spend.json: 42.0
```
