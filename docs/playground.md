# Browser playground

Run a real Artigraph graph in your browser. The Python runtime starts preparing in the background
when the playground scrolls into view, so the first run may still take a moment. Later runs reuse the
same initialized Pyodide worker and only reset the virtual filesystem before executing your code.

This is intentionally Artigraph-first: the sample defines artifacts, connects a producer, writes a
raw partition, builds a snapshot, and reads the produced artifact back. There is no cloud account,
external database, or DuckDB dependency in the playground path.

<div class="artigraph-playground" data-artigraph-playground data-timeout-ms="90000">
  <div class="artigraph-playground__toolbar">
    <button type="button" data-artigraph-run>Run</button>
    <button type="button" data-artigraph-stop disabled>Stop</button>
    <button type="button" data-artigraph-reset>Reset</button>
    <span class="artigraph-playground__status" data-artigraph-status role="status">Loading...</span>
  </div>
  <div class="artigraph-playground__editor" data-artigraph-editor>
    <textarea class="artigraph-playground__code" data-artigraph-code spellcheck="false" wrap="off" aria-label="Editable Artigraph Python example">
from datetime import date
from pathlib import Path
from typing import Annotated, TypedDict

from arti import Artifact, ArtifactStorage, Format, Graph, PartitionKey, Type, producer
from arti.formats.json import JSON
from arti.partitions import DateField
from arti.storage.local import LocalFile
from arti.types import Collection, Date, Float64, Int64, Struct
from arti.versions import SemVer


class Transactions(Artifact):
    type: Type = Collection(
        element=Struct(fields={"id": Int64(), "date": Date(), "amount": Float64()}),
        partition_by=("date",),
    )


class TotalSpend(Artifact):
    type: Type = Float64()
    format: Format = JSON()
    storage: ArtifactStorage = LocalFile(
        path="/tmp/produced/{input_fingerprint}/total.json",
    )


class Transaction(TypedDict):
    id: int
    amount: float


@producer(version=SemVer(major=1, minor=0, patch=0))
def aggregate_transactions(
    transactions: Annotated[list[Transaction], Transactions],
) -> Annotated[float, TotalSpend]:
    return float(sum(txn["amount"] for txn in transactions))


with Graph(name="browser-spend") as graph:
    graph.artifacts.transactions = Transactions(
        format=JSON(),
        storage=LocalFile(path="/tmp/transactions/{date.iso}.json"),
    )
    graph.artifacts.total = aggregate_transactions(transactions=graph.artifacts.transactions)


graph.write(
    [
        {"id": 1, "date": "2026-01-01", "amount": 18.0},
        {"id": 2, "date": "2026-01-01", "amount": 24.0},
    ],
    artifact=graph.artifacts.transactions,
    partition_key=PartitionKey(date=DateField(value=date(2026, 1, 1))),
)

snapshot = graph.build()
total = snapshot.read(snapshot.artifacts.total, annotation=float)

print(f"Snapshot id: {int(snapshot.id)}")
print(f"Total spend: {total}")
print("Produced files:")
for path in sorted(Path("/tmp/produced").rglob("*.json")):
    print(f"  {path}")

result = {"total": total, "snapshot_id": int(snapshot.id)}
</textarea>
  </div>
  <pre class="artigraph-playground__output" data-artigraph-output aria-live="polite"></pre>
</div>

## Browser sandbox

The playground runs in a Web Worker backed by Pyodide. It may download CodeMirror editor assets from
esm.sh, Pyodide runtime files from the Pyodide CDN, and Artigraph wheels from this docs site, but it
cannot access your local filesystem. Stopping a run, hitting the timeout, or reloading the page
discards the worker.
