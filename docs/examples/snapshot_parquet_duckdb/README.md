# Snapshot-aware Pandas + Parquet

This local demo shows the core Artigraph workflow. A graph defines raw and cleaned transaction
artifacts, a Pandas producer writes partitioned Parquet outputs, Artigraph records fingerprints for
each produced partition, and named graph snapshots let you read back a coherent version later. The
DuckDB query at the end shows the payoff: an outside tool gets the correct answer because it reads
exactly the files in the snapshot, not whatever a glob happens to match.

## Run it

From the repository root:

```shell
uv run --all-extras --with duckdb python docs/examples/snapshot_parquet_duckdb/demo.py
```

The command uses only local files, an in-memory DuckDB connection, and a local SQLite metadata
database under `_work/`. It does not require Docker, cloud credentials, or an external service.
Pass `--work-dir <dir>` to write elsewhere. Each run starts fresh: the demo deletes a work directory
it created before (marked by a `.artigraph-demo` file), and refuses to touch any other non-empty one.

Expected output includes:

```text
Naive glob files: 3 for 2 logical snapshot partitions
Shared files across snapshots: 1
Baseline-only files: 1
Corrected-only files: 1
Baseline snapshot files: 2
Corrected snapshot files: 2
Baseline snapshot total: 42.00
Corrected snapshot total: 39.50
Naive glob total: 51.50 (wrong: it mixes live versions from both snapshots)
Corrected snapshot statistics for the cleaned transactions:
  2026-01-01:
    DistinctCount(field='transaction_id') = 2
    Max(field='amount_usd') = 18.0
    RowCount() = 2
    NullCount(field='vendor') = 0
    Ratio(numerator=NullCount(field='vendor'), denominator=RowCount()) = 0.0
    DistinctValues(field='category') = ['meals', 'office']
    Min(field='amount_usd') = 12.0
  2026-01-02:
    DistinctCount(field='transaction_id') = 1
    Max(field='amount_usd') = 9.5
    RowCount() = 1
    NullCount(field='vendor') = 0
    Ratio(numerator=NullCount(field='vendor'), denominator=RowCount()) = 0.0
    DistinctValues(field='category') = ['software']
    Min(field='amount_usd') = 9.5
Build stopped by a Threshold: `clean` failed 1 Threshold check(s):
- {'transaction_date': DateField(value=datetime.date(2026, 1, 3))}: Range(statistic=Min(field='amount_usd'), min=0) got -18.0
Fix the data, revise the Thresholds, or accept a partition with `Graph.override_threshold`.
Overridden snapshot total: 21.50
```

The generated file list also exposes the input fingerprint in every produced path. One unchanged
day is reused, while the corrected day has old and new Parquet files under different fingerprint
directories. The naive glob reads three physical files for two logical partitions, mixing versions
from different snapshots and returning a wrong total. Snapshot metadata avoids exactly this.

## Statistics and quality gates

The cleaned artifact in [`model.py`](model.py) declares
[statistics and thresholds](../../concepts/statistics-and-thresholds.md). Each produced partition
records facts like its distinct transaction IDs and maximum amount. Its thresholds require at least
one row, at most 5% null vendors (a ratio derived from two statistics), only known categories, and
no negative amounts. Artigraph computes the statistics from the producer's in-memory DataFrame, so
it doesn't read the Parquet files back. The unchanged day reuses both its file and its statistics.

At the end, the demo writes a refund with a negative amount. The cleaned partition fails
`Range(Min("amount_usd"), min=0)`, so the build raises a `ThresholdError` before anything downstream
can read it. The `baseline` and `corrected` tags still point at their snapshots, and the failing
partition and its statistics stay recorded as facts.

Refunds are expected, so the demo then overrides the threshold for that partition, with a reason.
The next build reuses the recorded partition, logs the accepted failure, and tags the result as
`with-refund`. The override applies only to that partition's exact contents and that exact
threshold.

## Snapshot-aware ecosystem access

The cleaned artifact's storage template includes `{input_fingerprint}`. Correcting a raw daily
partition creates a new cleaned partition without deleting the old version. You get this for free:
past snapshots stay reproducible, while the corrected snapshot points at the new output.

When another tool wants to read those files, it should start from Artigraph's snapshot metadata
instead of reconstructing membership from storage layout conventions. The helper in
[`snapshot_duckdb.py`](snapshot_duckdb.py) looks up the partitions linked to
`(GraphSnapshot, artifact_key)`. Before passing those exact paths to DuckDB, it checks that every
partition is local Parquet, exists, and still matches its recorded content fingerprint. DuckDB stays
in memory and never creates a database file.

The interop pattern is: register a DuckDB view from a specific Artigraph snapshot, then query that
view like any other DuckDB relation.

```python
import duckdb

from snapshot_duckdb import register_snapshot_view

with duckdb.connect(":memory:") as connection:
    register_snapshot_view(
        connection,
        snapshot=corrected,
        artifact=corrected.artifacts.clean,
        view_name="clean_transactions",
    )
    total = connection.sql("SELECT sum(amount_usd) FROM clean_transactions").fetchone()[0]
```

## Limitation: raw local inputs

Overwriting a raw local path does not preserve the old raw bytes. Artigraph records their
fingerprints, but local storage is not automatically content-addressed. Cross-snapshot interop
queries in this demo therefore target produced artifacts, whose input-fingerprint paths preserve both
versions.

## Extending this to GCS later

The artifact format and Pandas producer code can remain the same; the storage implementation is the
main swap:

```python
# Local demo
from arti.storage.local import LocalFile

storage = LocalFile.rooted_at(work_dir / "data" / "produced")

# Possible future GCS variant
from arti.storage.google.cloud.storage import GCSFile

storage = GCSFile(
    bucket="my-artigraph-demo-bucket",
    path="demo/{graph_name}/{names}/{partition_key_spec}/{input_fingerprint}/{name}{extension}",
)
```

The GCS snippet shows a possible future extension shape. It is not a tested path today and this
example does not provision cloud resources.
