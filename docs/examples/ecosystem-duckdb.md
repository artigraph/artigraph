# DuckDB ecosystem interop

DuckDB is useful when you want SQL over Artigraph-produced Parquet files. Any Parquet-aware tool can
consume Artigraph outputs; DuckDB is a convenient example. The important part is that Artigraph tells
the tool exactly which files belong to a snapshot.

## The rule

Use Artigraph metadata to choose files, then hand those files to DuckDB. This snippet uses the
`corrected` snapshot and the `snapshot_duckdb.py` helper from the
[snapshot-aware Pandas + Parquet example](snapshot_parquet_duckdb/README.md).

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
    total = connection.sql(
        "SELECT sum(amount_usd) FROM clean_transactions",
    ).fetchone()[0]
```

You give DuckDB the files in a snapshot, not a folder to guess at. The helper resolves the Parquet
files recorded for `(GraphSnapshot, artifact_key)`, verifies that they still match their recorded
fingerprints, and registers an in-memory DuckDB view over those exact files. You can copy this helper
into your own project; it is not part of Artigraph's public API yet.

## Why not just use storage paths?

In simple storage layouts a glob might appear to work, but a glob is still a convention rather than a
snapshot. Artigraph's storage model allows multiple produced versions to remain live at once. That is
useful for reproducibility and cache reuse. When you want a coherent graph version, the source of
truth is the snapshot metadata.
