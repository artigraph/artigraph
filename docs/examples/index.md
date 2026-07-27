# Examples

Start with examples that produce and read real data locally. The examples are intentionally small,
but they exercise Artigraph's central model: artifacts, producers, fingerprints, snapshots, and
storage metadata.

## Snapshot-aware Pandas + Parquet

The flagship local example builds partitioned transaction data with Pandas, writes Parquet files, and
uses Artigraph snapshot metadata to read a coherent version back.

[Run the example](snapshot_parquet_duckdb/README.md)

## Spend

The smaller spend example uses JSON files and Python values. It is useful when you want the fewest
moving parts:

```shell
uv run python docs/examples/spend/demo.py
```

Or build and run it in Docker:

```shell
docker build --file docs/examples/spend/Dockerfile --tag artigraph/example-spend .
docker run --rm artigraph/example-spend
```

## Ecosystem interop

Artigraph-managed files are ordinary files. The difference is that Artigraph also records enough
metadata for tools outside Artigraph to know which files belong to a snapshot.

[Read about DuckDB interop](ecosystem-duckdb.md)
