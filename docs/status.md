# Status and fit

Artigraph is an early-stage, pre-1.0 project hosted by the
[LF AI & Data Foundation](https://lfaidata.foundation) as a Sandbox project. The core model —
artifacts, producers, fingerprints, and snapshots — is ready to explore in local workflows, and the
examples in these docs are tested. APIs in the [curated reference](reference/core.md) are the public
surface to start from; other modules may change faster. Use the project
[changelog](https://github.com/artigraph/artigraph/releases) to track changes between releases.

## What works today

- **Python:** 3.13 and 3.14, both tested in CI.
- **Storage:** local files.
- **Formats:** JSON and Parquet, with Parquet support focused on Pandas DataFrames.
- **Metadata backends:** in-memory by default, SQLite for durable local metadata, and Postgres for a
  shared durable backend.
- **Core workflow:** define artifacts and producers, write raw inputs, build a graph, tag a snapshot,
  and read a named snapshot back.
- **Data quality:** statistics (row, null, and distinct counts, distinct values, min and max, and
  derived ratios) for Python and Pandas views, recorded per partition. Thresholds (`Range`,
  `Equal`, and `Subset`) gate builds, and a failing partition can be accepted with a recorded
  override.
- **Ecosystem interop:** hand exact snapshot files to outside tools, such as DuckDB, without relying
  on path globs.
- **Experimental:** GCS storage (`GCSFile`), with JSON and pickle IO only.

## Planned / not yet supported

- Cloud Parquet IO.
- Thresholds that compare a partition with others, such as earlier partitions.
- Additional storage and format implementations.

The examples include short notes showing the intended shape of future cloud storage, but the tested
path today is local-first.

## Where Artigraph fits

Artigraph is a **library**, not a scheduler. Use it with a script, cron, Airflow, Dagster, or any
other runner when you want reproducible, versioned data artifacts with explicit snapshot lineage.

If you need task scheduling or a managed DAG, keep that tool. Let Artigraph own the data contract:
types, storage, fingerprints, and the metadata that says which partitions belong to each snapshot.
