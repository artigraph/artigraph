# Artigraph

Artigraph is a Python library for building data products as **versioned, fingerprinted artifacts** —
not just as task runs or loose files. You declare the artifacts your workflow should produce, connect
them with producers, and Artigraph records the fingerprints and snapshot metadata needed to
reproduce, inspect, and hand off a coherent version later.

Use Artigraph alongside the runner you already have. It is not a scheduler or orchestrator: scripts,
cron, Airflow, Dagster, or another tool can decide *when* a workflow runs while Artigraph owns the
data contract — types, storage, fingerprints, and snapshot lineage.

<div class="grid cards" markdown>

-   **Try the playground**

    The in-browser playground runs Artigraph in an isolated Pyodide filesystem so you can edit a
    small graph without installing Python locally.

    [Open the playground](playground.md)

-   **Run the local demo**

    Start with Pandas, Parquet, SQLite metadata, and a snapshot-aware readback flow.

    ```shell
    uv run --all-extras --with duckdb python docs/examples/snapshot_parquet_duckdb/demo.py
    ```

</div>

## What problem does this solve?

After a pipeline has run a few times, "which files make up this dataset?" often becomes guesswork.
A glob can pick up stale partitions; a rerun can overwrite an output you still needed; a downstream
tool can silently mix versions.

Artigraph makes snapshot membership explicit. A version is something you can tag and reload, not a
directory you hope is clean.

## What Artigraph gives you

- **Artifacts** describe the data product: type, format, storage, annotations, and statistics.
- **Producers** describe how artifacts are built from other artifacts.
- **Fingerprints** give artifacts, producers, partitions, and graph snapshots stable identities.
- **Snapshots** let you tag and reload a coherent version of a graph later.
- **Storage metadata** records which concrete partitions belong to a snapshot, so ecosystem tools
  can read the right files without guessing from path layout.

## Recommended path

1. [Get started](getting-started.md) by building a small graph and running the tested local demos.
2. Check [status and fit](status.md) for what works today and where Artigraph belongs in a stack.
3. Read [the graph lifecycle](concepts/lifecycle.md), then the core concepts:
   [artifacts](concepts/artifacts.md), [producers](concepts/producers.md), and
   [snapshots/fingerprints](concepts/snapshots-and-fingerprints.md).
4. Run the [snapshot-aware Pandas + Parquet example](examples/snapshot_parquet_duckdb/README.md).
5. Use the [curated API reference](reference/core.md) for the public objects used in those examples.
