# Dashboard

The dashboard is a read-only web page for browsing what a metadata backend knows: graphs, their
versions and snapshots, tags, and the partitions in each snapshot. It draws each graph version as a
lineage diagram of artifacts and the producers that connect them.

The dashboard is bundled with Artigraph and served with Python's standard library, so it needs no
extra install and no Node.js.

## Try it

Build the [Pandas + Parquet example](examples/snapshot_parquet_duckdb/README.md), which records two
tagged snapshots (`baseline` and `corrected`) in a SQLite backend:

```shell
uv run --all-extras --with duckdb python docs/examples/snapshot_parquet_duckdb/demo.py
```

Then serve the dashboard from the example's directory, so the example's modules are importable (see
[What it needs](#what-it-needs)):

```shell
cd docs/examples/snapshot_parquet_duckdb
uv run --all-extras arti dashboard --sqlite _work/artigraph.sqlite --open
```

Open the graph, then pick a snapshot. Badges on each artifact count the partitions linked to it in
that snapshot. Select an artifact to see its type, format, storage, and the exact partitions (files,
partition keys, and fingerprints) in the snapshot. Compare `baseline` and `corrected` to see one
partition reused and one rebuilt under a new input fingerprint.

## Point it at your metadata

`arti dashboard` takes a `module:attribute` reference to a `Backend` or a `Graph`, like
`python -m` would import it from the current directory:

```shell
arti dashboard my_project.metadata:backend   # every graph in the backend
arti dashboard my_project.graphs:graph       # the graph's backend, plus its current definition
arti dashboard --sqlite artigraph.sqlite     # a SQLite backend file
```

With a `Graph` target, the dashboard also shows the graph's current definition, even if it has
never been snapshotted, which makes it a quick way to check a graph's shape while writing it. A
graph using the default in-memory backend only has whatever the module itself built, so point the
dashboard at a durable backend (SQLite or Postgres) to browse history across runs. For Postgres,
define the backend in a module so the connection URL stays out of your shell history.

Options:

- `--host` and `--port` choose the address (default `127.0.0.1:8000`; `--port 0` picks a free one).
- `--open` opens the page in a browser.

The current view is kept in the page URL, so it can be bookmarked or shared with someone browsing
the same backend.

## What it needs

- **Importable definitions.** Restoring metadata imports the modules that defined your artifacts
  and producers (see the backend [trust boundary](concepts/metadata-backends.md#trust-boundary)), so
  run the dashboard where those modules import the same way as when the graph was built. If they
  can't be imported, the page shows the import error rather than a partial graph.
- **A trusted backend and network.** The dashboard has no authentication. It binds to `127.0.0.1`
  by default; only bind to other addresses on networks you trust.
- **An up-to-date schema.** `--sqlite` never migrates the database; upgrade it with the version of
  Artigraph that writes to it.

## What it shows (and doesn't)

- Graphs, versions, and snapshots come from the backend each time the page loads or you press
  **Refresh**, so new snapshots and tags appear without a restart.
- Backends don't record when metadata was written, so versions and snapshots are listed in a stable
  (fingerprint) order, not by time. Use tags to name the snapshots that matter.
- Partitions are shown per snapshot, from the snapshot's recorded membership. The dashboard never
  scans storage or reads artifact data.
- Annotations and statistics are shown as recorded on each artifact. Richer statistics views will
  follow the statistics interface.
- The dashboard only reads. It never builds, tags, or deletes anything.

## Use it from Python

```python
from arti.backends.sqlite import SQLiteBackend
from arti.dashboard import DashboardData, make_server

with SQLiteBackend(path="artigraph.sqlite") as backend:
    server = make_server(DashboardData(backend), port=8000)
    server.serve_forever()
```

`DashboardData` also returns the page's JSON documents directly (`graphs()`, `graph(name)`,
`graph_version(name, fingerprint)`, and `snapshot(name, fingerprint)`), which can be handy in
notebooks or tests. Fingerprints are sent as strings, since JavaScript numbers can't represent
every int64 value.

## Develop the page

The page's source lives in `tools/dashboard` (React, [React Flow](https://reactflow.dev), and
Vite). Its production build is committed to `src/arti/dashboard/static`, so changes to the source
must be rebuilt and committed together. CI checks that the two match:

```shell
mise run dashboard
```

For hot reloading, run `arti dashboard ...` on port 8000 and `npm run dev` in `tools/dashboard`,
which proxies API requests to it.
