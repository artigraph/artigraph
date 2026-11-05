# Metadata backends

A backend stores graph definitions, snapshots, tags, and storage partition metadata. The backend is
metadata-only; artifact bytes still live in the configured storage system.

## Memory

The default backend is in-process memory. It is useful for examples and tests, but it does not
persist snapshots across processes.

## SQLite

SQLite is the recommended local durable backend:

```python
from pathlib import Path

from arti.backends.sqlite import SQLiteBackend

backend = SQLiteBackend(path=Path("artigraph.sqlite"))
```

Use SQLite for local examples, notebooks, and demos where you want tags and snapshots to survive
process restarts without running a service.

## Postgres

Postgres is intended for shared durable metadata:

```python
from arti.backends.postgres import PostgresBackend

backend = PostgresBackend.from_url(
    "postgresql+psycopg://artigraph:password@localhost/artigraph",
)
```

Both SQL backends apply bundled Alembic migrations on first use by default. Set
`auto_upgrade=False` if you want to control migrations explicitly.

## Browsing metadata

Every backend connection can list what it knows, for tools that discover graphs rather than
define them:

- `read_graph_names()`: the names of all known graphs.
- `read_graphs(name)`: every version (fingerprint) of a graph.
- `read_snapshots(name)`: every snapshot of a graph.
- `read_snapshot_tags(name)`: the snapshot each tag points to.

Backends don't record when metadata was written, so versions and snapshots are ordered by
fingerprint: stable, but not chronological. Use tags to name the snapshots that matter.

## Trust boundary

Persisted metadata includes module-qualified Python type paths, so artifact and producer
definitions must live at module scope in an importable module. Definitions in a script run as
`__main__` cannot be persisted. Restoring graph metadata imports the recorded modules, so treat the
backend as trusted infrastructure.
