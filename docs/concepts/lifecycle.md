# The graph lifecycle

The other concept pages describe Artigraph's nouns: artifacts, producers, snapshots, and
fingerprints. This page describes the verbs that take a graph from definition to reproducible,
reloadable data.

## 1. Write raw inputs

Raw artifacts hold data you supply. `graph.write(...)` serializes that data through the artifact's
format and storage, then records a partition fingerprint.

```python
graph.write(data, artifact=graph.artifacts.transactions)
```

Write raw inputs before building. `graph.build()` snapshots raw data and raises an error if a raw
artifact has no data to snapshot.

## 2. Build the graph

`graph.build()` runs producers and returns a `GraphSnapshot`: a fingerprinted identity for this graph
version, including the graph structure, artifact and producer definitions, raw partition
fingerprints, and produced partition fingerprints.

```python
snapshot = graph.build()
```

Artigraph does not decide when builds happen. You choose when to call `build()`; Artigraph records
what data and definitions made that build reproducible. If an output partition with the same input
fingerprint already exists, the local executor links it to the new snapshot instead of rebuilding it.

## 3. Tag a snapshot

A fingerprint is exact, but it is not memorable. Tag a snapshot when you want to name a version and
load it later from a durable metadata backend.

```python
snapshot.tag("baseline")
```

Tags are most useful with SQLite or Postgres metadata. With the default in-memory backend, tags only
last as long as the Python process.

## 4. Read or reload a coherent version

Read an artifact from a snapshot you already have:

```python
total = snapshot.read(snapshot.artifacts.total, annotation=float)
```

Or reload a tagged snapshot in a later process from a durable backend, then read from that exact
version. This requires a graph built with that backend from importable definitions; see
[Metadata backends](metadata-backends.md#trust-boundary).

```python
from arti import GraphSnapshot

snapshot = GraphSnapshot.from_tag("first-graph", "baseline", connectable=backend)
total = snapshot.read(snapshot.artifacts.total, annotation=float)
```

## Why the order matters

`write → build → tag → read` makes a version something you can name instead of a directory you hope
is clean. Correcting an input and rebuilding produces a new snapshot without destroying the old one,
so both versions can remain readable.

For the identity model behind this lifecycle, see
[Snapshots and fingerprints](snapshots-and-fingerprints.md).
