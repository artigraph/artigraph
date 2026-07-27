# Snapshots and fingerprints

A **fingerprint** is Artigraph's stable identity primitive. Artifacts, producers, partition contents,
and graph snapshots all use fingerprints so Artigraph can detect meaningful changes and preserve
coherent versions.

## What a graph snapshot means

A `GraphSnapshot` identifies a graph at a point in time:

- the graph structure,
- artifact and producer definitions,
- raw partition fingerprints,
- produced partition fingerprints after a build,
- and the storage partitions that belong to each artifact.

You can tag a snapshot and load it later from a durable backend:

```python
snapshot = graph.build()
snapshot.tag("baseline")

same_snapshot = GraphSnapshot.from_tag(
    "spend",
    "baseline",
    connectable=backend,
)
```

## Why fingerprints matter

Fingerprints are not optional bookkeeping. They are the mechanism that lets Artigraph distinguish:

- a new producer version from an old one,
- a corrected partition from the partition it replaced,
- a reused output partition from one that must be rebuilt,
- and a coherent snapshot from a loose collection of files.

Artigraph currently uses FarmHash Fingerprint64 for string fingerprints and stores values as signed
64-bit integers for database and language interoperability.
