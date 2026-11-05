# Executors

An **executor** decides *how* the producer partitions of a snapshot are built: in which order, in
which thread, process, or machine. Every executor shares the same planning and building steps, so the
choice of executor never changes fingerprints, snapshot IDs, or which partitions belong to a
snapshot.

```python
from arti.executors.local import LocalExecutor

snapshot = graph.build(LocalExecutor())  # The default, also used by `graph.build()`
```

## Parallel builds

`ConcurrentExecutor` builds independent producers and partitions in parallel with
`concurrent.futures`. Each producer is planned as soon as the producers it depends on finish.

```python
from arti.executors.concurrent import ConcurrentExecutor

# Threads work with any graph, including the default in-memory backend.
snapshot = graph.build(ConcurrentExecutor(max_workers=8))

# Processes import the graph by reference and need metadata and data shared across processes.
snapshot = graph.build(
    ConcurrentExecutor(kind="process", graph_ref="my_project.graphs:graph", max_workers=8)
)
```

Threads suit producers that release the GIL (IO, NumPy, Pandas, DuckDB, ...); processes suit
pure-Python producers. If a partition fails, the executor starts no new partitions and raises the
first error once the running partitions finish.

## Orchestrators

Artigraph is a library, not a scheduler. These integrations hand the partitions of a build to an
orchestrator you already run, while Artigraph still decides what to build and records the
fingerprints, partitions, and snapshot. Each one is an optional extra, builds from
[work units](#work-units), and so needs an importable graph.

### Prefect

`pip install 'arti[prefect]'`

`arti.executors.prefect.build_graph` is a Prefect flow with one task per producer partition (plus a
task to snapshot the graph and one to plan each producer). The flow's task runner controls the
concurrency. Run it from `graph.build(...)`:

```python
from prefect.task_runners import ThreadPoolTaskRunner

from arti.executors.prefect import PrefectExecutor

snapshot = graph.build(
    PrefectExecutor(
        graph_ref="my_project.graphs:graph",
        task_runner=ThreadPoolTaskRunner(max_workers=8),
    )
)
```

Or deploy `build_graph` like any other flow, with a `graph_ref` parameter. Artigraph decides which
partitions to build, so the tasks disable Prefect's caching. The backend and storage must be shared
as widely as the task runner runs tasks: a `ThreadPoolTaskRunner` accepts any graph, a
`ProcessPoolTaskRunner` needs them shared across processes, and other task runners (eg: Dask or Ray)
need them shared across machines.

## How a build runs

For each producer, once all of its inputs are built, an executor:

1. **Plans** the producer (`Executor.plan_producer`): reads the input partitions linked to the
   snapshot, calls the producer's `map` to decide the output partitions and their input
   fingerprints, and links output partitions that were already built (eg: by an earlier snapshot)
   to this snapshot.
2. **Builds** each pending partition (`Executor.build_producer_partition`): reads the inputs, calls
   `build`, writes every output to storage, and then registers all of the outputs with the backend
   in one transaction.

A producer can only be planned after its inputs are built, because `map` decides the output
partitions from the input partitions. So the full set of partitions in a graph is only known while
the build runs.

## Failures and concurrent writers

Artigraph does not lock storage. Instead, building is idempotent and inconsistencies fail loudly:

- A partition is checked again right before it is built, so a partition published since planning
  (by a retried attempt or another build of the same snapshot) is linked rather than rebuilt.
- The outputs of one partition are registered together, so a failed build never registers only
  some of a producer's outputs. Storage writes are not transactional: a failed attempt may leave
  unregistered data behind, which the next attempt overwrites.
- Two builds of the same partition may still race. With a deterministic producer, both write the
  same contents to the same path and register the same partition. With a non-deterministic
  producer, the backend ends up with two partitions for one partition key, and the next plan of that
  producer raises an error rather than picking one.

## Work units

Executors that build in other processes or machines never send graphs, producers, or data to their
workers. Instead, they send a `WorkUnit`: a small, JSON-serializable reference to one producer
partition.

```python
from arti.executors.work_units import WorkUnit

WorkUnit(
    graph_ref="my_project.graphs:graph",  # "package.module:attribute"
    snapshot_id=...,
    producer_key="clean",  # The key of the producer's first output artifact
    partition_key={"date": {"value": "2024-01-01"}},
    input_fingerprint=...,
)
```

Workers run the same code (or image) as the process that planned the build. A worker imports the
graph from `graph_ref`, checks that the backend knows the snapshot for that graph, recomputes the
producer's partitions from the inputs linked to the snapshot, checks that the input fingerprint still
matches, and then builds the partition. A worker running different code fails fast instead of
building with it.

So graphs built this way must be importable: define them at module scope in an importable module,
not in a script run as `__main__`. The workers must also reach the same metadata and data, which
each executor checks before it starts:

| Reach | Backends | Storage |
| --- | --- | --- |
| Only the current process | `MemoryBackend`, in-memory `SQLiteBackend` | |
| Other processes on the same machine | file `SQLiteBackend` | `LocalFile` |
| Other machines | `PostgresBackend` | `GCSFile`, `StringLiteral` (stored in the backend) |

The `arti.executors.work_units` module also holds the three functions an orchestrator needs:
`snapshot_graph(graph_ref)`, `plan_work_units(graph_ref, snapshot_id, producer_key)`, and
`run_work_unit(unit)`.
