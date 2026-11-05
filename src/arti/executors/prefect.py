"""
Build a Graph as a Prefect flow, with one Prefect task per Producer partition.

Requires the `prefect` extra: `pip install 'arti[prefect]'`.

Prefect runs the tasks (and controls their concurrency with the flow's task runner) while Artigraph
still decides which partitions to build and records the fingerprints, partitions, and snapshot.
Tasks only receive `WorkUnit`s, so the Graph must be importable from a `graph_ref`.

Use `PrefectExecutor` to run the flow from `graph.build(...)`, or deploy `build_graph` like any
other flow.
"""

from __future__ import annotations

from typing import Any

from prefect import flow, task
from prefect.cache_policies import NO_CACHE
from prefect.context import FlowRunContext
from prefect.futures import PrefectFuture
from prefect.task_runners import ProcessPoolTaskRunner, TaskRunner, ThreadPoolTaskRunner
from pydantic import PrivateAttr

from arti.backends import Reach
from arti.executors import Executor, logger, producer_dependencies, producers_in_order
from arti.executors.work_units import (
    WorkUnit,
    check_graph_ref,
    check_reach,
    plan_work_units,
    producer_key,
    resolve_graph,
    run_work_unit,
    snapshot_graph,
)
from arti.fingerprints import Fingerprint
from arti.graphs import GraphSnapshot
from arti.producers import Producer


def required_reach(task_runner: TaskRunner[Any]) -> Reach:
    """Return how widely a task runner's tasks must share the Graph's metadata and data."""
    if isinstance(task_runner, ThreadPoolTaskRunner):
        return "process"
    if isinstance(task_runner, ProcessPoolTaskRunner):
        return "host"
    return "network"  # Eg: Dask or Ray task runners


# Artigraph decides whether a partition needs to be built, so disable Prefect's caching.
@task(name="arti-snapshot", cache_policy=NO_CACHE, task_run_name="snapshot {graph_ref}")
def snapshot_task(graph_ref: str) -> int:
    return snapshot_graph(graph_ref)


@task(name="arti-plan", cache_policy=NO_CACHE, task_run_name="plan {producer_key}")
def plan_task(graph_ref: str, snapshot_id: int, producer_key: str) -> tuple[WorkUnit, ...]:
    return plan_work_units(graph_ref, Fingerprint(snapshot_id), producer_key)


@task(name="arti-build-partition", cache_policy=NO_CACHE, task_run_name="build {unit.label}")
def build_partition_task(unit: WorkUnit) -> None:
    run_work_unit(unit)


@flow(name="arti-build")
def build_graph(graph_ref: str, snapshot_id: int | None = None) -> int:
    """
    Build the Graph imported from `graph_ref` and return the snapshot ID.

    Snapshots the Graph first, unless `snapshot_id` is passed. Producers are planned in dependency
    order, each once its inputs are built, and every pending partition becomes a task.
    """
    graph = resolve_graph(graph_ref)
    context = FlowRunContext.get()
    assert context is not None
    check_reach(graph, required_reach(context.task_runner))
    if snapshot_id is None:
        snapshot_id = snapshot_task(graph_ref)
    dependencies = producer_dependencies(graph)
    partitions = dict[Producer, list[PrefectFuture[None]]]()
    for producer in producers_in_order(graph):
        upstream = [future for dep in dependencies[producer] for future in partitions[dep]]
        plan = plan_task.submit(  # pyright: ignore[reportUnknownMemberType]
            graph_ref,
            snapshot_id,
            producer_key(graph, producer),
            return_state=False,
            wait_for=upstream,
        )
        units: tuple[WorkUnit, ...] = plan.result()
        partitions[producer] = [
            build_partition_task.submit(unit)  # pyright: ignore[reportUnknownMemberType]
            for unit in units
        ]
    for futures in partitions.values():
        for future in futures:
            future.result()  # Raise the first error
    return snapshot_id


class PrefectExecutor(Executor):
    """
    Build the snapshot by running the `build_graph` flow in this process.

    The flow's tasks run on `task_runner` (Prefect's default is a `ThreadPoolTaskRunner`). The
    Backend and Storage must be shared as widely as the task runner runs tasks: across processes
    for a `ProcessPoolTaskRunner` and across machines for others (eg: Dask or Ray).
    """

    graph_ref: str

    # Task runners aren't hashable (nor serializable) Model fields, so keep it private.
    _task_runner: TaskRunner[Any] | None = PrivateAttr(default=None)

    def __init__(self, *, task_runner: TaskRunner[Any] | None = None, **data: Any) -> None:
        super().__init__(**data)
        self._task_runner = task_runner  # pyright: ignore[reportAttributeAccessIssue]

    def build(self, snapshot: GraphSnapshot) -> None:
        check_graph_ref(self.graph_ref, snapshot.graph)
        build_graph.with_options(task_runner=self._task_runner)(self.graph_ref, snapshot.id)
        logger.info("Build finished.")


__all__ = [
    "PrefectExecutor",
    "build_graph",
    "build_partition_task",
    "plan_task",
    "required_reach",
    "snapshot_task",
]
