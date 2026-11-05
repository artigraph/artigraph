"""
Generate an Airflow DAG that builds a Graph.

Requires the `airflow` extra: `pip install 'arti[airflow]'`.

The DAG has a task to snapshot the Graph and a task group per Producer. Each group has a `plan` task,
which runs once the upstream Producers are built and returns the pending partitions, and a `build`
task mapped over them with Airflow's dynamic task mapping (partitions are only known at runtime,
after `Producer.map`). Tasks only exchange `WorkUnit`s (as JSON), so Airflow workers must run the
same code and import the Graph from its `graph_ref`.

Create the DAG in a file in your DAGs folder:

```python
from arti.executors.airflow import make_dag

dag = make_dag("my_project.graphs:graph", schedule="@daily")
```
"""

from __future__ import annotations

from typing import Any

from airflow.sdk import DAG, TaskGroup, get_current_context, task

from arti.backends import Reach
from arti.executors import producer_dependencies, producers_in_order
from arti.executors.work_units import (
    WorkUnit,
    check_reach,
    plan_work_units,
    producer_key,
    resolve_graph,
    run_work_unit,
    snapshot_graph,
)
from arti.fingerprints import Fingerprint
from arti.producers import Producer


@task
def snapshot(graph_ref: str) -> int:
    return int(snapshot_graph(graph_ref))


# Run even if an upstream `build` was skipped, which happens when it had no pending partitions.
@task(trigger_rule="none_failed")
def plan(graph_ref: str, snapshot_id: int, producer_key: str) -> list[str]:
    return [
        unit.to_json()
        for unit in plan_work_units(graph_ref, Fingerprint(snapshot_id), producer_key)
    ]


@task(map_index_template="{{ arti_partition }}")
def build(unit: str) -> None:
    work_unit = WorkUnit.from_json(unit)
    # Label each mapped task instance with its partition in the Airflow UI.
    get_current_context()["arti_partition"] = work_unit.label  # pyright: ignore[reportGeneralTypeIssues]
    run_work_unit(work_unit)


def group_id(producer_key: str) -> str:
    """Convert a Producer key (eg: `root.clean`) to an Airflow group ID (eg: `root__clean`)."""
    return producer_key.replace(".", "__")


def make_dag(
    graph_ref: str,
    *,
    dag_id: str | None = None,
    reach: Reach = "network",
    **dag_kwargs: Any,
) -> DAG:
    """
    Create a DAG that snapshots and builds the Graph imported from `graph_ref`.

    The Graph is imported when the DAG file is parsed, to find its Producers. By default, its Backend
    and Storage must be shared across machines (eg: Postgres and `GCSFile`), since Airflow may run
    each task on a different worker. Pass `reach="host"` if every task runs on the same machine (eg:
    Airflow's `LocalExecutor`). Other keyword arguments are passed to `DAG` (eg: `schedule`).
    """
    graph = resolve_graph(graph_ref)
    check_reach(graph, reach)
    keys = {producer: producer_key(graph, producer) for producer in graph.producers}
    group_ids = {producer: group_id(key) for producer, key in keys.items()}
    if len(set(group_ids.values())) != len(group_ids):
        raise ValueError(
            f"Producer keys must map to unique Airflow group IDs: {sorted(keys.values())}"
        )
    dependencies = producer_dependencies(graph)
    with DAG(dag_id=dag_id or f"arti-{graph.name}", **dag_kwargs) as dag:
        # Task calls return Airflow's references to their (future) results.
        snapshot_id: Any = snapshot(graph_ref)
        groups = dict[Producer, TaskGroup]()
        for producer in producers_in_order(graph):
            with TaskGroup(group_id=group_ids[producer]) as group:
                units = plan(graph_ref, snapshot_id, keys[producer])
                build.expand(unit=units)  # pyright: ignore[reportUnknownMemberType]
            for upstream in dependencies[producer]:
                group.set_upstream(groups[upstream])
            groups[producer] = group
    return dag


__all__ = ["build", "group_id", "make_dag", "plan", "snapshot"]
