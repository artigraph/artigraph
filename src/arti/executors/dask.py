"""
Build Producer partitions on a Dask cluster.

Requires the `dask` extra: `pip install 'arti[dask]'`.
"""

from __future__ import annotations

from typing import Any
from uuid import uuid4

from distributed import Client, Future
from pydantic import PrivateAttr

from arti.executors import Executor, logger, producer_dependencies, producers_in_order
from arti.executors.work_units import (
    WorkUnit,
    check_graph_ref,
    check_reach,
    plan_work_units,
    producer_key,
    run_work_unit,
)
from arti.fingerprints import Fingerprint
from arti.graphs import GraphSnapshot
from arti.producers import Producer


def plan_after(
    graph_ref: str, snapshot_id: Fingerprint, producer_key: str, upstream: list[None]
) -> tuple[WorkUnit, ...]:
    """Plan a Producer once the `upstream` partitions (Dask futures passed as arguments) finish."""
    return plan_work_units(graph_ref, snapshot_id, producer_key)


class DaskExecutor(Executor):
    """
    Build the snapshot's partitions on a Dask cluster with `Client.submit`.

    Each Producer is planned on the cluster in a task that depends on (the futures of) its upstream
    partitions. Once a plan finishes, each pending partition is submitted as a task that runs a
    `WorkUnit`. Dask only receives work units and module level functions, so the workers must run
    the same code, import the Graph from `graph_ref`, and share its Backend and Storage across
    machines (eg: Postgres and `GCSFile`).

    Pass a `client` to use an existing cluster, otherwise the executor connects to `address` (or
    starts a local cluster when it is not set) for the duration of the build.
    """

    graph_ref: str
    address: str | None = None

    # Clients aren't hashable (nor serializable) Model fields, so keep it private.
    _client: Client | None = PrivateAttr(default=None)

    def __init__(self, *, client: Client | None = None, **data: Any) -> None:
        super().__init__(**data)
        if client is not None and self.address is not None:
            raise ValueError("Pass either a `client` or an `address`, not both")
        self._client = client  # pyright: ignore[reportAttributeAccessIssue]

    def build(self, snapshot: GraphSnapshot) -> None:
        check_reach(snapshot.graph, "network")
        check_graph_ref(self.graph_ref, snapshot.graph)
        if self._client is not None:
            self._build(snapshot, self._client)
        else:
            with Client(self.address) as client:
                self._build(snapshot, client)
        logger.info("Build finished.")

    def _build(self, snapshot: GraphSnapshot, client: Client) -> None:
        graph = snapshot.graph
        dependencies = producer_dependencies(graph)
        partitions = dict[Producer, list[Future[None]]]()
        run_id = uuid4().hex[:8]  # Keep task keys unique across builds
        try:
            for producer in producers_in_order(graph):
                key = producer_key(graph, producer)
                upstream = [future for dep in dependencies[producer] for future in partitions[dep]]
                plan: Future[tuple[WorkUnit, ...]] = client.submit(
                    plan_after,
                    self.graph_ref,
                    snapshot.id,
                    key,
                    upstream,
                    key=f"arti-plan-{key}-{run_id}",
                    pure=False,
                )
                partitions[producer] = [
                    client.submit(
                        run_work_unit, unit, key=f"arti-build-{unit.label}-{run_id}", pure=False
                    )
                    for unit in plan.result()
                ]
            client.gather([future for futures in partitions.values() for future in futures])
        except BaseException:
            client.cancel([future for futures in partitions.values() for future in futures])
            raise


__all__ = ["DaskExecutor", "plan_after"]
