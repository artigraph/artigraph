from __future__ import annotations

import multiprocessing
from concurrent.futures import (
    FIRST_COMPLETED,
    Future,
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    wait,
)
from concurrent.futures import (
    Executor as PoolExecutor,
)
from typing import Literal, Self

from pydantic import model_validator

from arti.executors import Executor, ProducerPlan, logger, producer_dependencies
from arti.executors.work_units import check_graph_ref, check_reach, run_work_unit, work_units
from arti.graphs import GraphSnapshot
from arti.producers import Producer


class ConcurrentExecutor(Executor):
    """
    Build independent Producers and partitions in parallel with `concurrent.futures`.

    Producers are planned in this process as soon as the Producers they depend on finish. Their
    pending partitions are then built in a thread or process pool.

    - `kind="thread"` builds in threads of this process, so it works with any Graph, including
      in-memory backends. User code that holds the GIL won't run in parallel.
    - `kind="process"` builds in fresh ("spawn") processes, which import the Graph from
      `graph_ref` and receive only `WorkUnit`s. The Backend and Storage must be shared across
      processes (eg: a SQLite file and `LocalFile`, or Postgres).

    If a partition fails, no new partitions are started, queued partitions are cancelled, and the
    first error is raised once running partitions finish.
    """

    kind: Literal["thread", "process"] = "thread"
    max_workers: int | None = None
    graph_ref: str | None = None

    @model_validator(mode="after")
    def _check_graph_ref(self) -> Self:
        if self.kind == "process" and self.graph_ref is None:
            raise ValueError("`graph_ref` is required to build in processes")
        if self.kind == "thread" and self.graph_ref is not None:
            raise ValueError("`graph_ref` is only used to build in processes")
        return self

    def build(self, snapshot: GraphSnapshot) -> None:
        pool: PoolExecutor
        if self.graph_ref is not None:
            check_reach(snapshot.graph, "host")
            check_graph_ref(self.graph_ref, snapshot.graph)
            pool = ProcessPoolExecutor(
                self.max_workers, mp_context=multiprocessing.get_context("spawn")
            )
        else:
            pool = ThreadPoolExecutor(self.max_workers, thread_name_prefix="arti")
        with pool:
            try:
                self._build(snapshot, pool)
            except BaseException:
                pool.shutdown(cancel_futures=True)
                raise
        logger.info("Build finished.")

    def _submit(
        self, snapshot: GraphSnapshot, pool: PoolExecutor, plan: ProducerPlan
    ) -> list[Future[None]]:
        if self.graph_ref is None:
            return [
                pool.submit(self.build_planned_partition, snapshot, plan, partition_key)
                for partition_key in plan.pending
            ]
        return [
            pool.submit(run_work_unit, unit) for unit in work_units(self.graph_ref, snapshot, plan)
        ]

    def _build(self, snapshot: GraphSnapshot, pool: PoolExecutor) -> None:
        waiting = dict(producer_dependencies(snapshot.graph))
        finished = set[Producer]()
        running = dict[Future[None], Producer]()
        remaining = dict[Producer, int]()
        while True:
            # Plan every Producer whose inputs are built. Producers without pending partitions
            # finish immediately, which may unblock others.
            while ready := [p for p, upstream in waiting.items() if upstream <= finished]:
                for producer in ready:
                    del waiting[producer]
                    logger.info(f"Planning {producer}...")
                    futures = self._submit(snapshot, pool, self.plan_producer(snapshot, producer))
                    running.update(dict.fromkeys(futures, producer))
                    if futures:
                        remaining[producer] = len(futures)
                    else:
                        finished.add(producer)
            if not running:
                break
            done, _ = wait(running, return_when=FIRST_COMPLETED)
            for future in done:
                producer = running.pop(future)
                future.result()  # Raise the first error
                remaining[producer] -= 1
                if not remaining[producer]:
                    finished.add(producer)


__all__ = ["ConcurrentExecutor"]
