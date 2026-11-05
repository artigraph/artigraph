from __future__ import annotations

from arti.executors import Executor, logger, producers_in_order
from arti.graphs import GraphSnapshot


class LocalExecutor(Executor):
    """Build every Producer partition one after another in the current thread."""

    # NOTE: We may want to repeat planning in the future, if we wanted to support some sort of
    # iterated or cyclic Producers (eg: first pass output feeds into second run - in that case,
    # `.map` should describe how to "converge" by returning the same outputs as a prior call).

    def build(self, snapshot: GraphSnapshot) -> None:
        # NOTE: Raw Artifacts will already be discovered and linked in the backend to this snapshot.
        #
        # TODO: Compute Statistics (if not already computed for the partition) and check Thresholds
        # (every time, as they may be changed, dynamic, or overridden).
        for producer in producers_in_order(snapshot.graph):
            logger.info(f"Building {producer}...")
            plan = self.plan_producer(snapshot, producer)
            for partition_key in plan.pending:
                self.build_planned_partition(snapshot, plan, partition_key)
        logger.info("Build finished.")
