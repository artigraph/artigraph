"""
Build each Producer partition in its own Kubernetes Job.

Requires the `kubernetes` extra: `pip install 'arti[kubernetes]'`.
"""

from __future__ import annotations

import hashlib
import time
from concurrent.futures import Future, ThreadPoolExecutor
from copy import deepcopy
from typing import Any, Self
from uuid import uuid4

from kubernetes import client as k8s
from kubernetes import config
from pydantic import PrivateAttr, model_validator

from arti.cli import build_partition_args
from arti.executors import Executor, ProducerPlan
from arti.executors.concurrent import build_in_pool
from arti.executors.work_units import WorkUnit, check_graph_ref, check_reach, work_units
from arti.graphs import GraphSnapshot
from arti.internal.type_hints import assert_not_none

WORK_UNIT_ANNOTATION = "artigraph.io/work-unit"


class KubernetesExecutor(Executor):
    """
    Build each pending partition in its own Kubernetes Job.

    Producers are planned in this process, as soon as their upstream Producers finish. Each pending
    partition then runs `arti build-partition ...` (see `arti.cli`) in a Job, from an image with
    the same code (so that it can import the Graph from `graph_ref`). The Graph's Backend and
    Storage must be shared across machines (eg: Postgres and `GCSFile`).

    The Jobs come from `job_template`, a `kubernetes.client.V1Job` whose first container runs the
    command (set its image, resources, environment, service account, etc). Without a template, Jobs
    run a single container from `image`. Jobs are created with `backoff_limit=0` unless the template
    sets it: a failed partition fails the build (no new Jobs are created and the error is raised once
    running Jobs finish), rather than being retried by Kubernetes.

    At most `max_parallel` Jobs run at once, each polled every `poll_interval` seconds. Pass a
    `batch_api` to configure the Kubernetes connection, otherwise it is loaded from the environment
    (in cluster, or from a kubeconfig).
    """

    graph_ref: str
    image: str | None = None
    namespace: str = "default"
    command: tuple[str, ...] = ("arti",)
    max_parallel: int = 8
    poll_interval: float = 5.0

    # Kubernetes objects aren't hashable (nor serializable) Model fields, so keep them private.
    _batch_api: k8s.BatchV1Api | None = PrivateAttr(default=None)
    _job_template: k8s.V1Job | None = PrivateAttr(default=None)

    def __init__(
        self,
        *,
        batch_api: k8s.BatchV1Api | None = None,
        job_template: k8s.V1Job | None = None,
        **data: Any,
    ) -> None:
        super().__init__(**data)
        if (self.image is None) == (job_template is None):
            raise ValueError("Pass either an `image` or a `job_template`")
        self._batch_api = batch_api  # pyright: ignore[reportAttributeAccessIssue]
        self._job_template = job_template  # pyright: ignore[reportAttributeAccessIssue]

    @model_validator(mode="after")
    def _check_max_parallel(self) -> Self:
        if self.max_parallel < 1:
            raise ValueError("`max_parallel` must be at least 1")
        return self

    def build(self, snapshot: GraphSnapshot) -> None:
        check_reach(snapshot.graph, "network")
        check_graph_ref(self.graph_ref, snapshot.graph)
        if (batch_api := self._batch_api) is None:
            config.load_config()  # pyright: ignore[reportUnknownMemberType]
            batch_api = k8s.BatchV1Api()
        run_id = uuid4().hex[:8]
        pool = ThreadPoolExecutor(self.max_parallel, thread_name_prefix="arti-k8s")

        def submit(plan: ProducerPlan) -> list[Future[None]]:
            return [
                pool.submit(self._run_job, batch_api, unit, run_id)
                for unit in work_units(self.graph_ref, snapshot, plan)
            ]

        build_in_pool(self, snapshot, pool, submit)

    def job(self, unit: WorkUnit, *, name: str) -> k8s.V1Job:
        """Create the Job that builds the WorkUnit."""
        if self._job_template is None:
            job = k8s.V1Job(
                spec=k8s.V1JobSpec(
                    template=k8s.V1PodTemplateSpec(
                        spec=k8s.V1PodSpec(
                            containers=[k8s.V1Container(name="arti", image=self.image)],
                            restart_policy="Never",
                        )
                    )
                )
            )
        else:
            job = deepcopy(self._job_template)
        job.api_version, job.kind = "batch/v1", "Job"
        job.metadata = job.metadata or k8s.V1ObjectMeta()
        job.metadata.name = name
        job.metadata.labels = {
            **(job.metadata.labels or {}),
            "app.kubernetes.io/managed-by": "artigraph",
        }
        job.metadata.annotations = {
            **(job.metadata.annotations or {}),
            WORK_UNIT_ANNOTATION: unit.to_json(),
        }
        spec = assert_not_none(job.spec)
        if spec.backoff_limit is None:
            spec.backoff_limit = 0
        container = assert_not_none(spec.template.spec).containers[0]
        container.command = [*self.command, *build_partition_args(unit)]
        return job

    def _run_job(self, batch_api: k8s.BatchV1Api, unit: WorkUnit, run_id: str) -> None:
        # Job names must be short DNS labels, so identify the work unit by a hash (it is also stored
        # in an annotation) and keep names unique across builds with the run ID.
        digest = hashlib.sha256(unit.to_json().encode()).hexdigest()[:12]
        name = f"arti-{digest}-{run_id}"
        batch_api.create_namespaced_job(self.namespace, self.job(unit, name=name))
        while True:
            status: k8s.V1JobStatus | None = batch_api.read_namespaced_job_status(
                name, self.namespace
            ).status
            if status is not None:
                for condition in status.conditions or []:
                    if condition.status != "True":
                        continue
                    if condition.type == "Complete":
                        return
                    if condition.type == "Failed":
                        raise RuntimeError(
                            f"Job {self.namespace}/{name} for {unit.label} failed: {condition.reason}: {condition.message}"
                        )
            time.sleep(self.poll_interval)


__all__ = ["WORK_UNIT_ANNOTATION", "KubernetesExecutor"]
