from __future__ import annotations

from collections import Counter
from threading import Lock

from kubernetes import client as k8s

from arti.cli import main
from arti.internal.type_hints import assert_not_none


def first_container(job: k8s.V1Job) -> k8s.V1Container:
    return assert_not_none(assert_not_none(job.spec).template.spec).containers[0]


class FakeBatchApi:
    """
    Stand in for `kubernetes.client.BatchV1Api`, running each Job's `arti` command in this process.

    Each Job reports no status at first, then non-terminal conditions, and then its result.
    """

    def __init__(self) -> None:
        self.jobs = dict[str, tuple[k8s.V1Job, k8s.V1JobCondition]]()
        self.reads = Counter[str]()
        self._lock = Lock()

    def create_namespaced_job(self, namespace: str, body: k8s.V1Job) -> k8s.V1Job:
        command = assert_not_none(first_container(body).command)
        assert command[0] == "arti"
        try:
            main(command[1:])
        except Exception as error:  # noqa: BLE001
            result = k8s.V1JobCondition(
                type="Failed", status="True", reason="BackoffLimitExceeded", message=str(error)
            )
        else:
            result = k8s.V1JobCondition(type="Complete", status="True")
        with self._lock:
            self.jobs[assert_not_none(assert_not_none(body.metadata).name)] = (body, result)
        return body

    def read_namespaced_job_status(self, name: str, namespace: str) -> k8s.V1Job:
        with self._lock:
            self.reads[name] += 1
            reads, (_, result) = self.reads[name], self.jobs[name]
        if reads == 1:
            return k8s.V1Job()
        if reads == 2:
            pending = [
                k8s.V1JobCondition(type="Complete", status="False"),
                k8s.V1JobCondition(type="SuccessCriteriaMet", status="True"),
            ]
            return k8s.V1Job(status=k8s.V1JobStatus(conditions=pending))
        return k8s.V1Job(status=k8s.V1JobStatus(conditions=[result]))
