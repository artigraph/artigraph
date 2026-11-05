from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

import pytest
from kubernetes import client as k8s

from arti import Graph
from arti.backends.memory import MemoryBackend
from arti.executors import kubernetes as arti_kubernetes
from arti.executors.kubernetes import WORK_UNIT_ANNOTATION, KubernetesExecutor
from arti.executors.work_units import WorkUnit, plan_work_units, snapshot_graph
from arti.internal.type_hints import assert_not_none
from tests.arti.executors import graphs
from tests.arti.executors.fake_kubernetes import FakeBatchApi, first_container
from tests.arti.executors.graphs import GRAPH_REF, make_graph, write_raw


def executor(api: FakeBatchApi, **kwargs: Any) -> KubernetesExecutor:
    return KubernetesExecutor(
        graph_ref=GRAPH_REF, batch_api=cast(k8s.BatchV1Api, api), poll_interval=0, **kwargs
    )


def test_kubernetes_runs_a_job_per_partition(remote_graph: Graph) -> None:
    api = FakeBatchApi()

    snapshot = remote_graph.build(executor(api, image="example.com/project:1", namespace="data"))

    assert snapshot.read(remote_graph.artifacts.next, annotation=int) == 31
    # 4 `squared` partitions, then 1 each for `total`, `doubled`, and `previous` (+ `next`)
    assert len(api.jobs) == 7
    job, _ = next(iter(api.jobs.values()))
    metadata = assert_not_none(job.metadata)
    assert metadata.labels == {"app.kubernetes.io/managed-by": "artigraph"}
    unit = WorkUnit.from_json(assert_not_none(metadata.annotations)[WORK_UNIT_ANNOTATION])
    assert unit.snapshot_id == snapshot.id
    assert assert_not_none(job.spec).backoff_limit == 0
    assert first_container(job).image == "example.com/project:1"


def test_kubernetes_raises_failed_jobs(
    remote_graph: Graph, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fail(value: int) -> int:
        raise RuntimeError("boom")

    monkeypatch.setattr(graphs.double, "build", staticmethod(fail))

    with pytest.raises(RuntimeError, match=r"Job default/arti-\w+-\w+ for doubled failed: .*boom"):
        remote_graph.build(executor(FakeBatchApi(), image="example.com/project:1"))


def test_kubernetes_job_template(remote_graph: Graph) -> None:
    template = k8s.V1Job(
        metadata=k8s.V1ObjectMeta(labels={"team": "data"}, annotations={"note": "x"}),
        spec=k8s.V1JobSpec(
            backoff_limit=2,
            template=k8s.V1PodTemplateSpec(
                spec=k8s.V1PodSpec(
                    containers=[k8s.V1Container(name="main", image="example.com/project:2")],
                    restart_policy="Never",
                    service_account_name="artigraph",
                )
            ),
        ),
    )
    executor = KubernetesExecutor(
        graph_ref=GRAPH_REF, job_template=template, command=("uv", "run", "arti")
    )
    (unit, *_) = plan_work_units(GRAPH_REF, snapshot_graph(GRAPH_REF), "squared")

    job = executor.job(unit, name="arti-test")

    metadata = assert_not_none(job.metadata)
    assert metadata.name == "arti-test"
    assert metadata.labels == {"team": "data", "app.kubernetes.io/managed-by": "artigraph"}
    assert metadata.annotations == {"note": "x", WORK_UNIT_ANNOTATION: unit.to_json()}
    spec = assert_not_none(job.spec)
    assert spec.backoff_limit == 2
    assert assert_not_none(spec.template.spec).service_account_name == "artigraph"
    command = assert_not_none(first_container(job).command)
    assert command[:4] == ["uv", "run", "arti", "build-partition"]
    assert json.loads(command[command.index("--partition-key") + 1]) == unit.partition_key
    assert assert_not_none(template.metadata).name is None  # The template is copied, not modified


def test_kubernetes_loads_the_config(remote_graph: Graph, monkeypatch: pytest.MonkeyPatch) -> None:
    api, loaded = FakeBatchApi(), list[bool]()
    monkeypatch.setattr(arti_kubernetes.config, "load_config", lambda: loaded.append(True))
    monkeypatch.setattr(arti_kubernetes.k8s, "BatchV1Api", lambda: api)

    remote_graph.build(
        KubernetesExecutor(graph_ref=GRAPH_REF, image="example.com/project:1", poll_interval=0)
    )

    assert loaded == [True]
    assert api.jobs


def test_kubernetes_requires_shared_resources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    graph = make_graph(MemoryBackend(), str(tmp_path))
    write_raw(graph)
    monkeypatch.setattr(graphs, "graph", graph)

    with pytest.raises(ValueError, match="must be shared across a network"):
        graph.build(executor(FakeBatchApi(), image="example.com/project:1"))


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({}, "either an `image` or a `job_template`"),
        ({"image": "a", "job_template": k8s.V1Job()}, "either an `image` or a `job_template`"),
        ({"image": "a", "max_parallel": 0}, "`max_parallel` must be at least 1"),
    ],
)
def test_kubernetes_validation(kwargs: dict[str, Any], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        KubernetesExecutor(graph_ref=GRAPH_REF, **kwargs)
