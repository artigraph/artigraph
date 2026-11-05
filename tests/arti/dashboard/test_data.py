from __future__ import annotations

import json
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest

from arti import Backend, GraphSnapshot
from arti.backends.memory import MemoryBackend
from arti.backends.sqlite import SQLiteBackend
from arti.dashboard import DashboardData, NotFoundError
from arti.dashboard._data import _json_safe  # pyright: ignore[reportPrivateUsage]
from arti.fingerprints import Fingerprint
from tests.arti.dashboard.example import (
    build_graph,
    build_graph_with_external_input,
    write_transactions,
)


@pytest.fixture(params=["memory", "sqlite"])
def backend(request: pytest.FixtureRequest, tmp_path: Path) -> Generator[Backend[Any]]:
    with (
        MemoryBackend() if request.param == "memory" else SQLiteBackend(path=tmp_path / "db")
    ) as backend:
        yield backend


@pytest.fixture
def snapshot(tmp_path: Path, backend: Backend[Any]) -> GraphSnapshot:
    write_transactions(tmp_path)
    snapshot = build_graph(tmp_path, backend).build()
    snapshot.tag("prod")
    return snapshot


def test_json_safe_stringifies_large_ints() -> None:
    assert _json_safe({"a": [1, 2**53, -(2**53)], "b": True, "c": "x"}) == {
        "a": [1, str(2**53), str(-(2**53))],
        "b": True,
        "c": "x",
    }


def test_dashboard_lists_graphs(snapshot: GraphSnapshot, backend: Backend[Any]) -> None:
    data = DashboardData(backend)

    assert data.graphs() == {
        "graphs": [
            {"name": "spend", "versions": 1, "snapshots": 1, "tags": ["prod"], "defined": False}
        ]
    }
    graph = data.graph("spend")
    assert graph == {
        "name": "spend",
        "versions": [
            {
                "fingerprint": str(snapshot.graph.fingerprint),
                "persisted": True,
                "defined": False,
                "artifacts": 2,
                "producers": 1,
            }
        ],
        "snapshots": [
            {
                "fingerprint": str(snapshot.fingerprint),
                "id": str(snapshot.id),
                "graph_fingerprint": str(snapshot.graph.fingerprint),
                "tags": ["prod"],
            }
        ],
    }
    # Every document must survive a JSON round trip unchanged (eg: no lossy ints).
    assert json.loads(json.dumps(graph)) == graph


def test_dashboard_describes_a_graph_version(
    snapshot: GraphSnapshot, backend: Backend[Any]
) -> None:
    document = DashboardData(backend).graph_version("spend", str(snapshot.graph.fingerprint))

    assert document["name"] == "spend"
    assert document["fingerprint"] == str(snapshot.graph.fingerprint)
    artifacts = {artifact["id"]: artifact for artifact in document["artifacts"]}
    assert list(artifacts) == ["artifact:spend", "artifact:vendor.transactions"]
    transactions = artifacts["artifact:vendor.transactions"]
    assert transactions["class"] == "Transactions"
    assert transactions["description"] == "Transactions, partitioned by day."
    assert transactions["produced"] is False
    assert transactions["type"]["spec"]["partition_by"] == ["date"]
    assert transactions["type"]["description"] is None
    assert transactions["annotations"] == [{"_arti_type_": "Vendor", "name": "Acme"}]
    assert transactions["storage"]["_arti_type_"] == "LocalFile"
    assert artifacts["artifact:spend"]["produced"] is True

    [producer] = document["producers"]
    assert producer["class"] == "total_spend"
    assert producer["description"] == "Sum every transaction."
    assert producer["version"] == {
        "summary": "SemVer(major=1, minor=0, patch=0)",
        "spec": {"_arti_type_": "SemVer", "major": 1},
    }
    assert producer["inputs"] == {"transactions": "artifact:vendor.transactions"}
    assert producer["outputs"] == ["artifact:spend"]
    assert document["edges"] == [
        {
            "source": "artifact:vendor.transactions",
            "target": producer["id"],
            "label": "transactions",
        },
        {"source": producer["id"], "target": "artifact:spend", "label": None},
    ]


def test_dashboard_describes_a_snapshot(snapshot: GraphSnapshot, backend: Backend[Any]) -> None:
    document = DashboardData(backend).snapshot("spend", str(snapshot.fingerprint))

    assert document["snapshot"] == {
        "fingerprint": str(snapshot.fingerprint),
        "id": str(snapshot.id),
        "tags": ["prod"],
    }
    assert [artifact["id"] for artifact in document["artifacts"]] == [
        "artifact:spend",
        "artifact:vendor.transactions",
    ]
    partitions = document["partitions"]
    # Partitions are sorted by partition key, whatever order the Backend returns them in.
    assert [p["partition_key"] for p in partitions["vendor.transactions"]] == [
        {"date": {"_arti_type_": "DateField", "value": "2026-01-01"}},
        {"date": {"_arti_type_": "DateField", "value": "2026-01-02"}},
    ]
    assert all(p["input_fingerprint"] is None for p in partitions["vendor.transactions"])
    [spend] = partitions["spend"]
    assert spend["input_fingerprint"] is not None
    assert spend["storage_partition"]["value"] == "4.5"


def test_dashboard_shows_unpersisted_definitions(tmp_path: Path) -> None:
    backend = MemoryBackend()
    graph = build_graph_with_external_input(tmp_path, backend)
    data = DashboardData(backend, definitions=[graph])

    assert data.graphs() == {
        "graphs": [{"name": "external", "versions": 1, "snapshots": 0, "tags": [], "defined": True}]
    }
    assert data.graph("external")["versions"] == [
        {
            "fingerprint": str(graph.fingerprint),
            "persisted": False,
            "defined": True,
            "artifacts": 3,
            "producers": 2,
        }
    ]
    document = data.graph_version("external", str(graph.fingerprint))
    [external] = [a for a in document["artifacts"] if a["key"] is None]
    assert external["id"].startswith("external:")
    assert external["class"] == "Limit"
    assert external["description"] is None
    over_limit = next(p for p in document["producers"] if p["class"] == "over_limit")
    assert over_limit["description"] is None
    assert over_limit["inputs"] == {"spend": "artifact:spend", "limit": external["id"]}
    assert {"source": external["id"], "target": over_limit["id"], "label": "limit"} in document[
        "edges"
    ]


def test_dashboard_raises_not_found(snapshot: GraphSnapshot, backend: Backend[Any]) -> None:
    data = DashboardData(backend)
    missing = str(Fingerprint.from_int(1))

    with pytest.raises(NotFoundError, match="Unknown Graph: 'missing'"):
        data.graph("missing")
    with pytest.raises(NotFoundError, match="Unknown Graph version"):
        data.graph_version("spend", missing)
    with pytest.raises(NotFoundError, match="Unknown GraphSnapshot"):
        data.snapshot("spend", missing)
    with pytest.raises(NotFoundError, match="Invalid fingerprint: 'abc'"):
        data.snapshot("spend", "abc")
