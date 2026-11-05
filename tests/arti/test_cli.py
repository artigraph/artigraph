from __future__ import annotations

from pathlib import Path

import pytest

from arti import Fingerprint, Graph
from arti.backends.memory import MemoryBackend
from arti.cli import build_partition_args, main
from arti.executors.work_units import WorkUnit, plan_work_units
from tests.arti.executors import graphs
from tests.arti.executors.graphs import GRAPH_REF, make_graph, write_raw


@pytest.fixture
def graph(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Graph:
    graph = make_graph(MemoryBackend(), str(tmp_path))
    write_raw(graph)
    monkeypatch.setattr(graphs, "graph", graph)
    return graph


def test_cli_builds_a_graph(graph: Graph, capsys: pytest.CaptureFixture[str]) -> None:
    main(["snapshot", "--graph", GRAPH_REF])
    snapshot_id = int(capsys.readouterr().out)

    for producer in ("squared", "total", "doubled", "previous"):
        main(
            [
                "plan",
                "--graph",
                GRAPH_REF,
                "--snapshot-id",
                str(snapshot_id),
                "--producer",
                producer,
            ]
        )
        units = [WorkUnit.from_json(line) for line in capsys.readouterr().out.splitlines()]
        assert units
        for unit in units:
            main(build_partition_args(unit))

    snapshot = graph.snapshot()
    assert snapshot.id == snapshot_id
    assert snapshot.read(graph.artifacts.next, annotation=int) == 31


def test_cli_partition_key_defaults_to_not_partitioned(graph: Graph) -> None:
    snapshot_id = graph.snapshot().id
    for unit in plan_work_units(GRAPH_REF, snapshot_id, "squared"):
        main(build_partition_args(unit))
    (unit,) = plan_work_units(GRAPH_REF, snapshot_id, "total")
    args = build_partition_args(unit)
    del args[args.index("--partition-key") : args.index("--partition-key") + 2]

    main(args)

    assert plan_work_units(GRAPH_REF, Fingerprint(snapshot_id), "total") == ()


def test_cli_requires_a_command(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit):
        main([])
    assert "arti" in capsys.readouterr().err
