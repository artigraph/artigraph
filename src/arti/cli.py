r"""
The `arti` command line interface.

The commands mirror the work unit protocol (see `arti.executors.work_units`), so any orchestrator or
container platform can build a Graph:

```shell
arti snapshot --graph my_project.graphs:graph
arti plan --graph my_project.graphs:graph --snapshot-id ID --producer clean
arti build-partition --graph my_project.graphs:graph --snapshot-id ID --producer clean \
    --partition-key '{"date": {"value": "2024-01-01"}}' --input-fingerprint FINGERPRINT
```
"""

from __future__ import annotations

import argparse
import json
import logging
from collections.abc import Sequence

from arti.executors.work_units import WorkUnit, plan_work_units, run_work_unit, snapshot_graph
from arti.fingerprints import Fingerprint


def build_partition_args(unit: WorkUnit) -> list[str]:
    """Return the `arti build-partition` arguments that build the WorkUnit."""
    return [
        "build-partition",
        "--graph",
        unit.graph_ref,
        "--snapshot-id",
        str(unit.snapshot_id),
        "--producer",
        unit.producer_key,
        "--partition-key",
        json.dumps({name: dict(field) for name, field in unit.partition_key.items()}),
        "--input-fingerprint",
        str(unit.input_fingerprint),
    ]


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="arti", description="Build Artigraph Graphs.")
    commands = parser.add_subparsers(dest="command", required=True)

    def add_command(
        name: str, summary: str, *, snapshot_id: bool = True
    ) -> argparse.ArgumentParser:
        command = commands.add_parser(name, help=summary, description=summary)
        command.add_argument(
            "--graph", required=True, help="The Graph to import, eg: 'package.module:graph'"
        )
        if snapshot_id:
            command.add_argument("--snapshot-id", required=True, type=int)
        return command

    add_command("snapshot", "Snapshot the Graph and print the snapshot ID.", snapshot_id=False)
    plan = add_command(
        "plan",
        "Plan a Producer, once its inputs are built, and print its work units as JSON lines.",
    )
    plan.add_argument("--producer", required=True, help="The key of the Producer's first output")
    build = add_command("build-partition", "Build one Producer partition (a work unit).")
    build.add_argument("--producer", required=True, help="The key of the Producer's first output")
    build.add_argument(
        "--partition-key",
        default="{}",
        help='The JSON partition key, eg: \'{"i": {"value": "1"}}\'',
    )
    build.add_argument("--input-fingerprint", required=True, type=int)
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    args = _parser().parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    match args.command:
        case "snapshot":
            print(snapshot_graph(args.graph))
        case "plan":
            for unit in plan_work_units(args.graph, Fingerprint(args.snapshot_id), args.producer):
                print(unit.to_json())
        case _:
            run_work_unit(
                WorkUnit(
                    graph_ref=args.graph,
                    snapshot_id=Fingerprint(args.snapshot_id),
                    producer_key=args.producer,
                    partition_key=json.loads(args.partition_key),
                    input_fingerprint=Fingerprint(args.input_fingerprint),
                )
            )


__all__ = ["build_partition_args", "main"]
