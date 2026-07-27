from __future__ import annotations

import argparse
import shutil
from datetime import date
from pathlib import Path
from typing import Any, cast

import duckdb
import pandas as pd

if __package__:
    from .model import CleanTransactions, RawTransactions, clean_transactions
    from .snapshot_duckdb import register_snapshot_view, snapshot_parquet_paths
else:
    from model import CleanTransactions, RawTransactions, clean_transactions
    from snapshot_duckdb import register_snapshot_view, snapshot_parquet_paths

from arti import Graph, GraphSnapshot, PartitionKey
from arti.backends.sqlite import SQLiteBackend
from arti.partitions import DateField
from arti.storage.local import LocalFile

DIR = Path(__file__).parent
DEFAULT_WORK_DIR = DIR / "_work"
GRAPH_NAME = "snapshot-parquet-duckdb-demo"


def _transactions(
    transaction_date: date,
    records: list[tuple[str, str, str, int]],
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "transaction_id": transaction_id,
                "transaction_date": transaction_date,
                "vendor": vendor,
                "category": category,
                "amount_cents": amount_cents,
            }
            for transaction_id, vendor, category, amount_cents in records
        ]
    )


def _write_raw_partition(
    graph: Graph,
    *,
    transaction_date: date,
    records: list[tuple[str, str, str, int]],
) -> None:
    graph.write(
        _transactions(transaction_date, records),
        artifact=graph.artifacts.raw,
        partition_key=PartitionKey(transaction_date=DateField(value=transaction_date)),
    )


def _query_total(
    connection: duckdb.DuckDBPyConnection,
    *,
    snapshot: GraphSnapshot,
    view_name: str,
) -> float:
    artifact = snapshot.artifacts.clean
    register_snapshot_view(
        connection,
        snapshot=snapshot,
        artifact=artifact,
        view_name=view_name,
    )
    value: Any = connection.table(view_name).aggregate("sum(amount_usd)").fetchone()
    assert value is not None
    return float(value[0])


def run_demo(work_dir: Path) -> dict[str, float]:
    if work_dir.exists():
        shutil.rmtree(work_dir)
    work_dir.mkdir(parents=True)

    backend = SQLiteBackend(path=work_dir / "artigraph.sqlite")
    raw_storage = LocalFile(path=str(work_dir / "data" / "raw" / "{transaction_date.iso}.parquet"))
    produced_root = work_dir / "data" / "produced"
    clean_storage = LocalFile.rooted_at(produced_root)

    with Graph(name=GRAPH_NAME, backend=backend) as graph:
        graph.artifacts.raw = RawTransactions(storage=raw_storage)
        clean = clean_transactions(raw=graph.artifacts.raw)
        graph.artifacts.clean = clean.out(CleanTransactions(storage=clean_storage))

    first_day = date(2026, 1, 1)
    corrected_day = date(2026, 1, 2)
    _write_raw_partition(
        graph,
        transaction_date=first_day,
        records=[
            (" txn-001 ", " Paper & Co ", "Office", 1_800),
            ("txn-002", "Railway Cafe", "Meals", 1_200),
        ],
    )
    _write_raw_partition(
        graph,
        transaction_date=corrected_day,
        records=[("txn-003", "Cloud Hosting ", " Software ", 1_200)],
    )
    baseline = graph.build()
    baseline.tag("baseline")

    _write_raw_partition(
        graph,
        transaction_date=corrected_day,
        records=[("txn-003", "Cloud Hosting ", " Software ", 950)],
    )
    corrected = graph.build()
    corrected.tag("corrected")

    baseline = GraphSnapshot.from_tag(GRAPH_NAME, "baseline", connectable=backend)
    corrected = GraphSnapshot.from_tag(GRAPH_NAME, "corrected", connectable=backend)

    with duckdb.connect(":memory:") as connection:
        baseline_total = _query_total(
            connection, snapshot=baseline, view_name="baseline_transactions"
        )
        corrected_total = _query_total(
            connection, snapshot=corrected, view_name="corrected_transactions"
        )
        naive_row = cast(
            tuple[float] | None,
            connection.execute(
                "SELECT sum(amount_usd) FROM read_parquet(?)",
                [str(produced_root / "**" / "*.parquet")],
            ).fetchone(),
        )
        assert naive_row is not None
        naive_total = float(naive_row[0])

    baseline_paths = snapshot_parquet_paths(baseline, baseline.artifacts.clean)
    corrected_paths = snapshot_parquet_paths(corrected, corrected.artifacts.clean)
    baseline_path_set = set(baseline_paths)
    corrected_path_set = set(corrected_paths)
    produced_paths = sorted(produced_root.rglob("*.parquet"))

    print("Produced Parquet files (input fingerprints are path segments):")
    for path in produced_paths:
        print(f"  {path.relative_to(produced_root)}")
    print(
        f"Naive glob files: {len(produced_paths)} for {len(corrected_paths)} logical snapshot partitions"
    )
    print(f"Shared files across snapshots: {len(baseline_path_set & corrected_path_set)}")
    print(f"Baseline-only files: {len(baseline_path_set - corrected_path_set)}")
    print(f"Corrected-only files: {len(corrected_path_set - baseline_path_set)}")
    print(f"Baseline snapshot files: {len(baseline_paths)}")
    print(f"Corrected snapshot files: {len(corrected_paths)}")
    print(f"Baseline snapshot total: {baseline_total:.2f}")
    print(f"Corrected snapshot total: {corrected_total:.2f}")
    print(
        f"Naive glob total: {naive_total:.2f} (wrong: it mixes live versions from both snapshots)"
    )

    return {
        "baseline": baseline_total,
        "corrected": corrected_total,
        "naive": naive_total,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--work-dir", type=Path, default=DEFAULT_WORK_DIR)
    args = parser.parse_args()
    run_demo(args.work_dir)


if __name__ == "__main__":
    main()
