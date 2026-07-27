from __future__ import annotations

from pathlib import Path

import duckdb

from arti import Artifact, GraphSnapshot
from arti.formats.parquet import Parquet
from arti.storage.local import LocalFilePartition


def snapshot_parquet_paths(snapshot: GraphSnapshot, artifact: Artifact) -> list[str]:
    """Return verified local Parquet paths linked to an artifact in a graph snapshot."""
    if not isinstance(artifact.format, Parquet):
        raise TypeError("DuckDB demo helper only supports Parquet artifacts")

    key = snapshot.graph.artifact_to_key[artifact]
    with snapshot.backend.connect() as connection:
        partitions = connection.read_snapshot_partitions(snapshot, key, artifact)
    if not partitions:
        raise FileNotFoundError(f"No partitions for {key!r} in snapshot {snapshot.id}")

    paths: list[str] = []
    for partition_snapshot in partitions:
        storage_partition = partition_snapshot.storage_partition
        if not isinstance(storage_partition, LocalFilePartition):
            raise TypeError("DuckDB demo helper only supports LocalFile Parquet artifacts")
        path = Path(storage_partition.path)
        if not path.is_file():
            raise FileNotFoundError(f"Snapshot partition is missing: {path}")
        if (
            storage_partition.compute_content_fingerprint()
            != partition_snapshot.content_fingerprint
        ):
            raise ValueError(f"Partition content changed since snapshot: {path}")
        paths.append(str(path))
    return sorted(paths)


def register_snapshot_view(
    connection: duckdb.DuckDBPyConnection,
    *,
    snapshot: GraphSnapshot,
    artifact: Artifact,
    view_name: str,
) -> None:
    """Register an in-memory DuckDB view over only a snapshot's verified Parquet files."""
    paths = snapshot_parquet_paths(snapshot, artifact)
    connection.from_parquet(paths).create_view(view_name, replace=True)


__all__ = ["register_snapshot_view", "snapshot_parquet_paths"]
