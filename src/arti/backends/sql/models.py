from __future__ import annotations

from typing import Any

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

JSONMetadata = sa.JSON().with_variant(JSONB(), "postgresql")


class Base(DeclarativeBase):
    pass


class GraphRow(Base):
    __tablename__ = "arti_graph"

    name: Mapped[str] = mapped_column(sa.String, primary_key=True)
    fingerprint: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSONMetadata)


class GraphSnapshotRow(Base):
    __tablename__ = "arti_graph_snapshot"
    __table_args__ = (
        sa.ForeignKeyConstraint(
            ["name", "graph_fingerprint"],
            ["arti_graph.name", "arti_graph.fingerprint"],
            ondelete="CASCADE",
        ),
    )

    name: Mapped[str] = mapped_column(sa.String, primary_key=True)
    fingerprint: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True)
    snapshot_id: Mapped[int] = mapped_column(sa.BigInteger)
    graph_fingerprint: Mapped[int] = mapped_column(sa.BigInteger)


class StoragePartitionRow(Base):
    __tablename__ = "arti_storage_partition"
    __table_args__ = (
        sa.Index(
            "ix_arti_storage_partition_lookup",
            "storage_fingerprint",
            "input_fingerprint",
        ),
    )

    fingerprint: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True)
    storage_fingerprint: Mapped[int] = mapped_column(sa.BigInteger)
    input_fingerprint: Mapped[int | None] = mapped_column(sa.BigInteger)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSONMetadata)


class PartitionStatisticRow(Base):
    __tablename__ = "arti_partition_statistic"
    __table_args__ = (
        sa.ForeignKeyConstraint(
            ["partition_fingerprint"],
            ["arti_storage_partition.fingerprint"],
            ondelete="CASCADE",
        ),
    )

    partition_fingerprint: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True)
    statistic_fingerprint: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True)
    # The encoded Statistic, so the values are understandable without the Graph that computed them.
    statistic_json: Mapped[dict[str, Any]] = mapped_column(JSONMetadata)
    # Wrapped as {"value": ...}, so null values aren't confused with SQL NULL.
    value_json: Mapped[dict[str, Any]] = mapped_column(JSONMetadata)


class ThresholdOverrideRow(Base):
    __tablename__ = "arti_threshold_override"
    __table_args__ = (
        sa.ForeignKeyConstraint(
            ["partition_fingerprint"],
            ["arti_storage_partition.fingerprint"],
            ondelete="CASCADE",
        ),
    )

    partition_fingerprint: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True)
    threshold_fingerprint: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True)
    # The encoded Threshold, so overrides are understandable without the Graph that checked them.
    threshold_json: Mapped[dict[str, Any]] = mapped_column(JSONMetadata)
    reason: Mapped[str] = mapped_column(sa.String)


class SnapshotPartitionRow(Base):
    __tablename__ = "arti_snapshot_partition"
    __table_args__ = (
        sa.ForeignKeyConstraint(
            ["snapshot_name", "snapshot_fingerprint"],
            ["arti_graph_snapshot.name", "arti_graph_snapshot.fingerprint"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["partition_fingerprint"],
            ["arti_storage_partition.fingerprint"],
            ondelete="CASCADE",
        ),
    )

    snapshot_name: Mapped[str] = mapped_column(sa.String, primary_key=True)
    snapshot_fingerprint: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True)
    artifact_key: Mapped[str] = mapped_column(sa.String, primary_key=True)
    partition_fingerprint: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True)


class SnapshotTagRow(Base):
    __tablename__ = "arti_snapshot_tag"
    __table_args__ = (
        sa.ForeignKeyConstraint(
            ["graph_name", "snapshot_fingerprint"],
            ["arti_graph_snapshot.name", "arti_graph_snapshot.fingerprint"],
            ondelete="CASCADE",
        ),
    )

    graph_name: Mapped[str] = mapped_column(sa.String, primary_key=True)
    tag: Mapped[str] = mapped_column(sa.String, primary_key=True)
    snapshot_fingerprint: Mapped[int] = mapped_column(sa.BigInteger)


__all__ = [
    "Base",
    "GraphRow",
    "GraphSnapshotRow",
    "PartitionStatisticRow",
    "SnapshotPartitionRow",
    "SnapshotTagRow",
    "StoragePartitionRow",
    "ThresholdOverrideRow",
]
