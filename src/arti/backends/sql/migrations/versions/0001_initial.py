"""
Create the initial Artigraph SQL backend schema.

Revision ID: 0001
Revises:
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None

JSON_METADATA = sa.JSON().with_variant(postgresql.JSONB(), "postgresql")


def upgrade() -> None:
    op.create_table(
        "arti_graph",
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("fingerprint", sa.BigInteger(), nullable=False),
        sa.Column("metadata_json", JSON_METADATA, nullable=False),
        sa.PrimaryKeyConstraint("name", "fingerprint"),
    )
    op.create_table(
        "arti_graph_snapshot",
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("fingerprint", sa.BigInteger(), nullable=False),
        sa.Column("snapshot_id", sa.BigInteger(), nullable=False),
        sa.Column("graph_fingerprint", sa.BigInteger(), nullable=False),
        sa.ForeignKeyConstraint(
            ["name", "graph_fingerprint"],
            ["arti_graph.name", "arti_graph.fingerprint"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("name", "fingerprint"),
    )
    op.create_table(
        "arti_storage_partition",
        sa.Column("fingerprint", sa.BigInteger(), nullable=False),
        sa.Column("storage_fingerprint", sa.BigInteger(), nullable=False),
        sa.Column("input_fingerprint", sa.BigInteger(), nullable=True),
        sa.Column("metadata_json", JSON_METADATA, nullable=False),
        sa.PrimaryKeyConstraint("fingerprint"),
    )
    op.create_index(
        "ix_arti_storage_partition_lookup",
        "arti_storage_partition",
        ["storage_fingerprint", "input_fingerprint"],
        unique=False,
    )
    op.create_table(
        "arti_snapshot_partition",
        sa.Column("snapshot_name", sa.String(), nullable=False),
        sa.Column("snapshot_fingerprint", sa.BigInteger(), nullable=False),
        sa.Column("artifact_key", sa.String(), nullable=False),
        sa.Column("partition_fingerprint", sa.BigInteger(), nullable=False),
        sa.ForeignKeyConstraint(
            ["partition_fingerprint"],
            ["arti_storage_partition.fingerprint"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["snapshot_name", "snapshot_fingerprint"],
            ["arti_graph_snapshot.name", "arti_graph_snapshot.fingerprint"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "snapshot_name",
            "snapshot_fingerprint",
            "artifact_key",
            "partition_fingerprint",
        ),
    )
    op.create_table(
        "arti_snapshot_tag",
        sa.Column("graph_name", sa.String(), nullable=False),
        sa.Column("tag", sa.String(), nullable=False),
        sa.Column("snapshot_fingerprint", sa.BigInteger(), nullable=False),
        sa.ForeignKeyConstraint(
            ["graph_name", "snapshot_fingerprint"],
            ["arti_graph_snapshot.name", "arti_graph_snapshot.fingerprint"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("graph_name", "tag"),
    )


def downgrade() -> None:
    op.drop_table("arti_snapshot_tag")
    op.drop_table("arti_snapshot_partition")
    op.drop_index(
        "ix_arti_storage_partition_lookup",
        table_name="arti_storage_partition",
    )
    op.drop_table("arti_storage_partition")
    op.drop_table("arti_graph_snapshot")
    op.drop_table("arti_graph")
