"""
Record Statistic values and Threshold overrides for each storage partition.

Revision ID: 0002
Revises: 0001
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None

JSON_METADATA = sa.JSON().with_variant(postgresql.JSONB(), "postgresql")


def upgrade() -> None:
    op.create_table(
        "arti_partition_statistic",
        sa.Column("partition_fingerprint", sa.BigInteger(), nullable=False),
        sa.Column("statistic_fingerprint", sa.BigInteger(), nullable=False),
        sa.Column("statistic_json", JSON_METADATA, nullable=False),
        sa.Column("value_json", JSON_METADATA, nullable=False),
        sa.ForeignKeyConstraint(
            ["partition_fingerprint"],
            ["arti_storage_partition.fingerprint"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("partition_fingerprint", "statistic_fingerprint"),
    )
    op.create_table(
        "arti_threshold_override",
        sa.Column("partition_fingerprint", sa.BigInteger(), nullable=False),
        sa.Column("threshold_fingerprint", sa.BigInteger(), nullable=False),
        sa.Column("threshold_json", JSON_METADATA, nullable=False),
        sa.Column("reason", sa.String(), nullable=False),
        sa.ForeignKeyConstraint(
            ["partition_fingerprint"],
            ["arti_storage_partition.fingerprint"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("partition_fingerprint", "threshold_fingerprint"),
    )


def downgrade() -> None:
    op.drop_table("arti_threshold_override")
    op.drop_table("arti_partition_statistic")
