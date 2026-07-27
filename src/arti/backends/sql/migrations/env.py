from __future__ import annotations

from alembic import context
from sqlalchemy import Connection

from arti.backends.sql.models import Base

connection = context.config.attributes.get("connection")
if not isinstance(connection, Connection):
    raise RuntimeError("Artigraph database migrations require an existing SQLAlchemy connection")

context.configure(
    compare_server_default=True,
    compare_type=True,
    connection=connection,
    target_metadata=Base.metadata,
)
with context.begin_transaction():
    context.run_migrations()
