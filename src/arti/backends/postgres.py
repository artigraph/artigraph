from __future__ import annotations

from typing import Self

import sqlalchemy as sa
from pydantic import SecretStr
from sqlalchemy.engine import Engine

from arti.backends.sql import SQLBackend


class PostgresBackend(SQLBackend):
    r"""
    Persist Artigraph metadata in Postgres through SQLAlchemy and psycopg.

    Postgres JSONB cannot store the NUL character (`\u0000`), so writing metadata with one in a
    string raises an error (SQLite accepts it).
    """

    url: SecretStr

    @classmethod
    def from_url(cls, url: str, *, auto_upgrade: bool = True) -> Self:
        """Create a backend while keeping the connection URL secret in repr and serialization."""
        return cls(url=SecretStr(url), auto_upgrade=auto_upgrade)

    def _create_engine(self) -> Engine:
        return sa.create_engine(self.url.get_secret_value(), pool_pre_ping=True)


__all__ = ["PostgresBackend"]
