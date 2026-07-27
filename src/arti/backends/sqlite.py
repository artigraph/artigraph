from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Literal

import sqlalchemy as sa
from sqlalchemy import event
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.pool import ConnectionPoolEntry, StaticPool

from arti.backends.sql import SQLBackend


class SQLiteBackend(SQLBackend):
    """
    Persist Artigraph metadata in a file-backed or shared in-memory SQLite database.

    Pass `path=":memory:"` for an in-memory database, which is lost on `close()`. It is
    single-threaded: every `connect()` shares one SQLite connection, so overlapping transactions
    (nested or from other threads) fail, and concurrent first use may create separate databases.
    Use a file `path` for concurrent access.
    """

    path: Path | Literal[":memory:"]
    timeout: float = 5.0

    @property
    def in_memory(self) -> bool:
        return self.path == ":memory:"

    def _create_engine(self) -> Engine:
        connect_args: dict[str, Any] = {"timeout": self.timeout}
        engine_kwargs: dict[str, Any] = {}
        if self.in_memory:
            url = "sqlite+pysqlite://"
            connect_args["check_same_thread"] = False
            engine_kwargs["poolclass"] = StaticPool
        else:
            url = f"sqlite+pysqlite:///{self.path}"
        engine = sa.create_engine(url, connect_args=connect_args, **engine_kwargs)

        def _configure(
            database_connection: sqlite3.Connection,
            _connection_record: ConnectionPoolEntry,
        ) -> None:
            database_connection.isolation_level = None
            database_connection.execute("PRAGMA foreign_keys = ON")
            database_connection.execute(f"PRAGMA busy_timeout = {int(self.timeout * 1000)}")
            if not self.in_memory:
                database_connection.execute("PRAGMA journal_mode = WAL")

        def _begin(connection: Connection) -> None:
            # Explicit BEGIN restores transactional DDL and savepoint behavior. IMMEDIATE takes the
            # write lock up front so concurrent writers wait for `busy_timeout`. A deferred BEGIN
            # that reads and then writes fails with "database is locked" (without waiting) if
            # another connection committed in between.
            connection.exec_driver_sql("BEGIN IMMEDIATE")

        event.listen(engine, "connect", _configure)
        event.listen(engine, "begin", _begin)
        return engine


__all__ = ["SQLiteBackend"]
