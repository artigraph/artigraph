from __future__ import annotations

import os
from collections.abc import Generator
from contextlib import contextmanager

from testcontainers.community.postgres import PostgresContainer

POSTGRES_TEST_URL_ENV = "ARTIGRAPH_TEST_POSTGRES_URL"


@contextmanager
def postgres_server_url() -> Generator[str]:
    """Use a configured Postgres server or start a disposable container."""
    configured_url = os.environ.get(POSTGRES_TEST_URL_ENV)
    if configured_url is not None:
        yield configured_url
        return

    with PostgresContainer("postgres:18-alpine", driver="psycopg") as postgres:
        yield postgres.get_connection_url()


__all__ = ["POSTGRES_TEST_URL_ENV", "postgres_server_url"]
