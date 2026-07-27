import os
import socket
from collections.abc import Generator
from typing import Protocol, cast
from unittest import mock
from uuid import uuid4

import gcp_storage_emulator.server
import sqlalchemy as sa

# The experimental gcsfs backend uses gRPC APIs that the local HTTP emulator does not implement.
os.environ["GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT"] = "false"

import gcsfs
import pytest
from testcontainers.community.postgres import PostgresContainer

from arti.backends.postgres import PostgresBackend


class _NamedNode(Protocol):
    name: str


class _SocketServer(Protocol):
    socket: socket.socket


@pytest.fixture
def clean_test_name(request: pytest.FixtureRequest) -> str:
    # Strip malformed characters (eg: parametrized test names include brackets). Depending on
    # parametrize inputs, we may have to replace/escape more things.
    node = cast(_NamedNode, getattr(request, "node"))  # noqa: B009
    return node.name.replace("[", "").replace("]", "")


# For now, run a single emulator per session. Tests should use separate buckets for isolation.
@pytest.fixture(scope="session")
def gcs_emulator() -> Generator[tuple[str, int], None, None]:
    # port=0 -> run on a random open port; though we have to lookup later.
    with gcp_storage_emulator.server.create_server("localhost", 0, in_memory=True) as server:
        api = cast(object, getattr(server, "_api"))  # noqa: B009
        httpd = cast(_SocketServer, getattr(api, "_httpd"))  # noqa: B009
        host, port = cast(tuple[str, int], httpd.socket.getsockname())
        with mock.patch.dict(os.environ, {"STORAGE_EMULATOR_HOST": f"{host}:{port}"}):
            yield host, port


@pytest.fixture
def gcs(gcs_emulator: tuple[str, int]) -> gcsfs.GCSFileSystem:
    return gcsfs.GCSFileSystem()


@pytest.fixture
def gcs_bucket(clean_test_name: str, gcs: gcsfs.GCSFileSystem) -> Generator[str, None, None]:
    bucket = clean_test_name
    gcs.mkdir(bucket)
    try:
        yield bucket
    finally:
        gcs.rm(bucket, recursive=True)


def _database_url(url: str, database: str) -> str:
    return sa.make_url(url).set(database=database).render_as_string(hide_password=False)


@pytest.fixture(scope="session")
def postgres_admin_url() -> Generator[str, None, None]:
    """Run one Postgres container and prepare a migrated template database."""
    with PostgresContainer("postgres:17-alpine", driver="psycopg") as postgres:
        admin_url = postgres.get_connection_url()
        template_name = "artigraph_template"
        engine = sa.create_engine(admin_url, isolation_level="AUTOCOMMIT")
        with engine.connect() as connection:
            connection.execute(sa.text(f'CREATE DATABASE "{template_name}"'))
        engine.dispose()

        template_backend = PostgresBackend.from_url(_database_url(admin_url, template_name))
        template_backend.upgrade()
        template_backend.close()
        yield admin_url


@pytest.fixture
def postgres_backend(postgres_admin_url: str) -> Generator[PostgresBackend, None, None]:
    """Clone the migrated Postgres template into an isolated database for one test."""
    database = f"artigraph_test_{uuid4().hex}"
    engine = sa.create_engine(postgres_admin_url, isolation_level="AUTOCOMMIT")
    with engine.connect() as connection:
        connection.execute(sa.text(f'CREATE DATABASE "{database}" TEMPLATE "artigraph_template"'))
    backend = PostgresBackend.from_url(_database_url(postgres_admin_url, database))
    try:
        yield backend
    finally:
        backend.close()
        with engine.connect() as connection:
            connection.execute(
                sa.text(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = :database AND pid <> pg_backend_pid()"
                ),
                {"database": database},
            )
            connection.execute(sa.text(f'DROP DATABASE "{database}"'))
        engine.dispose()
