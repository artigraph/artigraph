import os
import socket
from collections.abc import Generator
from typing import Protocol, cast
from unittest import mock

import gcp_storage_emulator.server

# The experimental gcsfs backend uses gRPC APIs that the local HTTP emulator does not implement.
os.environ["GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT"] = "false"

import gcsfs
import pytest


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
