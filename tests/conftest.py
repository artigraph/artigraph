import os
from collections.abc import Generator
from unittest import mock

import gcp_storage_emulator.server
import gcsfs
import pytest


# For now, run a single emulator per session. Tests should use separate buckets for isolation.
@pytest.fixture(scope="session")
def gcs_emulator() -> Generator[tuple[str, int], None, None]:
    # port=0 -> run on a random open port; though we have to lookup later.
    server = gcp_storage_emulator.server.create_server("localhost", 0, in_memory=True)
    server.start()
    try:
        host, port = server._api._httpd.socket.getsockname()
        with mock.patch.dict(os.environ, {"STORAGE_EMULATOR_HOST": f"{host}:{port}"}):
            yield host, port
    finally:
        server.stop()


@pytest.fixture()
def gcs(gcs_emulator: tuple[str, int]) -> gcsfs.GCSFileSystem:
    return gcsfs.GCSFileSystem()


@pytest.fixture()
def gcs_bucket(
    request: pytest.FixtureRequest, gcs: gcsfs.GCSFileSystem
) -> Generator[str, None, None]:
    # Strip malformed characters (eg: parametrized test names include brackets). Depending on
    # parametrize inputs, we may have to replace/escape more things.
    bucket = request.node.name.replace("[", "").replace("]", "")
    gcs.mkdir(bucket)
    try:
        yield bucket
    finally:
        gcs.rm(bucket, recursive=True)
