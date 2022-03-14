import os
import time
from collections.abc import Generator
from unittest import mock

import gcsfs
import pytest
import requests
import sh


# For now, run a single emulator per session. Tests should use separate buckets for isolation.
#
# Modified from https://github.com/fsspec/gcsfs/blob/2022.02.0/gcsfs/tests/conftest.py#L58
@pytest.fixture(scope="session")
def gcs_emulator(
    fake_gcs_server_version: str = "1.37.0", port: int = 2784
) -> Generator[str, None, None]:
    if "STORAGE_EMULATOR_HOST" in os.environ:
        # assume using real API or otherwise have a server already set up
        yield os.environ["STORAGE_EMULATOR_HOST"]
        return
    if not sh.which("docker"):
        raise pytest.skip("docker not available to run fake-gcs-server")
    container = "arti-test-gcs-emulator"
    url = f"http://localhost:{port}"
    sh.docker.run(
        "-d",
        f"--name={container}",
        f"-p={port}:{port}",
        f"fsouza/fake-gcs-server:{fake_gcs_server_version}",
        "-backend=memory",
        "-scheme=http",
        f"-external-url={url}",
        f"-port={port}",
        f"-public-host={url}",
    )
    try:
        time.sleep(0.25)
        requests.get(url + "/storage/v1/b").raise_for_status()
        with mock.patch.dict(os.environ, {"STORAGE_EMULATOR_HOST": url}):
            yield url
    finally:
        sh.docker.rm("-fv", container)


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
