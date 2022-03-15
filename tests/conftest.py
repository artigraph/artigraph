import os
import platform
import shutil
import time
from collections.abc import Generator
from pathlib import Path
from unittest import mock

import gcsfs
import pytest
import requests
import sh

MACHINE_MAP = {
    "aarch64": "arm64",
    "arm64": "arm64",
    "x86_64": "amd64",
}
BIN_CACHE_DIR = Path(__file__).parent / ".bin_cache"
BIN_MACHINE = MACHINE_MAP[platform.machine()]
BIN_SYSTEM = platform.system()


def _get_fake_gcs_server_cmd(
    machine: str = BIN_MACHINE, system: str = BIN_SYSTEM, version: str = "1.37.0"
) -> sh.Command:
    binpath = BIN_CACHE_DIR / f"fake-gcs-server-{version}-{system}-{machine}"
    if not binpath.exists():
        tgz_name = f"{binpath}.tgz"
        url = f"https://github.com/fsouza/fake-gcs-server/releases/download/v{version}/fake-gcs-server_{version}_{system}_{machine}.tar.gz"
        with requests.get(url, stream=True) as resp:
            if resp.status_code != requests.codes.ok:
                pytest.skip(f"fake-gcs-server for {system} {machine} is not available")
            with open(tgz_name, "wb") as tgz:
                shutil.copyfileobj(resp.raw, tgz)
        sh.tar("-xf", tgz_name, "fake-gcs-server")
        sh.mv("fake-gcs-server", binpath)
        sh.rm(tgz_name)
    return sh.Command(binpath)


# For now, run a single emulator per session. Tests should use separate buckets for isolation.
@pytest.fixture(scope="session")
def gcs_emulator(port: int = 2784) -> Generator[str, None, None]:
    if "STORAGE_EMULATOR_HOST" in os.environ:
        # assume using real API or otherwise have a server already set up
        yield os.environ["STORAGE_EMULATOR_HOST"]
        return
    url = f"http://localhost:{port}"
    fake_gcs_server_proc = _get_fake_gcs_server_cmd()(
        "-backend=memory",
        "-scheme=http",
        f"-external-url={url}",
        f"-port={port}",
        f"-public-host={url}",
        _bg=True,
    )
    try:
        time.sleep(0.25)
        requests.get(url + "/storage/v1/b").raise_for_status()
        with mock.patch.dict(os.environ, {"STORAGE_EMULATOR_HOST": url}):
            yield url
    finally:
        fake_gcs_server_proc.terminate()
        fake_gcs_server_proc.wait()


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
