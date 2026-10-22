# /// script
# requires-python = ">=3.14,<3.15"
# dependencies = [
#   "pip",
#   "pyodide-build[resolve]==0.39.2",
# ]
# ///

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tarfile
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

PYODIDE_VERSION = "314.0.7"
PYODIDE_PLATFORM = "pyemscripten_2026_0_wasm32"
PYTHON_VERSION = "3.14"

# Pyodide 314 ships pydantic 2.12, typing-extensions 4.15, and typing-inspection 0.4.2, which are
# too old for Artigraph's pydantic, so bundle these instead.
PYDANTIC_VERSION = "2.14.1"
PYDANTIC_CORE_VERSION = "2.50.1"
TYPING_EXTENSIONS_VERSION = "4.16.0"
TYPING_INSPECTION_VERSION = "0.4.4"
PYFARMHASH_VERSION = "0.5.1"
PARSE_VERSION = "1.22.4"
# Keep in sync with the `playground-wheel` task in mise/config.toml.
ARTI_VERSION = "0.0.0+playground"

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_DIR = ROOT / "docs" / "assets" / "playground"
PACKAGE_DIR_NAME = "packages"


def _tool(name: str) -> str:
    path = shutil.which(name)
    if path is None:
        raise FileNotFoundError(f"Required command not found on PATH: {name}")
    return path


def _run(
    command: Sequence[str],
    *,
    cwd: Path = ROOT,
    env: Mapping[str, str] | None = None,
) -> None:
    subprocess.run(  # noqa: S603 - commands are fixed tool invocations assembled by this script.
        command,
        cwd=cwd,
        env=dict(os.environ | dict(env or {})),
        check=True,
    )


def _download_sdist(*, package: str, version: str, destination: Path) -> Path:
    _run(
        [
            sys.executable,
            "-m",
            "pip",
            "download",
            "--no-binary=:all:",
            "--no-deps",
            f"{package}=={version}",
            "-d",
            str(destination),
        ]
    )
    sdists = sorted(destination.glob(f"{package}-{version}.tar.gz"))
    if len(sdists) != 1:
        raise RuntimeError(f"Expected one {package} sdist, found: {sdists}")
    return sdists[0]


def _download_pure_wheels(*, destination: Path) -> list[Path]:
    requirements = [
        f"pydantic=={PYDANTIC_VERSION}",
        f"typing-extensions=={TYPING_EXTENSIONS_VERSION}",
        f"typing-inspection=={TYPING_INSPECTION_VERSION}",
        f"parse=={PARSE_VERSION}",
    ]
    _run(
        [
            sys.executable,
            "-m",
            "pip",
            "download",
            "--only-binary=:all:",
            "--no-deps",
            *requirements,
            "-d",
            str(destination),
        ]
    )
    wheels = sorted(destination.glob("*.whl"))
    if len(wheels) != len(requirements):
        raise RuntimeError(f"Expected {len(requirements)} pure wheels, found: {wheels}")
    return wheels


def _download_pydantic_core_wheel(*, destination: Path) -> Path:
    # pydantic-core publishes Pyodide wheels, so it doesn't need to be built with Rust.
    _run(
        [
            sys.executable,
            "-m",
            "pip",
            "download",
            "--only-binary=:all:",
            "--no-deps",
            f"--platform={PYODIDE_PLATFORM}",
            f"--python-version={PYTHON_VERSION}",
            "--implementation=cp",
            f"pydantic-core=={PYDANTIC_CORE_VERSION}",
            "-d",
            str(destination),
        ]
    )
    wheels = sorted(destination.glob(f"pydantic_core-{PYDANTIC_CORE_VERSION}-*.whl"))
    if len(wheels) != 1:
        raise RuntimeError(f"Expected one pydantic-core wheel, found: {wheels}")
    return wheels[0]


def _extract_sdist(sdist: Path, destination: Path) -> Path:
    with tarfile.open(sdist) as archive:
        archive.extractall(destination, filter="data")
    extracted = sorted(path for path in destination.iterdir() if path.is_dir())
    if len(extracted) != 1:
        raise RuntimeError(f"Expected one extracted sdist directory, found: {extracted}")
    return extracted[0]


def _install_xbuildenv(*, path: Path) -> None:
    # Without an explicit version, `pyodide build` uses whatever cross-build environment is cached
    # (or the latest compatible release), which may not match PYODIDE_VERSION.
    _run([_tool("pyodide"), "xbuildenv", "install", PYODIDE_VERSION, "--path", str(path)])


def _build_pyodide_wheel(
    requirement_or_path: str, *, outdir: Path, env: Mapping[str, str] | None = None
) -> Path:
    pyodide = _tool("pyodide")
    before = set(outdir.glob("*.whl"))
    _run([pyodide, "build", requirement_or_path, "-o", str(outdir)], env=env)
    after = set(outdir.glob("*.whl"))
    built = sorted(after - before)
    if len(built) != 1:
        raise RuntimeError(f"Expected one new wheel from {requirement_or_path}, found: {built}")
    return built[0]


def _build_artigraph_wheel(*, outdir: Path) -> Path:
    uv = _tool("uv")
    before = set(outdir.glob("arti-*.whl"))
    _run(
        [uv, "build", "--wheel", "--out-dir", str(outdir)],
        env={"SETUPTOOLS_SCM_PRETEND_VERSION": ARTI_VERSION},
    )
    after = set(outdir.glob("arti-*.whl"))
    built = sorted(after - before)
    if len(built) != 1:
        raise RuntimeError(f"Expected one new Artigraph wheel, found: {built}")
    return built[0]


def _copy_wheels(wheels: Iterable[Path], *, package_dir: Path) -> list[str]:
    package_dir.mkdir(parents=True, exist_ok=True)
    for existing in package_dir.glob("*.whl"):
        existing.unlink()
    names: list[str] = []
    for wheel in wheels:
        destination = package_dir / wheel.name
        shutil.copy2(wheel, destination)
        names.append(destination.name)
    return sorted(names)


def _write_manifest(*, output_dir: Path, wheel_names: list[str]) -> None:
    manifest: dict[str, Any] = {
        "pyodideVersion": PYODIDE_VERSION,
        "pythonVersion": PYTHON_VERSION,
        "pyodidePackages": ["micropip", "annotated-types"],
        "wheels": wheel_names,
        "packageDir": PACKAGE_DIR_NAME,
        "generatedBy": "tools/playground/build_pyodide_assets.py",
    }
    # Sorted keys match the `pretty-format-json` pre-commit hook.
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")


def build_assets(*, output_dir: Path) -> None:
    if sys.version_info[:2] != (3, 14):
        command = "uv run --script tools/playground/build_pyodide_assets.py"
        raise RuntimeError(f"Build Pyodide assets with Python 3.14, e.g. `{command}`.")

    with TemporaryDirectory(prefix="artigraph-pyodide-assets-") as tmp:
        workspace = Path(tmp)
        build_dir = workspace / "build"
        build_dir.mkdir()
        pure_wheel_dir = workspace / "pure-wheels"
        pure_wheel_dir.mkdir()
        xbuildenv_dir = workspace / "xbuildenv"
        _install_xbuildenv(path=xbuildenv_dir)
        pyodide_env = {"PYODIDE_XBUILDENV_PATH": str(xbuildenv_dir)}

        pure_wheels = _download_pure_wheels(destination=pure_wheel_dir)
        pydantic_core_wheel = _download_pydantic_core_wheel(destination=build_dir)

        pyfarmhash_workspace = workspace / "pyfarmhash"
        pyfarmhash_workspace.mkdir()
        pyfarmhash_sdist = _download_sdist(
            package="pyfarmhash",
            version=PYFARMHASH_VERSION,
            destination=pyfarmhash_workspace,
        )
        pyfarmhash_source = _extract_sdist(pyfarmhash_sdist, pyfarmhash_workspace)
        pyfarmhash_wheel = _build_pyodide_wheel(
            str(pyfarmhash_source), outdir=build_dir, env=pyodide_env
        )

        arti_wheel = _build_artigraph_wheel(outdir=build_dir)
        wheel_names = _copy_wheels(
            [*pure_wheels, pydantic_core_wheel, pyfarmhash_wheel, arti_wheel],
            package_dir=output_dir / PACKAGE_DIR_NAME,
        )
        _write_manifest(output_dir=output_dir, wheel_names=wheel_names)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()
    build_assets(output_dir=args.output_dir.resolve())


if __name__ == "__main__":
    main()
