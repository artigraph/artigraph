#!/usr/bin/env node
import { execFileSync } from "node:child_process";
import { existsSync, mkdtempSync, readdirSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
const defaultAssetsDir = join(repoRoot, "docs/assets/playground");
const artiSourceDir = join(repoRoot, "src/arti");

function argValue(name, fallback) {
  const index = process.argv.indexOf(name);
  if (index === -1) {
    return fallback;
  }
  const value = process.argv[index + 1];
  if (!value) {
    throw new Error(`Missing value for ${name}`);
  }
  return value;
}

function findWheel(manifest, prefix, assetsDir) {
  const wheel = manifest.wheels.find((name) => name.startsWith(prefix) && name.endsWith(".whl"));
  if (!wheel) {
    throw new Error(`No wheel with prefix ${prefix} in manifest: ${manifest.wheels.join(", ")}`);
  }
  const path = join(assetsDir, manifest.packageDir, wheel);
  if (!existsSync(path)) {
    throw new Error(`Missing ${path}; run \`mise run playground-wheel\` first.`);
  }
  return pathToFileURL(path).href;
}

function installPyodideNodePackage(version) {
  const npm = process.env.NPM || "npm";
  const installDir = mkdtempSync(join(tmpdir(), "artigraph-pyodide-node-"));
  execFileSync(npm, ["install", "--prefix", installDir, `pyodide@${version}`], {
    stdio: "inherit",
  });
  return installDir;
}

async function loadPyodideFromNodePackage(version) {
  const configuredDir = process.env.PYODIDE_NODE_PACKAGE_DIR;
  const installDir = configuredDir || installPyodideNodePackage(version);
  const modulePath = join(installDir, "node_modules/pyodide/pyodide.mjs");
  if (!existsSync(modulePath)) {
    throw new Error(`Could not find pyodide.mjs at ${modulePath}`);
  }
  const module = await import(pathToFileURL(modulePath).href);
  return { loadPyodide: module.loadPyodide, installDir: configuredDir ? null : installDir };
}

const sampleModule = String.raw`
def run():
    from datetime import date
    from typing import Annotated, TypedDict

    from arti import Artifact, Format, Graph, PartitionKey, Storage, Type, producer
    from arti.formats.json import JSON
    from arti.partitions import DateField
    from arti.storage.local import LocalFile
    from arti.types import Collection, Date, Float64, Int64, Struct
    from arti.versions import SemVer

    class Transactions(Artifact):
        type: Type = Collection(
            element=Struct(fields={"id": Int64(), "date": Date(), "amount": Float64()}),
            partition_by=("date",),
        )

    class TotalSpend(Artifact):
        type: Type = Float64()
        format: Format = JSON()
        storage: Storage = LocalFile(
            path="/tmp/produced/{input_fingerprint}/total.json",
        )

    class Transaction(TypedDict):
        id: int
        amount: float

    @producer(version=SemVer(major=1, minor=0, patch=0))
    def aggregate_transactions(
        transactions: Annotated[list[Transaction], Transactions],
    ) -> Annotated[float, TotalSpend]:
        return float(sum(txn["amount"] for txn in transactions))

    with Graph(name="pyodide-smoke") as graph:
        graph.artifacts.transactions = Transactions(
            format=JSON(),
            storage=LocalFile(path="/tmp/transactions/{date.iso}.json"),
        )
        graph.artifacts.total = aggregate_transactions(transactions=graph.artifacts.transactions)

    graph.write(
        [
            {"id": 1, "date": "2026-01-01", "amount": 18.0},
            {"id": 2, "date": "2026-01-01", "amount": 24.0},
        ],
        artifact=graph.artifacts.transactions,
        partition_key=PartitionKey(date=DateField(value=date(2026, 1, 1))),
    )
    snapshot = graph.build()
    return snapshot.read(snapshot.artifacts.total, annotation=float), int(snapshot.id)
`;

function artiSources() {
  const sources = {};
  for (const path of readdirSync(artiSourceDir, { recursive: true })) {
    if (path.endsWith(".py")) {
      sources[path] = readFileSync(join(artiSourceDir, path), "utf8");
    }
  }
  return JSON.stringify(sources);
}

async function main() {
  const assetsDir = resolve(argValue("--assets-dir", defaultAssetsDir));
  const manifestPath = join(assetsDir, "manifest.json");
  const manifest = JSON.parse(readFileSync(manifestPath, "utf8"));
  const { loadPyodide, installDir } = await loadPyodideFromNodePackage(manifest.pyodideVersion);

  try {
    const pyodide = await loadPyodide();
    await pyodide.loadPackage(manifest.pyodidePackages);
    pyodide.FS.writeFile("artigraph_smoke.py", sampleModule);
    // Install pydantic's dependencies first so micropip doesn't look for them on PyPI.
    const micropip = pyodide.pyimport("micropip");
    for (const prefix of [
      "typing_extensions-",
      "typing_inspection-",
      "pydantic_core-",
      "pydantic-",
      "pyfarmhash-",
      "parse-",
      "arti-",
    ]) {
      await micropip.install(findWheel(manifest, prefix, assetsDir));
    }

    pyodide.globals.set("arti_sources_json", artiSources());

    await pyodide.runPythonAsync(`
import json
from importlib.util import find_spec
from pathlib import Path

arti_root = Path(find_spec("arti").origin).parent
installed = {str(p.relative_to(arti_root)): p.read_text() for p in arti_root.rglob("*.py")}
expected = json.loads(arti_sources_json)
stale = sorted(k for k in installed.keys() | expected.keys() if installed.get(k) != expected.get(k))
assert not stale, f"Playground arti wheel is stale; run 'mise run playground-wheel'. Changed: {stale}"

import farmhash
assert farmhash.fingerprint64("artigraph") == 12072101138314094687

import pydantic
assert pydantic.__version__ == "2.14.1"

from artigraph_smoke import run
result, snapshot_id = run()
assert result == 42.0
assert snapshot_id != 0
print(f"Artigraph Pyodide smoke passed: result={result}, snapshot={snapshot_id}")
`);
  } finally {
    if (installDir) {
      rmSync(installDir, { recursive: true, force: true });
    }
  }
}

await main();
