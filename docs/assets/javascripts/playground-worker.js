// Install pydantic-core first so pydantic's pinned dependency is already satisfied.
const INSTALL_ORDER = ["pydantic_core-", "pydantic-", "pyfarmhash-", "parse-", "arti-"];

let pyodide = null;
let pyodidePromise = null;
let activeManifestUrl = null;
let running = false;

function postStatus(message) {
  self.postMessage({ type: "status", message });
}

function postError(error, phase) {
  self.postMessage({
    type: "error",
    phase,
    message: error?.message || String(error),
    // A PythonError's message already holds the Python traceback; its JS stack is Pyodide internals.
    stack: error?.name === "PythonError" ? "" : error?.stack || "",
  });
}

function findWheel(manifest, prefix, manifestUrl) {
  const wheel = manifest.wheels.find((name) => name.startsWith(prefix) && name.endsWith(".whl"));
  if (!wheel) {
    throw new Error(`No wheel with prefix ${prefix} in manifest: ${manifest.wheels.join(", ")}`);
  }
  return new URL(`${manifest.packageDir}/${wheel}`, manifestUrl).href;
}

async function installPlaygroundPackages(instance, manifest, manifestUrl) {
  postStatus("Loading Pyodide packages...");
  await instance.loadPackage(manifest.pyodidePackages);

  postStatus("Installing Artigraph wheels...");
  const micropip = instance.pyimport("micropip");
  for (const prefix of INSTALL_ORDER) {
    await micropip.install(findWheel(manifest, prefix, manifestUrl));
  }
}

async function loadPlayground(manifestUrl) {
  if (pyodide && activeManifestUrl === manifestUrl) {
    return pyodide;
  }
  if (pyodide && activeManifestUrl !== manifestUrl) {
    throw new Error("This playground worker already loaded a different manifest URL.");
  }
  if (pyodidePromise) {
    return pyodidePromise;
  }

  activeManifestUrl = manifestUrl;
  pyodidePromise = (async () => {
    postStatus("Loading playground manifest...");
    const manifestResponse = await fetch(manifestUrl);
    if (!manifestResponse.ok) {
      throw new Error(`Unable to fetch ${manifestUrl}: ${manifestResponse.status}`);
    }
    const manifest = await manifestResponse.json();
    const indexUrl = `https://cdn.jsdelivr.net/pyodide/v${manifest.pyodideVersion}/full/`;

    postStatus(`Loading Pyodide ${manifest.pyodideVersion}...`);
    const { loadPyodide } = await import(`${indexUrl}pyodide.mjs`);
    const instance = await loadPyodide({ indexURL: indexUrl });
    await installPlaygroundPackages(instance, manifest, manifestUrl);
    // Only forward output after setup so package-loading logs don't land in the run output.
    instance.setStdout({ batched: (text) => self.postMessage({ type: "stdout", text }) });
    instance.setStderr({ batched: (text) => self.postMessage({ type: "stderr", text }) });
    pyodide = instance;
    self.postMessage({ type: "ready" });
    return instance;
  })();

  try {
    return await pyodidePromise;
  } catch (error) {
    pyodide = null;
    pyodidePromise = null;
    activeManifestUrl = null;
    throw error;
  }
}

async function preparePlayground({ manifestUrl }) {
  await loadPlayground(manifestUrl);
}

async function resetPlaygroundFilesystem(instance) {
  await instance.runPythonAsync(`
import os
import shutil

os.chdir("/")
try:
    shutil.rmtree("/home/pyodide/playground")
except FileNotFoundError:
    pass

os.makedirs("/home/pyodide/playground", exist_ok=True)
os.makedirs("/tmp", exist_ok=True)
for name in os.listdir("/tmp"):
    path = os.path.join("/tmp", name)
    if os.path.isdir(path) and not os.path.islink(path):
        shutil.rmtree(path)
    else:
        os.remove(path)

os.chdir("/home/pyodide/playground")
`);
}

async function runCode({ code, manifestUrl }) {
  if (running) {
    throw new Error("A playground run is already in progress.");
  }

  running = true;
  try {
    const instance = await loadPlayground(manifestUrl);
    postStatus("Resetting playground filesystem...");
    await resetPlaygroundFilesystem(instance);

    instance.globals.set("__artigraph_user_code__", code);
    self.postMessage({ type: "started" });
    await instance.runPythonAsync(`
import os
import sys

namespace = {"__name__": "__main__"}
try:
    exec(compile(__artigraph_user_code__, "<playground>", "exec"), namespace)
    if "result" in namespace:
        print(f"result = {namespace['result']!r}")
finally:
    # Batched output is forwarded per line; fsync forwards any trailing partial line.
    for stream in (sys.stdout, sys.stderr):
        stream.flush()
        os.fsync(stream.fileno())
`);
    self.postMessage({ type: "done" });
  } finally {
    running = false;
  }
}

self.addEventListener("message", (event) => {
  const message = event.data || {};
  if (message.type === "prepare") {
    preparePlayground(message).catch((error) => postError(error, "prepare"));
  } else if (message.type === "run") {
    runCode(message).catch((error) => postError(error, "run"));
  }
});
