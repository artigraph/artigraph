# Browser playground

The playground target is a real Artigraph-in-Pyodide environment, not a DuckDB-only demo. It will run
an editable Python graph inside an isolated browser filesystem with sample data bundled into the docs
site.

Current status: the package assets are buildable and smoke-tested. The next jj change adds the
editable in-browser runner.

The fingerprinting implementation remains the same: the browser build uses a Pyodide-compiled
`pyfarmhash` wheel rather than replacing FarmHash with a different hash. The smoke test also builds a
small graph inside Pyodide and verifies the produced result.

```shell
uv run --script tools/playground/build_pyodide_assets.py
node tools/playground/smoke_pyodide_playground.mjs
```
