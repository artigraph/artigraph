# Browser playground

The playground target is a real Artigraph-in-Pyodide environment, not a DuckDB-only demo. It will run
an editable Python graph inside an isolated browser filesystem with sample data bundled into the docs
site.

Current status: the docs page exists so the information architecture is stable. The implementation is
built in the next jj changes:

1. Build and verify a Pyodide-compatible `pyfarmhash` wheel.
2. Package Artigraph and its browser-compatible dependencies for Pyodide.
3. Add the editable in-browser runner with resettable sample files and bounded execution.

The fingerprinting implementation remains the same: the browser build must use FarmHash-compatible
fingerprints rather than replacing them with a different hash.
