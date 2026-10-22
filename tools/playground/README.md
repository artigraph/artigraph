# Pyodide playground tooling

These scripts build and verify the static package assets used by the browser playground.

## Build the Artigraph wheel

The Artigraph wheel is built from `src/` rather than committed, so the published playground always
matches the source. Build (or rebuild) it before `zensical serve`, `zensical build`, or the smoke
test; CI does the same:

```shell
mise run playground-wheel
```

## Build third-party assets

The playground targets Pyodide 314.0.3, which uses Python 3.14 and requires Rust 1.93 for the
`pydantic-core` wheel.

```shell
uv run --script tools/playground/build_pyodide_assets.py
```

The builder creates `docs/assets/playground/manifest.json` and wheels under
`docs/assets/playground/packages/` for:

- Artigraph itself (ignored by git),
- `pyfarmhash` compiled for Pyodide,
- `pydantic-core` compiled for Pyodide,
- pure-Python wheels needed by the playground.

## Smoke test

```shell
node tools/playground/smoke_pyodide_playground.mjs
```

The smoke test installs the matching `pyodide` npm package in a temporary directory, loads the local
wheels from the generated manifest, checks that the bundled Artigraph wheel matches `src/arti`,
verifies the FarmHash golden value, builds a tiny Artigraph graph inside Pyodide's virtual filesystem,
and asserts that the produced result is `42.0`.

## Navigation test

```shell
node --test tools/playground/test_playground_navigation.mjs
```

This test loads `docs/assets/javascripts/playground.js` against small DOM fakes and checks that
instant navigation terminates the Pyodide worker and disconnects the observers of playgrounds that
left the page. It needs no browser, network access, or built wheels.
