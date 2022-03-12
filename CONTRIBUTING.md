# Contributing to Artigraph

Thank you for your interest in contributing to Artigraph! This document explains our contribution process and procedures.

If you just need help or have a question, refer to [SUPPORT.md](SUPPORT.md).

## How to Contribute a Bug Fix or Enhancement

Contributions can be submitted via [Pull Requests](https://github.com/lfai/artigraph/issues) to the `golden` branch and must:

- be submitted under the Apache 2.0 license.
- include a [Developer Certificate of Origin signoff](https://wiki.linuxfoundation.org/dco) (`git commit -s`)
- include tests and documentation
- match the [Coding Style](#coding-style)

Project committers will review the contribution in a timely manner and advise of any changes needed to merge the request.

## Coding Style

Code is formatted with [`black`](https://black.readthedocs.io/en/stable/) and [`isort`](https://pycqa.github.io/isort/). Docstring style is not yet standardized, but they should generally follow [PEP257](https://www.python.org/dev/peps/pep-0257/).

## Development Workflow

The default branch is `golden` (poking fun at "golden data"). The project is managed using [`poetry`](https://python-poetry.org/). We use [`pre-commit`](https://pre-commit.com/) to automate rapid feedback via git hooks.

### Environment Setup

If you work on macOS, the `.envrc` script (used by [`direnv`](https://direnv.net/)) in the repo root can automate project and environment setup for both Intel and M1 computers. Run `bash .envrc` to:
- install [`brew`](https://brew.sh/) (if necessary)
- install useful system packages ([`direnv`](https://direnv.net/), `git`, and [`pyenv`](https://github.com/pyenv/pyenv)) via the [`Brewfile`](Brewfile)
- install the correct python version ([`.python-version`](.python-version)) with pyenv
- create a virtual environment and install dependencies
- install [`pre-commit`](https://pre-commit.com/) and the hooks configured in [`.pre-commit-config.yaml`](.pre-commit-config.yaml)

After that completes, [configure `direnv`](https://direnv.net/docs/hook.html) for your shell and run `exec $SHELL`. With `direnv` configured, the project's virtual environment will automatically be activated (and python and package versions synced!) upon `cd` into the repo.

If you use another platform or would rather install manually, use `poetry` directly to manage your virtual environment(s). Contributions supporting `direnv` for other platforms would be appreciated!
