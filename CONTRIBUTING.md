# Contributing to Artigraph

Thank you for your interest in contributing to Artigraph! This document explains our contribution process and procedures.

If you just need help or have a question, refer to our [support page](SUPPORT.md).

## How to Contribute a Bug Fix or Enhancement

Contributions can be submitted via [Pull Requests](https://github.com/lfai/artigraph/issues) to the `main` branch and must:

- be submitted under the Apache 2.0 license.
- include a [Developer Certificate of Origin signoff](https://wiki.linuxfoundation.org/dco) (`git commit -s`)
- include tests and documentation
- match the [Coding Style](#coding-style)

Project committers will review the contribution in a timely manner and advise of any changes needed to merge the request.

## Coding Style

Code is formatted and linted with [`ruff`](https://docs.astral.sh/ruff/). Docstring style is not yet standardized, but they should generally follow [PEP257](https://www.python.org/dev/peps/pep-0257/).

## Development Workflow

The project is managed with [`rye`](https://rye.astral.sh). We use [`pre-commit`](https://pre-commit.com/) to apply and enforce code formatting and linting with git hooks.

### Environment Setup

If you work on Linux or macOS, the `.envrc` script (used by [`direnv`](https://direnv.net/)) in the repo root can automate project and environment setup. Run `bash .envrc` to:
- install [`rye`](https://rye.astral.sh)
- install the version of python set in `.python-version`
- create a virtual environment and install dependencies
- install and configure [`pre-commit`](https://pre-commit.com/)

If you'd like for the virtualenv to be activated automatically when you enter the repo, [install](https://direnv.net/docs/installation.html) and [configure](https://direnv.net/docs/hook.html) `direnv`.
