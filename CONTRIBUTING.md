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

Code is formatted and linted with [`ruff`](https://docs.astral.sh/ruff/) and spell checked with
[`codespell`](https://github.com/codespell-project/codespell).

## Development Workflow

The project uses [`mise`](https://mise.jdx.dev/) to manage development tools and
[`uv`](https://docs.astral.sh/uv/) to manage Python and project dependencies. We use
[`pre-commit`](https://pre-commit.com/) to apply and enforce all project checks.

### Environment Setup

Install [`mise`](https://mise.jdx.dev/getting-started.html), trust this repository's
configuration, and enter the directory:

```shell
mise trust
mise install
uv sync --all-extras
pre-commit install --install-hooks
```

Run the complete local check suite with:

```shell
mise check
```
