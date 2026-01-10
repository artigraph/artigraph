# CLAUDE.md - AI Assistant Guide for Artigraph

This document provides comprehensive guidance for AI assistants working with the Artigraph codebase.

## Project Overview

**Artigraph** is a declarative data production tool that emphasizes that the core deliverable of a data pipeline is the **data itself**, not the tasks. It's hosted by the LF AI and Data Foundation as a Sandbox project.

- **Package Name**: `arti` (on PyPI)
- **License**: Apache 2.0
- **Python Version**: 3.12+
- **Repository**: https://github.com/artigraph/artigraph

## Quick Reference

### Key Commands

```bash
# Setup (one-time)
bash .envrc                    # Auto-setup environment (installs rye, python, deps, pre-commit)

# Development
rye sync --all-features        # Install/update dependencies
rye run pytest                 # Run tests with coverage
rye run pre-commit run -a      # Run all linters and formatters
rye fmt                        # Format code with ruff
rye lint                       # Lint code with ruff
rye run mypy                   # Type check

# Git workflow
git commit -s                  # REQUIRED: Sign-off for DCO compliance
```

### Project Stats
- ~44 Python source files (~5,647 lines)
- ~39 test files
- 100% test coverage required
- Strict type checking with mypy

## Directory Structure

```
/home/user/artigraph/
├── .github/workflows/     # CI/CD (test, publish, CodeQL)
├── bin/                   # Utility scripts
├── docs/
│   └── examples/          # Example projects (e.g., spend example)
├── src/arti/             # Main source code
│   ├── annotations/      # User-defined metadata for artifacts
│   ├── artifacts/        # Dataset definitions (core concept)
│   ├── backends/         # Metadata storage (MemoryBackend)
│   ├── executors/        # Build execution (LocalExecutor)
│   ├── fingerprints/     # Content hashing (farmhash-based)
│   ├── formats/          # Serialization (JSON, Pickle)
│   ├── graphs/           # DAG of artifacts and producers
│   ├── internal/         # Internal utilities (models, dispatch, type hints)
│   ├── io/               # Read/write operations (multiple dispatch)
│   ├── partitions/       # Data partitioning support
│   ├── producers/        # Data transformation functions
│   ├── statistics/       # Data quality metrics (placeholder)
│   ├── storage/          # Storage backends (local, GCS, literal)
│   ├── thresholds/       # Data quality thresholds (placeholder)
│   ├── types/            # Type system & adapters
│   │   ├── adapters/     # Python, Pandas, PyArrow, BigQuery, NumPy
│   │   └── ...           # Core types (Int64, String, Date, etc.)
│   ├── versions/         # Producer versioning (SemVer, Git, Timestamp)
│   └── views/            # In-memory representations
├── tests/arti/           # Test suite (mirrors src structure)
├── stubs/                # Type stubs for third-party libraries
├── pyproject.toml        # Project configuration
├── .envrc                # Auto-setup script (direnv compatible)
└── .pre-commit-config.yaml
```

## Core Concepts & Architecture

### 1. Artifacts (Most Important!)

**Artifacts** are the fundamental building block representing datasets. They have three key components:

```python
class MyData(Artifact):
    type = Float64()              # Data structure specification
    format = JSON()               # Serialization format
    storage = LocalFile(path="...") # Persistence layer
```

**Key Components**:
- **Type**: Data structure (Int64, String, Collection, Struct, Date, etc.)
- **Format**: Serialization (JSON, Pickle, future: Parquet)
- **Storage**: Persistence (LocalFile, GCSFile, StringLiteral)
- **Annotations**: Human knowledge/metadata (e.g., vendor info)
- **Partitions**: Data can be partitioned (e.g., by date)

### 2. Producers

Functions that transform input Artifacts into output Artifacts:

```python
@producer(version=SemVer(major=1, minor=0, patch=0))
def aggregate_transactions(
    transactions: Annotated[list[dict], Transactions]
) -> Annotated[float, TotalSpend]:
    return sum(txn["amount"] for txn in transactions)
```

**Key Features**:
- Defined using `@producer` decorator or as classes
- Include versioning for change tracking
- Support both simple and partitioned data
- Can have multiple inputs/outputs
- Include `.build()` method for computation
- Optional `.map()` for partition dependency specification
- Optional `.validate_outputs()` for data quality checks

### 3. Graphs

DAG connecting Artifacts via Producers:

```python
with Graph(name="my-graph") as g:
    g.artifacts.vendor.transactions = Transactions(...)
    g.artifacts.spend = aggregate_transactions(
        transactions=g.artifacts.vendor.transactions
    )

# Execute
snapshot = g.build()
result = snapshot.read(snapshot.artifacts.spend, annotation=float)
```

**Key Features**:
- Named artifact namespace (e.g., `g.artifacts.vendor.transactions`)
- Context manager for definition
- Snapshot system for versioned execution
- Topological sorting for build order
- Backend integration for metadata tracking

### 4. Type System

Platform-agnostic type representation with adapters:

**Core Types**: Int8-64, UInt8-64, Float16-64, String, Boolean, Date, DateTime, Timestamp, Binary, Null

**Composite Types**: List, Set, Map, Struct, Collection (partitioned)

**Adapters**: Python, Pandas, PyArrow, BigQuery, NumPy, Pydantic

```python
# Example
Collection(
    element=Struct(fields={"id": Int64(), "date": Date(), "amount": Float64()}),
    partition_by=("date",),
)
```

### 5. Storage Backends

Abstract storage layer:
- **LocalFile**: Local filesystem storage
- **GCSFile**: Google Cloud Storage (`src/arti/storage/google/cloud/storage.py`)
- **StringLiteral**: In-memory string storage
- Template-based paths with partition key formatting: `{date.iso}.json`

### 6. Fingerprinting

Content-based identity using Farmhash Fingerprint64:
- Int64 hash (XOR-combinable)
- Used for change detection and caching
- Applied to Artifacts, Producers, and data partitions
- Identity value: 0 (neutral for XOR)

### 7. Backends

Metadata storage for tracking:
- Graph definitions and snapshots
- Artifact-Producer dependencies
- Partition metadata and fingerprints
- Currently: **MemoryBackend** (in-memory, non-persistent)
- Future: Database-backed implementations

### 8. Executors

Build orchestration:
- **LocalExecutor**: Sequential local execution
- Topological traversal of the graph
- Partition-aware building
- Skips existing partitions
- Future: Distributed/parallel execution

## Key Architectural Patterns

### 1. Pydantic-Based Models

All core classes inherit from custom `Model` (frozen, strict validation):

```python
from arti.internal.models import Model

class MyModel(Model):
    field: str
    # Automatically: frozen=True, strict validation, fingerprinting
```

**Important**: Models are immutable! Use `.model_copy(update={...})` to modify.

### 2. Multiple Dispatch

`io.read()` and `io.write()` use custom multiple dispatch:

```python
from arti.io import register_reader, register_writer

@register_reader
def read(format: JSON, storage: LocalFile, type: Float64) -> Any:
    # Implementation
    ...

@register_writer
def write(artifact: Artifact, view: Any, format: JSON, storage: LocalFile) -> None:
    # Implementation
    ...
```

**Extensible**: Add new format/storage/type combinations by registering functions.

### 3. Type System Adapters

Pluggable type system with priority-based lookup:

```python
from arti.types import TypeAdapter

class MyTypeAdapter(TypeAdapter):
    def to_artifact_type(self, native_type: Any) -> ArtiType:
        # Convert from native type system to Artigraph
        ...

    def from_artifact_type(self, artifact_type: ArtiType) -> Any:
        # Convert from Artigraph to native type system
        ...
```

### 4. Template-Based Storage Paths

Storage paths use format strings:

```python
LocalFile(path="/data/{date.iso}.json")
# Renders to: /data/2021-10-01.json

# Available variables:
# - Partition keys: {date.iso}, {date.Y}, {date.m}, {date.d}
# - Graph name: {graph_name}
# - Input fingerprint: {input_fingerprint}
```

### 5. Snapshot-Based Execution

Graph + raw data → GraphSnapshot (fingerprinted):

```python
snapshot = g.build()  # Creates immutable snapshot
result = snapshot.read(snapshot.artifacts.my_artifact, annotation=float)
```

Enables time-travel and reproducibility.

## Development Workflow

### Environment Setup

```bash
# Option 1: Auto-setup with direnv (recommended)
bash .envrc  # Installs everything automatically

# Option 2: Manual setup
curl -sSf https://rye.astral.sh/get | RYE_INSTALL_OPTION="-y" bash
source "$HOME/.rye/env"
rye sync --all-features
rye run pre-commit install --install-hooks
```

### Making Changes

1. **Read the code first**: Always read existing files before modifying them
2. **Make your changes**: Keep them focused and minimal
3. **Add tests**: 100% coverage required
4. **Format and lint**: `rye fmt && rye lint`
5. **Type check**: `rye run mypy`
6. **Run tests**: `rye run pytest`
7. **Commit with DCO**: `git commit -s -m "your message"`

### Testing Conventions

- Tests mirror `src/arti/` structure
- Located in `tests/arti/`
- Use pytest with parametrization
- 100% coverage required (branch coverage enabled)
- Use dummies from `tests.arti.dummies` for test artifacts

**Key Fixtures** (`tests/arti/conftest.py`):
```python
@pytest.fixture(scope="session")
def gcs_emulator() -> tuple[str, int]  # GCS emulator for cloud storage testing

@pytest.fixture()
def clean_test_name(request) -> str  # Clean test names for file paths

@pytest.fixture()
def gcs_bucket(clean_test_name, gcs) -> str  # GCS bucket per test
```

## Code Style & Standards

### Formatting & Linting

- **Tool**: `ruff` (replaces black, isort, flake8, etc.)
- **Line length**: 100 characters
- **Target**: Python 3.12
- **Config**: `pyproject.toml` → `[tool.ruff]`

### Type Checking

- **Tool**: `mypy` in strict mode
- **Config**: `pyproject.toml` → `[tool.mypy]`
- **Plugins**: pydantic
- **Coverage**: All files in `src/`, `tests/`, `docs/`

### Docstrings

- Generally follow PEP257
- Not yet standardized, but be descriptive
- Include examples where helpful

### Contribution Requirements

1. **License**: Apache 2.0
2. **DCO Sign-off**: `git commit -s` (required by pre-commit hook)
3. **Tests**: 100% coverage
4. **Documentation**: Update relevant docs
5. **Pre-commit**: All checks must pass
6. **Code style**: ruff formatting and linting
7. **Type hints**: Strict mypy compliance

## Important Patterns to Follow

### When Working with Models

```python
# CORRECT: Models are immutable
artifact = MyArtifact(type=Int64(), format=JSON())
updated = artifact.model_copy(update={"format": Pickle()})

# WRONG: Cannot mutate
artifact.format = Pickle()  # Raises FrozenInstanceError
```

### When Adding IO Support

```python
# Register a reader
from arti.io import register_reader

@register_reader
def read_my_format(format: MyFormat, storage: MyStorage, type: MyType) -> Any:
    # Read and return data
    ...

# Register a writer
from arti.io import register_writer

@register_writer
def write_my_format(artifact: Artifact, view: Any, format: MyFormat, storage: MyStorage) -> None:
    # Write data
    ...
```

### When Adding a Type Adapter

```python
from arti.types import TypeAdapter, register

class MyTypeAdapter(TypeAdapter):
    def to_artifact_type(self, native_type: Any) -> ArtiType:
        # Convert native → Artigraph
        ...

    def from_artifact_type(self, artifact_type: ArtiType) -> Any:
        # Convert Artigraph → native
        ...

# Register with priority
register(type_adapters, "my_system", MyTypeAdapter, priority=lambda: 100)
```

### When Creating Producers

```python
# Simple producer
@producer(version=SemVer(major=1, minor=0, patch=0))
def my_producer(
    input_data: Annotated[list[dict], InputArtifact]
) -> Annotated[float, OutputArtifact]:
    return process(input_data)

# Producer with partition mapping
class MyProducer(Producer):
    version = SemVer(major=1, minor=0, patch=0)

    def build(self, input_data: Annotated[list[dict], InputArtifact]) -> float:
        return process(input_data)

    def map(self, input_partitions: dict[PartitionKey, InputArtifact]) -> dict[PartitionKey, ...]:
        # Define partition-to-partition dependencies
        return {...}
```

## CI/CD Pipeline

### Workflows (`.github/workflows/`)

**CI** (`ci.yaml`):
- Runs on: PR, push to main, releases
- Matrix: macOS + Ubuntu, Python 3.12
- Steps:
  1. Pre-commit checks (format, lint, DCO)
  2. pytest with coverage → Codecov
  3. Publish to TestPyPI (main branch)
  4. Publish to PyPI (tagged releases)

**CodeQL**: Weekly security scanning

### Version Management

- Uses `hatch-vcs` (git tag-based)
- Local version identifiers stripped for PyPI uploads
- `SOURCE_DATE_EPOCH` for reproducible builds

### Publishing

- **TestPyPI**: Every commit to main
- **PyPI**: Tagged releases (`v*`)
- Token-based authentication

## Common Pitfalls & Solutions

### 1. Forgetting DCO Sign-off

**Problem**: Commit rejected by pre-commit hook

**Solution**: Use `git commit -s` or configure it globally:
```bash
git config --global format.signoff true
```

### 2. Test Coverage Below 100%

**Problem**: Coverage check fails

**Solution**: Add tests for uncovered lines. Use coverage report to identify gaps:
```bash
rye run pytest --cov-report=html
# Open htmlcov/index.html
```

### 3. Type Errors

**Problem**: mypy fails with strict mode errors

**Solution**: Add type hints everywhere. Use `assert isinstance(x, Type)` for narrowing:
```python
def my_func(x: int | str) -> int:
    assert isinstance(x, int)  # Type narrowing
    return x + 1
```

### 4. Import Errors

**Problem**: Module not found

**Solution**: Check mypy_path in `pyproject.toml`. Source is in `src/arti/`, stubs in `stubs/`.

### 5. Frozen Model Errors

**Problem**: `FrozenInstanceError` when trying to modify

**Solution**: Use `.model_copy(update={...})` instead of direct assignment:
```python
updated = model.model_copy(update={"field": new_value})
```

## Known Limitations

- **Statistics and Thresholds**: Placeholder modules (not yet implemented)
- **Side-effect Producers**: Not yet supported
- **Documentation generation**: Disabled (pdocs/portray unmaintained)
- **Distributed execution**: Only LocalExecutor implemented
- **Persistent backends**: Only MemoryBackend available

## Useful Code Locations

### Core Functionality
- Entry point: `src/arti/__init__.py`
- Artifacts: `src/arti/artifacts/__init__.py`
- Producers: `src/arti/producers/__init__.py`
- Graphs: `src/arti/graphs/__init__.py`
- Types: `src/arti/types/__init__.py`

### Testing
- Test utilities: `tests/arti/conftest.py`
- Test dummies: `tests/arti/dummies.py`

### Internal
- Base model: `src/arti/internal/models.py`
- Multiple dispatch: `src/arti/internal/dispatch.py`
- Type hints: `src/arti/internal/type_hints.py`

### Examples
- Spend example: `docs/examples/spend/demo.py`

## Quick Example

```python
from pathlib import Path
from typing import Annotated

from arti import Annotation, Artifact, Graph, producer
from arti.formats.json import JSON
from arti.storage.local import LocalFile
from arti.types import Collection, Date, Float64, Int64, Struct
from arti.versions import SemVer

# Define Artifacts
class Transactions(Artifact):
    """Transactions partitioned by day."""
    type = Collection(
        element=Struct(fields={"id": Int64(), "date": Date(), "amount": Float64()}),
        partition_by=("date",),
    )

class TotalSpend(Artifact):
    """Aggregate spend over all time."""
    type = Float64()
    format = JSON()
    storage = LocalFile()

# Define Producer
@producer(version=SemVer(major=1, minor=0, patch=0))
def aggregate_transactions(
    transactions: Annotated[list[dict], Transactions]
) -> Annotated[float, TotalSpend]:
    return sum(txn["amount"] for txn in transactions)

# Build Graph
with Graph(name="test-graph") as g:
    g.artifacts.vendor.transactions = Transactions(
        format=JSON(),
        storage=LocalFile(path="transactions/{date.iso}.json"),
    )
    g.artifacts.spend = aggregate_transactions(
        transactions=g.artifacts.vendor.transactions
    )

# Execute
snapshot = g.build()
result = snapshot.read(snapshot.artifacts.spend, annotation=float)
print(f"Total spend: {result}")
```

## When to Ask for Help

If you encounter:
- Unclear architectural decisions
- Missing documentation
- Complex fingerprinting logic
- Type adapter priority issues
- Partition mapping complexity
- Unknown test patterns

Check:
1. **Tests**: Often the best documentation
2. **Examples**: `docs/examples/`
3. **Issues**: https://github.com/artigraph/artigraph/issues
4. **Discussions**: https://github.com/artigraph/artigraph/discussions

## Summary for AI Assistants

**Key Principles**:
1. **Data-first**: Artifacts are first-class citizens
2. **Immutability**: All models are frozen
3. **Type safety**: Strict mypy, runtime validation with pydantic
4. **Test coverage**: 100% required
5. **DCO sign-off**: Always use `git commit -s`
6. **Simplicity**: Avoid over-engineering

**Common Tasks**:
- Adding storage backends → Implement `Storage` subclass + IO handlers
- Adding format support → Implement `Format` subclass + IO handlers
- Adding type adapters → Subclass `TypeAdapter` and register
- Adding producers → Use `@producer` decorator or `Producer` class
- Adding tests → Mirror source structure, use dummies, aim for 100% coverage

**Avoid**:
- Mutating frozen models (use `.model_copy(update={...})`)
- Committing without DCO sign-off (`git commit -s`)
- Breaking type safety (strict mypy must pass)
- Reducing test coverage below 100%
- Over-engineering (keep it simple!)

---

**Last Updated**: 2026-01-10 (matches repository state at commit a158a7e)
