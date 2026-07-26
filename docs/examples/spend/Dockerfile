FROM ghcr.io/astral-sh/uv:0.11.32 AS uv


FROM python:3.12-slim-bookworm AS build

COPY --from=uv /uv /uvx /bin/

ENV SETUPTOOLS_SCM_PRETEND_VERSION=0.0.0
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

WORKDIR /app

RUN apt-get update \
    && apt-get install --yes --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY README.md pyproject.toml uv.lock ./
COPY src/ src/

RUN uv sync --frozen --no-dev --no-editable


FROM python:3.12-slim-bookworm

WORKDIR /app

ENV PATH="/app/.venv/bin:$PATH"

COPY --from=build /app/.venv .venv/
COPY docs/examples/spend/demo.py .
COPY docs/examples/spend/transactions/ transactions/

CMD ["python", "demo.py"]
