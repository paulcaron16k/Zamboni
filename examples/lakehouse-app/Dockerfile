# syntax=docker/dockerfile:1
#
# The reference Lakehouse App: one image, two tiers (docs/maintenance-runtime.md §9.2).
#
#   web tier          python -m lakehouse_app.web            (the default command)
#   maintenance tier  python -m lakehouse_app.maintenance
#
# Built from the repository root, so Zamboni is installed from this tree and
# its lock -- the PyIceberg maintenance fork included -- rather than whatever
# PyPI resolves:
#
#   docker build -f examples/lakehouse-app/Dockerfile -t lakehouse-app .
#
# Dockerfile.dockerignore beside this file keeps .venv, .git and the caches out
# of the context.

FROM python:3.13-slim AS build

# git, because uv.lock takes PyIceberg from the maintenance fork's repository
# ([tool.uv.sources]), built from source here; a compiler for its optional
# extension. Neither reaches the runtime stage.
RUN apt-get update \
 && apt-get install -y --no-install-recommends git build-essential \
 && rm -rf /var/lib/apt/lists/*

# uv at the version CI uses (UV_VERSION in .github/workflows/ci.yml).
COPY --from=ghcr.io/astral-sh/uv:0.11.26 /uv /usr/local/bin/uv

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv

WORKDIR /opt/zamboni

# Zamboni from uv.lock: --frozen refuses a lock that disagrees with
# pyproject.toml, as CI does. `s3` for a catalog that vends S3 credentials;
# `sql` only so the app's tests can run against a local SQL catalog.
COPY . .
RUN uv sync --frozen --no-dev --no-editable --extra s3 --extra sql

# The app, and psycopg at the version uv.lock pins for this repository's tests.
RUN uv pip install --python /opt/venv/bin/python --no-deps ./examples/lakehouse-app \
 && uv pip install --python /opt/venv/bin/python "psycopg[binary]==3.3.6"


FROM python:3.13-slim

# The same base, so the venv's interpreter path resolves unchanged.
COPY --from=build /opt/venv /opt/venv
ENV PATH=/opt/venv/bin:$PATH \
    PYTHONUNBUFFERED=1

# Not root. The maintenance tier writes only to /run/lakehouse (its state
# file) and /tmp (workers' scratch, DuckDB's spill), both volumes in the
# Kubernetes manifests, so the root filesystem can be read-only.
RUN useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin lakehouse \
 && mkdir -p /run/lakehouse && chown lakehouse /run/lakehouse
USER 10001
WORKDIR /tmp

EXPOSE 8080
CMD ["python", "-m", "lakehouse_app.web"]
