FROM python:3.11-slim

# Copy the uv binary from its official image.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/usr/local

WORKDIR /app

# Install dependencies first so the layer is cached across source changes.
# --frozen installs exactly what's pinned in uv.lock (fails if it's stale).
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev

COPY . /app

# Run as an unprivileged user.
RUN useradd --create-home --uid 1000 appuser \
    && chown -R appuser:appuser /app
USER appuser

# Bake in the DuckDB extensions the load path needs, rather than installing them
# per run: that would make every job depend on DuckDB's extension repository
# being reachable and up. Installed after the USER switch so they land in this
# user's ~/.duckdb, which is where DuckDB looks by default — so nothing has to
# point at them, but it does assume the container runs as the user built here.
RUN python -c "import duckdb; con = duckdb.connect(); \
con.execute('INSTALL httpfs'); con.execute('INSTALL postgres')"

ENTRYPOINT ["python", "main.py"]
