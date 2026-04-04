FROM python:3.12-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Copy dependency files first (better layer caching — deps change less often than code)
COPY pyproject.toml uv.lock ./

# Install dependencies (frozen = use exact versions from uv.lock)
RUN uv sync --frozen --no-dev --no-install-project

# Copy application code
COPY pcp_to_cc/ pcp_to_cc/

# Cloud Run sets PORT automatically; default to 8080
ENV PORT=8080
# Make config.py importable as 'config' (main.py uses: import config)
ENV PYTHONPATH=/app/pcp_to_cc

CMD uv run gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 pcp_to_cc.main:app
