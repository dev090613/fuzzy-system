FROM python:3.13.11-slim

WORKDIR /app
# Copy uv binary from official uv image (multi-stage build pattern)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin

# Add virtual environment to PATH so we can use installed packages
ENV PATH="/app/.venv/bin:$PATH"

COPY pyproject.toml uv.lock .python-version .
RUN uv sync --locked

COPY pipeline.py .

ENTRYPOINT ["python3", "pipeline.py"]