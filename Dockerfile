# syntax=docker/dockerfile:1

FROM python:3.10-slim AS base

# Set up working directory
WORKDIR /app

# Builder stage: install dependencies and create venv
FROM base AS builder

# System dependencies for pip packages (e.g. for numpy, faiss, bs4, PyPDF2, etc.)
RUN --mount=type=cache,target=/var/cache/apt \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        gcc \
        libglib2.0-0 \
        libsm6 \
        libxrender1 \
        libxext6 \
        git \
    && rm -rf /var/lib/apt/lists/*

# Create virtual environment
RUN python -m venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"

# Copy only requirements for dependency install
COPY --link requirements.txt ./

# Install dependencies with pip cache
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY --link . .

# Final stage: minimal image with non-root user
FROM base AS final

# Create non-root user
RUN useradd -m mailmule
USER mailmule

WORKDIR /app

# Copy venv and app code from builder
COPY --from=builder /app /app

ENV PATH="/app/.venv/bin:$PATH"

# Expose FastAPI port (if running server.py)
EXPOSE 8000

# Default command (can be overridden)
CMD ["uvicorn", "server_client_local_files.server:app", "--host", "0.0.0.0", "--port", "8000"]
