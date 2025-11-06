FROM prefecthq/prefect:latest-python3.10
# Use a multi-stage build to keep the final image small
# Build stage to install dependencies and compile psycopg2
FROM python:3.10-slim AS builder

WORKDIR /app

# 1. Install necessary dependencies for compilation
# 'build-essential' provides tools like gcc, 'libpq-dev' provides pg_config
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

# This step will now succeed because the dependencies are present
RUN pip install --no-cache-dir -r requirements.txt

# Final stage - keep it minimal
FROM python:3.10-slim

WORKDIR /app

# 2. Copy compiled packages from the builder stage
# This only works if you're using system packages and not relying only on pip compilation
# However, if you stick to Solution 1 (psycopg2-binary), this complex structure isn't needed.

# --- If you use Solution 1 (psycopg2-binary), your Dockerfile is simpler: ---
FROM python:3.10-slim

WORKDIR /app
COPY requirements.txt .

# Install ONLY the run-time dependency for libpq
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    libpq5 && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir -r requirements.txt

COPY . .
CMD ["python", "main.py"]