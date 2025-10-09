FROM python:3.12-slim

# --- Fast, deterministic builds & sane defaults
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    DEBIAN_FRONTEND=noninteractive

# --- Install only necessary OS dependencies (as root)
RUN apt-get update && apt-get -y upgrade \
    && apt-get install -y --no-install-recommends \
       ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# --- Create non-root user
RUN groupadd --gid 1001 app \
    && useradd --uid 1001 --gid 1001 --create-home --shell /bin/bash app

# --- Switch to non-root user
USER app
WORKDIR /home/app

# --- Install Python dependencies using pip (instead of Conda)
COPY --chown=app:app requirements.txt ./
RUN pip install --user --no-cache-dir -r requirements.txt

# --- Copy application code
COPY --chown=app:app . ./

ENV PATH="/home/app/.local/bin:${PATH}"
