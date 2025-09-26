FROM python:3.12-slim

# --- Fast, deterministic builds & sane defaults
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    DEBIAN_FRONTEND=noninteractive

ARG DEBIAN_FRONTEND=noninteractive

# --- OS deps (as root)
RUN apt-get update && apt-get -y upgrade \
  && apt-get install -y --no-install-recommends \
     git wget g++ ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# --- Non-root user
RUN groupadd --gid 1001 app \
  && useradd --uid 1001 --gid 1001 --create-home --shell /bin/bash app

# --- Install Miniconda into /home/app/miniconda3 (owned by 'app')
RUN wget -q https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O /tmp/miniconda.sh \
  && mkdir -p /home/app/.conda \
  && bash /tmp/miniconda.sh -b -p /home/app/miniconda3 \
  && rm -f /tmp/miniconda.sh \
  && chown -R app:app /home/app

# --- Switch to non-root for everything else
USER app
WORKDIR /home/app

# PATH order: your miniconda first, then user-local bin for pip --user installs
ENV PATH="/home/app/miniconda3/bin:/home/app/.local/bin:${PATH}"

# --- Dependency layer (better cache): copy only requirements first
COPY --chown=app:app requirements.txt ./requirements.txt

# Install conda-pack and other app dependencies via conda 
RUN conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/main \
    && conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/r \
    && conda install -y -c conda-forge conda-pack \
    && conda install -y -c conda-forge --file requirements.txt

# --- Add your application code
COPY --chown=app:app ["kafka_connector_consumer.py", "kafka_connector_producer.py", "./"]