FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    DEBIAN_FRONTEND=noninteractive

# OS deps (as root)
RUN apt-get update && apt-get -y upgrade \
  && apt-get install -y --no-install-recommends \
    wget ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# Non-root user
RUN groupadd --gid 1001 app \
  && useradd --uid 1001 --gid 1001 --create-home --shell /bin/bash app

# Install Miniconda into /home/app/miniconda3 (owned by 'app')
# We select conda package manager as it proivdes nice features (e.g., packaging the virtual env)
RUN wget -q https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O /tmp/miniconda.sh \
  && mkdir -p /home/app/.conda \
  && bash /tmp/miniconda.sh -b -p /home/app/miniconda3 \
  && rm -f /tmp/miniconda.sh \
  && chown -R app:app /home/app

# Switch to non-root user
USER app
WORKDIR /home/app

ENV PATH="/home/app/miniconda3/bin:/home/app/.local/bin:${PATH}"

COPY --chown=app:app . ./

# Install python dependencies using conda
RUN conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/main \
    && conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/r \
    && conda install -y -c conda-forge conda-pack \
    && conda install -y -c conda-forge --file requirements.txt
