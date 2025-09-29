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
     git wget curl g++ ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# --- Install kubectl v1.29.2
# RUN set -eux; \
#     KVER="v1.29.2"; \
#     curl -fsSLo /usr/local/bin/kubectl "https://dl.k8s.io/release/${KVER}/bin/linux/amd64/kubectl"; \
#     curl -fsSLo /tmp/kubectl.sha256 "https://dl.k8s.io/release/${KVER}/bin/linux/amd64/kubectl.sha256"; \
#     echo "$(cat /tmp/kubectl.sha256)  /usr/local/bin/kubectl" | sha256sum -c -; \
#     chmod +x /usr/local/bin/kubectl; \
#     rm -f /tmp/kubectl.sha256; \
#     kubectl version --client

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

ENV PATH="/home/app/miniconda3/bin:/home/app/.local/bin:${PATH}"

COPY --chown=app:app . ./

RUN conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/main \
    && conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/r \
    && conda install -y -c conda-forge conda-pack \
    && conda install -y -c conda-forge --file requirements.txt