FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# Create non-root user 'app'
RUN groupadd --gid 1001 app \
    && useradd --uid 1001 --gid 1001 --create-home --shell /bin/bash app

# Switch to the non-root user for all subsequent steps    
USER app
WORKDIR /home/app

# Ensure the user's local bin is on PATH (pip --user installs here)
ENV PATH="/home/app/.local/bin:$PATH"

# Add the application code and dependencies (owned by 'app')
COPY --chown=app:app ["kafka_connector.py", "requirements.txt", "./"]

RUN pip install --no-cache-dir --upgrade --user pip \
    && pip install --no-cache-dir --user -r requirements.txt

CMD [ "python", "kafka_connector.py" ]