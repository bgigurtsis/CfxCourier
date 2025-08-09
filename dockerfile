# Dockerfile
# Pin the Playwright image & Python package versions to match
# See: https://playwright.dev/python/docs/docker (pin versions)
FROM mcr.microsoft.com/playwright/python:v1.54.0-jammy

ENV PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    # Keep cache location stable and readable in Lambda (baked into image)
    XDG_CACHE_HOME=/root/.cache \
    HOME=/root

# System deps are already in the base image. Install Python deps:
# - awslambdaric: Lambda runtime interface client
# - boto3: S3 access
# - playwright (pin to image version) and camoufox (latest as of Jan 2025)
RUN pip install --upgrade pip && \
    pip install \
      awslambdaric \
      boto3 \
      playwright==1.54.0 \
      camoufox[geoip]==0.4.11


RUN python -m camoufox fetch

WORKDIR /var/task
COPY app.py /var/task/app.py

# Lambda entrypoint using the runtime interface client
ENTRYPOINT [ "python", "-m", "awslambdaric" ]
CMD [ "app.handler" ]
