# Dockerfile
FROM mcr.microsoft.com/playwright/python:v1.54.0-jammy

# Put baked caches under /opt so non-root runtime can read them
ENV PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    XDG_CACHE_HOME=/opt/.cache \
    HOME=/tmp \
    PLAYWRIGHT_BROWSERS_PATH=/opt/pw-browsers

# System deps are already in base image. Install Python deps:
RUN pip install --upgrade pip && \
    pip install \
      awslambdaric \
      boto3 \
      playwright==1.54.0 \
      camoufox[geoip]==0.4.11 \
      browserforge>=1.2.3 \
      apify-fingerprint-datapoints

# PREBAKE model/data files so nothing tries to write at runtime
RUN python -m browserforge update && \
    python -m camoufox fetch

WORKDIR /var/task
COPY app.py /var/task/app.py

ENTRYPOINT [ "python", "-m", "awslambdaric" ]
CMD [ "app.handler" ]
