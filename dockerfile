# Dockerfile
FROM mcr.microsoft.com/playwright/python:v1.54.0-jammy

# Put baked caches under /opt so non-root runtime can read them
ENV PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    XDG_CACHE_HOME=/opt/.cache \
    HOME=/tmp \
    PLAYWRIGHT_BROWSERS_PATH=/opt/pw-browsers

# Create a writable directory for browserforge/camoufox data and set permissions
# This is the key change to fix the read-only filesystem error
RUN mkdir -p /opt/cfx-cache && \
    chown -R 165427:165427 /opt/cfx-cache

# Tell browserforge and camoufox to use this new directory
ENV BROWSERFORGE_HOME=/opt/cfx-cache \
    CAMOUFOX_HOME=/opt/cfx-cache

# Install Python deps
RUN pip install --upgrade pip && \
    pip install \
      awslambdaric \
      boto3 \
      playwright==1.54.0 \
      camoufox[geoip]==0.4.11 \
      browserforge>=1.2.3 \
      apify-fingerprint-datapoints

# PREBAKE model/data files into the writable directory
# Now these commands should succeed in the Lambda build environment
RUN python -m browserforge update && \
    python -m camoufox fetch

WORKDIR /var/task
COPY app.py /var/task/app.py

ENTRYPOINT [ "python", "-m", "awslambdaric" ]
CMD [ "app.handler" ]