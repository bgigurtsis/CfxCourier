FROM mcr.microsoft.com/playwright/python:v1.54.0-jammy

ENV PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    XDG_CACHE_HOME=/opt/.cache \
    HOME=/tmp \
    PLAYWRIGHT_BROWSERS_PATH=/opt/pw-browsers

# Install Python dependencies
RUN pip install --upgrade pip && \
    pip install \
      boto3 \
      requests \
      playwright==1.54.0 \
      camoufox[geoip]==0.4.11 \
      browserforge>=1.2.3 \
      apify-fingerprint-datapoints \
      fastapi \
      uvicorn[standard] \
      pytest \
      pytest-asyncio \
      moto[s3,sqs] \
      httpx

# Prebake data (so runtime is read-only friendly)
RUN python -m browserforge update && \
    python -m camoufox fetch

WORKDIR /var/task

# Copy the application code
COPY app.py /var/task/app.py

# Create a simple entrypoint script inline
RUN echo '#!/bin/bash\nexec python app.py' > /var/task/entrypoint.sh && \
    chmod +x /var/task/entrypoint.sh

EXPOSE 8080

ENTRYPOINT ["python", "app.py"]