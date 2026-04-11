FROM python:3.12-slim

WORKDIR /app

# gcc + libffi-dev are needed by yfinance's curl_cffi dependency when a
# pre-built wheel is unavailable for the target platform (e.g. ARM).
# libssl-dev is needed by curl_cffi's cffi compilation step.
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libffi-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Install deps first (cache-friendly layer)
COPY app/requirements.txt .
RUN pip install --no-cache-dir --prefer-binary -r requirements.txt

# Copy application code
COPY app/ .

# Copy universe / other config
COPY config/ /app/config/

RUN mkdir -p /data /app/providers

ENV PYTHONPATH=/app

EXPOSE 8501
