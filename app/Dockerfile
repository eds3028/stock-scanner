FROM python:3.12-slim

WORKDIR /app

COPY app/requirements.txt .

# Build tools are installed first so that any package lacking a pre-built
# binary wheel for the target architecture (e.g. ARM) can compile from source.
# yahooquery and yfinance are best-effort: each provider disables itself
# gracefully via ImportError if the package is absent.
# Everything is done in one layer so build tools are purged in the same step.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        gcc g++ make libffi-dev libssl-dev libxml2-dev libxslt1-dev \
    && pip install --no-cache-dir --prefer-binary -r requirements.txt \
    && { pip install --no-cache-dir --prefer-binary "yahooquery==2.3.7" "yfinance>=0.2.43" \
         || echo "WARNING: yahooquery/yfinance install failed — providers will be disabled"; } \
    && apt-get purge -y --auto-remove gcc g++ make libffi-dev libssl-dev libxml2-dev libxslt1-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy application code
COPY app/ .

# Copy universe / other config
COPY config/ /app/config/

RUN mkdir -p /data /app/providers

ENV PYTHONPATH=/app

EXPOSE 8501
