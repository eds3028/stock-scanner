FROM python:3.12-slim

WORKDIR /app

COPY app/requirements.txt .

# Primary path: install build tools so packages without a binary wheel for the
# target architecture can compile from source, then install all Python deps.
# Fallback path: if apt-get fails (network issue, dpkg lock, package conflict —
# exit code 100), skip the compiler and rely on binary wheels alone. On x86_64
# and ARM64 every package in requirements.txt ships a pre-built wheel, so the
# fallback succeeds on those platforms without any compilation.
# yahooquery and yfinance are best-effort in both paths.
RUN (apt-get update \
    && apt-get install -y --no-install-recommends \
        gcc g++ make libffi-dev libssl-dev libxml2-dev libxslt1-dev \
    && pip install --no-cache-dir --prefer-binary -r requirements.txt \
    && { pip install --no-cache-dir --prefer-binary "yahooquery==2.3.7" "yfinance>=0.2.43" \
         || echo "WARNING: yahooquery/yfinance install failed — providers will be disabled"; } \
    && apt-get purge -y --auto-remove gcc g++ make libffi-dev libssl-dev libxml2-dev libxslt1-dev \
    && rm -rf /var/lib/apt/lists/*) \
    || (echo "WARNING: build tools unavailable, falling back to binary wheels only" \
        && rm -rf /var/lib/apt/lists/* \
        && pip install --no-cache-dir --prefer-binary -r requirements.txt \
        && { pip install --no-cache-dir --prefer-binary "yahooquery==2.3.7" "yfinance>=0.2.43" \
             || echo "WARNING: yahooquery/yfinance install failed — providers will be disabled"; })

# Copy application code
COPY app/ .

# Copy universe / other config
COPY config/ /app/config/

RUN mkdir -p /data /app/providers

ENV PYTHONPATH=/app

EXPOSE 8501
