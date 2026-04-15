FROM python:3.12-slim

WORKDIR /app

# Install core dependencies (always succeeds on any architecture)
COPY app/requirements.txt .
RUN pip install --no-cache-dir --prefer-binary -r requirements.txt

# yahooquery and yfinance both pull in packages that may require compilation
# when no pre-built wheel exists for the host architecture.  Install them as
# best-effort steps so the image always builds — each provider disables itself
# gracefully via ImportError if the package is absent.
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc g++ make libffi-dev libssl-dev libxml2-dev libxslt1-dev \
    && pip install --no-cache-dir --prefer-binary "yahooquery==2.3.7" "yfinance>=0.2.43" \
    && apt-get purge -y --auto-remove gcc g++ make libffi-dev libssl-dev libxml2-dev libxslt1-dev \
    && rm -rf /var/lib/apt/lists/* \
    || (apt-get purge -y --auto-remove gcc g++ make libffi-dev libssl-dev libxml2-dev libxslt1-dev 2>/dev/null; \
        rm -rf /var/lib/apt/lists/*; \
        echo "WARNING: yahooquery/yfinance install failed — providers will be disabled")

# Copy application code
COPY app/ .

# Copy universe / other config
COPY config/ /app/config/

RUN mkdir -p /data /app/providers

ENV PYTHONPATH=/app

EXPOSE 8501
