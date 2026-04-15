FROM python:3.12-slim

WORKDIR /app

# Install core dependencies (always succeeds on any architecture)
COPY app/requirements.txt .
RUN pip install --no-cache-dir --prefer-binary -r requirements.txt

# yfinance pulls in curl_cffi which requires compilation when no pre-built
# wheel exists for the host architecture.  Install it as a best-effort step
# so the image always builds — yfinance_provider.py disables itself gracefully
# via ImportError if the package is absent.
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc g++ make libffi-dev libssl-dev \
    && pip install --no-cache-dir --prefer-binary "yfinance>=0.2.43" \
    && apt-get purge -y --auto-remove gcc g++ make libffi-dev libssl-dev \
    && rm -rf /var/lib/apt/lists/* \
    || (apt-get purge -y --auto-remove gcc g++ make libffi-dev libssl-dev 2>/dev/null; \
        rm -rf /var/lib/apt/lists/*; \
        echo "WARNING: yfinance install failed — provider will be disabled")

# Copy application code
COPY app/ .

# Copy universe / other config
COPY config/ /app/config/

RUN mkdir -p /data /app/providers

ENV PYTHONPATH=/app

EXPOSE 8501
