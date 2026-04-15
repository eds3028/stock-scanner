FROM python:3.12-slim

WORKDIR /app

COPY app/requirements.txt .

# Upgrade pip/setuptools first so the bundled version in the base image
# cannot cause resolution or metadata-parsing failures.
RUN pip install --upgrade pip setuptools wheel

# All packages in requirements.txt ship pre-built binary wheels for x86_64
# (and ARM64), so no compiler or system build tools are needed.
# yahooquery and yfinance are best-effort: each provider disables itself
# gracefully via ImportError if the package cannot be installed.
RUN pip install --no-cache-dir --prefer-binary -r requirements.txt \
    && { pip install --no-cache-dir --prefer-binary "yahooquery==2.3.7" "yfinance>=0.2.43" \
         || echo "WARNING: yahooquery/yfinance install failed — providers will be disabled"; }

# Copy application code
COPY app/ .

# Copy universe / other config
COPY config/ /app/config/

RUN mkdir -p /data /app/providers

ENV PYTHONPATH=/app

EXPOSE 8501
