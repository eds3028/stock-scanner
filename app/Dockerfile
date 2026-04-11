FROM python:3.12-slim

WORKDIR /app

# Install deps first (cache-friendly layer)
COPY app/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app/ .

# Copy universe / other config
COPY config/ /app/config/

RUN mkdir -p /data /app/providers

ENV PYTHONPATH=/app

EXPOSE 8501
