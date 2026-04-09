FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN ls -la
COPY . .

RUN mkdir -p /data

EXPOSE 8501

CMD ["sh", "-c", "python scheduler.py & streamlit run dashboard.py --server.port=8501 --server.address=0.0.0.0 --server.headless=true --browser.gatherUsageStats=false"]
