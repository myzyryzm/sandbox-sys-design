FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

# 8000 = HTTP (health/metrics via the LB); 50051 = gRPC (ChunkTransfer server, intra-network).
# Shared gRPC package + chunk store are mounted read-only at runtime by docker-compose.
EXPOSE 8000 50051

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
