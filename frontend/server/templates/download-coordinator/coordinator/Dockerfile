FROM python:3.12-slim

WORKDIR /app

# Install deps first so layer caching survives app.py edits.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

# 8000 = HTTP (health/metrics/control via the LB); 50051 = gRPC (node-to-node, intra-network).
# The shared gRPC package (/app/grpc_pkg) and chunk store (/app/dc_common) are mounted
# read-only at runtime by docker-compose — single source of truth, no vendoring.
EXPOSE 8000 50051

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
