FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py model.py hooks.py ./

# 8000 = HTTP (health/metrics/state via the LB); 50051 = gRPC (Worker server, intra-network).
# The shared gRPC package, live config (worker.json) and hooks.py are bind-mounted by
# docker-compose at runtime — the copied hooks.py is only a fallback baseline.
EXPOSE 8000 50051

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
