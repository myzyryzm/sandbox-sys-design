FROM python:3.12-slim

WORKDIR /app

# Install deps first so layer caching survives app.py edits.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# gRPC files — generated stubs + the shared Consumer servicer, copied from
# systems/llm-app/grpc/ (the compose build context is this folder only, so they
# live here as copies). Worker_* : client of the Worker contract on llm-worker.
# Consumer_* : this service SERVES the Consumer contract (shared servicer).
COPY Worker_pb2.py Worker_pb2_grpc.py ./
COPY Consumer_pb2.py Consumer_pb2_grpc.py Consumer_servicer.py ./
COPY app.py .

# 8000 HTTP (health/metrics); 50051 the intra-network gRPC server (Consumer).
EXPOSE 8000 50051

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
