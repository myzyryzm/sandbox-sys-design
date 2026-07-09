FROM python:3.12-slim

WORKDIR /app

# Install deps first so layer caching survives app.py edits.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Worker gRPC stubs — generated files copied from systems/llm-app/grpc/ (the
# compose build context is this folder only, so they live here as copies).
COPY Worker_pb2.py Worker_pb2_grpc.py ./
COPY app.py .

EXPOSE 8000

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
