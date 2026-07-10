FROM python:3.12-slim

WORKDIR /app

# Install deps first so layer caching survives app.py edits.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# gRPC client stubs — coordinator is a CLIENT of the Worker (UpdateConsumer) and
# Consumer (UpdateWorkers) contracts. Vendored copies of the shared grpc/ stubs
# with the protobuf-5.28 runtime guard stripped so they load under the protobuf
# 3.20.3 the etcd3 client pins (see the NOTE header in each *_pb2.py).
COPY Worker_pb2.py Worker_pb2_grpc.py ./
COPY Consumer_pb2.py Consumer_pb2_grpc.py ./

COPY app.py .

EXPOSE 8000

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
