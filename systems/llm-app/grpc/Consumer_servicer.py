"""Shared Consumer servicer — the single shared implementation for the contract.

A thin adapter: protobuf <-> plain-Python translation only. The actual worker
registry (the consumer's local set of worker host:port endpoints) is injected by
the consumer service's app.py, so this file ships in the shared grpc bank like
any modal-authored servicer — no business logic here.
"""

import Consumer_pb2 as pb2
import Consumer_pb2_grpc as pb2_grpc


class ConsumerServicer(pb2_grpc.ConsumerServicer):
    def __init__(self, engine):
        self.engine = engine  # the consumer's worker registry (thread-safe)

    async def UpdateWorkers(self, request, context):
        # Replace the consumer's local workers list with the full set of
        # endpoints just handed to it (each a worker host:port to connect to).
        workers = [(w.host, w.port) for w in request.workers]
        self.engine.update_workers(workers)
        return pb2.UpdateWorkersReply(ok=True, count=len(workers))
