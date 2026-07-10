"""Shared Consumer servicer — the single shared implementation for the contract.

A thin adapter: protobuf <-> plain-Python translation only. The actual worker
registry (the consumer's local set of worker host:port endpoints) is injected by
the consumer service's app.py, so this file ships in the shared grpc bank like
any modal-authored servicer — no business logic here.
"""

import grpc

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

    async def OnChatEvict(self, request, context):
        # Drop this chat's sticky chat->worker affinity from the consumer's local
        # chat_id->worker map, so the chat's next prompt is routed to a freshly
        # chosen worker. That map (chat_worker_dict) lives on the running app
        # module and is mutated by the Kafka routing loop; a deferred import
        # avoids a circular import at load time (app.py imports this servicer),
        # and pop(..., None) is a GIL-atomic, lock-free delete consistent with
        # how that loop already touches the map.
        import app

        app.chat_worker_dict.pop(request.chat_id, None)
        return pb2.OnChatEvictReply(ok=True)
