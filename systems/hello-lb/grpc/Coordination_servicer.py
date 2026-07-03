"""Shared Coordination servicer — served only by the coordinator node.

A thin gRPC adapter over the coordinator's Orchestrator object. ALL orchestration
state (the worker registry, the file manifest, the assignment log, source selection)
lives in the Orchestrator, not here — that separation is the seam a future hot standby
would mirror. This file stays a dumb translator between protobuf messages and plain
Python calls, so it can ship in the shared bank like any other servicer.
"""

import Coordination_pb2 as pb2
import Coordination_pb2_grpc as pb2_grpc


class CoordinationServicer(pb2_grpc.CoordinationServicer):
    def __init__(self, orch):
        self.orch = orch

    async def Register(self, request, context):
        m = self.orch.register(request.worker_id)
        return pb2.FileManifest(
            ready=m["ready"],
            chunk_count=m["chunk_count"],
            chunk_size=m["chunk_size"],
            file_size=m["file_size"],
            full_hash=m["full_hash"],
            chunk_checksums=m["chunk_checksums"],
        )

    async def RequestAssignment(self, request, context):
        a = self.orch.request_assignment(request.worker_id, request.bitmap)
        return pb2.Assignment(
            kind=pb2.Assignment.Kind.Value(a["kind"]),
            chunk_id=a.get("chunk_id", 0),
            source_addr=a.get("source_addr", ""),
        )

    async def Heartbeat(self, request, context):
        self.orch.heartbeat(request.worker_id, request.bitmap, request.status)
        return pb2.Ack(ok=True)

    async def ReportComplete(self, request, context):
        self.orch.report_complete(request.worker_id, request.full_hash_ok)
        return pb2.Ack(ok=True)
