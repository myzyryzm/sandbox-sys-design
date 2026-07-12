"""Shared Worker servicer — the single shared implementation for the contract.

A thin adapter: protobuf <-> plain-Python translation only. The inference engine
(batching, prefix cache, redis streaming) is injected by the worker's app.py —
there is no business logic here, so the file can ship in the shared grpc bank
like any modal-authored servicer.
"""

import asyncio

import Worker_pb2 as pb2
import Worker_pb2_grpc as pb2_grpc


class WorkerServicer(pb2_grpc.WorkerServicer):
    def __init__(self, engine):
        self.engine = engine  # the worker's InferenceEngine (thread-safe)

    async def AddPrompt(self, request, context):
        # The admit path may query postgres for chat history — keep it off the
        # event loop.
        r = await asyncio.to_thread(
            self.engine.add_prompt,
            request.id,
            request.content,
            request.chat if request.HasField("chat") else None,
            request.message if request.HasField("message") else None,
        )
        return pb2.AddPromptReply(
            accepted=r["accepted"], seq_id=r["seq_id"], reason=r["reason"], cache_hit=r["cache_hit"]
        )

    async def GetStatus(self, request, context):
        s = self.engine.status()
        return pb2.StatusReply(
            has_space=s["has_space"],
            active_count=s["active_count"],
            cached_count=s["cached_count"],
            max_active=s["max_active"],
        )
