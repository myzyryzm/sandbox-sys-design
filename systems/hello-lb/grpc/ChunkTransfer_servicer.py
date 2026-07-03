"""Shared ChunkTransfer servicer — imported by EVERY node (coordinator + workers).

This is the single shared implementation the gRPC brief calls for: a node serves a
chunk it holds (bitmap bit set) to any peer that asks. There is no per-node bespoke
code here — a node differs only by which chunks its ChunkStore currently holds.
"""

import grpc

import ChunkTransfer_pb2 as pb2
import ChunkTransfer_pb2_grpc as pb2_grpc

# Stream the chunk body in frames this big, so a 64 MB chunk doesn't go in one giant message.
DATA_FRAME_SIZE = 1 << 20  # 1 MiB


class ChunkTransferServicer(pb2_grpc.ChunkTransferServicer):
    def __init__(self, store):
        self.store = store

    async def GetChunk(self, request, context):
        cid = request.chunk_id
        if not self.store.has(cid):
            await context.abort(grpc.StatusCode.NOT_FOUND, f"chunk {cid} not held here")
            return
        data = self.store.read_chunk(cid)
        # Header frame first (id + size + checksum), then the data frames.
        yield pb2.ChunkFrame(
            header=pb2.ChunkHeader(chunk_id=cid, size=len(data), checksum=self.store.checksum(cid))
        )
        for off in range(0, len(data), DATA_FRAME_SIZE):
            yield pb2.ChunkFrame(data=data[off:off + DATA_FRAME_SIZE])
