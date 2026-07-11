"""on_cache_evict hook — authored via the worker's Edit tab (sandbox-llm-worker skill).

Called by the reaper thread once per evicted prefix-cache entry. `entry` is:
  { chat, seq_id, user_message_id, prompt_tokens, generated_tokens, text,
    cached_at, evicted_at }
The caller guards with try/except, but keep this quick and exception-safe anyway
(it runs on the reaper's 2s cadence). This file is bind-mounted: after editing,
`docker compose restart <worker>` applies it — no rebuild.

Behavior: when a chat's prefix-cache entry is evicted, notify this worker's
PAIRED CONSUMER — the usr-msg-consumer feeding it, whose host:port was recorded
on the worker via the Worker `UpdateConsumer` gRPC (stored as engine.source).
We call that consumer's `Consumer.OnChatEvict(chat_id=<evicted chat>)` gRPC so it
can react to the eviction. If no consumer has been paired yet (source is None,
i.e. UpdateConsumer was never called), there is nothing to notify — we log and
return.
"""

import os
import sys

# The shared gRPC bank is bind-mounted here (app.py inserts it too, but keep this
# self-contained and idempotent so the module imports safely on its own).
sys.path.insert(0, "/app/grpc_pkg")

import grpc  # noqa: E402

import Consumer_pb2 as consumer_pb2  # noqa: E402
import Consumer_pb2_grpc as consumer_grpc  # noqa: E402

SERVICE_ID = os.environ.get("SERVICE_ID", "llm-worker")


def _paired_consumer():
    """host:port of the consumer feeding this worker, as recorded by the Worker
    UpdateConsumer gRPC (engine.source). None until a consumer is paired. Read
    lazily from the already-loaded app module (uvicorn runs `app:app`), so this
    module stays import-safe even if app is not loaded for some reason."""
    try:
        import app  # already in sys.modules under uvicorn — no re-exec

        return app.engine.source
    except Exception:
        return None


def on_cache_evict(entry):
    chat = entry.get("chat")
    target = _paired_consumer()  # "host:port" of the paired consumer, or None
    if not target:
        print(
            f"[{SERVICE_ID}] cache evict chat={chat}: no paired consumer set "
            f"(UpdateConsumer not called) — skipping OnChatEvict",
            flush=True,
        )
        return
    if chat is None:  # a chat-less entry never carries a chat_id to report
        return
    try:
        with grpc.insecure_channel(target) as channel:
            stub = consumer_grpc.ConsumerStub(channel)
            reply = stub.OnChatEvict(
                consumer_pb2.OnChatEvictRequest(chat_id=int(chat)), timeout=5
            )
        print(
            f"[{SERVICE_ID}] cache evict chat={chat}: OnChatEvict -> {target} ok={reply.ok}",
            flush=True,
        )
    except Exception as exc:
        # Best-effort notification: the reaper's try/except also guards us, but
        # swallow here too so one unreachable consumer never stalls eviction.
        print(
            f"[{SERVICE_ID}] cache evict chat={chat}: OnChatEvict -> {target} failed: {exc}",
            flush=True,
        )
