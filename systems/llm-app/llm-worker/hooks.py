"""on_cache_evict hook — authored via the worker's Edit tab (sandbox-llm-worker skill).

Called by the reaper thread once per evicted prefix-cache entry. `entry` is:
  { chat, seq_id, user_message_id, prompt_tokens, generated_tokens, text,
    cached_at, evicted_at }
The caller guards with try/except, but keep this quick and exception-safe anyway
(it runs on the reaper's 2s cadence). This file is bind-mounted: after editing,
`docker compose restart <worker>` applies it — no rebuild.

Default: log the eviction.
"""


def on_cache_evict(entry):
    print(
        f"[llm-worker] cache evict: chat={entry.get('chat')} seq={entry.get('seq_id')}",
        flush=True,
    )
