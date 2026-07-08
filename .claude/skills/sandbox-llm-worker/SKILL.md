---
name: sandbox-llm-worker
description: >-
  Work on an LLM Worker custom service in a "Distributed Systems Sandbox" system
  (systems/<id>/) — a simulated-LLM-inference container (gRPC AddPrompt/GetStatus,
  continuous batching, TTL prefix cache, tokens streamed to its linked redis). Use
  whenever the task is to implement or update a worker's on_cache_evict hook (authored
  into systems/<id>/<worker>/hooks.py from a description), understand its live tunables
  (worker.json: ttl_seconds / chat_db / max_active), or verify its token stream. The
  type itself is registered via the [[sandbox-custom-service-type]] mechanism; its
  Worker contract lives in the grpc bank like [[sandbox-grpc-contract]] ones.
---

# LLM Worker

You are in the "Distributed Systems Sandbox" web app. An **LLM Worker** is a custom
service type: one container that simulates LLM inference (a tiny 3-layer numpy
transformer, vocabulary a-z → tokens 0-25) and, created together with it, a linked
redis **`<worker>-stream`** it streams output tokens into.

**Never run `./start.sh`** — it tears down the dev server you are attached to. Control
the stack only with `docker compose -f systems/<id>/docker-compose.yml ...`.

## Architecture (what runs inside the container)

- **Worker gRPC server** (`:50051`, contract `Worker` in `systems/<id>/grpc/`):
  - `AddPrompt(id, content, chat?, message?)` — admits a user message into the batch.
    Rejects with `worker full` at capacity (`max_active`) or `empty prompt` when the
    tokenized content is empty. Content is lowercased; only a-z become tokens.
  - `GetStatus()` — `has_space` + active/cached counts.
- **Worker loop** (thread): every ~0.25s steps all active sequences — prefill first
  (history + prompt through the model, filling per-layer KV caches), then one decode
  step per tick. Each decoded token is `XADD`ed to the linked redis, key
  **`tokens:<user_message_id>`** (field `t`); when a sequence hits its random target
  length (1-100), **END token `26`** is XADDed and the key gets a 600s expiry.
- **Prefix cache**: a finished sequence's KV caches are kept, keyed by **chat id** —
  a follow-up AddPrompt in the same chat pops the entry and prefills only its new
  tokens (`cache_hit: true`). On a cache miss with a chat id, prior messages come from
  the configured chat-history postgres (`SELECT content FROM message WHERE chat_id=…
  ORDER BY created_at`, excluding the prompt's own `message` row).
- **Reaper loop** (thread): every 2s evicts cache entries older than `ttl_seconds`
  and calls `on_cache_evict(entry)` once per eviction. `ttl_seconds: 0` disables
  caching (nothing enters; leftovers are flushed silently — it is an eviction-policy
  hook, not a completion hook).

## File map (per worker `<w>` in `systems/<id>/`)

| File | What | Yours to edit? |
| --- | --- | --- |
| `<w>/app.py`, `<w>/model.py` | engine, loops, gRPC/HTTP wiring, transformer | **NO** — off-limits |
| `<w>/hooks.py` | `on_cache_evict(entry)` — bind-mounted at `/app/hooks.py` | **YES** (the hook task) |
| `<w>/worker.json` | live tunables, bind-mounted + mtime-polled (no rebuild) | via the config route / Edit tab |
| `<w>/hook.json` | hook registry: `{ description, implemented, conversationId, history }` | you set `implemented` |
| `grpc/Worker*` | the shared contract + servicer | NO (bank-owned) |

`worker.json` / `hooks.py` are **single-file bind mounts — always edit them in place**
(tmp+rename swaps the inode and detaches the mount on macOS Docker Desktop).

## Scaling: worker replicas (no load balancer)

A worker can run as **N instances under one service id**, with **no load balancer** — the
consumer that calls it does its own request forwarding across the group (see the
[[sandbox-grpc-attach]] entry+`instanceOf` expansion). Set the count in the worker's
Edit ▸ **Replicas** section (1 = a single worker); it POSTs
`/api/custom/llm-worker/scale {system, node, instances}` — mechanical, no launched session.

- The base `<w>` node **stays a real serving worker**; instances `<w>-2..N` are added as
  `type:"service"`, `service_type:"llm_worker"`, `instanceOf:"<w>"` nodes, and the base
  gains `replicas:{ instances:[…] }`. The diagram renders the group as a compact `<w>`
  header card with all workers (`<w>-1` = the base container, `<w>-2..N`) stacked below it
  in one dotted box.
- All instances **`build: ./<w>`** (one image) and bind-mount the **base's** `worker.json`,
  `hooks.py`, `grpc/`, `manifest.json` — so **config, hook and code are shared** across the
  group. They also share the one **`<w>-stream`** redis (`REDIS_HOST=<w>-stream`; tokens are
  keyed by `user_message_id`, not by worker). Each instance has its own `SERVICE_ID=<w>-i`,
  Prometheus scrape job, and gRPC endpoint `<w>-i:50051`. Request traffic is **gRPC-only**
  (endpoints stay owned by the base); each instance also has a plain nginx `/<w>-i/` route,
  but that exists purely so the control plane can poll its `/llm/state` for the diagram.

When you work on a **replicated** worker:
- `worker.json` is live (mtime-poll, no restart) and updates the whole group in one write.
- `hooks.py` needs a **group restart** (all instances mount the base's copy):
  `docker compose -f systems/<id>/docker-compose.yml restart <w> <w>-2 … <w>-N`.
- An `app.py` / `requirements.txt` change rebuilds the group automatically
  (`resolveBuildTargets` returns the base + every instance for the per-service rebuild).
- The prefix cache is **per-container** (in-memory KV, keyed by chat), so a chat's follow-up
  only `cache_hit`s if the consumer routes it back to the same instance; otherwise it's a
  cache miss that prefills from the `chat_db` history (still correct, just no cache benefit).

## The hook contract

```python
def on_cache_evict(entry): ...
```

- Runs on the **reaper thread**, once per evicted chat. The caller wraps it in
  try/except, but keep it quick and exception-safe anyway (it shares the 2s cadence).
- `entry` dict: `{ chat, seq_id, user_message_id, prompt_tokens, generated_tokens,
  text, cached_at, evicted_at }` — `text` is the detokenized full conversation (a-z),
  `*_tokens` are int lists, timestamps are `time.time()` floats.
- Available in-container: stdlib, `redis` 5.2.1 (`redis.Redis(host=os.environ["REDIS_HOST"])`
  — the linked stream redis), `psycopg` 3.2.3 (postgres DSN convention:
  `postgresql://sandbox:sandbox@<db-node>:5432/<db_node_with_underscores>`), `numpy`,
  `prometheus_client` (new metrics land on the existing `/metrics` — no scrape change).
- Keep the module import-safe: a hooks.py that fails to import downgrades the worker
  to the built-in log-only behavior (app.py logs the import error at startup).

## Procedure (implement / update the hook)

1. Read the task's registry entry: `systems/<id>/<w>/hook.json` (`description` is what
   to build; `history` holds prior descriptions).
2. Author `systems/<id>/<w>/hooks.py` — keep the `on_cache_evict(entry)` name; module-level
   imports are fine. Do not touch `app.py` / `model.py` / `worker.json`.
3. Apply it — hooks.py is bind-mounted, so a **restart** suffices (no rebuild):
   ```
   docker compose -f systems/<id>/docker-compose.yml restart <w>
   ```
   If the worker is **scaled to replicas** (`<w>` has a `replicas.instances` list), every
   instance mounts the base's `hooks.py`, so restart the whole group instead:
   `… restart <w> <w>-2 … <w>-N`.
   (Only if you genuinely need a new pip package: add it to `<w>/requirements.txt` and
   `up -d --build <w>` instead — but prefer the preinstalled clients above.)
4. Verify (below), then set `"implemented": true` in `systems/<id>/<w>/hook.json`
   (in-place edit; preserve the other fields).

## Verify

```bash
S=<id>; W=<worker>
# 1. Drop the TTL low so an eviction happens fast (live config — no rebuild):
curl -s -X POST localhost:5173/api/custom/llm-worker/config -H 'Content-Type: application/json' \
  -d '{"system":"'$S'","node":"'$W'","ttl_seconds":3,"max_active":5,"chat_db":null}'
# 2. Drive one prompt WITH a chat id (a chat-less prompt is never cached):
docker compose -f systems/$S/docker-compose.yml exec -T $W python - <<'EOF'
import sys; sys.path.insert(0, "/app/grpc_pkg")
import grpc, Worker_pb2 as pb, Worker_pb2_grpc as pbg
s = pbg.WorkerStub(grpc.insecure_channel("localhost:50051"))
print(s.AddPrompt(pb.AddPromptRequest(id=901, content="hello world", chat=901)))
EOF
# 3. Wait for it to finish + expire (target_len is random 1-100 → up to ~30s + ttl),
#    then confirm the hook's side effect and the log line:
docker compose -f systems/$S/docker-compose.yml logs --tail 50 $W
# tokens on the stream (END=26 last):
docker compose -f systems/$S/docker-compose.yml exec -T $W-stream redis-cli XRANGE tokens:901 - +
# 4. Restore the TTL via the same config route, and set implemented:true in hook.json.
```

The worker's live view is `curl -s localhost:8080/<w>/llm/state` (active sequences,
cached chats with ages, effective config) — the Edit tab and diagram poll the same data
through `GET /api/custom/llm-worker/state?system=<id>`.
