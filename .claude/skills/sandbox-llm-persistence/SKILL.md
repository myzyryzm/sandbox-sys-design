---
name: sandbox-llm-persistence
description: >-
  Implement or update a PERSISTENCE READER group in a "Distributed Systems Sandbox"
  system (systems/<id>/) — a consumer group of containers that XREADGROUP an LLM
  worker's runs:started announcements, accumulate each claimed run's typed token
  stream (tokens:<run_id>, {type: token|done|error, text}), and persist the finished
  output to a database table/field (or per a freeform spec). Use whenever the task is
  to author or change a reader's claim/accumulate/persist loop in
  systems/<id>/<reader>/app.py from its persistence.json entry. The worker side of the
  contract (typed entries, announcements) is [[sandbox-llm-worker]]; the type is
  registered via the [[sandbox-custom-service-type]] mechanism.
---

# LLM Persistence Readers

You are in the "Distributed Systems Sandbox" web app. A **persistence reader group**
is a custom service type (`persistence_reader`): N member containers under one service
id that together drain an LLM worker's **run announcements** and write each finished
generation to a database. Without readers a generation is ephemeral — the worker's
token stream expires ~600s after it finishes and nothing lands in a DB.

**Never run `./start.sh`** — it tears down the dev server you are attached to. Control
the stack only with `docker compose -f systems/<id>/docker-compose.yml ...`.

## The stream contract (what the worker writes — see [[sandbox-llm-worker]])

- **`runs:started`** (on the worker's linked redis, env `REDIS_HOST`): every ACCEPTED
  `AddPrompt` XADDs one entry `{run_id: <user_message_id>}` (maxlen ~1024). This is
  the group's work queue.
- **`tokens:<run_id>`** (same redis): the run's typed token stream. Every entry is
  `{type, text}`:
  - `{type:"token", text:<one a-z char>}` — one generated token
  - `{type:"done", text:<marker>}` — generation complete. The text is a configurable
    marker string (etcd-live, default "DONE") — **informational only: key off
    `type=="done"`, NEVER compare the text.**
  - `{type:"error", text:<reason>}` — the generation aborted; no more entries follow.
  - Streams are capped (maxlen 256, approximate) and get a **600s expiry once the
    done/error entry is written** — an old announcement may point at an already
    expired (missing) stream. Tolerate it (persist partial/failed), never crash.

## What the backend already scaffolded (NOT yours to edit)

The reader service was created from the worker's Persistence tab: plain FastAPI
template + compose entry + nginx route + Prometheus scrape job + manifest node +
`persistence.json` entry. Its compose env is pre-wired — **do not edit
docker-compose.yml, nginx.conf or prometheus.yml**:

| env | meaning |
| --- | --- |
| `SERVICE_ID` | this member's XREADGROUP **consumer name** (unique per member: base `<r>`, instances `<r>-2..N`) |
| `REDIS_HOST` | the worker's linked stream redis (port 6379) |
| `ANNOUNCE_STREAM` | `runs:started` |
| `READER_GROUP` | the group name (= the service name) |
| `DB_NODE` | the target database node id (absent on freeform readers) |

The registry entry in `systems/<id>/persistence.json` carries the spec:
`{ service, worker, stream, announce, group, db, table, field, freeform, description,
implemented, conversationId, members, history }`. The app owns the spec; **you own
`implemented`** — flip it to `true` when the loop is authored, rebuilt and verified.

## Files YOU edit

- `systems/<id>/<reader>/app.py` — add the background loop (daemon thread started at
  import or via lifespan). **Keep the template's explicit metrics middleware** and the
  `/health` + `/metrics` routes.
- `systems/<id>/<reader>/requirements.txt` — add `redis==5.2.1` and the DB client
  (`psycopg[binary]==3.2.3` for postgres, `pymongo` for mongo).
- `systems/<id>/persistence.json` — set `implemented: true` on this entry (directly or
  via `POST /api/custom/persistence-reader/update {system, node, implemented: true}`).

## The canonical reading algorithm

One loop per container; every member runs the same code — the group divides
announcements, **one reader per run**:

```python
GROUP = os.environ["READER_GROUP"]; CONSUMER = os.environ["SERVICE_ID"]
ANNOUNCE = os.environ.get("ANNOUNCE_STREAM", "runs:started")

def _ensure_group(stream):
    try:
        r.xgroup_create(stream, GROUP, id="0", mkstream=True)
    except redis.ResponseError as e:          # BUSYGROUP = already exists
        if "BUSYGROUP" not in str(e): raise

# claim loop
_ensure_group(ANNOUNCE)
while True:
    resp = r.xreadgroup(GROUP, CONSUMER, {ANNOUNCE: ">"}, count=1, block=5000)
    if not resp: continue
    for entry_id, fields in resp[0][1]:
        run_id = fields[b"run_id"].decode()
        read_full_message(run_id)             # accumulate + persist (below)
        r.xack(ANNOUNCE, GROUP, entry_id)     # ack ONLY after persisting (crash-safe)
```

`read_full_message(run_id)` — XREADGROUP on `tokens:{run_id}` with the SAME group
(`_ensure_group` it first; mkstream recreates an expired key as empty):

- append `text` on `type=="token"` entries, XACK each as consumed;
- `type=="done"` → persist the joined buffer with **status complete**, return;
- `type=="error"` → persist what accumulated with **status failed**, return;
- ~6 consecutive empty reads (block=5000 → ~30s without progress) → persist with
  **status partial**, return (covers dropped runs AND already-expired streams);
- after finishing, `r.expire(f"tokens:{run_id}", 600)` — mkstream may have recreated
  an expired key; without this the empty stream leaks forever.

**Persisting.** For a structured target (db/table/field): connect to `DB_NODE` with
the repo DSN convention (`postgresql://sandbox:sandbox@<db>:5432/<db_with_underscores>`
/ `mongodb://<db>:27017/<db_with_underscores>`) and write the accumulated text into
`<table>.<field>` — filling the row's OTHER required columns with judgment. For the
llm-app chat flow, `run_id == user_message.id == the assistant message id`: derive
`chat_id` from the `user_message` row, insert a `message` row with `role='assistant'`,
a fresh id, and the accumulated text as content; represent partial/failed status
sensibly (e.g. a suffix marker or skip failed runs — follow the description). For a
freeform reader, the `freeform` spec in persistence.json is the specification —
implement it literally. Make the write **idempotent per run_id** (upsert / ON
CONFLICT) — a reader crash after persist but before XACK redelivers the run.

## Metrics + state contract (the diagram's cards read these)

- `persistence_runs_total{status="complete"|"partial"|"failed"}` — Counter, one inc
  per persisted run.
- `persistence_active_runs` — Gauge, runs currently being accumulated.
- `GET /reader/state` — control-plane introspection (hidden from endpoint lists):
  return at least `{ "group": <READER_GROUP>, "consumer": <SERVICE_ID> }` plus live
  counters (active/persisted); the Readers tab and the aggregate state route
  (`GET /api/custom/persistence-reader/state?system=<id>`) poll it through the lb.
- The diagram shows the group's claim loop as a clickable **`PULL <fn>`** row (fn
  from the manifest node's `persistence` block, default `readLlmStream`); clicking
  it traces reader → stream redis (labeled with the announce stream) and reader →
  target db. Rendering is automatic — nothing for a session to wire.

## Scaling

Members are managed by the app (Readers tab → the shared replica reconciler) — never
add instance containers yourself. All members share `READER_GROUP`; each claims with
its own `SERVICE_ID`, so scale requires no code change. A rebuild of the base
(`up -d --build <reader>`) rebuilds every instance with it.

## Rebuild + Verify

```bash
S=<id>; R=<reader>; W=<worker>
docker compose -f systems/$S/docker-compose.yml up -d --build $R
# 1. The group exists on the announce stream, with one consumer per member:
docker compose -f systems/$S/docker-compose.yml exec -T $W-stream redis-cli XINFO GROUPS runs:started
# 2. Drive one run end to end (llm-app: POST /chat-service/messages through the lb,
#    or gRPC AddPrompt per the sandbox-llm-worker skill), wait for it to finish, then:
docker compose -f systems/$S/docker-compose.yml logs --tail 30 $R
#    - the run's row landed in the target db (psql/mongosh through compose exec)
#    - XPENDING runs:started <group> is 0 (announcement acked after persist)
curl -s localhost:8080/$R/reader/state   # group + counters
# 3. Set implemented:true on this entry in systems/$S/persistence.json.
```
