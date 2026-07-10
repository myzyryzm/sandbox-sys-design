---
name: sandbox-end-to-end-process
description: >-
  RUN an end-to-end test PROCESS in a "Distributed Systems Sandbox" system (systems/<id>/). Use
  whenever the task is to execute a process defined in systems/<id>/endtoend.json — seed the
  out-of-scope preconditions its constraints imply, drive its client methods for a duration
  (stateless methods at their configured req/s; stateful clients as N concurrent session-loop
  instances kept alive; synthesizing legal arguments), then probe for its failure states (design
  defects) and report a PASS/FAIL verdict with a persisted run report. You do ALL the
  coordination; the backend only holds a run flag you poll for early-stop.
---

# Running an end-to-end process

You are operating inside the "Distributed Systems Sandbox" web app. Each system lives in
`systems/<id>/`; your session's system prompt names the specific `<id>` and inlines its current
`manifest.json`. The frontend runs under `npm run dev` and reads these files live, so **never run
`./start.sh`** — it tears down the dev server you're attached to.

An **end-to-end process** is a user-defined test declared in `systems/<id>/endtoend.json`. Your task
prompt gives you: the **process id**, the **run duration** (seconds), and the **control-plane base
URL** (`apiBase`, e.g. `http://localhost:5173`). Your job is to *run the whole thing* against the
already-running system and report. This skill **changes no components** — no manifest, no service
code, no rebuilds. You may **seed data directly into the datastores** (setup only) and you drive the
system through its load balancer.

> **Permissions (why a run shouldn't re-prompt).** The command families a run uses —
> `docker compose … exec` (seed/probe), `python3 …` (the orchestrator), `curl …` (Prometheus
> probes + the stop call), and the two writes (the scratchpad orchestrator script and the
> `systems/<id>/endtoend-runs/…` report) — are pre-authorized in the project's
> `.claude/settings.json` so grants carry across invocations instead of being re-asked every
> run. If you add a step that uses a **new command shape** (a different tool, a new write
> location), add a matching `permissions.allow` rule there so the next run stays quiet.

## What a process definition looks like

Read `systems/<id>/endtoend.json` and find the entry whose `id` matches your task prompt:

```json
{ "id": "…", "name": "Checkout under load",
  "client_list": [ { "client": "mobile-app", "method": "checkout", "requestsPerSecond": 2 },
                   { "client": "frontend", "method": "endToEnd", "instances": 3 } ],
  "websocket_list": [ { "client": "ws-client", "clientCount": 50, "messagesPerSecond": 2 } ],
  "constraint_list": [ "every seller has an Account", "checkout is only called on an Order that exists" ],
  "failure_list": [ "two OrderPayments for the same order both have status=success (double charge)" ] }
```

- **`client_list`** — the client methods to drive. Each `(client, method)` names a client function
  in `systems/<id>/scenarios.json`, implemented as `def <method>(…)` in
  `systems/<id>/clients/<module>.py` (`<module>` = client id with hyphens → underscores). The
  row's third field depends on the client node's `stateful` flag — **the manifest is the source
  of truth at run time**:
  - **stateless** client → `requestsPerSecond`: how many calls to make per second (fractional;
    `0.1` = one call every 10s).
  - **stateful** client → `instances`: how many concurrent instances of the function to keep
    alive for the whole run. A stateful function is a session-style loop that self-bounds under
    the 30s subprocess kill (~25s), so you respawn each instance when it exits (same identity,
    same store — see step 4).

  If a row doesn't match its client's current mode, derive the effective setting from the
  manifest: stateful → `instances = row.instances or 1` (ignore any rate field); stateless →
  `requestsPerSecond = row.requestsPerSecond or 1/row.intervalSeconds or 1` (legacy
  `intervalSeconds` rows read as their reciprocal). Never crash on a mismatched row.
- **`websocket_list`** (optional; absent on most processes) — websocket client **pools** to keep
  connected for the whole run. Each row names a websocket client node (`origin:
  "create-websockets"`) whose behavior is a host-run pool script
  `systems/<id>/ws-clients/<client>.mjs` (node ≥ 22, zero deps); `clientCount` is **how many pool
  clients to spawn** and `messagesPerSecond` how chatty each is. See the [[sandbox-websocket]]
  skill for the tier's anatomy and the script's contract.
- **`constraint_list`** — the **rules of the valid world** the system *assumes* but does not create
  itself. They are **rules YOU must uphold**, not things to passively watch. They tell you two
  things: (a) what **out-of-scope preconditions to seed** so the world is valid (e.g. "every seller
  has an Account" → create the Accounts the Orders/Items reference; keep referential integrity), and
  (b) which **inputs are legal** (e.g. "checkout is only called on an Order that exists" → only pass
  ids of Orders you actually seeded). The system-under-test often does **not** create this data
  (in a payment service, Accounts and Orders are created elsewhere) — so **you** seed it.
- **`failure_list`** — the system's own **design defects**: states that mean it is broken or poorly
  designed **if they ever occur** given a valid world (e.g. two successful payments for one order =
  a double charge; an orphaned payment with no order). You **actively probe** for these.

The mental model: **seed a valid world (constraints) → drive it with legal inputs → prove no defect
state (failure) ever appears.** Verdict is FAIL if any failure state is observed.

## Procedure

### 1. Resolve the definition

For each `client_list` row, look the function up in `scenarios.json` to get its `args` signature
and confirm it's implemented (a `def <method>(` in `clients/<module>.py` **and** non-empty `steps`).
A row that doesn't resolve / is still "pending" is **recorded as skipped** — never crash the run.
Read the constraints and failures now; they drive steps 2 and 5.

### 2. Seed the out-of-scope preconditions (from the constraints)

Work out what data the constraints + the methods' preconditions require that the system won't create
itself, then create it so the world is valid **before** you drive anything.

- **Find the datastores** from the inlined `manifest.json` (nodes with `origin: "create-database"`)
  and learn their schema: read the init script under `systems/<id>/<db>/` (and `models.json` /
  `endpoints.json` for the shapes), or introspect the live DB. DB containers are **not** published
  to host ports — reach them with `docker compose exec`:
  - postgres: `docker compose -f systems/<id>/docker-compose.yml exec -T <db> psql -U <user> -d <db> -c "INSERT …"`
  - mongo:    `docker compose -f systems/<id>/docker-compose.yml exec -T <db> mongosh <db> --quiet --eval "db.<coll>.insertMany([…])"`
- **Prefer the system's own seam** when one exists (an endpoint that creates the entity) — fall back
  to a direct datastore write only for genuinely out-of-scope entities the system never creates.
- **Uphold every constraint while seeding**: satisfy referential integrity (no Item with a
  `seller_id` that has no Account; every seller has an Account; every Order the methods will touch
  actually exists). Keep the ids you seeded — the methods' arguments must reference them (step 3).
- Record what you seeded (table/collection, counts, key ids) for the report.

### 3. Synthesize arguments — legal per the constraints

The definition captured method names but **no argument values** — you supply them, and they must be
**legal inputs** under the constraints. Prefer ids of entities **you seeded** (call `checkout`/
`refund` only on Orders that exist); generate other fields by type/name; **chain** where the flow
implies it (a create/checkout call returns an id a later refund/ship reuses — the `lb` helper
returns the parsed JSON body). **Record every value you use.**

### 4. Run a bounded orchestrator

Author a **throwaway Python orchestrator in the session scratchpad** (see the environment's
scratchpad dir — never in the repo). Run it in the **foreground of a single bounded Bash call** —
**no `nohup`, no `&`, no detaching** — so it dies with this session, and give it its **own local
hard deadline** (`deadline = now + durationSeconds`) so it self-terminates even if the control plane
goes silent. Invoke each due method as a **subprocess** (the exact path the Run panel uses) and parse
the trailing sentinel line:

```
python3 systems/<id>/clients/<module>.py --<method> <arg1> <arg2> …   # positional, signature order
```

It prints one `__LB_RESULTS__ [ {method,path,sentBody,status,ok,response}, … ]` line; parse the
**last** such line (a 4xx/5xx is still a recorded call; a network failure raises and still emits the
calls made so far). **Wrap every invocation in try/except** so one bad call never kills the loop.

Partition `client_list` by the client's `stateful` flag (manifest = source of truth). **Stateless
rows** are a req/s scheduler: blocking `subprocess.run` can't sustain more than ~1 req/s serially,
so dispatch each due invocation to a small `ThreadPoolExecutor` and advance each row's next fire
from its *scheduled* time (clamped forward if it falls behind) so the average rate holds.
**Stateful rows** are instance pools: spawn `instances` concurrent `Popen` children per row at run
start and keep N alive — instance `n` gets its own durable store (`n=1` keeps the canonical
`clients/<module>.state.json`, the one the client's State tab shows; `n>1` →
`clients/<module>.i<n>.state.json`, same gitignored `*.state.json` family, cleared by the client's
Clear-state action) plus a **stable synthesized identity argv** (same across respawns *and* runs,
e.g. `user_id = "e2e-<client>-u<n>"`) so the store keeps meaning. When an instance exits (the
functions self-bound ~25s), parse its `__LB_RESULTS__`, log it, and respawn with the same store +
argv; `Popen` has no timeout, so enforce the Run panel's 30s kill yourself.

Skeleton (adapt it — don't run verbatim):

```python
import json, os, subprocess, time, urllib.request
from concurrent.futures import ThreadPoolExecutor

SYSTEM, PID, APIBASE, DURATION = "<id>", "<processId>", "<apiBase>", <durationSeconds>
deadline = time.time() + DURATION
log = []

STATEFUL = {n["id"] for n in json.load(open(f"systems/{SYSTEM}/manifest.json"))["nodes"]
            if n.get("type") == "client" and n.get("stateful")}
stateless_rows = [r for r in client_list if r["client"] not in STATEFUL]
stateful_rows  = [r for r in client_list if r["client"] in STATEFUL]

def still_running():
    try:
        with urllib.request.urlopen(f"{APIBASE}/api/endtoend?system={SYSTEM}", timeout=3) as r:
            run = json.load(r).get("run", {})
        return run.get("running") and run.get("id") == PID
    except Exception:
        return True   # control plane unreachable → fall back to the local deadline only

def invoke(row, argv):                  # stateless one-shot (no env override — today's behavior)
    try:
        out = subprocess.run(["python3", f"systems/{SYSTEM}/clients/{module(row)}.py",
                              "--" + row["method"], *argv],
                             capture_output=True, text=True, timeout=30).stdout
        return parse_lb_results(out)
    except Exception as e:
        return [{"ok": False, "status": 0, "error": str(e)}]

pool = ThreadPoolExecutor(max_workers=8)
period = {i: 1.0 / rps(r) for i, r in enumerate(stateless_rows)}   # rps() applies the mismatch rule
next_call = {i: time.time() for i in range(len(stateless_rows))}

def instance_env(row, n):               # per-instance durable store (n=1 = the canonical file)
    suffix = ".state.json" if n == 1 else f".i{n}.state.json"
    return {**os.environ,
            "LB_CLIENT_STATE": os.path.abspath(f"systems/{SYSTEM}/clients/{module(row)}{suffix}")}

live = {}                               # (row_idx, n) -> {"proc", "argv", "started"}
def spawn(ri, n):
    row = stateful_rows[ri]
    argv = live.get((ri, n), {}).get("argv") or identity_args(row, n)   # stable per instance
    p = subprocess.Popen(["python3", f"systems/{SYSTEM}/clients/{module(row)}.py",
                          "--" + row["method"], *argv],
                         stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
                         env=instance_env(row, n))
    live[(ri, n)] = {"proc": p, "argv": argv, "started": time.time()}

for ri, row in enumerate(stateful_rows):
    for n in range(1, instances(row) + 1):    # instances() applies the mismatch rule
        spawn(ri, n)

while time.time() < deadline and still_running():
    now = time.time()
    for i, row in enumerate(stateless_rows):
        if now < next_call[i]:
            continue
        next_call[i] = max(next_call[i] + period[i], now)   # scheduled-time advance, clamped
        argv = legal_args(row)          # step 3 — reference seeded ids; record what you used
        pool.submit(lambda r=row, a=argv, t=now:
                    log.append({"row": r, "argv": a, "calls": invoke(r, a), "at": t}))
    for key, inst in list(live.items()):
        p = inst["proc"]
        if p.poll() is None and now - inst["started"] > 30:
            p.kill()                    # mirror the Run panel's 30s hard kill
        if p.poll() is None:
            continue
        out = p.communicate()[0] or ""
        log.append({"instance": key, "argv": inst["argv"], "calls": parse_lb_results(out),
                    "loop_seconds": round(now - inst["started"], 1)})
        if time.time() < deadline:
            if now - inst["started"] < 1.0:
                time.sleep(1.0)         # spawn-storm guard for quick-exit functions
            spawn(*key)                 # same store + identity args
        else:
            del live[key]
    time.sleep(0.05)

pool.shutdown(wait=True)
for key, inst in live.items():          # deadline: collect stragglers (they self-bound ≤30s)
    try:
        out = inst["proc"].communicate(timeout=30)[0]
    except subprocess.TimeoutExpired:
        inst["proc"].kill()
        out = inst["proc"].communicate()[0]
    log.append({"instance": key, "argv": inst["argv"], "calls": parse_lb_results(out or "")})
```

Why this shape (tradeoffs): a Bash+curl loop is fine only for the trivial single-row case;
`POST <apiBase>/api/scenarios/run` is a valid fallback but can't pre-synthesize / chain args as
flexibly; importing the client modules in-process is wrong — they share `lbclient._calls` and an
`atexit` emit, and a process has only one `LB_CLIENT_STATE`, so multiple calls (let alone
concurrent stateful instances) in one process tangle the output.

**WebSocket pools (`websocket_list`)** ride alongside the loop, not inside it: spawn each row's
pool ONCE at run start as a **background subprocess of the orchestrator** (still a child of the
foreground Bash call — dies with the session) and collect it at the deadline:

```python
pools = [subprocess.Popen(["node", f"systems/{SYSTEM}/ws-clients/{row['client']}.mjs",
                           "--count", str(row["clientCount"]), "--duration", str(DURATION),
                           "--rate", str(row.get("messagesPerSecond", 1))],
                          stdout=subprocess.PIPE, text=True)
         for row in websocket_list]
# … the client_list loop runs as above …
for p, row in zip(pools, websocket_list):          # after the loop: the scripts self-terminate
    out = p.communicate(timeout=60)[0]             # at --duration; parse the LAST sentinel line
    ws_results = json.loads(next(l for l in reversed(out.splitlines())
                                 if l.startswith("__WS_RESULTS__ ")).split(" ", 1)[1])
```

Each report is `{spawned, connected, sent, delivered, duplicates, errors, latencyMs:{p50,p95,max}}`.
`delivered` < `sent` (beyond a small in-flight tail) or `duplicates` > 0 are exactly the kind of
states `failure_list` entries probe for. For pools anywhere near the 200 cap, run
`ulimit -n 4096` in the spawning shell first (each pool client is one host fd).

### 5. Probe for failure states (and confirm constraints held)

**`failure_list` — the point of the run.** For each, decide the concrete bad state and look for it
in the **datastore** and the **call log**, sampling a few times **during** the run and once at the
**end** (steady state):
- Datastore queries via `docker compose exec` — e.g. a double charge is
  `SELECT order_id FROM order_payments WHERE status='success' GROUP BY order_id HAVING COUNT(*) > 1`;
  an orphaned payment is a payment whose `order_id` isn't in `orders`.
- **Prometheus** (`curl -s 'http://localhost:9090/api/v1/query?query=<PromQL>'`) for rate/error/
  latency-shaped failures; **call results** from the loop's `log` for per-request errors.
Keep the exact evidence (the offending rows / values) for the report.

**`constraint_list` — confirm you upheld it.** These were your setup + input rules; report each as
upheld (with how — what you seeded / how inputs were kept legal). Flag any you could **not** uphold,
or that the **system itself** violated (e.g. it deleted an Account a live Order still referenced) —
that's worth surfacing even though the primary signal is the failure list.

### 6. Halt → report → persist → stop → done

When the deadline passes **or** the early-stop poll says the run is no longer active (user hit Stop,
or `run.id` changed), stop invoking and:

1. **Print a report** to the terminal: what you **seeded**; per client method — calls, ok/fail
   counts, the args used, sample responses; per **failure** — occurred? + evidence (offending
   rows); per **constraint** — upheld? + how; then an overall **PASS / FAIL** (FAIL if any failure
   state was observed), noting skipped/pending methods and whether it stopped early.
2. **Persist the run report** as JSON at
   `systems/<id>/endtoend-runs/${PROCESS_NAME}_${TIMESTAMP}.json` — sanitize the process name for
   the filename (spaces/punctuation → `_`), UTC timestamp (`date -u +%Y%m%dT%H%M%SZ`). It MUST
   include `processId` (so the modal matches it) and `verdict` (`"PASS"`/`"FAIL"`):

   ```json
   { "processId": "…", "processName": "…", "verdict": "PASS",
     "startedAt": "…Z", "endedAt": "…Z", "durationSeconds": 60, "stoppedEarly": false,
     "seeded":      [ { "store": "order-db", "entity": "orders", "count": 20, "ids": ["…"] } ],
     "clientCalls": [ { "client": "mobile-app", "method": "checkout", "mode": "stateless",
                        "requestsPerSecond": 2, "calls": 118, "ok": 118, "failed": 0,
                        "argsUsed": ["…"], "samples": ["…"] },
                      { "client": "frontend", "method": "endToEnd", "mode": "stateful",
                        "instances": 3, "loops": 7, "calls": 96, "ok": 95, "failed": 1,
                        "identities": ["e2e-frontend-u1", "…"], "samples": ["…"] } ],
     "websocketPools": [ { "client": "ws-client", "count": 50, "connected": 50, "sent": 6000,
                           "delivered": 5991, "duplicates": 0,
                           "latencyMs": { "p50": 4, "p95": 12, "max": 40 } } ],
     "failures":    [ { "condition": "double charge", "occurred": false, "evidence": "…" } ],
     "constraints": [ { "constraint": "every seller has an Account", "upheld": true, "note": "seeded 5 accounts" } ],
     "skipped":     [ ] }
   ```

3. **Clear the run flag** so the modal's button flips back:
   `curl -s -X POST "${apiBase}/api/endtoend/stop" -H 'Content-Type: application/json' -d '{"system":"<id>","id":"<processId>"}'`
4. **Print the completion sentinel as your VERY LAST line, on its own line** (this session was
   launched from the edit queue, which advances only when it sees it):

   ```
   <<<SANDBOX_QUEUE_DONE>>>
   ```

## Verify

- `systems/<id>/endtoend.json` parses and contains the process id from your task prompt.
- Every `(client, method)` in `client_list` was resolved + implemented, or explicitly reported as
  skipped (never a silent crash).
- The out-of-scope preconditions the constraints imply were **seeded** with referential integrity,
  and the methods' arguments referenced the seeded ids (legal inputs) — both captured in the report.
- The orchestrator ran ~`duration` seconds (or until an early Stop); every implemented
  **stateless** method was called **at roughly its `requestsPerSecond`** — confirm via the `log`
  and/or moving Prometheus metrics.
- Every **stateful** row kept ~`instances` subprocesses alive for the whole run (respawned on
  exit, killed at 30s), every exit's `__LB_RESULTS__` was folded into the `log`, and the
  per-instance stores landed at `clients/<module>.state.json` / `clients/<module>.i<n>.state.json`.
- Every `websocket_list` row's pool was spawned with its `clientCount`, its `__WS_RESULTS__`
  report was parsed into the run report's `websocketPools`, and its delivered/duplicates numbers
  were checked against the failure conditions.
- Every `failure_list` entry was probed against the datastore / metrics / call log with concrete
  evidence, and every `constraint_list` entry has an upheld/flagged outcome.
- A report file `systems/<id>/endtoend-runs/<name>_<timestamp>.json` was written with `processId`
  and `verdict`; `POST ${apiBase}/api/endtoend/stop` was sent and
  `GET ${apiBase}/api/endtoend?system=<id>` now shows `run.running: false`.
- The final printed line is exactly `<<<SANDBOX_QUEUE_DONE>>>`.
