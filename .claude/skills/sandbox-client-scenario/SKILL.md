---
name: sandbox-client-scenario
description: >-
  Implement a CLIENT FUNCTION as real Python in a "Distributed Systems Sandbox" system
  (systems/<id>/clients/<client>.py). Use whenever the task is to write / update a client
  function's behavior from its description + arguments — the function is identified by its
  owner client and name — by authoring a `def <name>(...)` that calls existing load-balancer
  endpoints through the `lb` helper, with real control flow (if/else, loops) and by chaining
  one call's response into the next. Pure Python; no docker, no service code.
---

# Implementing a client function in Python

You are operating inside the "Distributed Systems Sandbox" web app. Each system lives in
`systems/<id>/`; your session's system prompt names the specific `<id>` and inlines its
current `manifest.json`. The web frontend runs under `npm run dev` and reads these files
live, so **never run `./start.sh`** — it tears down the dev server you're attached to.

A **client** is an external caller (a node with `type: "client"`, no container). Its functions
are a **real Python script** at `systems/<id>/clients/<module>.py`, where `<module>` is the
client id with hyphens turned into underscores (`mobile-app` → `mobile_app.py`). Each function
is a top-level `def <name>(...)`. The web app runs one like:

```
python3 mobile_app.py --checkout "<order_id>"
```

i.e. `--<function>` selects it and the remaining tokens are its declared arguments, **positional,
in signature order**. Each function is **owned by exactly one client** — identity is
`(client, name)`, so different clients may have same-named functions. There is no shared bank.
**This task touches no docker, no service code, no nginx, no prometheus** — only one client script.

## The registry vs. the code

`systems/<id>/scenarios.json` is the **registry** — it already holds this function's entry
(`client`, `name`, `args`, `description`, an empty `steps`, etc.). **Do not author `steps`** —
the app **infers them by statically scanning your code** (every `lb.<method>("/path")` call,
across both branches) and writes them back itself. Your job is purely to write the function in
the client's `.py`. Leave `scenarios.json` alone.

## The script: `systems/<id>/clients/<module>.py`

It already exists (scaffolded when the client/function was created), importing the shared `lb`
helper and exposing a `FUNCTIONS` dispatch map:

```python
import sys
from lbclient import lb

# === functions ===
# === end functions ===

FUNCTIONS = {
}

def main(argv):
    ...
```

The shared `lbclient.py` (do **not** edit it) gives you `lb`:

- `lb.get(path)`, `lb.post(path, body)`, `lb.put(path, body)`, `lb.patch(path, body)`,
  `lb.delete(path, body)` — each makes a **real** call through the load balancer
  (`http://localhost:8080` + `path`) and **returns the parsed JSON response body** (a dict/list),
  or the raw text if the response wasn't JSON.
- `lb.stream(path, max_events=20, timeout=5.0)` — consume a **Server-Sent Events**
  (`text/event-stream`) endpoint (an SSE route, `protocol: sse` in `endpoints.json`). It's a GET
  that reads the stream incrementally and **returns the list of `data:` payloads** it collected
  (each parsed to JSON when possible), recording one call. It is **bounded** — it stops after
  `max_events` events or `timeout` seconds — so the run always finishes. Use it (not `lb.get`)
  whenever the endpoint streams SSE. The whole client run is killed at 30s, so keep `timeout` well
  under that.
- `path` is the **load-balancer path** `/<service>/<local>` — the same path the lb routes and
  the same path under `/<service>/openapi.json` after the `/<service>` prefix (a route defined
  inside `service-1` as `/orders` is called here as `/service-1/orders`).
- Every call is recorded and shown in the client's Run panel; you don't print anything special.

## What to write

1. A top-level **`def <name>(<args…>)`** between the `# === functions ===` markers, with the
   arguments in the **same order** as the function's signature.
2. **Register it** in the `FUNCTIONS` map: `FUNCTIONS = { "<name>": <name>, ... }` (keep any
   existing entries).
3. Inside the body, call real endpoints with `lb.*` and use **whatever control flow the
   description needs** — this is the whole point of code over the old flat steps:

```python
def checkout(order_id):
    r = lb.post("/orders-service/orders/checkout", {"order_id": order_id})
    # Only complete the payment if checkout came back valid — otherwise cancel.
    if r.get("status") == "valid":
        lb.post("/payments-api/complete-payment", {"token": r["token"]})
    else:
        lb.post("/orders-service/orders/cancel", {"order_id": order_id})
```

### Rules

- **Use real endpoints only.** Pick paths from the endpoint list in your task prompt (or read
  `systems/<id>/endpoints.json` / `curl -s http://localhost:8080/<service>/openapi.json`).
  Never invent a route — if the description needs one that doesn't exist, stop and say so.
- **Keep paths as string literals** (or f-strings for path params, e.g.
  `f"/orders-service/orders/{order_id}"`). The static scanner reads `lb.<method>("…")` literals
  (including `lb.stream("…")`) to build the diagram trace, so a path assembled by string
  concatenation won't be traced.
- **CLI args arrive as strings.** Coerce where a function declares a number/boolean arg
  (`qty = int(qty)`, `flag = flag == "true"`).
- Chain calls with plain Python variables (`r = lb.post(...)`, then read `r["field"]`).
- Branch on responses freely (if/elif/else, early `return`, loops). A call that fails at the
  network level raises — let it propagate (the run reports the calls made so far).

## Verify

- Re-read `systems/<id>/clients/<module>.py`: it imports `lb`, defines `def <name>(...)` with the
  right argument order, registers it in `FUNCTIONS`, and only real load-balancer paths are used.
- It's valid Python: `python3 -c "import ast; ast.parse(open('systems/<id>/clients/<module>.py').read())"`.
- (Optional) Run it directly to smoke-test:
  `python3 systems/<id>/clients/<module>.py --<name> <args…>` — it should print a
  `__LB_RESULTS__ […]` line with the calls it made. Or use the client's Run panel (its Functions
  tab), which executes the same thing and shows each call's response; the diagram then traces the
  endpoints your code calls (both branches). An `lb.stream(...)` consume prints the same line once
  its bounded stream ends, with the recorded call's `response` set to the list of `data:` frames it
  collected.
