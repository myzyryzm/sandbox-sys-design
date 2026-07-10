---
name: sandbox-grpc-attach
description: >-
  Serve or call a gRPC contract from a service in a "Distributed Systems Sandbox" system
  (systems/<id>/). SERVE: a contract has exactly ONE owning server — author its servicer
  from the per-method descriptions the user wrote in the service's gRPC tab, wire
  app.py/Dockerfile/compose, rebuild; also description edits (modify one method body in
  place) and detach (unwire + delete the servicer). CLIENT: the canonical procedure other
  flows (endpoints, consumers, custom types) follow to make a service call a contract —
  write the manifest grpc.clients block + a stub per target. The contract's shape (.proto,
  _pb2, registry) is bank-owned and already generated — see [[sandbox-grpc-contract]].
---

# Serving / calling a sandbox gRPC contract

You are operating inside the "Distributed Systems Sandbox" web app. Each system lives in
`systems/<id>/`; the frontend runs under `npm run dev` and reads these files live, so
**never run `./start.sh`**. Rebuild a changed service with `docker compose` directly.

Attaching is **implementation + wiring**, never shape: the contract's `.proto` and
`_pb2*.py` already exist under `systems/<id>/grpc/` (the backend generated them with real
protoc — do NOT regenerate or edit them). A contract has exactly **one owning server**
(the web app enforces this with a 409; only custom service types install multi-server
contracts). Behavior comes from the **per-method `description`s** stored on the registry's
method records — written in the service Edit modal's gRPC tab at attach time and appended
to on later edits.

## The places an attachment lives (working dir is the repo root)

1. `systems/<id>/manifest.json` — the service node's `grpc` block (single source of truth
   for the wiring and the diagram's gRPC edges). The web app writes the server side before
   launching you; the CLIENT side is written by whatever flow makes the service a caller
   (below):
   ```json
   "grpc": {
     "servers": ["Ping"],
     "clients": [{ "contract": "Worker", "targets": ["llm-worker"] }],
     "overrides": []
   }
   ```
   (`overrides` is legacy — tolerated, scrubbed on detach/delete, never written anew.)
2. `systems/<id>/grpc/<Contract>_servicer.py` — the owning service's implementation, a
   subclass of the generated `<Contract>Servicer`. It lives in the shared grpc dir (the
   whole dir is bind-mounted read-only into services), but it belongs to the ONE owner.
3. `systems/<id>/<service>/` — the service's own `app.py`, `Dockerfile`,
   `requirements.txt`, compose entry.
4. `systems/<id>/grpc/_registry.json` — the method records whose `description`s you
   implement (see [[sandbox-grpc-contract]] for the full shape). Never write `instruction`.

### Conventions

- Every service runs **one** `grpc.aio` server on the fixed internal port **`50051`**,
  hosting all contracts it serves. No nginx route (gRPC is binary); peers reach it by
  container name at `<service>:50051` on the compose network.
- The shared grpc package is **bind-mounted, not baked in**: compose mounts
  `./grpc:/app/grpc_pkg:ro`, app.py does `sys.path.insert(0, "/app/grpc_pkg")`, and both
  the generated modules and the servicer use **bare imports**
  (`import Ping_pb2 as pb2`, `from Ping_servicer import PingServicer`). A regenerated
  binding or servicer is picked up by a `restart` — no image rebuild needed for the
  shared code itself.
- `requirements.txt` needs `grpcio` (pin compatible with the generated bindings —
  `grpcio==1.68.1` matches the backend's pinned `grpcio-tools`). `EXPOSE 50051`.
- Client `targets` are **editable config**: read them at startup from the mounted
  manifest (`./manifest.json:/manifest.json:ro` + `SERVICE_ID` env), so re-pointing needs
  only a `restart`, never a regen.

## SERVE: implement + wire the owning server

Your launch prompt lists each method with its description. Do this:

1. **Author the servicer** `systems/<id>/grpc/<Contract>_servicer.py`:
   ```python
   import grpc
   import <Contract>_pb2 as pb2
   import <Contract>_pb2_grpc as pb2_grpc

   class <Contract>Servicer(pb2_grpc.<Contract>Servicer):
       async def Check(self, request, context):
           # implement exactly what the method's description says
           return pb2.CheckReply(ok=True)

       async def Undescribed(self, request, context):
           await context.abort(grpc.StatusCode.UNIMPLEMENTED, "no behavior described yet")
   ```
   - A method with **no description** gets the UNIMPLEMENTED stub — the user describes it
     later from the gRPC tab and a new session fills it in.
   - A streaming response method is an async generator (`yield pb2.<...>Reply(...)`).
   - Keep bodies behavior-only; inject service state via the constructor if needed.
2. **Wire app.py** (alongside the existing FastAPI app + hand-written metrics middleware —
   leave those intact), starting the server in the **lifespan**:
   ```python
   import sys
   sys.path.insert(0, "/app/grpc_pkg")
   import grpc
   import <Contract>_pb2_grpc as c_grpc
   from <Contract>_servicer import <Contract>Servicer

   @asynccontextmanager
   async def lifespan(app):
       server = grpc.aio.server(interceptors=[GrpcMetricsInterceptor()])
       c_grpc.add_<Contract>Servicer_to_server(<Contract>Servicer(), server)
       server.add_insecure_port("[::]:50051")
       await server.start()
       yield
       await server.stop(None)
   ```
   Add a small server interceptor incrementing `prometheus_client` counters in the default
   registry so gRPC calls show on the existing `/metrics` — **no new scrape job**.
3. **Wire Dockerfile / requirements / compose**: `grpcio` pinned, `EXPOSE 50051`, and the
   compose service gets the `./grpc:/app/grpc_pkg:ro` mount (+ `./manifest.json` and
   `SERVICE_ID` if it will also be a client).
4. **Rebuild just this service**:
   ```
   docker compose -f systems/<id>/docker-compose.yml up -d --build <service>
   ```

### Description edits (a later "Save & update methods")

The registry descriptions are already saved (the new chunk was **appended** to the old
text). FIRST read the current `systems/<id>/grpc/<Contract>_servicer.py`, then modify
ONLY the listed methods **in place** — preserve behavior the change doesn't mention, leave
other methods and the app wiring untouched. Rebuild the service.

### Detach (unwire the owner)

The manifest/registry are already updated and the web app verified no client still dials
this service. Remove the servicer import + `add_<Contract>Servicer_to_server` from app.py
(drop the grpc server itself only if nothing else is served), **delete**
`systems/<id>/grpc/<Contract>_servicer.py` (the contract no longer has an owner; its
.proto/_pb2 stay in the bank), and rebuild the service.

## CLIENT: the canonical caller-wiring procedure

The gRPC tab does **not** wire clients. When YOUR flow makes a service call a contract —
an endpoint handler ([[sandbox-endpoint]]), a Kafka consumer function, a custom type —
you wire the caller yourself:

1. **Manifest**: add/extend the caller node's `grpc.clients` entry:
   `{ "contract": "<C>", "targets": ["<owning service id>"] }`. With single ownership the
   target is simply the contract's owner (`server` in the registry / `grpc.servers` in the
   manifest). This is what draws the purple gRPC edge — keep it accurate, and prune the
   entry when the call is removed.
2. **Compose**: mount the manifest read-only + set `SERVICE_ID`, and add the
   `./grpc:/app/grpc_pkg:ro` mount.
3. **Code**: read targets from the mounted manifest at startup and build one stub per
   target:
   ```python
   def _grpc_targets(contract):
       m = json.load(open("/manifest.json"))
       node = next(n for n in m["nodes"] if n["id"] == os.environ["SERVICE_ID"])
       for c in node.get("grpc", {}).get("clients", []):
           if c["contract"] == contract:
               return c["targets"]
       return []
   # stubs = [c_grpc.<C>Stub(grpc.aio.insecure_channel(f"{t}:50051")) for t in _grpc_targets("<C>")]
   ```

### Client-side balancing across a replica group (no load balancer)

Some services scale to **N instances sharing one service id with no LB** — notably an LLM
worker replica group ([[sandbox-llm-worker]]): a base `<w>` plus `<w>-2..N`, each
`type:"service"` carrying `instanceOf:"<w>"`, all serving the same contract at `:50051`.
Point `targets` at **just the group entry** (`targets:["<w>"]`) and expand at runtime:

```python
def _worker_targets(contract="Worker"):
    m = json.load(open("/manifest.json"))
    node = next(n for n in m["nodes"] if n["id"] == os.environ["SERVICE_ID"])
    ids = []
    for c in node.get("grpc", {}).get("clients", []):
        if c["contract"] != contract:
            continue
        for entry in c["targets"]:
            ids.append(entry)                             # the base IS a real worker
            ids += [n["id"] for n in m["nodes"] if n.get("instanceOf") == entry]
    return ids                                            # -> ["<w>", "<w>-2", ...]
# one stub per id; pick per request in app code — round-robin, capacity-gated, or hash-by-key.
```

Targeting the entry keeps the diagram to one edge into the group's box, and scaling needs
no re-attach (targets are read at startup from the mounted manifest).

## Rebuild + verify

```
docker compose -f systems/<id>/docker-compose.yml up -d --build <service>
```

1. `:50051` listens: `docker compose ... exec -T <service> python -c
   "import socket; socket.create_connection(('localhost',50051),2)"`.
2. Each described method behaves per its description (call it with a throwaway stub from
   inside a container); undescribed methods return UNIMPLEMENTED.
3. The manifest `grpc` block is correct: the owner in `servers`, each caller's
   `clients[].targets` naming the owner — and the diagram draws the served RPC rows and a
   purple dashed edge per client target.
4. **Editable target** check (clients): change `targets` in the manifest, then
   `docker compose ... restart <service>` — the new target resolves with no regen.
5. **Single owner** check: exactly one node's `grpc.servers` lists the contract, and
   exactly one `<Contract>_servicer.py` exists (no per-service copies).
