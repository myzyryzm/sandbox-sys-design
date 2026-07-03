---
name: sandbox-grpc-attach
description: >-
  Attach an existing gRPC contract to a service in a "Distributed Systems Sandbox" system
  (systems/<id>/) as server and/or client. Use whenever the task is to make a service serve a
  contract (import the shared servicer + run a gRPC server), consume one (a client stub pointed
  at editable targets), or use a service-specific override servicer — it covers the manifest
  `grpc` block, the app.py / Dockerfile / requirements wiring, and the rebuild/verify steps. To
  define or change the contract itself (the .proto + shared servicer) use [[sandbox-grpc-contract]].
---

# Attaching a sandbox gRPC contract to a service

You are operating inside the "Distributed Systems Sandbox" web app. Each system lives in
`systems/<id>/`; the frontend runs under `npm run dev` and reads these files live, so **never run
`./start.sh`**. Rebuild a changed service with `docker compose` directly (commands below).

Attaching is **wiring**, not authoring. The contract's `.proto`, `_pb2*.py`, and the **single
shared servicer** `systems/<id>/grpc/<Contract>_servicer.py` already exist (see
[[sandbox-grpc-contract]]). Roles are **per-contract, not per-node**: a service can be a server for
one contract and a client for another, or **both server and client of the same contract** (e.g. a
peer that others pull from and that also pulls from peers). The web app's per-service gRPC modal
drives this (`POST /api/grpc-attach` writes the manifest block, then launches this session); by
hand, reproduce the same shape.

## The two places an attachment lives (working dir is the repo root)

1. `systems/<id>/manifest.json` — the service **node's `grpc` block** (single source of truth for
   both the wiring and the diagram's gRPC edges). The web app writes this before launching you:
   ```json
   "grpc": {
     "servers": ["ChunkTransfer"],
     "clients": [{ "contract": "ChunkTransfer", "targets": ["worker-2"] },
                 { "contract": "Coordination",  "targets": ["coordinator"] }],
     "overrides": ["ChunkTransfer"]
   }
   ```
   - `servers` — contracts this service serves (runs the servicer for).
   - `clients` — contracts this service calls, each with **editable `targets`** (the service node
     ids it dials). The diagram draws a gRPC edge client → each target.
   - `overrides` — contracts this service serves with a **service-specific** override servicer
     instead of the shared one.
2. `systems/<id>/<service>/` — the service's code: `app.py`, `Dockerfile`, `requirements.txt`,
   and (only for an override) `systems/<id>/<service>/grpc/<Contract>_servicer_override.py`.

### Conventions
- Every service runs **one** `grpc.aio` server on the fixed internal port **`50051`**, hosting all
  contracts it serves. No nginx route (gRPC is a binary protocol); peers reach it by container name
  at `<service>:50051` on the compose network.
- `targets` are **editable config**: re-pointing a client is a manifest edit + a service `restart`
  — **never** a regen of the proto/pb2/servicer. So read targets at runtime from the mounted
  manifest, not from a baked-in constant (below).
- An override is generated **only** when the user supplies override text; it lives in a clearly
  named file and is flagged in `overrides` so the divergence is explicit, never accidental drift.

## Wiring the service code

The shared `grpc/` folder must be importable from the service. Add to the service's `Dockerfile`
a copy of the system's grpc package and `EXPOSE 50051`, and to `requirements.txt`:
`grpcio`, `grpcio-tools`. Then in `app.py`, alongside the existing FastAPI app + metrics
middleware (leave those intact), start a gRPC server in the **lifespan**:

```python
import grpc, json, os
from grpc_pkg import ChunkTransfer_pb2_grpc as ct_grpc   # the system's shared grpc package
from grpc_pkg.ChunkTransfer_servicer import ChunkTransferServicer
# override instead, when this service is in `overrides`:
# from ChunkTransfer_servicer_override import ChunkTransferServicer

def _grpc_targets(contract):
    # targets are editable config — read them from the mounted manifest at startup,
    # so changing a target needs only `docker compose restart <service>`, no regen.
    m = json.load(open("/manifest.json"))
    node = next(n for n in m["nodes"] if n["id"] == os.environ["SERVICE_ID"])
    for c in node.get("grpc", {}).get("clients", []):
        if c["contract"] == contract:
            return c["targets"]
    return []

@asynccontextmanager
async def lifespan(app):
    server = grpc.aio.server()
    ct_grpc.add_ChunkTransferServicer_to_server(ChunkTransferServicer(), server)  # if a server
    server.add_insecure_port("[::]:50051")
    await server.start()
    # if a client: stubs = [ct_grpc.ChunkTransferStub(grpc.aio.insecure_channel(f"{t}:50051"))
    #                        for t in _grpc_targets("ChunkTransfer")]
    yield
    await server.stop(None)
```

- **Server role**: import the **shared** servicer and `add_<Contract>Servicer_to_server`. Add a
  small server interceptor that increments `prometheus_client` counters in the default registry so
  gRPC calls show on the existing `/metrics` (no new scrape job).
- **Client role**: build a `<Contract>Stub` per target channel from `_grpc_targets(...)` (mount the
  manifest read-only into the container, e.g. `./manifest.json:/manifest.json:ro`, and set
  `SERVICE_ID` in the compose env).
- **Override**: when this service is in `overrides`, write/import
  `systems/<id>/<service>/grpc/<Contract>_servicer_override.py` (a `<Contract>Servicer` subclass
  that deliberately diverges) and use it for the server registration **instead of** the shared one.
  Leave the shared servicer and other services untouched.
- **Do not implement** the chunk-transfer/distribution behavior (timing, bitmaps, backpressure)
  unless explicitly asked — wiring + a minimal correct method body is the scope.

## Rebuild + verify

```
docker compose -f systems/<id>/docker-compose.yml up -d --build <service>
```

1. The container is up and `:50051` is listening (`docker compose ... exec -T <service> python -c
   "import socket; socket.create_connection(('localhost',50051),2)"`).
2. The manifest `grpc` block is correct and the diagram draws a purple dashed gRPC edge from each
   client `target`.
3. **Shared servicer** check: two services serving the same contract with no override both import
   `systems/<id>/grpc/<Contract>_servicer.py` — assert the identical import, no duplicated impl.
4. **Override** check: an overriding service imports its own
   `<service>/grpc/<Contract>_servicer_override.py` and is listed in `overrides`; others stay shared.
5. **Editable target** check: change a client's `targets` in the manifest, then
   `docker compose ... restart <service>` — the new target resolves with **no** proto/pb2/servicer
   regeneration.

## Detach / change roles

Edit the service node's `grpc` block: drop the contract from `servers`/`clients`/`overrides` as
needed (remove the whole `grpc` block when all three are empty), remove the corresponding wiring
from `app.py` (and the override file if any), then rebuild that service. Re-pointing a client's
`targets` is a manifest edit + `restart` only.
