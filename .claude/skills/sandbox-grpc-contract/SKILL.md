---
name: sandbox-grpc-contract
description: >-
  Propagate a gRPC contract SHAPE change in a "Distributed Systems Sandbox" system
  (systems/<id>/). The contract bank is pure shape: the web backend itself synthesizes/
  splices the .proto from the registry's method records and regenerates the _pb2 bindings
  with real protoc — no session authors protos. Use whenever contracts were just edited or
  deleted in the bank (methods added/changed/removed, a re-uploaded .proto, a contract
  delete) and the ATTACHED services' code must catch up: update the owning service's
  servicer to the new signatures, update client call sites, unwire deletions, rebuild.
  For attaching/serving a contract (implementing methods from their descriptions) use
  [[sandbox-grpc-attach]].
---

# Propagating a sandbox gRPC contract change

You are operating inside the "Distributed Systems Sandbox" web app. Each system lives in
`systems/<id>/`; your session's system prompt names the specific `<id>` and inlines its
current `manifest.json`. The web frontend runs under `npm run dev` and reads these files
live, so **never run `./start.sh`** — it tears down the dev server you're attached to.

A **contract** is a `.proto` *service*: a set of RPC methods + their request/response
message types — pure **shape**, no behavior. It lives in the per-system bank
(`systems/<id>/grpc/`), is **not** a diagram node, and is served by exactly **one owning
service** (endpoint-like ownership; only custom service types install multi-server
contracts). Behavior comes from **per-method `description`s** written when a service
attaches the contract as its server (the [[sandbox-grpc-attach]] flow).

## The division of labor (what is ALREADY DONE when you launch)

The bank's "Review & save" (`POST /api/grpc-apply`) — like the model bank — has already,
**mechanically**, before your session starts:

- rewritten every changed `systems/<id>/grpc/<Contract>.proto` (synthesized from the
  registry field maps for `source:"form"` contracts; spliced for uploaded ones),
- re-run the real `protoc` and regenerated `<Contract>_pb2.py` / `<Contract>_pb2_grpc.py`
  (nothing was persisted unless every changed contract compiled),
- updated `_registry.json`, and for **deleted** contracts removed the four generated files
  and scrubbed the contract from every node's manifest `grpc` block.

So: **do NOT re-run protoc, do NOT edit any `.proto`, do NOT touch `_registry.json`.**
Your job is only the affected services' CODE. Your launch prompt lists what changed and
which services are affected (the owner that serves each contract + the clients that call
it, joined from the manifest before the change).

## The registry (read it for descriptions + shapes)

`systems/<id>/grpc/_registry.json`:

```json
{ "contracts": {
  "Ping": {
    "source": "form",                    // "form" | "upload" | a custom type
    "server": "chat-service",            // owning server (manifest grpc.servers wins)
    "methods": [{
      "name": "Check",
      "request": { "tag": "int32" }, "response": { "ok": "bool" },
      "requestType": "CheckRequest", "responseType": "CheckReply",
      "requestStreaming": false, "responseStreaming": false,
      "formAuthored": true,
      "description": "what the owning service's method body should do",
      "conversationId": null
    }],
    "conversationId": "<uuid>", "createdAt": "<iso>" } } }
```

`description` is the method's behavior text (accumulates endpoint-style). A legacy
`instruction` key may exist on old contracts — ignore it, never write it. Message naming
is `<Method>Request` / `<Method>Reply` (stored `requestType`/`responseType` are
authoritative). Field numbers are append-only; removed fields become `reserved` — the
backend enforces this, you never renumber anything.

## What you do, per affected service

Work from the change list in your prompt. For **each contract that changed**:

1. **The owning service** (serves the contract — manifest `grpc.servers`):
   update `systems/<id>/grpc/<Contract>_servicer.py` to the new method set:
   - unchanged methods: keep their behavior exactly;
   - changed signatures: reconcile the body with the new request/response fields;
   - new methods: implement from their registry `description`; a method with **no
     description** gets a stub that `context.abort(grpc.StatusCode.UNIMPLEMENTED, ...)`;
   - deleted methods: remove them.
   The servicer subclasses the generated `<Contract>Servicer` and uses bare imports
   (`import <Contract>_pb2 as pb2`) — the grpc dir is bind-mounted at `/app/grpc_pkg`
   and put on `sys.path` by the service (see [[sandbox-grpc-attach]]).
2. **Each client service** (manifest `grpc.clients` names the contract): update its call
   sites to the new message shapes; remove calls to deleted methods.
3. **A DELETED contract**: its files and manifest entries are already gone — remove the
   dead code: the former owner's server registration (servicer import +
   `add_<Contract>Servicer_to_server`; drop the grpc server itself only if the service
   serves nothing else) and each former client's stub wiring, plus any leftover
   per-service `*_servicer_override.py`.
4. **Rebuild** each service you touched:
   ```
   docker compose -f systems/<id>/docker-compose.yml up -d --build <service>
   ```
   (The grpc dir is a read-only bind mount, so a servicer-only change strictly needs just
   a restart — but rebuild when you also touched the service's own files.)

## Verify

1. The generated files import cleanly:
   ```
   docker run --rm -v "$PWD/systems/<id>/grpc":/g -w /g python:3.12-slim \
     sh -c "pip install -q grpcio && python -c 'import <Contract>_pb2, <Contract>_pb2_grpc'"
   ```
2. Each rebuilt server still listens: `docker compose -f systems/<id>/docker-compose.yml
   exec -T <service> python -c "import socket; socket.create_connection(('localhost',50051),2)"`.
3. A changed method responds per its description (call it with a throwaway stub from
   inside a container); an undescribed new method returns UNIMPLEMENTED.
4. The diagram still draws the served RPC rows and the purple client edges (they read the
   registry + manifest, both already updated).

## Working by hand (no web app)

If you must create or reshape a contract without the backend, reproduce its end state
exactly: write the self-contained one-service `.proto` (proto3, PascalCase service =
file stem = registry key, `<Method>Request`/`<Method>Reply` messages, sequential
append-only field numbers, `reserved` for removed ones), compile it with the pinned
toolchain the backend uses —
```
docker run --rm -v "$PWD/systems/<id>/grpc":/g -w /g python:3.12-slim \
  sh -c "pip install -q grpcio-tools==1.68.1 && python -m grpc_tools.protoc -I /g --python_out=/g --grpc_python_out=/g <Contract>.proto"
```
— and upsert the `_registry.json` entry (shape above). Custom service types install fixed
contracts through `frontend/server/grpcInstall.js`, which does the same thing
deterministically.
