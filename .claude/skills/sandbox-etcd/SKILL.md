---
name: sandbox-etcd
description: >-
  Wire real etcd service discovery in a "Distributed Systems Sandbox" system
  (systems/<id>/). Use whenever the task is to implement or update a service's
  REGISTRATION with the etcd cluster (each worker keeps a leased key alive under
  /services/<service>/) or a LISTENER on a keyspace (a watch_prefix loop keeping a
  live in-memory worker map, updated by pushed etcd events), or to remove either —
  it covers the etcd.json registry, the pre-wired compose env/mount, the pinned
  python client deps, the registration/watcher loop contracts (survive cluster
  recreation, TTL re-read by mtime), and the docker rebuild/verify steps. The
  cluster itself (N members, Raft knobs, member stop/start) is provisioned and
  reconfigured by the web app — not by sessions.
---

# Working on sandbox etcd service discovery

You are operating inside the "Distributed Systems Sandbox" web app. Each system lives in
`systems/<id>/`; your session's system prompt names the specific `<id>` and inlines its
current `manifest.json`. The web frontend runs under `npm run dev` and reads these files
live, so **never run `./start.sh`** — it tears down the dev server you're attached to.
Rebuild with `docker compose` directly (commands below).

The etcd cluster is a real N-member Raft cluster (N odd — 3/5/7 — one container per
member, `<etcd>-1..N`, no host ports). The app's "Add etcd" flow (`POST /api/etcd`,
`frontend/server/etcd.js`) provisions it; the Cluster tab reconfigures it. **Sessions
never create or resize the cluster** — your job is the CODE half of discovery: the
registration loop in a registering service and the watch loop in a listener. Related:
[[sandbox-event-stream]] consumer functions follow the same mechanical-scaffold-then-
session split; [[sandbox-endpoint]] covers plain routes.

## The five places etcd lives (working dir is the repo root)

1. `systems/<id>/etcd.json` — the **discovery registry** AND the live tunables file:
   ```json
   { "cluster": { "id": "etcd", "size": 3, "heartbeatMs": 100, "electionMs": 1000,
                  "leaseTtlSeconds": 15 },
     "keyspaces": [
       { "service": "workers", "prefix": "/services/workers/", "description": "…",
         "implemented": false, "conversationId": "…", "history": [ … ],
         "listeners": [ { "service": "api", "description": "…", "implemented": false } ] } ] }
   ```
   The app writes everything here EXCEPT the `implemented` flags — **you** flip one to
   `true` after the matching loop is written, rebuilt, and verified (an app edit never
   resets it). Registering services get this file bind-mounted read-only at
   `/etcd/etcd.json`; their keepalive loop **re-reads `leaseTtlSeconds` by mtime** so a
   TTL change in the UI applies live with no rebuild.
2. `systems/<id>/docker-compose.yml` — N member services `<etcd>-1..N`
   (`gcr.io/etcd-development/etcd:v3.5.21`, static `--initial-cluster` bootstrap, the
   Raft knobs as flags, **no data volume** — a recreate is deliberately a fresh cluster).
   Registering/listening services carry app-written env: `ETCD_ENDPOINTS`
   (`etcd-1:2379,…`), and registrants also `ETCD_WORKER_ID` + the `/etcd/etcd.json:ro`
   mount. **Do not add or edit these yourself** — the app writes them when a keyspace or
   listener is created; your rebuild of the service applies them.
3. `systems/<id>/prometheus/prometheus.yml` — ONE scrape job named `<etcd>` with all N
   members as targets (etcd serves Prometheus metrics natively on the client port 2379).
4. `systems/<id>/manifest.json` — a node `{ id, type:"etcd", origin:"create-etcd",
   etcd:{ size, quorum, heartbeatMs, electionMs, leaseTtlSeconds, members }, metrics,
   health }`. Health is quorum-aware: red below quorum, yellow degraded, green full. The
   diagram draws per-member dots (leader ringed) and the quorum caption from this + the
   per-member `up`/`etcd_server_is_leader` series.
5. **No nginx route and no manifest edges.** etcd speaks gRPC (not HTTP through the lb),
   and discovery arrows are drawn only while a KEY row is selected on the diagram — the
   trace comes from etcd.json, not `manifest.edges`.

Conventions: keyspace prefix is always `/services/<service>/`; each worker registers
`<prefix><worker-id>` with value `"<container-dns>:8000"`; worker id comes from
`ETCD_WORKER_ID` (`<service>-1` for a plain service; a load-balanced service's instances
are `<service>-1..N` and each registers itself — the app maintains per-instance env).

## Talking to etcd from the host

The member containers are **distroless — there is no shell**; exec `etcdctl` directly
(never `sh -c`). `COMPOSE=systems/<id>/docker-compose.yml`:

```bash
docker compose -f $COMPOSE exec -T etcd-1 etcdctl member list -w table
docker compose -f $COMPOSE exec -T etcd-1 etcdctl endpoint status --cluster -w table  # leader column
docker compose -f $COMPOSE exec -T etcd-1 etcdctl get --prefix /services/ --keys-only
docker compose -f $COMPOSE exec -T etcd-1 etcdctl get --prefix /services/workers/     # keys + values
docker compose -f $COMPOSE exec -T etcd-1 etcdctl watch --prefix /services/workers/   # live push (Ctrl-C)
docker compose -f $COMPOSE exec -T etcd-1 etcdctl lease list
```

`-w json` base64-encodes keys/values — prefer the plain output when eyeballing. If
`etcd-1` is down (someone ran the quorum demo), exec into any other member.

## Python client deps (pinned — add to the service's requirements.txt)

```
etcd3==0.12.0
protobuf==3.20.3
grpcio>=1.60
```

`etcd3` (python-etcd3) is unmaintained but works: it needs `protobuf<4` (3.20.3 ships a
pure-python wheel, fine on python:3.12-slim) and any modern `grpcio` (arm64 wheels
exist). This exact trio is verified against the sandbox's etcd v3.5 image. If the
install ever fails, the documented fallback is etcd's built-in JSON gRPC-gateway over
`httpx` (no new deps): `POST http://<member>:2379/v3/lease/grant` `{"TTL":15}`,
`/v3/kv/put` `{"key":base64,"value":base64,"lease":id}`, `/v3/lease/keepalive`, and a
streaming `POST /v3/watch` — the gateway is stock etcd, so watch is still server-push.

## Registration loop (the code you author into a registering service's app.py)

Contract — a **daemon thread started at import time** (never blocking the request path),
one per worker container:

- Identity/config from env: `WORKER_ID = os.environ.get("ETCD_WORKER_ID", socket.gethostname())`,
  `ENDPOINTS = os.environ.get("ETCD_ENDPOINTS", "etcd-1:2379").split(",")`.
- TTL from the mounted registry, **re-read by mtime, keep-last-good on a mid-write read**
  (the same pattern as a consumer loop's pause flag):
  ```python
  _ETCD_CFG = "/etcd/etcd.json"
  _ttl_cache = {"mtime": 0.0, "ttl": 15}
  def _lease_ttl():
      try:
          m = os.stat(_ETCD_CFG).st_mtime
          if m != _ttl_cache["mtime"]:
              with open(_ETCD_CFG) as f:
                  ttl = int(json.load(f)["cluster"]["leaseTtlSeconds"])
              _ttl_cache.update(mtime=m, ttl=ttl)
      except Exception:
          pass  # keep last good value
      return _ttl_cache["ttl"]
  ```
- The loop (reference shape — adapt names to the service):
  ```python
  def _register_worker():
      key = f"/services/{SERVICE}/{WORKER_ID}"
      value = f"{WORKER_ID}:8000"   # container DNS: instances resolve by their own name
      while True:
          try:
              host, port = random.choice(ENDPOINTS).split(":")
              client = etcd3.client(host=host, port=int(port), timeout=5)
              ttl = _lease_ttl()
              lease = client.lease(ttl)
              client.put(key, value, lease=lease)
              while True:
                  time.sleep(max(1, ttl / 3))
                  if _lease_ttl() != ttl:
                      break            # TTL changed in the UI -> re-grant with the new TTL
                  r = lease.refresh()
                  if not r or getattr(r[0], "TTL", 0) <= 0:
                      break            # lease VANISHED (cluster recreated) -> re-grant + re-put
          except Exception:
              time.sleep(2)            # quorum lost / member down / connect failure
          # fall through: reconnect (maybe another member), re-grant, re-put

  threading.Thread(target=_register_worker, daemon=True).start()
  ```
- **The silent-death trap (do not skip the refresh check):** when the UI recreates the
  cluster, gRPC transparently reconnects to the NEW cluster — where the old lease id
  doesn't exist — and `LeaseKeepAlive` on an unknown lease is NOT an error: `refresh()`
  returns a response with `TTL: 0` and never raises. A loop that only catches exceptions
  will "refresh" a dead lease forever and the worker key never comes back. Checking the
  returned TTL is what makes the loop actually survive cluster recreation (verified the
  hard way in this sandbox).
- **Hard requirements**: survive cluster recreation (the refresh-TTL check above);
  survive quorum loss (calls raise → keep retrying); pick members with failover (don't
  hardcode `etcd-1`); on TTL change grant a fresh lease and re-put (the old lease just
  expires). For a plain service, `value` should be `f"{SERVICE}:8000"` (its own compose
  DNS name); for a load-balanced instance, `WORKER_ID` doubles as its DNS name, so
  `f"{WORKER_ID}:8000"` is correct for both when `ETCD_WORKER_ID` is set (it always is —
  the app writes it).
- Keep the hand-written prometheus metrics middleware and every other route/loop intact.
- Rebuild ONLY that service (for a load-balanced service this builds the instances),
  then force-recreate the lb — a recreated service can land on a new IP and nginx's
  static upstream doesn't re-resolve (the 502-through-the-lb trap):
  ```bash
  docker compose -f systems/<id>/docker-compose.yml up -d --build <service>
  docker compose -f systems/<id>/docker-compose.yml up -d --force-recreate lb
  ```
- Verify (below), then set `"implemented": true` on the keyspace entry in
  `systems/<id>/etcd.json`.

## Watcher loop (the code you author into a listener's app.py)

Contract — a **daemon thread**, one per watched keyspace; etcd **pushes** every change
over the watch stream (this is stock etcd watch — **never poll** the prefix):

```python
WORKERS: dict[str, str] = {}   # worker id -> host:port — the live view other code reads

def _watch_workers():
    prefix = "/services/<keyspace>/"
    while True:
        client = None
        try:
            host, port = random.choice(ENDPOINTS).split(":")
            client = etcd3.client(host=host, port=int(port), timeout=5)

            def _apply(resp):            # etcd PUSHES each change here — the hot path
                for ev in resp.events:
                    wid = ev.key.decode()[len(prefix):]
                    if isinstance(ev, etcd3.events.PutEvent):
                        WORKERS[wid] = ev.value.decode()
                    else:                # DeleteEvent: explicit delete OR lease expiry
                        WORKERS.pop(wid, None)

            client.add_watch_prefix_callback(prefix, _apply)
            # baseline AFTER the watch is armed, so no change can fall in the gap
            fresh = {m.key.decode()[len(prefix):]: v.decode()
                     for v, m in client.get_prefix(prefix)}
            WORKERS.clear(); WORKERS.update(fresh)
            while True:                  # slow ANTI-ENTROPY sweep — see the trap below
                time.sleep(30)
                fresh = {m.key.decode()[len(prefix):]: v.decode()
                         for v, m in client.get_prefix(prefix)}
                if fresh != WORKERS:
                    raise RuntimeError("stale watch — full resync")
        except Exception:
            time.sleep(2)                # reconnect + full resync from scratch
        finally:
            try:
                if client is not None:
                    client.close()       # tears down the watch stream + its threads
            except Exception:
                pass

threading.Thread(target=_watch_workers, daemon=True).start()
```

- A lease expiring (worker died, TTL passed with no keepalive) arrives as a
  **DeleteEvent** — that's the whole point: the map self-heals with no polling.
- **The stale-watch trap (why the sweep exists):** after a UI cluster recreate, gRPC
  reconnects and the client re-arms the watch at its OLD revision — which is a *future*
  revision on the fresh cluster, so the watch waits silently and delivers nothing (no
  error to catch). The 30-second `get_prefix` comparison is a correctness backstop, not
  the delivery mechanism — updates still arrive pushed, at watch latency; the sweep only
  tears down a watch that has silently gone stale. During quorum loss the sweep's read
  raises and the loop retries, keeping the last-known view (stale beats empty).
- Expose the debug route so humans (and end-to-end tests) can see the live view:
  ```python
  @app.get("/discovery/<keyspace>")
  def discovery_view():
      return {"keyspace": "/services/<keyspace>/", "workers": dict(WORKERS)}
  ```
- Resync-from-scratch on ANY watch error (cluster recreate invalidates revisions — a
  fresh `get_prefix` baseline is always correct). Use the worker map for whatever the
  listener's description says (e.g. client-side balancing across `WORKERS.values()`).
- Same deps, same rebuild (incl. the lb force-recreate), then flip this **listener's**
  `"implemented": true` in `etcd.json` (under its keyspace's `listeners`).

## Update / delete

- **Update** (description changed): read the existing loop first, modify in place, keep
  everything else untouched, rebuild that one service. The registry entry is already
  updated by the app; `implemented` stays true.
- **Delete**: the app has already removed the registry entry and scrubbed the compose
  env/mount (`wasImplemented` told the frontend to launch you). Strip the loop (and the
  `/discovery/*` route for a listener; drop the etcd deps from requirements.txt only if
  nothing else in that service uses them), rebuild that one service.
- The app BLOCKS deleting a keyspace that still has listeners, deleting a watched
  service, and deleting the cluster while keyspaces exist (`remove.js` dependents) — so
  you'll never be asked to orphan a watcher.

## Verify

```bash
COMPOSE=systems/<id>/docker-compose.yml
# 1. registration: every worker listed with host:port
docker compose -f $COMPOSE exec -T etcd-1 etcdctl get --prefix /services/<service>/
# 2. the lease is real: TTL counts down between keepalives
docker compose -f $COMPOSE exec -T etcd-1 etcdctl lease list
# 3. liveness: kill a worker -> its key vanishes within one TTL; restart -> it returns
docker compose -f $COMPOSE kill <service>   # or one instance of an lb'd service
docker compose -f $COMPOSE start <service>
# 4. watcher: the live view tracks 1-3 (and the KEY row trace on the diagram lights up)
curl -s http://localhost:8080/<listener>/discovery/<service>
# 5. push, not poll: watch from the host while killing/starting a worker
docker compose -f $COMPOSE exec -T etcd-1 etcdctl watch --prefix /services/<service>/
```

The UI-side checks: the etcd node's `keys` metric counts the worker keys; the Keyspaces
tab lists them live; killing the leader from the Cluster tab moves the ⭘ ring on the
member strip; stopping ⌈N/2⌉ members turns the node red and registrations error-loop
until quorum returns (then everything self-heals — that's the demo).
