---
name: sandbox-redis-topology
description: >-
  Retrofit the writer/reader services of a redis node in a "Distributed Systems Sandbox" system
  (systems/<id>/) after its TOPOLOGY changed — standalone ↔ replicated-with-Sentinel ↔ Redis
  Cluster — and implement per-keyspace WAIT write modes. Use whenever a redis Topology apply just
  ran (the web app already reconciled containers/scrape/manifest) and the attached services' code
  must catch up: Sentinel-based master discovery for writes, RedisCluster clients with MOVED
  handling and hash-tag awareness, `r.wait(numreplicas, timeoutMs)` pseudo-sync writes, or the
  strip-back to a plain single-host client. Covers the `sentinel` / `redisCluster` manifest
  blocks, the keyspace `writeModes` contract, the per-mode client wiring, and the docker
  rebuild/verify steps.
---

# Redis topology retrofits (Sentinel · Cluster · WAIT)

You are operating inside the "Distributed Systems Sandbox" web app. Each system lives in
`systems/<id>/`; the frontend (`npm run dev`) reads these files live, so **never run `./start.sh` /
`./stop.sh`**. Rebuild with `docker compose` directly.

A redis database's **topology is MECHANICAL**: the web app's `POST /api/redis/topology`
(`frontend/server/redisTopology.js`, driven by the redis node's **Topology** tab) provisions or
tears down the containers, scrape jobs and manifest blocks itself — **sessions never provision
topology**. Your job is the judgment half: making the services that USE the redis speak to the new
shape, then rebuilding only them. You are usually launched right after an apply, with the affected
services and their keyspace roles in the prompt.

## The identity model

One user-created redis node `<name>` (`origin: "create-database"`), three shapes:

- **standalone** — a single `<name>` container. No extra manifest keys.
- **replicated** — `<name>` (the primary, `role:"primary"`) + N read replicas `<name>-1..N`
  (separate manifest nodes carrying `replicaOf:"<name>"`, real `--replicaof` containers) + a
  3-sentinel monitor. The sentinels are **containers, not nodes** (`<name>-sentinel-1..3`,
  port 26379), tracked on the primary as:
  ```json
  "sentinel": { "size": 3, "quorum": 2, "masterName": "<name>",
                "members": ["<name>-sentinel-1", "…-2", "…-3"],
                "downAfterMs": 5000, "failoverTimeoutMs": 10000 }
  ```
- **cluster** — a real Redis Cluster. **No `<name>` container exists**; the members
  `<name>-1..M` (M = shards × (1 + replicasPerShard), port 6379, hostnames announced) are
  containers behind the ONE manifest node, which carries:
  ```json
  "redisCluster": { "shards": 3, "replicasPerShard": 1, "members": ["<name>-1", "…"] }
  ```
  A one-shot `<name>-cluster-init` forms the cluster and replays the keyspace seeds.

`sentinel` and `redisCluster` are mutually exclusive. The node's `keyspaces` block (see the
sandbox-database skill) is unchanged by topology — but each keyspace may now carry per-writer
**write modes**:

```json
"writeModes": { "<writerId>": { "mode": "wait", "numreplicas": 1, "timeoutMs": 500,
                                 "implemented": false, "updatedAt": "…" } }
```

Absent writer ⇒ async (the default; never stored). `implemented` is owned by the keyspace SCAN
(`POST /api/redis/scan`), which greps the writer's source for a real WAIT call — your retrofit is
what flips it.

## Client contracts (redis-py; the services are FastAPI + `redis` pip package)

**Replicated — writes MUST discover the master through sentinel.** After a failover the promoted
replica is the master; writes to a hardcoded `<name>` host fail with `ReadOnlyError`. Wire a
module-level:

```python
from redis.sentinel import Sentinel
_sentinel = Sentinel([("<name>-sentinel-1", 26379), ("<name>-sentinel-2", 26379),
                      ("<name>-sentinel-3", 26379)], socket_timeout=1.0)
r = _sentinel.master_for("<masterName>", decode_responses=True)   # writes (and safe for reads)
r_read = _sentinel.slave_for("<masterName>", decode_responses=True)  # optional read scaling
```

`master_for` re-resolves on every reconnect, so the handle survives failovers. `masterName` is the
node id (see the `sentinel` block).

**Cluster — a cluster-aware client, started from the member list.** Plain clients die on the first
`MOVED` redirect:

```python
from redis.cluster import RedisCluster, ClusterNode
r = RedisCluster(startup_nodes=[ClusterNode("<name>-1", 6379), ClusterNode("<name>-2", 6379)],
                 decode_responses=True)
```

Needs `redis>=4.3` — check/bump the service's `requirements.txt`. The client follows MOVED/ASK
itself. **Multi-key operations only work within one hash slot**: if the service pipelines or
MULTIs across keys, co-locate them with hash tags (`{user:42}:profile`, `{user:42}:sessions`). A
*prefix* keyspace's keys spread across all shards by design — that's the point of sharding.

**Standalone (the strip-back).** Converting back means removing the Sentinel/Cluster imports and
restoring `redis.Redis(host="<name>", port=6379, decode_responses=True)`. Leave no dead sentinel
member lists behind.

**WAIT write mode (pseudo-synchronous replication).** For each keyspace the service writes with
`writeModes[<service>].mode == "wait"`, call WAIT immediately after the write:

```python
r.hset(f"session:{sid}", mapping=data)
acked = r.wait(<numreplicas>, <timeoutMs>)          # blocks ≤ timeoutMs
if acked < <numreplicas>:
    log.warning("degraded ack on session:*: %d/%d replicas", acked, <numreplicas>)
```

WAIT does not roll back — it *reports* how many replicas acknowledged; a short count is a degraded
ack to log (or handle per the endpoint's description), not an exception. In cluster mode WAIT runs
against the key's shard master and acks that shard's replicas. Writers without a write mode stay
fire-and-forget — do NOT add WAIT calls nobody declared.

## Procedure

1. Read the redis node in `systems/<id>/manifest.json` — its `sentinel`/`redisCluster` block and
   `keyspaces` (writers/readers + `writeModes`) are the ground truth; the prompt summarizes them.
2. For every affected service (`systems/<id>/<service>/app.py`), rewire its redis connection per
   the mode contract above. Keep the metrics middleware and unrelated routes untouched. A
   load-balanced service's code lives in the same `./<service>` folder (one build context for all
   instances) — edit once, rebuild the entry.
3. Wire declared WAIT calls at each write site of that keyspace (match by key name/prefix).
4. Rebuild ONLY the touched services:
   ```
   docker compose -f systems/<id>/docker-compose.yml up -d --build <svc-a> <svc-b>
   ```

## Verify

Replicated:
```
docker compose -f systems/<id>/docker-compose.yml exec -T <name>-sentinel-1 \
  redis-cli -p 26379 sentinel get-master-addr-by-name <name>     # → the primary
docker compose -f systems/<id>/docker-compose.yml exec -T <name> redis-cli WAIT <n> 1000
# → replica count; a writer endpoint through the lb must succeed and its WAIT log stay quiet
docker compose -f systems/<id>/docker-compose.yml kill <name>    # failover drill
# … within ~10s get-master-addr-by-name names a replica; the writer endpoint STILL works
docker compose -f systems/<id>/docker-compose.yml start <name>   # rejoins as replica
```

Cluster:
```
docker compose -f systems/<id>/docker-compose.yml exec -T <name>-1 redis-cli cluster info | grep cluster_state
docker compose -f systems/<id>/docker-compose.yml exec -T <name>-1 redis-cli -c GET <some key>
# exercise each retrofitted endpoint through the lb: curl http://localhost:8080/<svc>/<path>
```

Then re-run the keyspace scan so wait-mode writers flip to implemented (the tab shows the badge):
```
curl -s -X POST localhost:5173/api/redis/scan -H 'Content-Type: application/json' \
  -d '{"system":"<id>","id":"<name>"}' | python3 -m json.tool | grep -A2 waitChecks
```
