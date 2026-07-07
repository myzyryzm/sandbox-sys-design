---
name: sandbox-service-lb
description: >-
  Put a per-service load balancer in front of a service in a "Distributed Systems Sandbox" system
  (systems/<id>/) — run it as N real instances behind its own haproxy sidecar, transparently, so
  every existing caller balances with no code changes. Use whenever the task is to enable/scale/
  disable per-service load balancing, change its algorithm, or work on the code of a service that is
  load balanced. Covers the cluster-entry identity model (the `<name>` node becomes the sidecar and
  keeps owning its endpoints/gRPC; instances are `<name>-1..N` carrying `instanceOf`), the
  transparent-routing invariant (nginx untouched; `<name>` DNS fronts the cluster), the haproxy.cfg
  contract + algorithm set, the cluster-aware rebuild, and how resilience stays a single cluster
  breaker.
---

# Per-service load balancing

You are operating inside the "Distributed Systems Sandbox" web app. Each system lives in
`systems/<id>/`; the frontend (`npm run dev`) reads these files live, so **never run `./start.sh` /
`./stop.sh`**. Rebuild with `docker compose` directly.

A **load-balanced service** runs as **N interchangeable instances** behind its **own haproxy
sidecar**. The whole flow is **mechanical** — the web app's `POST /api/service-lb`
(`frontend/server/serviceLb.js`) enables / scales / re-balances / disables it in one docker rebuild,
with **no launched Claude session**. You'll mostly read this skill when writing the *code* of a
service that happens to be load balanced (adding an endpoint, gRPC, resilience) and need to know the
identity model so you don't fight it.

## The identity model — the `<name>` node BECOMES the cluster entry

Enabling load balancing on service `<name>` restructures it **without migrating any registry**:

- **`<name>` (the node) stays** — it flips `type: "service" → "service-lb"` and becomes the **cluster
  entry**: its compose service is swapped from the FastAPI `build` to an **haproxy sidecar** that
  **binds the same `:8000`**, so it keeps the `<name>` network name. Its metrics become haproxy's
  (sessions / req-s / instances-up), scraped at `<name>:8405`. It **keeps owning `endpoints.json`,
  gRPC, and consumers under `<name>`** — nothing moves.
- **Instances `<name>-1 … <name>-N`** are added: real containers, each `build: ./<name>` (the same
  image/folder), each its own Prometheus job `<name>-i` scraping `<name>-i:8000`. Each is a
  `type:"service"` manifest node carrying **`instanceOf: "<name>"`** (the grouping key, like
  `wsTier`/`replicaOf`). They serve the same routes but are **never addressed individually**.
- The `<name>` node carries **`svcLb: { algorithm, instances: ["<name>-1", …] }`** and
  `loadBalanced: true`, plus balancing edges `{ from:"<name>", to:"<name>-i", origin:"service-lb" }`.

**Transparency (the whole point): nginx.conf is NEVER touched.** The main lb's
`upstream <name> { server <name>:8000; }` and every internal `http://<name>:8000/…` call resolve to
the haproxy sidecar and balance automatically. `/<name>/…` stays routable and discoverable
(endpoints are still owned by `<name>`; discovery goes through the sidecar to a real instance).

Disabling (instances → 1) reverses it exactly: `<name>` goes back to a `build` FastAPI service,
instances + sidecar + the `<name>-lb/` config folder are removed, endpoints untouched.

## haproxy.cfg (generated — do not hand-edit server lines)

`systems/<id>/<name>-lb/haproxy.cfg`, rendered from the node's `svcLb` block (mirrors the websocket
tier's cfg, but **HTTP mode**): `mode http`, `balance <algorithm>`, **`option httpchk GET /health`**
(this is the per-instance ejection the uniform breaker relies on), one `server` line per instance
with `check resolvers docker init-addr libc,none` (runtime DNS re-resolution so a recreated instance
recovers without an lb restart), and haproxy's native prometheus exporter on a `stats` frontend
(`:8405`). Algorithms: **`roundrobin | leastconn | source`**. Change the algorithm or instance count
only via `POST /api/service-lb` (the Load Balancing tab), which regenerates the cfg and recreates the
sidecar — never edit the server lines by hand.

## Rebuilding a load-balanced service's code

A load-balanced service's code lives in its **instances** (`<name>-1..N`, all `build: ./<name>`),
**not** in `<name>` (now image-only haproxy). The shared `rebuild(system, '<name>')`
(`frontend/server/scaffold.js`) is **cluster-aware**: it builds every `instanceOf` sibling and
recreates the sidecar. So the normal single-service rebuild after editing `<name>/app.py`:

```
docker compose -f systems/<id>/docker-compose.yml build <name>-1 <name>-2 …   # every instance
docker compose -f systems/<id>/docker-compose.yml up -d
docker compose -f systems/<id>/docker-compose.yml up -d --force-recreate <name>   # the sidecar
docker compose -f systems/<id>/docker-compose.yml exec -T lb nginx -t
docker compose -f systems/<id>/docker-compose.yml restart prometheus
```

Because all instances share `./<name>`, a code edit must rebuild **all** of them (the cluster-aware
`rebuild` does this for you). Any `SERVICE_ID` / manifest mount already wired onto `<name>` (e.g. a
resilience wrapper's) is cloned onto every instance, so behavior is uniform across the cluster.

## Resilience is one cluster breaker

A resilience policy targeting a load-balanced service is a **single cluster-level breaker** keyed
`<from>-><name>` — the caller reaches the cluster transparently through the sidecar, and haproxy's
health checks eject a failing instance underneath. Do **not** split it per instance. See
[[sandbox-resilience]] (its runtime engine is unchanged: `<name>` is just a normal `to`).

## Edges / limits

- **gRPC server role.** haproxy `mode http` won't proxy gRPC (h2). Load-balancing a service that
  **serves** a gRPC contract is out of scope for now (HTTP endpoints and gRPC *client* roles are
  unaffected).
- **Kafka consumers scale with instances.** All N instances run the same `app.py`, so a consumer
  function runs in each → N members in the consumer group sharing partitions (correct, scaled
  consumption).
- **Per-instance shutdown** isn't a UI action (the group is edited as a unit). To exercise a single
  instance failing, `docker compose … stop <name>-2` directly.

## Verify

1. Enable via a service's **Edit ▸ Load Balancing** (instances = 3, roundrobin). The diagram shows
   the `<name>` sidecar at the group's left-middle, a dotted box around the stacked `<name>-1..3`
   with the service's methods + Edit on the entry, and Prometheus healthy for all four jobs.
2. `docker compose -f systems/<id>/docker-compose.yml ps` → `<name>` (haproxy) + `<name>-1..3`.
3. `for i in $(seq 20); do curl -s localhost:8080/<name>/<route>; done` — watch `haproxy_server_*`
   sessions spread across the instances on the entry node.
4. `docker compose … stop <name>-2` — haproxy ejects it (instances-up drops); calls keep succeeding
   from the survivors.
5. Attach resilience `caller → <name>` → one breaker circle on the caller→entry edge; stopping ALL
   instances trips it OPEN. Set instances back to 1 → plain service restored, endpoints intact.
