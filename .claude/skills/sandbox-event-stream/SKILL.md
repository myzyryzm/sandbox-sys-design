---
name: sandbox-event-stream
description: >-
  Add, update, or delete an event stream (Kafka) in a "Distributed Systems Sandbox"
  system (systems/<id>/). Use whenever the task is to provision a Kafka cluster, manage
  its topics, declare which services produce to or consume from a topic, attach a message
  schema (model-bank reference) to a topic so consumers know what to expect, or remove the
  cluster — it covers the broker + exporter compose services, the Prometheus scrape job,
  the manifest node + producer/consumer edges, the streams.json topic registry, and the
  docker rebuild/verify steps.
---

# Working on a sandbox event stream

You are operating inside the "Distributed Systems Sandbox" web app. Each system lives in
`systems/<id>/`; your session's system prompt names the specific `<id>` and inlines its
current `manifest.json`. The web frontend runs under `npm run dev` and reads these files
live, so **never run `./start.sh`** — it tears down the dev server you're attached to.
Rebuild with `docker compose` directly (commands below).

Today the only event-stream engine is **Kafka** (single broker, KRaft mode — no Zookeeper),
provisioned by the web app's "Add event stream" button (`POST /api/event-streams`). When you
work by hand from a terminal session, reproduce the same shape. Related: the
[[sandbox-database]] and [[sandbox-endpoint]] skills work the same way for datastores/routes.

## The five places an event stream lives (working dir is the repo root)

1. `systems/<id>/<cluster>/streams.json` — the **topic registry**. This is what the diagram's
   read-only topics modal and the producer/consumer edges are drawn from (a Kafka broker
   can't report producers, and only sees consumer-group membership while clients are
   connected, so this is declarative). Shape:
   ```json
   { "topics": [
     { "id": "orders",
       "producers": ["service-1"],
       "consumers": [ { "groupId": "fulfillment", "members": ["service-2"] } ],
       "schemaModel": "OrderEvent",
       "enforceSchema": false } ] }
   ```
   `schemaModel` (optional) names a model in the bank (`systems/<id>/models.json`) that is the
   topic's **message contract** — what a consumer should expect when reading the topic. It's the
   same reusable TypeScript a `requestModel`/`responseModel` endpoint uses; resolve referenced
   models from `models.json`. `enforceSchema` (optional, default false) says whether producer/
   consumer code must **validate** messages against it at runtime. Both are absent when a topic
   has no declared schema.
2. `systems/<id>/docker-compose.yml` — **three** services: `<cluster>` (the `apache/kafka`
   broker), `<cluster>-exporter` (`danielqsj/kafka-exporter`, scraped at `:9308`), and
   `<cluster>-init` (a one-shot `apache/kafka` sidecar that waits for the broker, then
   `kafka-topics.sh --create`s each declared topic).
3. `systems/<id>/prometheus/prometheus.yml` — a **scrape job** named exactly `<cluster>`,
   target `<cluster>-exporter:9308`. The `job_name` is the `job="<cluster>"` label metrics use.
4. `systems/<id>/manifest.json` — a **node** `{ id:<cluster>, label:"Kafka · <cluster>",
   type:"kafka", origin:"create-event-stream", position, metrics, health }`. Metric queries
   are PromQL filtered on `job="<cluster>"` (topics, partitions, msgs/s, consumer lag).
5. `systems/<id>/manifest.json` **edges** — a new cluster is **not** wired to any service.
   To make a producer/consumer relationship show on the diagram, add edges
   `{from:<producer-service>, to:<cluster>}` and `{from:<consumer-service>, to:<cluster>}`
   (both a producer and a consumer point AT the cluster — the arrow is "connects to the stream").
   The diagram tells them apart by style: a producer/plain edge is solid gray; a consumer edge
   (one tagged `origin:"consumer-fn"`) renders amber + dashed.

### Conventions (the delete path and PromQL depend on these)
- `<cluster>` is the node id **and** the primary compose service name **and** the folder name.
  Lowercase, digits, hyphens, starts with a letter (e.g. `events`).
- `origin: "create-event-stream"` marks it frontend-deletable — keep it.
- Health rules are the shared pair: `[{color:"red", when:"value < 1"}, {color:"green", when:"value >= 1"}]`.
- Topic ids match `^[a-zA-Z0-9._-]+$`.
- Kafka is a **binary protocol, not HTTP**, so there is **no nginx route** (unlike a service).

### Talking to the broker (all via `docker compose exec`)
```
# list / create / describe topics:
docker compose -f systems/<id>/docker-compose.yml exec -T <cluster> \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server <cluster>:9092 --list
docker compose -f systems/<id>/docker-compose.yml exec -T <cluster> \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server <cluster>:9092 \
  --create --if-not-exists --topic <topic> --partitions 1 --replication-factor 1
# inspect live consumer groups:
docker compose -f systems/<id>/docker-compose.yml exec -T <cluster> \
  /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server <cluster>:9092 --describe --all-groups
```

## Rebuilding (you run INSIDE the web app's dev server)

- Editing only `manifest.json` or `streams.json` appears within seconds — no rebuild.
- After compose / prometheus changes:
  ```
  docker compose -f systems/<id>/docker-compose.yml up -d
  docker compose -f systems/<id>/docker-compose.yml restart prometheus
  ```
- Confirm the exporter is scraped at `http://localhost:9090/targets` (job `<cluster>`).

## Add an event stream

(The web app's "Add event stream" button does all of this; by hand:)
1. Pick `<cluster>` and the initial topics. Add the broker + `-exporter` + `-init` services to
   `docker-compose.yml` (copy the shape the button emits / an existing cluster).
2. Add a scrape job `job_name: <cluster>` → `<cluster>-exporter:9308` to `prometheus/prometheus.yml`.
3. Add the manifest node (`type:"kafka"`, `origin:"create-event-stream"`, metrics + health on
   `job="<cluster>"`, a free `position`).
4. Write `systems/<id>/<cluster>/streams.json` with the topics (`producers`/`consumers` empty).
5. `docker compose ... up -d` then `restart prometheus`. The `-init` sidecar creates the topics.
6. Verify: `kafka-topics.sh --list` shows the topics; the node appears with live metrics.

## Update an event stream

- **Add/remove a topic**: create it on the live broker (`kafka-topics.sh --create`, or
  `--delete`), and add/remove its entry in `streams.json`. When the task supplies a message
  schema, write `"schemaModel": "<Model>"` (and `"enforceSchema": true` if it's to be enforced)
  into the new entry. No rebuild — the read-only modal re-reads the registry and re-checks the
  broker live.
- **Set / change a topic's message schema**: set/clear `schemaModel` (and `enforceSchema`) on
  the topic's entry in `streams.json` — `schemaModel` must be a model that exists in
  `models.json`. This alone is a registry edit, **no rebuild**; the topic view resolves and shows
  the model's TypeScript so consumers see the expected shape. (The "Set message schema" control
  in the Topics tab does this via `POST /api/event-stream`.)
- **Enforce a topic's message schema**: when a topic has `enforceSchema: true` *and* real
  producers/consumers, make their code validate against `schemaModel` (resolve it + any models it
  references from `models.json`):
  - each **producer** validates every outgoing payload against the model before `send()` (raise/
    reject on mismatch);
  - each **consumer** parses/validates every message against the model after read;
  - then rebuild **only** those producing/consuming services
    (`docker compose -f systems/<id>/docker-compose.yml up -d --build <service>`) and verify.
  When `enforceSchema` is false the schema is documentation only: shape payloads to match it, but
  don't add hard validation.
- **Wire a producer or consumer** (this is the common task): edit `streams.json` to add the
  service to a topic's `producers`, or to a consumer group under `consumers`
  (`{groupId, members:[<service>]}`). Then add the matching manifest **edge(s)** so the
  diagram draws the relationship: `{from:<service>, to:<cluster>}` for a producer,
  `{from:<service>, to:<cluster>}` for a consumer too (both point at the cluster). (If a service should really publish/subscribe,
  implement that in its `app.py` too — see [[sandbox-endpoint]] — but the diagram only needs the
  registry + edges.) **If the topic has a `schemaModel`, shape the message payload to that model**;
  if `enforceSchema` is true, also add the produce/consume validation above and rebuild the service.
- **Change which metrics show**: edit the node's `metrics` in `manifest.json` — no rebuild.

## Consumer function (a service consuming a topic via a named poll loop)

A **consumer function** is a first-class "service X consumes topic T of cluster C" object created from
the event stream's **Consumers** tab (`POST /api/consumers`). It is owned by one internal service
(identity `(service, name)`, like an endpoint alias) and registered in `systems/<id>/consumers.json`:
```json
{ "consumers": [
  { "service": "service-1", "name": "processRefunds", "cluster": "refund-stream",
    "topic": "refunds", "pollRate": 1000, "downstream": ["ledger-db"],
    "downstreamDescriptions": { "ledger-db": "Writes a refund row per consumed message." },
    "description": "…", "implemented": false,
    "conversationId": "…", "createdAt": "…", "updatedAt": "…", "history": [ … ] } ] }
```
(`downstream` + `downstreamDescriptions` are Claude-managed — the node ids the loop calls/reads/writes
and a one-line blurb per id; absent until you fill them in. See the implement step below; they're what
make the diagram draw the consumer's outbound lines and print each connection's description on the trace.)
The app has **already** done the mechanical scaffold before launching you (do NOT redo these):
the consumers.json entry exists; the consumer group `{groupId:"<service>-<name>", members:["<service>"]}`
is registered under the topic in `<cluster>/streams.json`; and the manifest edge
`{from:<service>, to:<cluster>, origin:"consumer-fn"}` is added (this is what draws the
`<service> → <cluster>` line + lets the diagram trace it). **Your job is the CODE half:** implement
(or update) the real poll loop in the service and rebuild that one service.

- **Implement** a background Kafka consumer in `systems/<id>/<service>/app.py` (the service is a
  FastAPI app, so run the loop in a **daemon thread** started at import/startup — never block the
  request path). Use `kafka-python` (the same client the CDC worker uses — add `kafka-python` to
  `systems/<id>/<service>/requirements.txt`).

  **The loop MUST be pause-aware** — the Topics tab has a cluster-level "Pause consumers" toggle
  that flips a top-level `consumersPaused` flag in `<cluster>/streams.json`, and consumers honor it
  *live* (no rebuild). So two things are required:
  1. **Mount the registry read-only** on the service in `docker-compose.yml` (same idiom as the CDC
     worker's `cdc.json`), so the loop can read the flag:
     ```yaml
     <service>:
       volumes:
         - ./<cluster>/streams.json:/streams/<cluster>.json:ro   # live consumersPaused flag
     ```
  2. **Write the loop as a `while True` poll** (NOT `for msg in consumer:` — a paused consumer
     yields nothing and the iterator would `StopIteration` and kill the thread) that checks the flag
     each cycle, re-reading the file only when its mtime changes and keeping last-good on a mid-write
     read (the pattern `resilience/engine.py` uses), and pause/resume the assigned partitions:
  ```python
  from kafka import KafkaConsumer
  import threading, json, os, time
  _PAUSE = "/streams/<cluster>.json"
  _pc = {"mtime": 0, "paused": False}
  def _consumers_paused():
      try:
          m = os.stat(_PAUSE).st_mtime
      except OSError:
          return _pc["paused"]
      if m != _pc["mtime"]:
          try:
              _pc["paused"] = bool(json.load(open(_PAUSE)).get("consumersPaused"))
              _pc["mtime"] = m
          except Exception:        # mid-write file: keep last-good
              pass
      return _pc["paused"]
  def _consume_<name>():
      consumer = KafkaConsumer(
          bootstrap_servers="<cluster>:9092",
          group_id="<service>-<name>",
          auto_offset_reset="earliest",
          value_deserializer=lambda b: b,
      )
      consumer.subscribe(["<topic>"])
      while True:
          if _consumers_paused():
              if consumer.assignment():
                  consumer.pause(*consumer.assignment())   # stop fetching; offsets/lag hold
              time.sleep(1)
              continue
          if consumer.paused():
              consumer.resume(*consumer.paused())          # resume where we left off
          for records in consumer.poll(timeout_ms=<pollRate>).values():
              for msg in records:
                  ...  # process per the description
  threading.Thread(target=_consume_<name>, daemon=True).start()
  ```
  Keep the existing metrics middleware and every other route/loop untouched. If the topic has a
  `schemaModel`, shape/parse the message to that model; if `enforceSchema` is true, validate it.
- **Record the loop's `downstream` + `downstreamDescriptions`** in this consumer's `consumers.json`
  entry. `downstream` is the array of node ids the loop CALLS / reads / writes — every in-system
  service whose endpoint it hits (e.g. an `<api>` it POSTs to), every database it queries, every
  cluster it produces to. This is exactly an endpoint's `downstream`, and the diagram draws a
  persistent `<service> → <node>` line for each (the `<service> → <cluster>` consume edge is separate,
  already added). **Without it the diagram shows the consumer reading the topic but NOT what it then
  does** — so a loop that calls `payout-api` and writes `ledger-db` must carry
  `"downstream": ["payout-api", "ledger-db"]`. Alongside it, write `downstreamDescriptions` — a map
  (node id → one short line) of what the loop uses each connection for, keys kept a subset of
  `downstream` (e.g. `{"payout-api": "POSTs a payout per message.", "ledger-db": "Records the debit."}`);
  the diagram prints these on the trace when the consumer's `CONS` row is clicked, exactly as it does
  for an endpoint. Edit the entry directly (both are Claude-managed metadata; the modal never sends
  them). Keep both in sync on every update — add/drop a node and its blurb as the loop's calls change.
- **Rebuild ONLY that service**, then mark it done:
  ```
  docker compose -f systems/<id>/docker-compose.yml up -d --build <service>
  ```
  Then set `"implemented": true` on this entry in `consumers.json` (match by `service` + `name`).
- **Update** (topic / poll-rate change): read the existing loop in `app.py` and modify it in place
  (new `subscribe([...])` topic / `poll(timeout_ms=...)` cadence), keep the same group id and the
  pause-aware `while True` structure, rebuild that service, leave `implemented` true.
- **Rename** (`PUT /api/consumers {service, oldName, newName}` from the Consumers tab's per-row
  Rename action): the consumers.json entry and its streams.json consumer group are renamed by the app
  first (`<service>-<oldName>` → `<service>-<newName>`). For an **implemented** consumer you only do
  the CODE half: in `app.py` rename the loop function `_consume_<oldName>` → `_consume_<newName>` (and
  its `threading.Thread(...).start()`) and change that consumer's `group_id` from `<service>-<oldName>`
  to `<service>-<newName>` — leave the topic, poll cadence, pause-awareness and every other loop intact
  — then rebuild only that service. The new group id is a fresh Kafka consumer group, so it starts from
  `auto_offset_reset` (earliest). A **pending** (not-yet-implemented) consumer is registry-only — nothing
  to do in code.
- **Delete**: the consumers.json entry, the streams.json group, and the manifest edge are removed by
  the app first; you only strip the matching loop (group id `<service>-<name>`) from `app.py` and
  rebuild that service.
- **Update connection descriptions only** (the Consumers tab's "Update descriptions" button): if the
  task is to (re)generate just this consumer's connection metadata, read the loop in `app.py` and edit
  **only** its `downstream` list and `downstreamDescriptions` map on its `consumers.json` entry (match
  by `service` + `name`) — `downstream` = every node id the loop calls/reads/writes, `downstreamDescriptions`
  = one short line per id. This is a **pure JSON edit**: do not touch `app.py`, and do **not** rebuild.
  (Also how you backfill descriptions on a consumer created before `downstreamDescriptions` existed.)
- **Verify**: `kafka-consumer-groups.sh --describe --all-groups` lists group `<service>-<name>`; the
  cluster's `lag` metric reflects consumption; the service's `CONS <name>` row traces on the diagram
  (service → cluster + service → each `downstream`, each downstream line labelled with its `downstreamDescriptions`
  blurb), and a persistent line runs from the service to every `downstream` node it calls/reads/writes.

### Pause consumers (cluster-level kill switch)

The Topics tab's **Pause consumers** checkbox writes a top-level `"consumersPaused": true` to
`<cluster>/streams.json` (`POST /api/event-stream { system, id, consumersPaused }` — a pure registry
write, **no rebuild, no session**). Every pause-aware consumer loop (above) mounts that file and stops
fetching within one poll cycle; unchecking it removes the flag and they resume from their committed
offsets. While paused, producers keep writing, so the cluster's `lag` metric climbs, then drains on
resume. The diagram badges paused clusters (`GET /api/consumer-pause` lists them). A consumer authored
**before** this convention won't honor the flag until rebuilt once with the mount + `while True` loop.

## Delete an event stream

Remove it from **all five** places, then reconcile (the web app's ✕/delete does this):
1. `docker-compose.yml`: delete `<cluster>`, `<cluster>-exporter`, `<cluster>-init`, and scrub
   any `depends_on` references to them.
2. `prometheus/prometheus.yml`: delete the `job_name: <cluster>` scrape job.
3. `manifest.json`: remove the node **and any edges** with `from`/`to` equal to `<cluster>`.
4. Delete the `systems/<id>/<cluster>/` folder (with its `streams.json`).
5. Reconcile:
   ```
   docker compose -f systems/<id>/docker-compose.yml up -d --remove-orphans
   docker compose -f systems/<id>/docker-compose.yml restart prometheus
   ```
6. Verify the node is gone and `docker compose ... ps` no longer lists the cluster services.
