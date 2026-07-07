---
name: sandbox-database-cdc
description: >-
  Build the Change Data Capture (CDC) worker for a database in a "Distributed Systems
  Sandbox" system (systems/<id>/). Use when a CDC rule was added to a
  postgres/mongodb/dynamodb/cassandra database and a real per-database worker container
  (`<db>-cdc`) must be authored to stream row changes (INSERT/UPDATE/DELETE) to a Kafka
  topic. Covers the worker's Dockerfile/app.py, postgres logical replication / mongo
  change streams / DynamoDB Streams / Cassandra polling capture, Kafka production, the
  metrics it exports, and the docker rebuild/verify steps.
---

# Building a sandbox CDC worker

You are operating inside the "Distributed Systems Sandbox" web app. Each system lives in
`systems/<id>/`; your session's system prompt names the specific `<id>` and inlines its
current `manifest.json`. The web frontend runs under `npm run dev` and reads these files
live, so **never run `./start.sh`** — it tears down the dev server you're attached to.

CDC capture is **real**: a per-database worker container (`<db>-cdc`) streams changes
from the database's own replication mechanism and produces them to a Kafka broker. There
is no mock layer. **Your job is to author that worker's code and build it.**

## What the backend already did (do NOT redo)

When the first CDC rule was added in the modal, the backend (`frontend/server/cdc.js`)
already performed the mechanical scaffold:

- Wrote `systems/<id>/<db>/cdc.json` — the rule list, **mounted into the worker at
  `/cdc.json:ro`**.
- **Enabled the engine for CDC** (postgres/mongodb only) and recreated the database container:
  - postgres → added `command: postgres -c wal_level=logical -c max_wal_senders=10 -c max_replication_slots=10`.
  - mongodb → added `command: mongod --replSet rs0 --bind_ip_all` and ran `rs.initiate(...)`
    (single-node replica set `rs0`).
  - dynamodb → **nothing to enable**: the tables were created with DynamoDB Streams on
    (`StreamViewType=NEW_AND_OLD_IMAGES`), so the db container is untouched — tail the streams.
  - cassandra → **nothing to enable**: the db container is untouched. Use **polling capture**
    (query each table on an interval and diff), NOT commitlog CDC — see the Cassandra section.
- Added the `<db>-cdc` compose service (`build: ./<db>-cdc`, the env below, the cdc.json
  mount), its Prometheus scrape job (job `<db>-cdc`, target `<db>-cdc:8000`), the manifest
  node (`type: "cdc"`, `origin: "create-cdc"`, `cdcOf: "<db>"`) and edges
  (`<db>` → `<db>-cdc` → each target stream), and registered `<db>-cdc` as a producer in
  each target stream's `streams.json`.

So **do not** touch `cdc.json`, `streams.json`, the manifest, the compose service
definition, the scrape job, nginx, or the database's `wal_level`/replica-set setup. Only
author the worker dir and build it.

> The `<db>-cdc` node has no Edit/Delete on the diagram — it is managed entirely from the
> database's **CDC** tab. Removing the last rule there tears the worker down (and the
> backend drops the postgres slot); adding/removing other rules just rewrites `cdc.json`
> and restarts the worker — no new session.

## The worker dir — `systems/<id>/<db>-cdc/`

Author three files, then build. Working directory is the repo root.

- `requirements.txt` — engine driver + Kafka client + metrics:
  - postgres: `psycopg2-binary`, `kafka-python`, `prometheus_client`
  - mongodb: `pymongo`, `kafka-python`, `prometheus_client`
  - dynamodb: `boto3`, `kafka-python`, `prometheus_client`
  - cassandra: `cassandra-driver`, `kafka-python`, `prometheus_client` (set `ENV CASS_DRIVER_NO_EXTENSIONS=1` in the Dockerfile to skip the C-extension build)
- `Dockerfile`:
  ```dockerfile
  FROM python:3.12-slim
  WORKDIR /app
  COPY requirements.txt .
  RUN pip install --no-cache-dir -r requirements.txt
  COPY app.py .
  EXPOSE 8000
  CMD ["python", "app.py"]
  ```
- `app.py` — see the per-engine reference below.

### Runtime contract

- **Rules** come from `/cdc.json` (mounted): `{ "rules": [ { table, operations:[…], stream, topic } ] }`.
  Read it at startup; **do not hardcode** the list. Each rule routes one table's
  `operations` to `stream`/`topic`.
- **Connection** comes from env: `CDC_ENGINE`, `CDC_DB_HOST`, `CDC_DB_PORT`, `CDC_DB_NAME`,
  `CDC_DB_USER`, `CDC_DB_PASSWORD`, and `CDC_PG_SLOT` (postgres — the logical replication
  slot name you MUST use, so the backend can drop it on teardown). **DynamoDB** instead gets
  `DDB_ENDPOINT`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION` (no
  `CDC_DB_NAME`). **Cassandra** uses `CDC_DB_NAME` as the keyspace.
- **Kafka**: each rule's broker bootstrap is `<stream>:9092` (PLAINTEXT). Keep one producer
  per distinct stream. Auto-create is OFF, so ensure the topic exists (create it with
  `kafka-python`'s `KafkaAdminClient` if missing — ignore "already exists").
- **Metrics**: hand-written `prometheus_client` on `:8000` (mirror the service template
  `frontend/server/templates/service/app.py` — explicit counters, never an auto-instrumentor):
  `cdc_events_captured_total{table,op}`, `cdc_events_produced_total{topic}`, `cdc_errors_total`.
  Serve them with `start_http_server(8000)` in a daemon thread; run capture in the main thread.

## Postgres — logical decoding with `test_decoding`

`wal_level=logical` is already on. Use the built-in **`test_decoding`** output plugin (it
ships with the official `postgres:16-alpine` image and emits an easy-to-parse text format —
`wal2json` is NOT available, and pgoutput's binary format needs a decoder). `test_decoding`
streams **every** table in the database, so filter by table in code.

```python
import os, json, psycopg2
from psycopg2.extras import LogicalReplicationConnection
from prometheus_client import Counter, start_http_server
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from threading import Thread

CAPTURED = Counter('cdc_events_captured_total', 'changes captured', ['table', 'op'])
PRODUCED = Counter('cdc_events_produced_total', 'events produced', ['topic'])
ERRORS = Counter('cdc_errors_total', 'cdc errors')

rules = json.load(open('/cdc.json'))['rules']
# table -> list of {ops:set, stream, topic}
routes = {}
for r in rules:
    routes.setdefault(r['table'], []).append(
        {'ops': set(r['operations']), 'stream': r['stream'], 'topic': r['topic']})

producers = {}  # stream -> KafkaProducer
def producer_for(stream):
    if stream not in producers:
        try:
            admin = KafkaAdminClient(bootstrap_servers=f'{stream}:9092')
            for r in rules:
                if r['stream'] == stream:
                    try: admin.create_topics([NewTopic(r['topic'], 1, 1)])
                    except Exception: pass
            admin.close()
        except Exception: pass
        producers[stream] = KafkaProducer(
            bootstrap_servers=f'{stream}:9092',
            value_serializer=lambda v: json.dumps(v).encode())
    return producers[stream]

start_http_server(8000)

slot = os.environ['CDC_PG_SLOT']
conn = psycopg2.connect(
    host=os.environ['CDC_DB_HOST'], port=int(os.environ.get('CDC_DB_PORT', 5432)),
    dbname=os.environ['CDC_DB_NAME'], user=os.environ['CDC_DB_USER'],
    password=os.environ['CDC_DB_PASSWORD'], connection_factory=LogicalReplicationConnection)
cur = conn.cursor()
try:
    cur.create_replication_slot(slot, output_plugin='test_decoding')
except psycopg2.errors.DuplicateObject:
    conn.rollback()

def on_msg(msg):
    payload = msg.payload  # e.g. "table public.account: INSERT: id[integer]:1 name[text]:'cash'"
    try:
        if payload.startswith('table '):
            rest = payload[len('table '):]
            ident, after = rest.split(':', 1)
            table = ident.split('.', 1)[1].strip().strip('"')
            op = after.strip().split(':', 1)[0].strip()  # INSERT | UPDATE | DELETE
            for route in routes.get(table, []):
                if op in route['ops']:
                    CAPTURED.labels(table, op).inc()
                    producer_for(route['stream']).send(
                        route['topic'], {'table': table, 'op': op, 'raw': payload})
                    PRODUCED.labels(route['topic']).inc()
    except Exception:
        ERRORS.inc()
    msg.cursor.send_feedback(flush_lsn=msg.data_start)

cur.start_replication(slot_name=slot, decode=True)
cur.consume_stream(on_msg)
```

Notes: `test_decoding` gives the full new tuple for INSERT and the new values for UPDATE;
for UPDATE/DELETE **old** column values, set `ALTER TABLE <t> REPLICA IDENTITY FULL` (the
default identity is the PK only). Always `send_feedback` so the slot advances and WAL is
released.

## MongoDB — change streams

The database is already a single-node replica set `rs0` (change streams require one). Open
one **database-level** change stream and filter by collection + `operationType` in code.

```python
import os, json
from pymongo import MongoClient
from prometheus_client import Counter, start_http_server
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

CAPTURED = Counter('cdc_events_captured_total', 'changes captured', ['table', 'op'])
PRODUCED = Counter('cdc_events_produced_total', 'events produced', ['topic'])
ERRORS = Counter('cdc_errors_total', 'cdc errors')

rules = json.load(open('/cdc.json'))['rules']
OP_MAP = {'insert': 'INSERT', 'update': 'UPDATE', 'replace': 'UPDATE', 'delete': 'DELETE'}
routes = {}
for r in rules:
    routes.setdefault(r['table'], []).append(
        {'ops': set(r['operations']), 'stream': r['stream'], 'topic': r['topic']})

producers = {}
def producer_for(stream, topic):
    if stream not in producers:
        try:
            admin = KafkaAdminClient(bootstrap_servers=f'{stream}:9092')
            try: admin.create_topics([NewTopic(topic, 1, 1)])
            except Exception: pass
            admin.close()
        except Exception: pass
        producers[stream] = KafkaProducer(
            bootstrap_servers=f'{stream}:9092',
            value_serializer=lambda v: json.dumps(v, default=str).encode())
    return producers[stream]

start_http_server(8000)
client = MongoClient(f"mongodb://{os.environ['CDC_DB_HOST']}:{int(os.environ.get('CDC_DB_PORT',27017))}/?replicaSet=rs0")
db = client[os.environ['CDC_DB_NAME']]

with db.watch(full_document='updateLookup') as stream:
    for change in stream:
        try:
            coll = change['ns']['coll']
            op = OP_MAP.get(change['operationType'])
            if not op:
                continue
            for route in routes.get(coll, []):
                if op in route['ops']:
                    CAPTURED.labels(coll, op).inc()
                    producer_for(route['stream'], route['topic']).send(
                        route['topic'],
                        {'table': coll, 'op': op, 'doc': change.get('fullDocument'),
                         'key': change.get('documentKey')})
                    PRODUCED.labels(route['topic']).inc()
        except Exception:
            ERRORS.inc()
```

Notes: DELETE change events carry only `documentKey` (the `_id`) unless pre-images are
enabled on the collection (`changeStreamPreAndPostImages`) — fine for "a row was deleted".

## DynamoDB — Streams

The tables already have Streams on (`NEW_AND_OLD_IMAGES`). Tail them with the
**`dynamodbstreams`** boto3 client: for each captured table, find `LatestStreamArn` from
`describe_table`, list its shards, take a `LATEST` (or `TRIM_HORIZON`) iterator per shard,
and poll `get_records`. `eventName` maps `INSERT→INSERT`, `MODIFY→UPDATE`, `REMOVE→DELETE`.

```python
import os, json, time
import boto3
from prometheus_client import Counter, start_http_server
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

CAPTURED = Counter('cdc_events_captured_total', 'changes captured', ['table', 'op'])
PRODUCED = Counter('cdc_events_produced_total', 'events produced', ['topic'])
ERRORS = Counter('cdc_errors_total', 'cdc errors')

rules = json.load(open('/cdc.json'))['rules']
routes = {}
for r in rules:
    routes.setdefault(r['table'], []).append({'ops': set(r['operations']), 'stream': r['stream'], 'topic': r['topic']})

producers = {}  # stream -> KafkaProducer (create topic on first use; see the postgres example)
def producer_for(stream, topic): ...  # same shape as the other engines

REGION = os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
ep = os.environ['DDB_ENDPOINT']
ddb = boto3.client('dynamodb', endpoint_url=ep, region_name=REGION)
streams = boto3.client('dynamodbstreams', endpoint_url=ep, region_name=REGION)
OP_MAP = {'INSERT': 'INSERT', 'MODIFY': 'UPDATE', 'REMOVE': 'DELETE'}

def shard_iters(table):
    arn = ddb.describe_table(TableName=table)['Table'].get('LatestStreamArn')
    if not arn:
        return {}
    shards = streams.describe_stream(StreamArn=arn)['StreamDescription'].get('Shards', [])
    return {sh['ShardId']: streams.get_shard_iterator(
        StreamArn=arn, ShardId=sh['ShardId'], ShardIteratorType='LATEST')['ShardIterator'] for sh in shards}

start_http_server(8000)
iters = {t: shard_iters(t) for t in routes}
while True:
    for table, sh in iters.items():
        for sid, it in list(sh.items()):
            if not it:
                continue
            try:
                resp = streams.get_records(ShardIterator=it, Limit=100)
            except Exception:
                ERRORS.inc(); sh[sid] = None; continue
            for rec in resp.get('Records', []):
                op = OP_MAP.get(rec['eventName'])
                img = rec.get('dynamodb', {})
                for route in routes.get(table, []):
                    if op and op in route['ops']:
                        CAPTURED.labels(table, op).inc()
                        producer_for(route['stream'], route['topic']).send(route['topic'],
                            {'table': table, 'op': op, 'keys': img.get('Keys'),
                             'new': img.get('NewImage'), 'old': img.get('OldImage')})
                        PRODUCED.labels(route['topic']).inc()
            sh[sid] = resp.get('NextShardIterator')
    time.sleep(2)
```

Notes: dynamodb-local usually has a single shard per table; re-discover shards periodically
if you want to survive shard splits. `LATEST` captures only changes after the worker starts
(use `TRIM_HORIZON` to replay from the beginning).

## Cassandra — polling capture (no commitlog CDC)

Cassandra's native commitlog CDC needs a binary commitlog reader and is too heavy for the
sandbox. Use **polling**: on an interval, `SELECT *` each captured table and diff against
the previous snapshot — new primary keys are INSERTs, changed rows are UPDATEs, vanished
keys are DELETEs. Sandbox tables are tiny, so a full scan is fine. This is an approximation
(it can miss rapid intermediate states between polls) — say so if asked.

```python
import os, json, time
from cassandra.cluster import Cluster
from prometheus_client import Counter, start_http_server
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

CAPTURED = Counter('cdc_events_captured_total', 'changes captured', ['table', 'op'])
PRODUCED = Counter('cdc_events_produced_total', 'events produced', ['topic'])
ERRORS = Counter('cdc_errors_total', 'cdc errors')

KS = os.environ['CDC_DB_NAME']
rules = json.load(open('/cdc.json'))['rules']
routes = {}
for r in rules:
    routes.setdefault(r['table'], []).append({'ops': set(r['operations']), 'stream': r['stream'], 'topic': r['topic']})

producers = {}
def producer_for(stream, topic): ...  # same shape as the other engines

session = Cluster([os.environ['CDC_DB_HOST']], port=int(os.environ.get('CDC_DB_PORT', 9042))).connect(KS)

def pk_cols(table):
    rows = session.execute(
        "SELECT column_name, kind FROM system_schema.columns WHERE keyspace_name=%s AND table_name=%s", (KS, table))
    return [r.column_name for r in rows if r.kind in ('partition_key', 'clustering')]

def emit(table, op, row):
    for route in routes.get(table, []):
        if op in route['ops']:
            CAPTURED.labels(table, op).inc()
            producer_for(route['stream'], route['topic']).send(route['topic'], {'table': table, 'op': op, 'row': row})
            PRODUCED.labels(route['topic']).inc()

start_http_server(8000)
pks = {t: pk_cols(t) for t in routes}
state = {}  # table -> { pk_tuple: row_json }
while True:
    for table in routes:
        try:
            rows = list(session.execute(f'SELECT * FROM {table}'))
        except Exception:
            ERRORS.inc(); continue
        prev, seen = state.get(table, {}), {}
        for r in rows:
            d = r._asdict()
            key = tuple(str(d[c]) for c in pks[table])
            js = json.dumps(d, sort_keys=True, default=str)
            seen[key] = js
            if key not in prev:
                emit(table, 'INSERT', d)
            elif prev[key] != js:
                emit(table, 'UPDATE', d)
        for key in prev:
            if key not in seen:
                emit(table, 'DELETE', dict(zip(pks[table], key)))
        state[table] = seen
    time.sleep(int(os.environ.get('CDC_POLL_SECONDS', '3')))
```

Notes: seed the snapshot on the first pass without emitting if you don't want the initial
table contents treated as INSERTs — or embrace it (a fresh worker replays current rows once).

## Build + verify

```
docker compose -f systems/<id>/docker-compose.yml up -d --build <db>-cdc
```

Then:
1. `docker compose -f systems/<id>/docker-compose.yml ps` → `<db>-cdc` is Up; its Prometheus
   target is UP at `http://localhost:9090/targets`.
2. Make a change the rules cover — e.g. the db's **Seed** tab, or:
   - postgres: `… exec -T <db> psql -U sandbox -d <dbname> -c "INSERT …"`
   - mongodb: `… exec -T <db> mongosh <dbname> --eval 'db.<coll>.insertOne({…})'`
   - cassandra: `… exec -T <db> cqlsh -e "INSERT INTO <dbname>.<table> (id) VALUES ('x');"`
   - dynamodb: `… exec -T <db>-exporter python -c "import os,boto3;boto3.client('dynamodb',endpoint_url=os.environ['DDB_ENDPOINT'],region_name='us-east-1').put_item(TableName='<table>',Item={'id':{'S':'x'}})"`
3. Consume the topic:
   `docker compose -f systems/<id>/docker-compose.yml exec -T <stream> /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server <stream>:9092 --topic <topic> --from-beginning`
   → the change event appears.
4. The diagram shows `<db>-cdc` with live `captured/s` / `produced/s` and the edges
   `<db>` → `<db>-cdc` → `<stream>`.

If the worker crash-loops, read its logs:
`docker compose -f systems/<id>/docker-compose.yml logs <db>-cdc`. A common cause is the
broker not yet ready — `restart: unless-stopped` lets it retry; the Kafka producer should
tolerate startup races (wrap the first connect/produce and let `cdc_errors_total` count).

## Editing rules later (usually NOT a session)

Adding/removing rules or toggling operations from the CDC tab is mechanical: the backend
rewrites `cdc.json` and restarts the worker, which re-reads `/cdc.json` on start. You only
need to revisit the worker code if a new rule needs capture the current code can't do
(e.g. a brand-new engine path) — otherwise the existing generic loop already handles it.
If you ever edit `cdc.json` by hand, also keep each target stream's `streams.json`
producer list and the manifest `<db>-cdc` → `<stream>` edges in sync.
