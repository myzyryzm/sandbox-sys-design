---
name: sandbox-database
description: >-
  Add, update, or delete a database in a "Distributed Systems Sandbox" system
  (systems/<id>/). Use whenever the task is to provision a new datastore (postgres,
  mongodb, redis, object-store/MinIO, dynamodb, or cassandra), change an existing one's
  schema or metrics, or remove it — it covers the compose service + exporter, the
  Prometheus scrape job, the manifest node, the init script, and the docker rebuild/verify steps.
---

# Working on a sandbox database

You are operating inside the "Distributed Systems Sandbox" web app. Each system lives in
`systems/<id>/`; your session's system prompt names the specific `<id>` and inlines its
current `manifest.json`. The web frontend runs under `npm run dev` and reads these files
live, so **never run `./start.sh`** — it tears down the dev server you're attached to.
Rebuild with `docker compose` directly (commands below).

A database is not one file — it spans **five** places, and the running diagram only stays
correct if you keep them in sync. The web app's "Add database" / delete buttons do all of
this automatically (`POST /api/databases`, `POST /api/delete`); when you do it by hand from
a terminal session, reproduce the same shape.

## The five places a database lives (working dir is the repo root)

1. `systems/<id>/<db>/` — the datastore's init script (first-boot seeding):
   - **postgres** → `init.sql`, mounted at `/docker-entrypoint-initdb.d/init.sql`
   - **mongodb** → `init.js`, mounted at `/docker-entrypoint-initdb.d/init.js`
   - **cassandra** → `init.cql` (keyspace + tables), run by a `<db>-init` sidecar via `cqlsh -f`.
   - **redis** / **object-store** / **dynamodb** → no init dir; a one-shot `<db>-init` sidecar
     seeds them instead (dynamodb runs a mounted `init.sh` of `aws dynamodb create-table` calls).
2. `systems/<id>/docker-compose.yml` — the db service, plus a Prometheus **exporter**
   service (and an **`-init`** sidecar for redis/object-store/dynamodb/cassandra). See the engine
   table below.
3. `systems/<id>/prometheus/prometheus.yml` — a **scrape job** named exactly `<db>` whose
   target is the exporter. The `job_name` is the `job="<db>"` label every metric query uses.
4. `systems/<id>/manifest.json` — a **node** the diagram draws: `{ id:<db>, label, type,
   origin:"create-database", position, metrics:[{label, query, unit, scale?}],
   health:{query, rules} }`. Metric `query`s are PromQL filtered on `job="<db>"`.
5. `systems/<id>/manifest.json` **edges** — a new DB is intentionally **not** wired to any
   service. To make a service's call show on the diagram, add an edge `{from:<service>,
   to:<db>}` AND set that endpoint's `downstream` to `[<db>]` (see the **sandbox-endpoint**
   skill).

### Conventions (match these exactly — the delete path and PromQL depend on them)
- `<db>` is the node id **and** the primary compose service name **and** the folder name.
  Lowercase, digits, hyphens, starts with a letter (e.g. `orders-db`).
- The in-engine database name is `<db>` with hyphens → underscores (`orders-db` → `orders_db`).
- `origin: "create-database"` marks it as frontend-deletable — keep it.
- Health rules are the shared pair: `[{color:"red", when:"value < 1"}, {color:"green", when:"value >= 1"}]`.
- `position`: lay generated nodes out below the hand-authored ones (rows of three); copy the
  pattern of existing `origin`-tagged nodes in the manifest.

### Engine reference (image / exporter / port / creds / a metric)
- **postgres**: `postgres:16-alpine`; exporter `quay.io/prometheuscommunity/postgres-exporter:v0.16.0`
  at `<db>-exporter:9187`; env `POSTGRES_USER=sandbox POSTGRES_PASSWORD=sandbox POSTGRES_DB=<dbname>`;
  health `pg_up{job="<db>"}`; e.g. `sum(pg_stat_database_numbackends{job="<db>"})`.
- **mongodb**: `mongo:7`; exporter `percona/mongodb_exporter:0.40` (`--mongodb.uri=mongodb://<db>:27017
  --collect-all --compatible-mode`) at `<db>-exporter:9216`; env `MONGO_INITDB_DATABASE=<dbname>`;
  health `mongodb_up{job="<db>"}`.
- **redis**: `redis:7-alpine` (no auth); exporter `oliver006/redis_exporter:v1.62.0`
  (`REDIS_ADDR=redis://<db>:6379`) at `<db>-exporter:9121`; a `<db>-init` `redis:7-alpine`
  sidecar `SET`s one sample key per namespace; health `redis_up{job="<db>"}`.
- **object-store (MinIO)**: `minio/minio:latest` (`server /data --console-address :9001`,
  creds `sandbox`/`sandbox123`, `MINIO_PROMETHEUS_AUTH_TYPE=public`); **no separate exporter** —
  scrape MinIO directly with `metrics_path: /minio/v2/metrics/cluster` at `<db>:9000`; a
  `<db>-init` `minio/mc:latest` sidecar makes the buckets; health `up{job="<db>"}`.
- **dynamodb**: `amazon/dynamodb-local:latest` (`command: -jar DynamoDBLocal.jar -sharedDb
  -inMemory`; `-sharedDb` so every client sees one DB, `-inMemory` = ephemeral, seeds give
  rebuild-durability). No off-the-shelf exporter → a **custom exporter** `<db>-exporter` built
  from `<db>/exporter/{Dockerfile,exporter.py}` (python + boto3, `start_http_server(9100)`,
  emits `dynamodb_up`/`dynamodb_table_count`/`dynamodb_item_count`); scrape `<db>-exporter:9100`.
  A `<db>-init` `amazon/aws-cli:latest` sidecar runs a mounted `init.sh` (`aws dynamodb
  create-table … --stream-specification …`, streams on for CDC); health `dynamodb_up{job="<db>"}`.
- **cassandra**: `cassandra:5` (`CASSANDRA_CLUSTER_NAME=sandbox`, cap `MAX_HEAP_SIZE`/`HEAP_NEWSIZE`;
  slow ~30–90s start). Custom exporter `<db>-exporter` from `<db>/exporter/` (python +
  cassandra-driver, `CASS_DRIVER_NO_EXTENSIONS=1`, emits `cassandra_up`/`cassandra_node_count`/
  `cassandra_table_count`); scrape `<db>-exporter:9100`. A `<db>-init` `cassandra:5` sidecar waits
  for CQL then `cqlsh -f /init.cql`; health `cassandra_up{job="<db>"}`.

> **Custom exporters** (dynamodb/cassandra): neither engine has a drop-in Prometheus exporter that
> fits the "separate exporter container" pattern here (DynamoDB Local has none; Cassandra only via
> fragile JMX), so each ships a tiny python `prometheus_client` sidecar built from
> `<db>/exporter/{Dockerfile,exporter.py}` that probes the DB and sets a `<engine>_up` 0/1 gauge.
> The `<db>-init` sidecar also replays a seed file if the Seed tab mounted one (dynamodb `seed.sh`
> after `init.sh`; cassandra `seed.cql` after `init.cql`).

## Rebuilding (you run INSIDE the web app's dev server)

- Editing only `manifest.json` (e.g. changing which metrics show) appears within seconds — no rebuild.
- After compose / prometheus / init-script changes:

  ```
  # add or change services (pulls images, creates only what's new/changed):
  docker compose -f systems/<id>/docker-compose.yml up -d
  # prometheus.yml is mounted, so make Prometheus reload the new/removed scrape job:
  docker compose -f systems/<id>/docker-compose.yml restart prometheus
  ```

- Reach Prometheus targets to confirm scraping at `http://localhost:9090/targets` (the
  diagram reads metrics through `/api/prometheus`).

## Add a database

1. Pick the engine and a `<db>` id. Write the init script under `systems/<id>/<db>/`
   (postgres `init.sql` / mongodb `init.js`); redis/object-store get a seeding `-init`
   sidecar in compose instead.
2. Add the db service **and** its exporter (and `-init` sidecar where applicable) to
   `docker-compose.yml`, following the engine reference above.
3. Add a scrape job `job_name: <db>` targeting the exporter to `prometheus/prometheus.yml`.
4. Add the manifest node (`type` = `postgres`/`mongodb`/`redis`/`object-store`,
   `origin:"create-database"`, metrics + health filtered on `job="<db>"`, a free position).
5. `docker compose ... up -d` then `restart prometheus` (commands above).
6. Verify: `docker compose -f systems/<id>/docker-compose.yml ps` shows `<db>` and
   `<db>-exporter` up, and the node appears on the diagram with live metrics (target is UP
   at `http://localhost:9090/targets`).

## Authoring schema from models (postgres / mongodb / dynamodb / cassandra)

The web app can build a db's schema from the **model bank** (`systems/<id>/models.json`, each model a
TypeScript interface). "Add database ▸ From model bank" provisions an **empty** container and records
the chosen models on the node as `schemaModels`; the **Schema** tab's "Apply models" does the same for
an existing db (additive). Either way it launches a session (this skill) to write the actual schema.
The launched prompt inlines the right per-engine rules; the postgres/mongodb rules below are the
relational/document case. **The NoSQL engines differ sharply — follow the prompt's guidance:**

- **dynamodb** — schemaless beyond keys. Each model → a table; pick the partition (HASH) key from a
  `// PK` comment (else `id`), optional sort (RANGE) key from `// SK`. NO joins: a referenced model is
  DENORMALIZED (embed as a map attribute), never a foreign key. The "schema" is table + key
  definitions authored into `<db>/init.sh` (`aws dynamodb create-table … --stream-specification …`),
  applied live via the same `aws` calls (ignore ResourceInUseException).
- **cassandra** — query-driven, denormalized. Each model → a table in keyspace `<dbname>`; PRIMARY KEY
  from `// PK` (partition) + `// CK` (clustering) comments (else `id text`). NO joins: denormalize a
  referenced model into columns or a UDT. Author `<db>/init.cql` (`CREATE TABLE IF NOT EXISTS …`),
  apply live via `cqlsh -f`.

For postgres / mongodb:

- **`//` comments in the model definitions are authoritative schema directives** — honor them
  (PK/FK/unique/index/length/check/default/nullable/type) and let them **override** the defaults below.
  Comments may be leading (`// id => snowflake id; primary key`, `// unique constraint on (owner_id,
  name)`) or trail a field. E.g. `// id => primary key` on `id: number` snowflake → `id bigint primary
  key` (not a synthetic uuid); `// currency => max length of 3` → `varchar(3)`; `// amount => integer
  only` → `integer`; `// payment_order_id => add an index` → a `CREATE INDEX`.
- **One table (postgres) / collection (mongodb) per selected model.** snake_case postgres table names.
  If no field/comment designates a primary key, add a synthetic `id uuid primary key default
  gen_random_uuid()`; otherwise use the designated key with its stated type.
- **Foreign keys come from model-to-model references** (a field whose type is another *selected* model):
  - `f: OtherModel` (singular) → postgres: a column `f_id` (typed to match the referenced PK, **not**
    assumed uuid) with a FK to that table; mongo: a field holding the referenced doc's `_id`.
  - `f: OtherModel[]` (array) → one-to-many: postgres puts a FK column on the **child** table
    referencing this table's PK; mongo stores an array of referenced `_id`s (or embeds subdocuments).
  - A reference to a model **not** in the selected set degrades to a plain `jsonb` (pg) / `object`
    (mongo) field — no FK.
- **Default type mapping** (unless a comment narrows it) — postgres: `string→text, number→numeric,
  boolean→boolean, Date→timestamptz, Record<…>/nested object/array-of-primitive→jsonb`; mongo: the BSON
  equivalents. A `?` (optional) field is nullable.
- **New db**: write the full `systems/<id>/<db>/init.sql` (or `init.js`), apply it to the live (empty)
  container, rebuild/verify (the container was just provisioned with a header-only init script).
- **Update (additive)**: apply **idempotent** DDL to the live container (`CREATE TABLE IF NOT EXISTS …`
  / guard `createCollection` with `getCollectionNames()`; add FK constraints only if absent) **and**
  append the new tables/collections to the init script so a rebuild reproduces them. **Never drop**
  existing tables/data. After the migration, if `systems/<id>/<db>/seed.sql` (or `seed.js`) exists,
  **re-run it against the live container** (it is idempotent) so any seeded data is preserved.

The launched session's prompt already inlines the selected models' TypeScript and these rules; keep the
node's `schemaModels` list and the on-disk schema in sync.

## Seeding data (auto-replayed fixtures)

A postgres/mongodb/cassandra/dynamodb database can carry **seed rows** that survive resets. The web
app's **Seed** tab (`/api/db-seed`) writes these — by hand, reproduce the same shape. The idempotent
artifact + replay path is engine-specific: postgres `seed.sql` / mongodb `seed.js` mount into the init
dir; **cassandra `seed.cql`** and **dynamodb `seed.sh`** have no init dir, so they mount into the
`<db>-init` sidecar, which runs them AFTER the schema (see the exporter note above). For pg/mongo:

- **`systems/<id>/<db>/seeds.json`** is the source of truth:
  `{ "tables": [ { "table": "<name>", "rows": [ { "<field>": "<value>", … } ] } ] }`. `tables[]` is
  **ordered** = FK-safe insertion order (parents before children). A blank/omitted field means "use the
  DB default" (serial PKs, `created_at default now()`, …).
- **`systems/<id>/<db>/seed.sql`** (postgres) / **`seed.js`** (mongodb) is the generated, **idempotent**
  artifact: postgres `INSERT … ON CONFLICT DO NOTHING;`, mongo `updateOne({id}, {$setOnInsert:…},
  {upsert:true})`. It is **mounted into the init dir after the schema script**
  (`./<db>/seed.sql:/docker-entrypoint-initdb.d/seed.sql:ro`, which sorts after `init.sql`) so a fresh
  `down -v` rebuild runs schema **then** seed automatically.
- Keep `seeds.json` and the `seed.sql`/`seed.js` artifact in sync; apply changes to the live container
  too (the init mount only fires on a fresh volume): postgres `psql … -c "<inserts>"`, mongo
  `mongosh … --eval "<js>"`. Because the artifact is idempotent it is always safe to re-run.

## Update a database

- **The init script only runs on FIRST boot** (an existing volume is already initialized).
  To change the schema of a *running* DB, apply the change to the live container, and also
  update the init script so a fresh rebuild reproduces it. Inspect / mutate live with:
  - postgres: `docker compose -f systems/<id>/docker-compose.yml exec -T -e PGPASSWORD=sandbox <db> psql -U sandbox -d <dbname> -c "<SQL>"`
  - mongodb: `docker compose -f systems/<id>/docker-compose.yml exec -T <db> mongosh <dbname> --quiet --eval "<JS>"`
  - redis: `docker compose -f systems/<id>/docker-compose.yml exec -T <db> redis-cli ...`
  - object-store: `docker compose -f systems/<id>/docker-compose.yml exec -T <db> ls -1 /data` (one dir per bucket)
  - cassandra: `docker compose -f systems/<id>/docker-compose.yml exec -T <db> cqlsh -e "<CQL>"` (keyspace-qualify, e.g. `<dbname>.<table>`)
  - dynamodb: the db container has no CLI — run boto3 in the exporter: `docker compose -f systems/<id>/docker-compose.yml exec -T <db>-exporter python -c "import os,boto3;c=boto3.client('dynamodb',endpoint_url=os.environ['DDB_ENDPOINT'],region_name='us-east-1');print(c.list_tables())"`
  (If you'd rather start clean, `docker compose ... down -v <db>` drops its volume so the
  init script re-runs on the next `up -d` — destroys existing data.)
- **Change which metrics/health the diagram shows**: edit the node's `metrics`/`health` in
  `manifest.json` only — no rebuild. Keep every query filtered on `job="<db>"`.
- **Rename or change engine**: treat as delete + add (the id is wired into compose,
  prometheus, the manifest, the folder, and PromQL labels).

## Read replicas (primary / secondary)

A database can have **read replicas**: a primary that keeps taking writes plus one or more
**real, read-only streaming standbys**. Supported for **postgres, mongodb, redis**; **object-store
and dynamodb have no replica concept** (DynamoDB Local has zero replication — no option is offered).
**Cassandra is different**: its "replica" is a **second node that JOINS the ring** (via
`CASSANDRA_SEEDS=<primary>` + the shared cluster name), NOT a read-only standby — it accepts writes
like any Cassandra node (`readonly:false`, `replication:"peer"`, labeled "cluster node"). The backend
raises the keyspace RF to 2 before the node bootstraps so it streams existing data; after a
from-scratch rebuild the keyspace is recreated at RF=1, so re-add the node (or `ALTER KEYSPACE` +
`nodetool repair`) to restore RF=2. The web app's per-database modal ("Add read replica") and
`POST /api/db-replicas { system, primary, mode }` do all of the below automatically; reproduce the
same shape by hand.

### Conventions
- **Secondary id = `<primary>-<N>`** (`catalog-db-1`, `catalog-db-2`, …), `N` = max existing
  ordinal + 1. It is a normal db node (`origin:"create-database"`) with its **own** exporter and a
  `<secondaryId>` scrape job, **plus** replica fields:
  ```json
  { "id":"catalog-db-1", "type":"postgres", "origin":"create-database", "role":"secondary",
    "replicaOf":"catalog-db", "replication":"async", "readonly":true, "position":{...},
    "metrics":[ /* job="catalog-db-1" */ ], "health":{ "query":"pg_up{job=\"catalog-db-1\"}", ... } }
  ```
  The primary gets `role:"primary"`. Lay the secondary **next to its primary** (e.g.
  `x = primary.x + (N-1)*220`, `y = primary.y + 170`) so the cluster stays tight.
- The diagram's **double-headed arrow** and **dotted cluster box** are derived from `replicaOf` —
  do **not** add manifest `edges` for replication.
- `replication` is `"async"` or `"sync"`. **`sync` is postgres-only** (real
  `synchronous_standby_names`); mongo/redis stream asynchronously by nature (`async`).

### Per-engine streaming (real replication)
- **postgres** — make the primary replication-ready idempotently: allow replication in
  `pg_hba.conf` (`host replication all 0.0.0.0/0 trust`) — persist it via an initdb script
  `systems/<id>/<primary>/repl-hba.sh` mounted at `/docker-entrypoint-initdb.d/00-repl-hba.sh`,
  and apply it live so the running primary isn't recreated:
  `docker compose ... exec -T <primary> sh -c 'echo "host replication all 0.0.0.0/0 trust" >> "$PGDATA/pg_hba.conf"; psql -U sandbox -d postgres -c "SELECT pg_reload_conf();"'`.
  The secondary service (`postgres:16-alpine`, `user: postgres`) runs an entrypoint that waits for
  the primary, `pg_basebackup -h <primary> -U sandbox -D "$PGDATA" -Fp -Xs -R -P`, appends a
  `primary_conninfo` with `application_name=<secondaryId>`, then `exec postgres` (a standby is
  read-only automatically). **sync**: on the primary
  `ALTER SYSTEM SET synchronous_standby_names = '"<id>",…'; SELECT pg_reload_conf();`.
- **mongodb** — first replica converts the standalone to a replica set: give the primary
  `command: [mongod, --replSet, rs0, --bind_ip_all]` (recreates it), `rs.initiate(...)`, then each
  secondary runs the same `--replSet rs0` and is added with `rs.add({host:"<secondaryId>:27017",
  priority:0})`. Members are read-only secondaries; point each exporter at its own member with
  `--mongodb.uri=mongodb://<id>:27017/?directConnection=true`.
- **redis** — the secondary runs `redis-server --replicaof <primary> 6379 --replica-read-only yes`
  (`replica-read-only` is the default). No primary change.
- **cassandra** — the second node runs `cassandra:5` with `CASSANDRA_SEEDS=<primary>` and the same
  `CASSANDRA_CLUSTER_NAME=sandbox`; it bootstraps into the ring automatically (no primary recreate).
  Reuse the primary's exporter build (`build: ./<primary>/exporter`, `CASSANDRA_HOST=<secondaryId>`).
  Before bringing it up, `ALTER KEYSPACE <ks> … replication_factor: 2` on the primary. It is a ring
  peer (accepts writes), NOT read-only. Verify with `nodetool status` (two `UN` nodes).

### Rebuild + verify
- Bring up **only the new services** so the running primary isn't disrupted:
  `docker compose -f systems/<id>/docker-compose.yml up -d <secondaryId> <secondaryId>-exporter`
  then `restart prometheus`. (Mongo's *first* replica must also recreate the primary for
  `--replSet`.)
- Verify: `docker compose ... ps` shows `<secondaryId>` up and its target is UP; a write to the
  primary appears when read from the secondary; a write **to** the secondary is rejected
  (read-only); the diagram shows the arrow + dotted box.

### Delete a replica / primary
- **Secondary**: delete it like any db (below) **and** reconcile the primary — postgres: drop its
  name from `synchronous_standby_names` + reload; mongo: `rs.remove("<secondaryId>:27017")`.
- **Primary** with replicas: **cascade** — delete every node whose `replicaOf` points at it first,
  then the primary (an orphan standby is meaningless). The web app's delete does this.

## Delete a database

Remove it from **all five** places, then reconcile:

1. `docker-compose.yml`: delete `<db>`, `<db>-exporter`, and `<db>-init` if present, and
   scrub any `depends_on` references to them from the remaining services (a dangling
   `depends_on` makes the whole compose project invalid).
2. `prometheus/prometheus.yml`: delete the `job_name: <db>` scrape job.
3. `manifest.json`: remove the node **and any edges** with `from`/`to` equal to `<db>`.
4. Delete the `systems/<id>/<db>/` folder.
5. Reconcile and reload:

   ```
   docker compose -f systems/<id>/docker-compose.yml up -d --remove-orphans
   docker compose -f systems/<id>/docker-compose.yml restart prometheus
   ```

6. Verify: the node is gone from the diagram and `docker compose ... ps` no longer lists
   `<db>`/`<db>-exporter`/`<db>-init`.
