---
name: sandbox-postgres-topology
description: >-
  Retrofit the services that USE a postgres node in a "Distributed Systems Sandbox" system
  (systems/<id>/) after its TOPOLOGY changed — standalone ↔ replicated-with-failover — so they
  survive a failover. Use whenever a postgres Topology apply just ran (the web app already
  reconciled the standby containers, the `<db>-failover` watcher, scrape jobs and manifest) and the
  attached services' code must catch up: a multi-host libpq DSN with
  `target_session_attrs=read-write` so writers follow a promoted standby with no reconnect logic,
  an optional read-only DSN that prefers standbys, or the strip-back to a single-host DSN. Covers
  the `postgresHa` manifest block, synchronous replication (`ANY k` quorum + synchronous_commit),
  fencing, the Promote / Rejoin actions, and the docker rebuild/verify steps.
---

# Postgres topology retrofits (streaming standbys · sync replication · failover)

You are operating inside the "Distributed Systems Sandbox" web app. Each system lives in
`systems/<id>/`; the frontend (`npm run dev`) reads these files live, so **never run `./start.sh` /
`./stop.sh`**. Rebuild with `docker compose` directly.

A postgres database's **topology is MECHANICAL**: the web app's `POST /api/postgres/topology`
(`frontend/server/postgresTopology.js`, driven by the postgres node's **Topology** tab) provisions
or tears down the standby containers, the failover watcher, the scrape jobs and the manifest block
itself — **sessions never provision topology**. Your job is the judgment half: making the services
that USE the database speak to the new shape, then rebuilding only them. You are usually launched
right after an apply, with the affected services in the prompt.

## The identity model

One user-created postgres node `<db>` (`origin: "create-database"`), two shapes:

- **standalone** — a single `<db>` container. What "Add database" creates.
- **replicated** — `<db>` plus N streaming standbys `<db>-1..N` (separate `replicaOf` manifest
  nodes), plus a `<db>-failover` **watcher container** (a container, not a manifest node — the same
  convention as the redis sentinels). The primary carries:

```json
"postgresHa": {
  "enabled": true, "autoDegrade": true, "downAfterMs": 5000,
  "primary": "<db>", "members": ["<db>", "<db>-1", "<db>-2"], "watcher": "<db>-failover",
  "sync": { "method": "ANY", "quorum": 1, "commitLevel": "on", "standbys": ["<db>-1"] },
  "dsn": { "readWrite": "postgresql://…", "readOnly": "postgresql://…" }
}
```

**ROLES ARE RUNTIME, MEMBERSHIP IS MANIFEST.** `replicaOf` means "member of `<db>`'s cluster", NOT
"is currently a standby". After a failover the live primary is a `<db>-<n>` container while `<db>`
is still the manifest's cluster entry. Never infer the primary from the manifest — the authority is
the watcher's `pg_ha_is_primary{member="…"}` series (that is what the diagram's ringed member dot
and the tab's "Live cluster" panel read).

## What the watcher does (so you don't)

`<db>-failover` is the postgres analog of redis Sentinel. It is pure SQL — it has no docker socket:
- polls every member; when the primary has been unreachable for `downAfterMs`, promotes the standby
  that has replayed the most WAL (`pg_promote()`), then repoints the survivors
  (`ALTER SYSTEM SET primary_conninfo` + reload — SIGHUP-reloadable, no restart);
- maintains `synchronous_standby_names` as `ANY k ("<db>-1", …)` from the sync set, and
  **auto-degrades** a dead sync standby out of it (otherwise every commit on the primary blocks
  forever waiting for an ack that can never come);
- **FENCES** a returning stale primary: `ALTER SYSTEM SET default_transaction_read_only = on`. This
  is the whole anti-split-brain mechanism, and it is why the client contract below works — see next.

## The client contract — a multi-host DSN

**This is the only code change a postgres failover needs.** A hardcoded single-host DSN keeps
dialing the dead primary and every write fails. libpq itself solves this: give it every member and
let `target_session_attrs` pick.

```python
# WRITES (and anything transactional)
LEDGER_DB_DSN = os.environ.get(
    "LEDGER_DB_DSN",
    "postgresql://sandbox:sandbox@ledger-db:5432,ledger-db-1:5432,ledger-db-2:5432/ledger_db"
    "?target_session_attrs=read-write&connect_timeout=2",
)
```

libpq tries each host in turn and keeps the first that reports `SHOW transaction_read_only` = `off`.
That single mechanism gets you three things for free:
- a **promoted standby** is found automatically — no reconnect logic, no retry loop, no restart;
- a **dead host** is skipped (hence `connect_timeout=2` — without it a dead host stalls the request
  while libpq waits on it);
- a **fenced ex-primary** is skipped, because fencing makes it answer `on`. Do not defeat this by
  pinning a host or by setting `target_session_attrs=any` for writes.

Read-only work may go to the standbys (accepting replica lag):

```python
LEDGER_DB_RO_DSN = ("postgresql://sandbox:sandbox@ledger-db:5432,ledger-db-1:5432,ledger-db-2:5432"
                    "/ledger_db?target_session_attrs=prefer-standby&load_balance_hosts=random"
                    "&connect_timeout=2")
```

`prefer-standby` falls back to the primary when no standby is up, so read scaling degrades to "still
works". **Never send writes there** — a standby rejects them outright.

Call sites are unchanged: `with psycopg.connect(DSN, autocommit=True) as conn:` already re-resolves
on every connect. Only the constant moves.

Stripping back to **standalone**: restore the plain single-host DSN
(`postgresql://sandbox:sandbox@<db>:5432/<db_name>`) and remove the read-only DSN + any read/write
split that used it.

## Synchronous replication

Per-standby, set in the tab, enforced by the watcher as `synchronous_standby_names = ANY k (…)`.
It changes durability, not the client — **you write no code for it**. What it means:
- `async` (default): the primary commits once it has written its own WAL. A crash can lose the last
  transactions — they were never on a standby.
- `sync`: the commit does not return until `k` of the named standbys acknowledge. A promoted standby
  therefore has every committed row. The cost is a network round-trip on every write.
- `synchronous_commit`: `on` waits for the standby to *flush* WAL; `remote_apply` also waits for it
  to be *replayed*, which is what makes a read on that standby guaranteed to see the write you just
  made (read-your-writes).

## Procedure

1. **Read the manifest block** (`systems/<id>/manifest.json`, the `postgresHa` on `<db>`) — it is
   ground truth for the member list and the DSNs. The prompt hands you the ready-made strings; do
   not assemble your own.
2. **Find the DSN constant in each affected service.** They are recorded in `endpoints.json` /
   `consumers.json` `downstream` (the tab passes you the list). The host is nearly always a
   module-level `os.environ.get("<X>_DB_DSN", "postgresql://…")` default in
   `systems/<id>/<service>/app.py` — grep for `psycopg.connect(` and `_DSN`.
3. **Swap in the multi-host DSN.** If the service has read-only endpoints and you add a read DSN,
   only route genuinely read-only handlers to it.
4. **Rebuild ONLY the touched services:**
   ```
   docker compose -f systems/<id>/docker-compose.yml up -d --build <svc-a> <svc-b>
   ```

## Verify — the failover drill

This is the point of the whole feature; actually run it.

```bash
C="docker compose -f systems/<id>/docker-compose.yml"

# sync replication is real (the sync standby reports quorum, not async)
$C exec -T <db> psql -U sandbox -d <db_name> \
  -c "SELECT application_name, state, sync_state FROM pg_stat_replication;"
$C exec -T <db> psql -U sandbox -d postgres -tAc "SHOW synchronous_standby_names;"
#   -> ANY 1 ("<db>-1")

# drive a WRITE endpoint through the lb, then kill the primary
curl -s -XPOST localhost:8080/<service>/<write-endpoint> -d '{...}'
$C kill <db>

# within downAfterMs a standby is promoted ...
$C exec -T <db>-1 psql -U sandbox -d postgres -tAc "SELECT pg_is_in_recovery();"   # -> f

# ... and THE SAME WRITE ENDPOINT STILL SUCCEEDS. This is the multi-host-DSN payoff.
curl -s -XPOST localhost:8080/<service>/<write-endpoint> -d '{...}'

# bring the old primary back: it is FENCED, not a second primary
$C start <db>
$C exec -T <db> psql -U sandbox -d postgres -tAc "SHOW transaction_read_only;"     # -> on
# writes STILL land on the real primary even though <db> is up and first in the host list
```

Then **Rejoin** the fenced node from the Topology tab (or
`POST /api/postgres/rejoin {"system":"<id>","id":"<db>","member":"<db>"}`): it discards its stale
data dir and re-clones from the live primary, coming back as a healthy standby.

If a write hangs forever instead of failing, that is synchronous replication doing exactly what it
promises: a sync standby is down and `auto-degrade` is off, so the primary is waiting for an ack
that cannot come. Turn auto-degrade on, or bring the standby back.

## Gotchas that will bite you

- **Never hardcode the primary's hostname for writes.** After a failover `<db>` is a fenced,
  read-only container. It is up, it accepts connections, and it will not take your writes.
- **Every member must run with the same postgres flags.** Standbys inherit the primary's `-c`
  settings (e.g. CDC's `wal_level=logical`) because they base-backup its data dir — a member without
  them can fail to start outright, and would silently break CDC if promoted. The backend handles
  this; don't hand-edit a member's compose entry.
- **A fence is per-node and per-moment.** `pg_basebackup` copies `postgresql.auto.conf`, so a clone
  of a fenced node would inherit `default_transaction_read_only`. The standby entrypoint strips it;
  don't re-add it.
