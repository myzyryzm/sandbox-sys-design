"""pg-failover — the HA watcher for one postgres cluster in the sandbox.

It is the postgres answer to redis Sentinel: it watches every member of a cluster,
promotes the most caught-up standby when the primary dies, repoints the survivors at
the new primary, and keeps `synchronous_standby_names` honest.

Everything it does is PURE SQL, over ordinary libpq connections. It deliberately has
no docker socket: container lifecycle belongs to the dev-server backend (see the repo's
localhost-only security posture), and a watcher that could recreate containers would be
a much bigger blast radius than one that can only issue statements. That constraint
shapes two things:

  * PROMOTION is in-place (`pg_promote()`), so the promoted standby keeps its data and
    its container is never recreated.
  * FENCING replaces demotion. When a dead primary is restarted it comes back believing
    it is still a primary — classic split brain, and with the multi-host DSN our writers
    use it would happily attract writes again. We cannot stop its postgres to rebuild it
    as a standby, but we can do something better and cheaper: set
    `default_transaction_read_only = on`. libpq's `target_session_attrs=read-write`
    decides a host is writable by asking it `SHOW transaction_read_only` — a fenced node
    answers `on`, so every writer SKIPS it and moves to the real primary, automatically.
    Turning it back into a standby ("Rejoin") is a container operation, so the web app's
    Topology tab owns it.

Config is the read-only /ha.json mount, re-read by mtime — so the web app can change the
sync set, the quorum or the failure threshold with no rebuild.
"""
import json
import os
import re
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import psycopg
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, generate_latest

CONF_PATH = os.environ.get("PGHA_CONF", "/ha.json")
PORT = 8000
PROBE_INTERVAL = 1.0

MEMBER_UP = Gauge("pg_ha_member_up", "member accepted a connection", ["member"])
IS_PRIMARY = Gauge("pg_ha_is_primary", "1 when the member is a writable primary", ["member"])
IS_FENCED = Gauge("pg_ha_is_fenced", "1 when the member was fenced read-only (stale primary)", ["member"])
REPLAY_LAG = Gauge("pg_ha_replay_lag_seconds", "standby replay lag behind its primary", ["member"])
SYNC_CONFIGURED = Gauge("pg_ha_sync_standbys", "standbys named in synchronous_standby_names")
SYNC_ACKING = Gauge("pg_ha_sync_acking", "standbys pg_stat_replication reports as sync")
FAILOVERS = Counter("pg_ha_failovers_total", "automatic promotions performed")
FENCES = Counter("pg_ha_fences_total", "stale primaries fenced read-only")

# What /state serves and what the loop reasons over.
STATE = {"primary": None, "members": {}, "sync": [], "warnings": [], "updated": 0}
_conf_cache = {"mtime": None, "data": None}
# member -> monotonic timestamp we first saw it unreachable (None once it answers again)
_down_since = {}
# monotonic timestamp of our last promotion, in a 1-cell list so tick() can rebind it
_last_promotion = [None]


def conf():
    """Re-read /ha.json when it changes on disk (no rebuild for a config edit)."""
    st = os.stat(CONF_PATH)
    if st.st_mtime != _conf_cache["mtime"]:
        with open(CONF_PATH) as fh:
            _conf_cache["data"] = json.load(fh)
        _conf_cache["mtime"] = st.st_mtime
    return _conf_cache["data"]


def dsn(c, member):
    return (
        f"postgresql://{c['user']}:{c['password']}@{member}:5432/{c['db']}"
        "?connect_timeout=2"
    )


def sql(c, member, statements):
    """Run statements on one member, autocommit (ALTER SYSTEM cannot run in a tx block)."""
    out = []
    with psycopg.connect(dsn(c, member), autocommit=True) as conn:
        with conn.cursor() as cur:
            for s in statements:
                cur.execute(s)
                out.append(cur.fetchall() if cur.description else None)
    return out


# Replay lag, done properly. The obvious `now() - pg_last_xact_replay_timestamp()` is a trap:
# it measures "time since the last transaction I replayed", so on an idle database it grows
# forever and a perfectly caught-up standby reads as minutes behind. When the standby has
# replayed everything it has received, the lag is zero by definition — check that first.
PROBE_SQL = """
SELECT pg_is_in_recovery(),
       CASE
         WHEN NOT pg_is_in_recovery() THEN 0
         WHEN pg_last_wal_receive_lsn() = pg_last_wal_replay_lsn() THEN 0
         ELSE COALESCE(EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp())), 0)
       END::float8,
       COALESCE(pg_wal_lsn_diff(pg_last_wal_replay_lsn(), '0/0'), 0)::float8,
       current_setting('default_transaction_read_only'),
       current_setting('primary_conninfo')
"""


def probe(c, member):
    try:
        in_recovery, lag, lsn, read_only, conninfo = sql(c, member, [PROBE_SQL])[0][0]
        return {
            "up": True,
            "in_recovery": bool(in_recovery),
            # Replay lag only means anything while replaying. A promoted node keeps its last
            # replay timestamp from its standby days, which would otherwise read as an
            # ever-growing lag on the node that is actually serving writes.
            "lag": float(lag) if in_recovery else 0.0,
            "lsn": float(lsn),
            "fenced": read_only == "on",
            "conninfo": conninfo or "",
        }
    except Exception as err:  # unreachable, still booting, or mid-promotion
        return {"up": False, "error": str(err).strip().splitlines()[0][:120]}


def can_ack(st):
    """Can this member actually acknowledge WAL for a synchronous commit?

    Being reachable is NOT enough, and getting this wrong hangs the database. A fenced
    ex-primary is up and answers queries, but it is not in recovery — it never connects to
    the new primary as a walreceiver, so naming it in synchronous_standby_names makes every
    commit wait forever for an ack that cannot come. Only a live, replicating standby counts.
    """
    return bool(st and st.get("up") and st.get("in_recovery") and not st.get("fenced"))


def sync_standby_names(c, live_standbys):
    """The synchronous_standby_names value for the standbys that can currently acknowledge.

    Auto-degrade is the whole point of computing this from live state: a sync standby that
    is down (or fenced, or otherwise not replicating) would otherwise block every commit on
    the primary forever. With it off, we keep naming the dead standby — which is exactly how
    you demonstrate that stall.
    """
    cfg = c.get("sync") or {}
    wanted = [s for s in cfg.get("standbys", [])]
    if c.get("autoDegrade", True):
        wanted = [s for s in wanted if can_ack(live_standbys.get(s))]
    if not wanted:
        return "", []
    quorum = min(int(cfg.get("quorum", 1)), len(wanted))
    method = "FIRST" if cfg.get("method") == "FIRST" else "ANY"
    names = ",".join(f'"{s}"' for s in wanted)
    return f"{method} {quorum} ({names})", wanted


def apply_primary_config(c, primary, live):
    """Keep the live primary's replication settings in step with /ha.json."""
    standbys = {m: st for m, st in live.items() if m != primary}
    value, wanted = sync_standby_names(c, standbys)
    commit = (c.get("sync") or {}).get("commitLevel", "on")
    if commit not in ("on", "remote_write", "remote_apply", "local", "off"):
        commit = "on"
    try:
        sql(c, primary, [
            f"ALTER SYSTEM SET synchronous_standby_names = '{value}'",
            f"ALTER SYSTEM SET synchronous_commit = '{commit}'",
            # Retain enough WAL that a standby which was briefly disconnected (or one
            # repointed onto a freshly promoted primary) can still catch up. Set at
            # runtime rather than in the compose command ON PURPOSE: changing the
            # primary's compose entry would recreate its container, and the container
            # IS the data (there is no named volume).
            "ALTER SYSTEM SET wal_keep_size = '256MB'",
            "SELECT pg_reload_conf()",
        ])
        SYNC_CONFIGURED.set(len(wanted))
    except Exception as err:
        STATE["warnings"].append(f"could not configure primary {primary}: {err}")
    try:
        rows = sql(c, primary, [
            "SELECT count(*) FROM pg_stat_replication WHERE sync_state = 'sync'"
        ])[0]
        SYNC_ACKING.set(rows[0][0])
    except Exception:
        SYNC_ACKING.set(0)
    return wanted


def repoint(c, standby, primary):
    """Point a surviving standby at the new primary. primary_conninfo is SIGHUP-
    reloadable (PG13+): the walreceiver restarts and reconnects, no container restart."""
    conninfo = (
        f"host={primary} port=5432 user={c['user']} password={c['password']} "
        f"application_name={standby}"
    )
    sql(c, standby, [
        f"ALTER SYSTEM SET primary_conninfo = '{conninfo}'",
        "SELECT pg_reload_conf()",
    ])


def fence(c, member):
    """Make a stale primary refuse writes (see the module docstring)."""
    sql(c, member, [
        "ALTER SYSTEM SET default_transaction_read_only = on",
        "SELECT pg_reload_conf()",
    ])
    FENCES.inc()


def promote(c, candidate, others):
    # UNFENCE FIRST. `default_transaction_read_only` lives in postgresql.auto.conf, and
    # pg_basebackup copies that file — so a standby cloned from a node that was ever
    # fenced starts life read-only. Promoting it without clearing that leaves a "primary"
    # that is still read-only, which we would not count as a primary on the next tick, so
    # we would promote the NEXT standby, and the next — walking the whole cluster to death.
    # A promotion must always produce a WRITABLE primary; that is what promotion means.
    sql(c, candidate, [
        "ALTER SYSTEM RESET default_transaction_read_only",
        "SELECT pg_reload_conf()",
        "SELECT pg_promote(true, 60)",
    ])
    FAILOVERS.inc()
    for m in others:
        try:
            repoint(c, m, candidate)
        except Exception as err:
            STATE["warnings"].append(f"could not repoint {m} at {candidate}: {err}")


def tick(c):
    members = c["members"]
    down_after = float(c.get("downAfterMs", 5000)) / 1000.0
    live = {m: probe(c, m) for m in members}
    now = time.monotonic()

    for m, st in live.items():
        MEMBER_UP.labels(m).set(1 if st["up"] else 0)
        IS_PRIMARY.labels(m).set(1 if st.get("up") and not st.get("in_recovery") and not st.get("fenced") else 0)
        IS_FENCED.labels(m).set(1 if st.get("fenced") else 0)
        REPLAY_LAG.labels(m).set(st.get("lag", 0) if st.get("in_recovery") else 0)
        _down_since[m] = None if st["up"] else (_down_since.get(m) or now)

    # A member is a "live primary" only if it is up, out of recovery, and not fenced.
    primaries = [m for m, st in live.items()
                 if st["up"] and not st["in_recovery"] and not st["fenced"]]
    STATE["warnings"] = []

    if len(primaries) > 1:
        # Split brain: keep the one that has been primary (or the first), fence the rest.
        keep = STATE["primary"] if STATE["primary"] in primaries else primaries[0]
        for m in primaries:
            if m == keep:
                continue
            try:
                fence(c, m)
            except Exception as err:
                STATE["warnings"].append(f"could not fence stale primary {m}: {err}")
        primaries = [keep]

    if primaries:
        primary = primaries[0]
        STATE["primary"] = primary
        # Self-heal the replication graph. A standby can end up following the WRONG host: a
        # manual promote from the web app fences + promotes but leaves the survivors pointing
        # at the old primary, which then becomes a standby itself — leaving a CASCADE
        # (primary -> ex-primary -> standby) that silently works until the middle node dies.
        # Rather than make every promotion path remember to repoint, converge here: anyone not
        # following the live primary gets repointed. Idempotent, so it costs nothing when the
        # graph is already correct.
        for m, st in live.items():
            if m == primary or not st.get("up") or not st.get("in_recovery"):
                continue
            if not re.search(rf"host={re.escape(primary)}(\s|$)", st.get("conninfo") or ""):
                try:
                    repoint(c, m, primary)
                    print(f"repointed {m} at {primary}", flush=True)
                except Exception as err:
                    STATE["warnings"].append(f"could not repoint {m} at {primary}: {err}")
        STATE["sync"] = apply_primary_config(c, primary, live)
    elif c.get("enabled", True):
        # No live primary. Only act once the configured primary has been unreachable
        # for downAfterMs — a restart or a slow query must not trigger a failover.
        candidates = {m: st for m, st in live.items() if st["up"] and st["in_recovery"]}
        stale = [m for m in members
                 if _down_since.get(m) and (now - _down_since[m]) >= down_after]
        # One promotion per down_after window. Belt-and-braces against a promoted node
        # that somehow does not come back writable: without this, every tick would elect
        # another standby and we would burn the whole cluster instead of failing one node.
        cooling = _last_promotion[0] is not None and (now - _last_promotion[0]) < down_after
        if candidates and stale and not cooling:
            # Most caught-up standby wins: the one that has replayed the furthest WAL.
            winner = max(candidates, key=lambda m: candidates[m]["lsn"])
            try:
                promote(c, winner, [m for m in candidates if m != winner])
                _last_promotion[0] = now
                STATE["primary"] = winner
            except Exception as err:
                STATE["warnings"].append(f"promotion of {winner} failed: {err}")
        elif not candidates:
            STATE["warnings"].append("no primary and no reachable standby to promote")

    # Standing conditions, re-derived every tick — a warning that is only emitted on the
    # transition disappears a second later, which is exactly when someone goes looking.
    primary = STATE["primary"]
    for m, st in live.items():
        if st.get("fenced"):
            STATE["warnings"].append(
                f"{m} is a stale primary, fenced read-only (writers skip it automatically) — "
                f"rejoin it as a standby of {primary} from the Topology tab to make it useful again"
            )
    configured_sync = set((c.get("sync") or {}).get("standbys", []))
    if configured_sync and primary in configured_sync:
        STATE["warnings"].append(
            f"the synchronous standby {primary} was promoted, so no synchronous standby is left — "
            "commits are no longer waiting for a replica. Re-apply the topology to designate a new one"
        )
    elif configured_sync and not STATE["sync"]:
        STATE["warnings"].append(
            "no synchronous standby is currently acknowledging — commits are effectively asynchronous"
        )

    STATE["members"] = live
    STATE["updated"] = time.time()


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith("/metrics"):
            body, ctype = generate_latest(), CONTENT_TYPE_LATEST
        elif self.path.startswith("/state"):
            body, ctype = json.dumps(STATE).encode(), "application/json"
        else:
            self.send_response(404)
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        pass  # the loop already logs what matters


def serve():
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()


threading.Thread(target=serve, daemon=True).start()
print(f"pg-failover watching {conf()['members']}", flush=True)

while True:
    try:
        tick(conf())
    except Exception as err:  # never let one bad round kill the watcher
        print(f"pg-failover: {err}", flush=True)
    time.sleep(PROBE_INTERVAL)
