#!/usr/bin/env python3
"""Client script for "ws-client".

This client's functions are plain Python functions defined below. The web app runs one like:

    python3 ws_client.py --<function> <arg1> <arg2> ...

The arguments are passed positionally, in the order the function declares them.

Make calls to the system through the load balancer with the `lb` helper (see lbclient.py):

    def checkout(order_id):
        r = lb.post("/orders-service/orders/checkout", {"order_id": order_id})
        if r.get("status") == "valid":
            lb.post("/payments-api/complete-payment", {"token": r["token"]})
        else:
            lb.post("/orders-service/orders/cancel", {"order_id": order_id})

Use real control flow — if/else, loops, and chaining one call's response into the next. `lb`
records every call so the web app can show the results and trace them on the diagram. CLI
arguments arrive as strings; coerce them (int(...), == "true", …) where a function needs a
number or boolean.
"""
import json
import os
import sys
from urllib.parse import quote

from lbclient import lb


# This client's on-disk "local storage" — the host-side stand-in for a browser's
# per-client localStorage. It's a single JSON object keyed by websocket client id;
# each client's bucket holds the messages it currently knows about. Kept next to this
# script (script-relative, not cwd-relative) so it lands in the same place whether the
# app spawns us from the system dir or the repo root.
LOCAL_STORAGE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ws-client.storage.json")


def _load_local_storage():
    try:
        with open(LOCAL_STORAGE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (FileNotFoundError, ValueError):
        return {}


def _save_local_storage(store):
    with open(LOCAL_STORAGE_PATH, "w", encoding="utf-8") as f:
        json.dump(store, f, indent=2)
        f.write("\n")


def _to_message(notification):
    """Transform a Notification {id, to, from, message, sentAt} into the client's current
    message structure {msgId, from, to, body, sentAt} (the shape ws-client.mjs sends and
    dedupes on), so restored notifications look just like messages the client received."""
    return {
        "msgId": notification.get("id"),
        "from": notification.get("from"),
        "to": notification.get("to"),
        "body": notification.get("message"),
        "sentAt": notification.get("sentAt"),
    }


# === functions ===
# Authored functions go here. Each is a top-level `def <name>(<args>): ...` and is registered
# in FUNCTIONS below so `--<name>` can invoke it.


def getNotifications(id):
    """Fetch every notification addressed to websocket client `id` (Notification.to == id)
    and fill that client's local storage with them, transformed to the current message shape."""
    # notification-service GET /notifications takes the user id as a query param (lb.get can't
    # carry a body). The path literal is kept clean (no query string) so the diagram statically
    # traces this to notification-service; the ?id=... is appended only at call time.
    r = lb.get("/notification-service/notifications" + "?id=" + quote(id))

    # A healthy response is {"notifications": [...]}; a db outage comes back as a 503
    # {"detail": ...} with no "notifications" key. Only populate storage on a valid list.
    if not (isinstance(r, dict) and isinstance(r.get("notifications"), list)):
        return r

    messages = [_to_message(n) for n in r["notifications"]]

    # Fill up this client's bucket in local storage (keyed by its websocket client id).
    store = _load_local_storage()
    store[id] = {"notifications": messages}
    _save_local_storage(store)
    return r


# === end functions ===


# Maps a function name to its callable. Register every function defined above.
FUNCTIONS = {
    "getNotifications": getNotifications,
}


def main(argv):
    if not argv or not argv[0].startswith("--"):
        sys.exit("usage: python3 ws_client.py --<function> [args...]")
    name = argv[0][2:]
    fn = FUNCTIONS.get(name)
    if fn is None:
        sys.exit("unknown function: %s" % name)
    fn(*argv[1:])


if __name__ == "__main__":
    main(sys.argv[1:])
