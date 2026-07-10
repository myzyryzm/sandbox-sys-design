#!/usr/bin/env python3
"""Client script for "frontend".

This client's functions are plain Python functions defined below. The web app runs one like:

    python3 frontend.py --<function> <arg1> <arg2> ...

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

If this client is STATEFUL, use `state` to remember data across runs (a later run reads what an
earlier one saved) — otherwise `state` is an in-memory scratchpad discarded at exit:

    def login(username, password):
        r = lb.post("/auth-service/login", {"username": username, "password": password})
        state.set("token", r.get("token"))   # persisted for the next run (if stateful)

    def get_profile():
        token = state.get("token")           # value login() saved on an earlier run
        return lb.get("/auth-service/me?token=" + (token or ""))
"""
import sys
import time
import urllib.request

from lbclient import lb, state


# === functions ===
# Authored functions go here. Each is a top-level `def <name>(<args>): ...` and is registered
# in FUNCTIONS below so `--<name>` can invoke it.


def _current_user_id():
    """Return this client's user_id, always a well-formed integer.

    `frontend` is a stateful client, so the id is minted once and persisted; every later run
    reuses the same user_id — the way a signed-in frontend keeps identifying the same user.
    """
    user_id = state.get("user_id")
    if not user_id:
        user_id = int(time.time() * 1000)  # a fresh, sortable, well-formed integer id
        state.set("user_id", user_id)
    return int(user_id)


def _send_user_header(user_id):
    """Make every lb call this run carry `X-User-Id: <user_id>`.

    chat-service reads the caller's user_id from the X-User-Id header, but the shared `lb`
    helper takes no headers (and we must not edit lbclient.py). urllib's public opener API lets
    us add a default header to the very `urlopen(req)` call lb makes, so `lb.get(...)` below is
    still a plain, statically-traceable call to the real endpoint — it just now identifies us.
    """
    opener = urllib.request.build_opener()
    opener.addheaders.append(("X-User-Id", str(user_id)))
    urllib.request.install_opener(opener)


def getChats():
    """List the frontend user's chats, newest-updated first, via chat-service.getChats.

    Always supplies a valid user_id (from this stateful client's persisted identity) in the
    X-User-Id header, then reads the chats back if the call succeeded.
    """
    user_id = _current_user_id()
    _send_user_header(user_id)

    # Real endpoint, literal path -> the diagram traces frontend -> chat-service -> chat-db.
    result = lb.get("/chat-service/chats")

    # Branch on the response: only walk the chats when the call came back with a chat list.
    if isinstance(result, dict) and isinstance(result.get("chats"), list):
        chats = result["chats"]
        state.set("last_chat_count", len(chats))
        return chats
    return result


# === end functions ===


# Maps a function name to its callable. Register every function defined above.
FUNCTIONS = {
    "getChats": getChats,
}


def main(argv):
    if not argv or not argv[0].startswith("--"):
        sys.exit("usage: python3 frontend.py --<function> [args...]")
    name = argv[0][2:]
    fn = FUNCTIONS.get(name)
    if fn is None:
        sys.exit("unknown function: %s" % name)
    fn(*argv[1:])


if __name__ == "__main__":
    main(sys.argv[1:])
