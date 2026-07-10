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
import json
import random
import string
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


# --- endToEnd: simulate a full, human-like chat session -------------------------
# Mimics a person using the chat UI for `user_id`: open a chat (a random existing
# one, else create one), load its history, then loop — send a random message,
# stream the assistant's reply, pause proportional to the reply length, and
# probabilistically stay / return to a previous chat / start a new one.

# The client run is hard-killed at 30s, so we bound our own loop well under that and
# exit cleanly (so the recorded calls are still emitted). These are safety caps, not
# part of the behavior the description asks for.
_END_TO_END_BUDGET_SECONDS = 25.0
_END_TO_END_MAX_TURNS = 100


def _random_content():
    """A user message: lower-case letters, 1..100 characters long."""
    n = random.randint(1, 100)
    return "".join(random.choices(string.ascii_lowercase, k=n))


def _as_messages(resp):
    """Normalize a getMessagesForChat response into [{role, content}] (earliest first)."""
    rows = resp.get("messages", []) if isinstance(resp, dict) else []
    return [{"role": m.get("role"), "content": m.get("content")} for m in rows]


def _assistant_text_from_sse(raw):
    """Reconstruct the assistant's reply from the createMessage SSE response.

    createMessage streams `data: {"token": "<char>"}` frames, then a done/error
    frame. lb.post returns the raw event-stream text; we concatenate the per-token
    payloads into the full assistant message (non-token frames contribute nothing).
    """
    if not isinstance(raw, str):
        return ""
    out = []
    for line in raw.splitlines():
        line = line.strip()
        if not line.startswith("data:"):
            continue
        try:
            frame = json.loads(line[5:].strip())
        except ValueError:
            continue
        if isinstance(frame, dict) and "token" in frame:
            out.append(str(frame["token"]))
    return "".join(out)


def endToEnd(user_id):
    """Drive a full chat session for `user_id` against chat-service.

    `user_id` (a string arg) identifies the caller in the X-User-Id header on every
    call. Opens a chat, loads its messages, then loops: send a random user message,
    stream the assistant's reply, wait a bit, then decide to stay on the chat, go
    back to a previous chat, or start a new one.
    """
    # Every lb.* call this run carries `X-User-Id: <user_id>` (chat-service reads the
    # caller from this header for all four endpoints below).
    _send_user_header(user_id)

    # chats = chat-service.getChats()
    listed = lb.get("/chat-service/chats")
    chats = listed.get("chats", []) if isinstance(listed, dict) else []

    # cur_chat = a random existing chat, or a freshly created one if the user has none.
    if chats:
        cur_chat = random.choice(chats)
    else:
        created = lb.post("/chat-service/chats", {})
        cur_chat = created if isinstance(created, dict) else {}
        chats = [cur_chat]
    cur_chat_id = cur_chat.get("id")

    # messages = chat-service.getMessagesForChat(cur_chat)
    messages = _as_messages(lb.get(f"/chat-service/chat-messages/{cur_chat_id}"))

    deadline = time.monotonic() + _END_TO_END_BUDGET_SECONDS
    for _ in range(_END_TO_END_MAX_TURNS):
        if time.monotonic() >= deadline:
            break

        # A random user turn.
        content = _random_content()
        messages.append({"role": "user", "content": content})

        # assistant_content = stream chat-service.createMessage(chat_id, content).
        # createMessage is an SSE POST; lb.post sends the body, reads the whole
        # event-stream, and we rebuild the reply text from its token frames.
        assistant_content = _assistant_text_from_sse(
            lb.post(
                "/chat-service/messages",
                {"chat_id": cur_chat_id, "content": content},
            )
        )
        messages.append({"role": "assistant", "content": assistant_content})

        # Read time: proportional to reply length, capped at 10s.
        time.sleep(min(len(assistant_content) * 0.1, 10))

        # Decide what to do next. The longer the current conversation, the more
        # likely the user moves on (switches chats).
        turns = len(messages) // 2
        p_switch = min(0.9, 0.2 * turns)
        if random.random() >= p_switch:
            continue  # stay on the same chat

        # Switching: start a NEW chat vs. return to a PREVIOUS one. With fewer chats
        # to return to, a new chat is more likely (only the current chat -> always new).
        n_chats = max(1, len(chats))
        others = [c for c in chats if c.get("id") != cur_chat_id]
        if not others or random.random() < 1.0 / n_chats:
            # New chat -> create it, switch to it, and clear the conversation.
            created = lb.post("/chat-service/chats", {})
            new_id = created.get("id") if isinstance(created, dict) else None
            if new_id is not None:
                cur_chat = created
                cur_chat_id = new_id
                chats.append(cur_chat)
                messages = []
        else:
            # Previous chat -> switch to a different existing chat and load its history.
            cur_chat = random.choice(others)
            cur_chat_id = cur_chat.get("id")
            messages = _as_messages(
                lb.get(f"/chat-service/chat-messages/{cur_chat_id}")
            )


# === end functions ===


# Maps a function name to its callable. Register every function defined above.
FUNCTIONS = {
    "getChats": getChats,
    "endToEnd": endToEnd,
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
