"""The simulated LLM: a tiny 3-layer transformer with real per-sequence KV caches.

Deterministic weights (fixed rng seed), greedy argmax decoding. The vocabulary is
a-z (tokens 0-25); END_TOKEN (26) is emitted onto the redis stream when a sequence
finishes but is never fed back through the model. Output length is random per
sequence (1-100 tokens).

A finished Sequence IS the prefix cache entry: its KV caches hold every token of
the conversation so far, so a follow-up prompt in the same chat only prefills the
new tokens (continue_with) instead of re-running the whole history.
"""

import numpy as np

VOCAB = 26       # a-z
END_TOKEN = 26   # end-of-output marker on the stream (outside the model vocab)
D = 8
N_LAYERS = 3

# Deterministic model weights; a separate generator draws the random output lengths.
_weights_rng = np.random.default_rng(0)
_len_rng = np.random.default_rng()

W_embed = _weights_rng.normal(size=(VOCAB, D)) * 0.1
W_unembed = _weights_rng.normal(size=(D, VOCAB)) * 0.1

LAYERS = [
    {
        "Wq": _weights_rng.normal(size=(D, D)) * 0.1,
        "Wk": _weights_rng.normal(size=(D, D)) * 0.1,
        "Wv": _weights_rng.normal(size=(D, D)) * 0.1,
        "Wo": _weights_rng.normal(size=(D, D)) * 0.1,
        "Wup": _weights_rng.normal(size=(D, 4 * D)) * 0.1,
        "Wdown": _weights_rng.normal(size=(4 * D, D)) * 0.1,
    }
    for _ in range(N_LAYERS)
]


def tokenize(text):
    """Lowercase, keep only a-z (each mapped to 0-25), skip everything else."""
    return [ord(c) - ord("a") for c in (text or "").lower() if "a" <= c <= "z"]


def detokenize(tokens):
    return "".join(chr(ord("a") + t) for t in tokens if 0 <= t < VOCAB)


def _softmax(x, axis=-1):
    x = x - x.max(axis=axis, keepdims=True)
    e = np.exp(x)
    return e / e.sum(axis=axis, keepdims=True)


def _attention_step(layer, x_new, kv_cache):
    q = x_new @ layer["Wq"]
    k = x_new @ layer["Wk"]
    v = x_new @ layer["Wv"]

    kv_cache["k"].append(k)
    kv_cache["v"].append(v)

    K = np.stack(kv_cache["k"])
    V = np.stack(kv_cache["v"])

    scores = (q @ K.T) / np.sqrt(D)
    weights = _softmax(scores)
    ctx = weights @ V
    return ctx @ layer["Wo"]


def _mlp(layer, x):
    h = np.maximum(0, x @ layer["Wup"])
    return h @ layer["Wdown"]


def forward_one_token(token, caches):
    """One incremental forward pass: extend every layer's KV cache, return logits."""
    x = W_embed[token]
    for i, layer in enumerate(LAYERS):
        x = x + _attention_step(layer, x, caches[i])
        x = x + _mlp(layer, x)
    return x @ W_unembed


class Sequence:
    """One prompt working through the batch. Only the worker loop touches a
    Sequence while it is in `active`; only add_prompt touches it while cached —
    so per-sequence state needs no lock of its own."""

    def __init__(self, seq_id, user_message_id, chat, prompt_tokens):
        self.seq_id = seq_id
        self.user_message_id = user_message_id
        self.chat = chat
        self.caches = [{"k": [], "v": []} for _ in range(N_LAYERS)]
        self.tokens = []                    # everything fed through the model so far
        self.pending = list(prompt_tokens)  # waiting to be prefilled
        self.generated = []
        self.next_token = None
        self.target_len = int(_len_rng.integers(1, 101))
        self.done = False

    @property
    def prefilled(self):
        return not self.pending

    def continue_with(self, seq_id, user_message_id, new_tokens):
        """Prefix-cache reuse: keep the KV caches + token history, take on a new
        identity, and schedule only the NEW tokens for prefill."""
        self.seq_id = seq_id
        self.user_message_id = user_message_id
        self.pending = list(new_tokens)
        self.generated = []
        self.next_token = None
        self.target_len = int(_len_rng.integers(1, 101))
        self.done = False
        return self

    def prefill(self):
        """Feed all pending tokens through the model (extending the KV caches);
        the last token's logits pick the first generated token."""
        logits = None
        for tok in self.pending:
            logits = forward_one_token(tok, self.caches)
            self.tokens.append(tok)
        self.pending = []
        self.next_token = int(np.argmax(logits))

    def decode_step(self):
        """Commit the pending next_token, compute the one after. Returns the
        token generated this step (what the caller streams to redis)."""
        tok = self.next_token
        self.generated.append(tok)
        self.tokens.append(tok)
        if len(self.generated) >= self.target_len:
            self.done = True
            return tok
        logits = forward_one_token(tok, self.caches)
        self.next_token = int(np.argmax(logits))
        return tok
