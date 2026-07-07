import numpy as np
import threading
import time 

VOCAB = 27
LAST_TOKEN = 26
D = 8
N_LAYERS = 3
MAX_ACTIVE = 5
TTL = 30 # tunable param
np.random.seed(0)

W_embed = np.random.randn(VOCAB, D) * 0.1
W_unembed = np.random.randn(D, VOCAB) * 0.1

layers = []
for _ in range(N_LAYERS):
    layers.append({
        "Wq": np.random.randn(D, D) * 0.1,
        "Wk": np.random.randn(D, D) * 0.1,
        "Wv": np.random.randn(D, D) * 0.1,
        "Wo": np.random.randn(D, D) * 0.1,
        "Wup":   np.random.randn(D, 4 * D) * 0.1,
        "Wdown": np.random.randn(4 * D, D) * 0.1,
    })

class Sequence:
    def __init__(self, seq_id, prompt):
        self.id = seq_id
        self.prompt = prompt
        self.caches = [{"k": [], "v": []} for _ in range(N_LAYERS)]
        self.target_len = np.random.randint(1, 101)
        self.generated = []
        self.next_token = None
        self.prefilled = False
        self.done = False

    def prefill(self):
        logits = None
        for tok in self.prompt:
            logits = self.forward_one_token(tok, self.caches)
        self.next_token = int(np.argmax(logits))
        self.prefilled = True

    def decode_step(self):
        self.generated.append(self.next_token)
        if len(self.generated) == self.target_len:
            self.done = True
            return
        logits = self.forward_one_token(self.next_token, self.caches)
        self.next_token = int(np.argmax(logits))


    def forward_one_token(token_id, caches):
        x = W_embed[token_id]
        for i, layer in enumerate(layers):
            x = x + self.attention_step(layer, x, caches[i])
            x = x + self.mlp(layer, x)
        return x @ W_unembed

    def attention_step(layer, x_new, kv_cache):
        q = x_new @ layer["Wq"]
        k = x_new @ layer["Wk"]
        v = x_new @ layer["Wv"]

        kv_cache["k"].append(k)
        kv_cache["v"].append(v)

        K = np.stack(kv_cache["k"])
        V = np.stack(kv_cache["v"])

        scores = (q @ K.T) / np.sqrt(D)
        weights = self.softmax(scores)
        ctx = weights @ V
        return ctx @ layer["Wo"]

    def mlp(layer, x):
        h = np.maximum(0, x @ layer["Wup"])
        return h @ layer["Wdown"]
    
    def softmax(x, axis=-1):
        x = x - x.max(axis=axis, keepdims=True)
        e = np.exp(x)
        return e / e.sum(axis=axis, keepdims=True)


lock = threading.Lock()
active = []
cached = []
counter = 0

# Worker.proto.AddSequence
def add_sequence(prompt):
    # if in cache then load from cache and append prompt
    # else query database
    seq = Sequence(counter, prompt)
    counter += 1
    active.append(seq)
    return seq.id

# Worker.proto.GetStatus
def get_status():
    if len(active) > MAX_ACTIVE:
        return False
    return True

def step():
    for seq in active:
        if not seq.prefilled:
            seq.prefill()
        else:
            seq.decode_step()

        redis.stream(seq.next_token) # stream next token to redis stream

def worker():
    while active:
        step()
        with lock:
            new_active = []
            for s in active:
                if s.done:
                    cached.append((s, time.time()))
                else:
                    new_active.append(s)
            active = new_active

def reaper():
    while True:
        time.sleep(2)
        now = time.time()
        with lock:
            stale = [k for k, t in cached if now - t > TTL]
            for k in stale:
                del cached[k]

t1 = threading.Thread(target=foreground, daemon=True)
t2 = threading.Thread(target=reaper, daemon=True)
t1.start()
t2.start()