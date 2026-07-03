"""Durable, on-disk chunk storage — the single shared store used by every node.

Layout under `root` (a per-node bind-mounted volume, so it survives a restart):
    chunks/<id>.bin   one file per chunk the node holds
    bitmap.bin        one byte per chunk (1 = held on disk, 0 = missing)
    source/           pre-staged / downloaded source files (coordinator only)

The bitmap is the authoritative "what do I have" record. It is persisted with an
atomic replace immediately after each chunk lands — and crucially BEFORE the node
asks the coordinator for more work — so the on-disk bitmap can never claim a chunk
that isn't fully written. That invariant is what makes resume-after-restart correct.
"""

import hashlib
import os
import threading


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class ChunkStore:
    def __init__(self, root: str):
        self.root = root
        self.chunks_dir = os.path.join(root, "chunks")
        self.bitmap_path = os.path.join(root, "bitmap.bin")
        os.makedirs(self.chunks_dir, exist_ok=True)
        self._lock = threading.Lock()
        self.checksums = []  # per-chunk sha256 hex, from the manifest (for serving/verify)
        self.bitmap = bytearray()
        self._load_bitmap()

    # --- manifest / sizing -------------------------------------------------
    def set_manifest(self, chunk_count, checksums=None):
        """Size the bitmap to the file's chunk count, marking any chunk already on
        disk as held (this is the resume path), and remember per-chunk checksums."""
        with self._lock:
            self.checksums = list(checksums) if checksums else []
            bm = bytearray(chunk_count)
            for i in range(chunk_count):
                if os.path.exists(self._chunk_path(i)):
                    bm[i] = 1
            self.bitmap = bm
            self._persist_bitmap_locked()

    def _chunk_path(self, i):
        return os.path.join(self.chunks_dir, f"{i}.bin")

    def _load_bitmap(self):
        try:
            with open(self.bitmap_path, "rb") as f:
                self.bitmap = bytearray(f.read())
        except FileNotFoundError:
            self.bitmap = bytearray()

    def _persist_bitmap_locked(self):
        tmp = self.bitmap_path + ".tmp"
        with open(tmp, "wb") as f:
            f.write(self.bitmap)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.bitmap_path)

    # --- queries -----------------------------------------------------------
    def has(self, i):
        with self._lock:
            return 0 <= i < len(self.bitmap) and self.bitmap[i] == 1

    def bitmap_bytes(self):
        with self._lock:
            return bytes(self.bitmap)

    def held(self):
        with self._lock:
            return [i for i, b in enumerate(self.bitmap) if b == 1]

    def missing(self):
        with self._lock:
            return [i for i, b in enumerate(self.bitmap) if b == 0]

    def is_complete(self):
        with self._lock:
            return len(self.bitmap) > 0 and all(self.bitmap)

    def count_held(self):
        with self._lock:
            return sum(1 for b in self.bitmap if b == 1)

    # --- chunk IO ----------------------------------------------------------
    def read_chunk(self, i):
        with open(self._chunk_path(i), "rb") as f:
            return f.read()

    def checksum(self, i):
        if i < len(self.checksums) and self.checksums[i]:
            return self.checksums[i]
        return sha256_hex(self.read_chunk(i))

    def write_chunk(self, i, data, checksum=None):
        """Verify (optional) then atomically write chunk i, then flip + persist the
        bitmap bit BEFORE returning. The order is the whole point: bitmap reflects
        on-disk state, never the other way around."""
        if checksum is not None and sha256_hex(data) != checksum:
            raise ValueError(f"checksum mismatch for chunk {i}")
        tmp = self._chunk_path(i) + ".tmp"
        with open(tmp, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self._chunk_path(i))
        with self._lock:
            if i >= len(self.bitmap):
                self.bitmap.extend(bytes(i + 1 - len(self.bitmap)))
            self.bitmap[i] = 1
            self._persist_bitmap_locked()

    def full_hash(self, chunk_count):
        """sha256 of the whole file, reassembled from chunks 0..chunk_count-1 in order."""
        h = hashlib.sha256()
        for i in range(chunk_count):
            h.update(self.read_chunk(i))
        return h.hexdigest()


def chunk_file(path, chunk_size):
    """Yield (index, data) for each chunk of a file. Used by the coordinator to seed
    its own store from the staged source file."""
    with open(path, "rb") as f:
        i = 0
        while True:
            data = f.read(chunk_size)
            if not data:
                break
            yield i, data
            i += 1
