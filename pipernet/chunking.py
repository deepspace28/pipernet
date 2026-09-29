"""Content-addressed chunking for PiperNet.

Files are split into fixed-size chunks. Every chunk is addressed by its
SHA-256 hash (its CID), which makes storage verifiable and duplication
free.
"""

import hashlib

CHUNK_SIZE = 1 * 1024 * 1024  # 1 MB chunks (deck spec: 1-4 MB)


def chunk_data(data: bytes, chunk_size: int = CHUNK_SIZE) -> list[bytes]:
    return [data[i : i + chunk_size] for i in range(0, len(data), chunk_size)]


def cid(chunk: bytes) -> str:
    return hashlib.sha256(chunk).hexdigest()


def chunk_to_cids(data: bytes, chunk_size: int = CHUNK_SIZE):
    """Return (chunks, cids) for a payload."""
    chunks = chunk_data(data, chunk_size)
    return chunks, [cid(c) for c in chunks]
