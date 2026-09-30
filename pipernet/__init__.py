"""PiperNet — Pied Piper prototype V0.2."""

from .chunking import CHUNK_SIZE, chunk_data, cid, chunk_to_cids
from .crypto import aead_chacha20_poly1305_open, aead_chacha20_poly1305_seal, x25519_secret, x25519_shared
from .erasure import DATA_SHARDS, PARITY_SHARDS, ErasureCodec, InsufficientShards, STRIP
from .node import Node
from .dashboard import Dashboard
from .protocol import SecureChannel

__version__ = "0.3.0"
__all__ = [
    "Node",
    "Dashboard",
    "ErasureCodec",
    "InsufficientShards",
    "SecureChannel",
    "chunk_data",
    "cid",
    "chunk_to_cids",
    "CHUNK_SIZE",
    "DATA_SHARDS",
    "PARITY_SHARDS",
    "STRIP",
]
