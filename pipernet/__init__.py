"""PiperNet — Pied Piper prototype V0.2."""

from .chunking import CHUNK_SIZE, chunk_data, cid, chunk_to_cids
from .erasure import DATA_SHARDS, PARITY_SHARDS, ErasureCodec, InsufficientShards, STRIP
from .node import Node
from .dashboard import Dashboard

__version__ = "0.2.0"
__all__ = [
    "Node",
    "Dashboard",
    "ErasureCodec",
    "InsufficientShards",
    "chunk_data",
    "cid",
    "chunk_to_cids",
    "CHUNK_SIZE",
    "DATA_SHARDS",
    "PARITY_SHARDS",
    "STRIP",
]
