"""Persistent node identity — load or create an X25519 keypair on disk.

File format (one line each, hex):
    priv <64 hex chars>
    pub  <64 hex chars>

The public key is re-derived from the private key on load; a mismatch
means the file was tampered with or corrupted and the load refuses.
"""

from __future__ import annotations

import os

from .crypto import x25519, x25519_secret

_BASE = (9).to_bytes(32, "little")


def load_identity(path: str) -> tuple[bytes, bytes] | None:
    """Return (priv, pub) from an identity file, or None if it does not exist."""
    if not os.path.exists(path):
        return None
    priv_hex = pub_hex = None
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line.startswith("priv "):
                priv_hex = line[5:]
            elif line.startswith("pub "):
                pub_hex = line[4:]
    if not priv_hex or not pub_hex:
        raise ValueError(f"{path}: malformed identity file (need 'priv <hex>' and 'pub <hex>')")
    priv = bytes.fromhex(priv_hex)
    pub = bytes.fromhex(pub_hex)
    if len(priv) != 32 or len(pub) != 32:
        raise ValueError(f"{path}: identity keys must be 32 bytes each")
    if x25519(priv, _BASE) != pub:
        raise ValueError(f"{path}: public key does not match private key (corrupted or forged)")
    return priv, pub


def create_identity(path: str) -> tuple[bytes, bytes]:
    """Generate a fresh keypair and persist it. Overwrites an existing file."""
    priv, pub = x25519_secret()
    _write_identity(path, priv, pub)
    return priv, pub


def _write_identity(path: str, priv: bytes, pub: bytes):
    body = f"priv {priv.hex()}\npub {pub.hex()}\n"
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(body)
    try:
        os.chmod(tmp, 0o600)
    except OSError:
        pass
    os.replace(tmp, path)


def load_or_create_identity(path: str) -> tuple[bytes, bytes]:
    """The one-call API for Node(key_path=...): load the keypair, or mint
    and persist one on first run. Raises on any tampering."""
    ident = load_identity(path)
    if ident is None:
        ident = create_identity(path)
    return ident
