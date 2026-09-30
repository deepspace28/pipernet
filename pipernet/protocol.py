"""Length-prefixed binary framing for node-to-node messages.

Frame = 8-byte header (body_len u32, payload_len u32) + JSON body + payload.
Payload carries raw chunk/shard bytes; body carries the operation.

Secure mode: every request opens with an X25519 handshake (Curve25519),
then all subsequent frames are ChaCha20-Poly1305 sealed. Sealed frame
= 4-byte outer length + sealed(t_len || body || payload).
"""

import asyncio
import json
import struct

from .crypto import (
    aead_chacha20_poly1305_open,
    aead_chacha20_poly1305_seal,
    hkdf_sha256,
    x25519,
    x25519_secret,
)

_HDR = struct.Struct(">II")
_PREFIX = struct.Struct(">I")


async def send_msg(writer: asyncio.StreamWriter, body: dict, payload: bytes = b""):
    raw = json.dumps(body).encode()
    writer.write(_HDR.pack(len(raw), len(payload)) + raw + payload)
    await writer.drain()


async def recv_msg(reader: asyncio.StreamReader):
    """Returns (body, payload). (None, None) on clean EOF."""
    try:
        hdr = await reader.readexactly(8)
    except (asyncio.IncompleteReadError, ConnectionError):
        return None, None
    blen, plen = _HDR.unpack(hdr)
    try:
        body_raw = await reader.readexactly(blen) if blen else b"{}"
        payload = await reader.readexactly(plen) if plen else b""
    except (asyncio.IncompleteReadError, ConnectionError):
        return None, None
    return json.loads(body_raw), payload


# ---- secure channel (Curve25519 -> HKDF -> ChaCha20-Poly1305) --------------

class SecureChannel:
    """Framed request/response over one negotiated session.

    Key schedule per connection: fresh X25519 keypair each side,
    HKDF-SHA256 splits the shared secret into per-direction 32-byte
    ChaCha20 keys; frame sequence number doubles as the AEAD nonce.
    """

    def __init__(self, tx_key: bytes, rx_key: bytes):
        self.tx_key = tx_key
        self.rx_key = rx_key
        self.tx_seq = 0
        self.rx_seq = 0

    async def send(self, writer, body: dict, payload: bytes = b""):
        raw = json.dumps(body).encode()
        inner = _HDR.pack(len(raw), len(payload)) + raw + payload
        sealed = aead_chacha20_poly1305_seal(
            self.tx_key, self.tx_seq.to_bytes(12, "big"), inner
        )
        self.tx_seq += 1
        writer.write(_PREFIX.pack(len(sealed)) + sealed)
        await writer.drain()

    async def recv(self, reader):
        try:
            sealed = await reader.readexactly(_PREFIX.unpack(
                await reader.readexactly(4))[0])
        except (asyncio.IncompleteReadError, ConnectionError):
            return None, None
        inner = aead_chacha20_poly1305_open(
            self.rx_key, self.rx_seq.to_bytes(12, "big"), sealed
        )
        self.rx_seq += 1
        blen, plen = _HDR.unpack(inner[:8])
        body = json.loads(inner[8 : 8 + blen])
        payload = inner[8 + blen : 8 + blen + plen]
        return body, payload


def _session_keys(priv: bytes, peer_pub: bytes) -> tuple[bytes, bytes]:
    """(initiator->responder, responder->initiator) 32-byte keys."""
    shared = x25519(priv, peer_pub)
    okm = hkdf_sha256(shared, b"pipernet", b"PiperNet secure channel v1", 64)
    return okm[:32], okm[32:64]


async def client_channel(reader, writer, timeout: float = 5.0) -> SecureChannel:
    priv, pub = x25519_secret()
    await send_msg(writer, {"op": "kx", "pub": pub.hex()})
    body, _ = await asyncio.wait_for(recv_msg(reader), timeout)
    if not body or body.get("op") != "kx" or "pub" not in body:
        raise ValueError("peer did not answer the X25519 handshake")
    ikm_keys = _session_keys(priv, bytes.fromhex(body["pub"]))
    return SecureChannel(ikm_keys[0], ikm_keys[1])


async def server_channel(reader, writer) -> SecureChannel:
    body, _ = await recv_msg(reader)
    if not body or body.get("op") != "kx" or "pub" not in body:
        raise ValueError("expected X25519 kx first on a secure node")
    priv, pub = x25519_secret()
    await send_msg(writer, {"op": "kx", "pub": pub.hex()})
    ikm_keys = _session_keys(priv, bytes.fromhex(body["pub"]))
    return SecureChannel(ikm_keys[1], ikm_keys[0])
