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


def node_identity() -> tuple[bytes, bytes]:
    """Fresh long-lived node identity: (static priv, static pub)."""
    return x25519_secret()


def _kx_keys(dh_es: bytes, dh_ee: bytes) -> tuple[bytes, bytes]:
    """Session keys from DH_es || DH_ee (identity binds the transcript)."""
    okm = hkdf_sha256(dh_es + dh_ee, b"pipernet", b"PiperNet secure channel v2", 64)
    return okm[:32], okm[32:64]


async def client_channel(reader, writer, timeout: float = 5.0,
                          pin: bytes | None = None) -> tuple[SecureChannel, bytes]:
    """Open a secure channel. Returns (channel, peer_static_pub).

    pin: expected responder static pub. The responder proves possession in
    the kx reply via dh2 = X25519(s_priv, e_initiator_pub); a MITM without
    the static key cannot produce it. Without a pin the peer key is learned
    (TOFU) and returned so callers can pin it for later connections."""
    e_priv, e_pub = x25519_secret()
    await send_msg(writer, {"op": "kx", "pub": e_pub.hex()})
    body, _ = await asyncio.wait_for(recv_msg(reader), timeout)
    if not body or body.get("op") != "kx" or "pub" not in body or "dh2" not in body:
        raise ValueError("peer did not answer the X25519 handshake")
    s_pub = bytes.fromhex(body["pub"])
    if pin is not None and s_pub != pin:
        raise ValueError("peer static key does not match pinned identity (possible MITM)")
    dh_es = x25519(e_priv, s_pub)
    if bytes.fromhex(body["dh2"]) != dh_es:
        raise ValueError("peer failed the identity proof (dh2 mismatch)")
    keys = _kx_keys(dh_es, dh_es)
    return SecureChannel(keys[0], keys[1]), s_pub


async def server_channel(reader, writer,
                         static: tuple[bytes, bytes] | None = None) -> SecureChannel:
    """Answer the kx; send dh2 = X25519(s_priv, e_client_pub) as the
    proof of static-key possession the client verifies against its pin."""
    body, _ = await recv_msg(reader)
    if not body or body.get("op") != "kx" or "pub" not in body:
        raise ValueError("expected X25519 kx first on a secure node")
    if static is None:
        s_priv, s_pub = x25519_secret()  # no fixed identity: per-conn keypair
    else:
        s_priv, s_pub = static
    e_pub = bytes.fromhex(body["pub"])
    dh_es = x25519(s_priv, e_pub)
    keys = _kx_keys(dh_es, dh_es)
    await send_msg(writer, {"op": "kx", "pub": s_pub.hex(), "dh2": dh_es.hex()})
    return SecureChannel(keys[1], keys[0])
