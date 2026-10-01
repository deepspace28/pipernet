"""PiperNet crypto — pure stdlib, RFC-vector-verified.

- X25519 key agreement  (RFC 7748, Montgomery ladder over GF(2^255-19))
- ChaCha20 stream cipher (RFC 8439 sec 2.4)
- Poly1305 authenticator (RFC 8439 sec 2.5)
- HKDF-SHA256 key derivation (RFC 5869)

No third-party dependencies — same zero-dep stance as the rest of PiperNet.
The "curve5221" ticket name resolves to Curve25519; this module lands the
handshake so wire encryption can be layered on `protocol.py` next.
"""

from __future__ import annotations

import hashlib
import hmac
import secrets

P = 2**255 - 19
A24 = 121665  # (A - 2) / 2 for the Montgomery curve y^2 = x^3 + 486662x^2 + x
_BASE = (9).to_bytes(32, "little")

# ---------------------------------------------------------------------------
# X25519 (RFC 7748)
# ---------------------------------------------------------------------------

def x25519_clamp(scalar: bytes) -> bytes:
    if len(scalar) != 32:
        raise ValueError("scalar must be 32 bytes")
    s = bytearray(scalar)
    s[0] &= 248
    s[31] &= 127
    s[31] |= 64
    return bytes(s)


def _decode_scalar(s: bytes) -> int:
    return int.from_bytes(x25519_clamp(s), "little")


def _decode_u(u: bytes) -> int:
    if len(u) != 32:
        raise ValueError("u-coordinate must be 32 bytes")
    ub = bytearray(u)
    ub[31] &= 127  # RFC 7748: clear the most significant bit (bit 255)
    return int.from_bytes(ub, "little")


def x25519(scalar: bytes, u_bytes: bytes) -> bytes:
    """RFC 7748 X25519(k, u) -> 32-byte shared secret / public key."""
    k = _decode_scalar(scalar)
    u = _decode_u(u_bytes)
    x_2, z_2 = 1, 0
    x_3, z_3 = u, 1
    swap = 0
    for t in range(254, -1, -1):
        k_t = (k >> t) & 1
        swap ^= k_t
        if swap:
            x_2, x_3 = x_3, x_2
            z_2, z_3 = z_3, z_2
        swap = k_t
        a = (x_2 + z_2) % P
        aa = (a * a) % P
        b = (x_2 - z_2) % P
        bb = (b * b) % P
        e = (aa - bb) % P
        c = (x_3 + z_3) % P
        d = (x_3 - z_3) % P
        da = (d * a) % P
        cb = (c * b) % P
        x_3 = ((da + cb) * (da + cb)) % P
        z_3 = (u * ((da - cb) * (da - cb))) % P
        x_2 = (aa * bb) % P
        z_2 = (e * (aa + A24 * e)) % P
    if swap:
        x_2, x_3 = x_3, x_2
        z_2, z_3 = z_3, z_2
    return ((x_2 * pow(z_2, P - 2, P)) % P).to_bytes(32, "little")


def x25519_secret() -> tuple[bytes, bytes]:
    """Return (private scalar, public key)."""
    priv = secrets.token_bytes(32)
    return priv, x25519(priv, _BASE)


def x25519_shared(priv: bytes, peer_pub: bytes) -> bytes:
    return x25519(priv, peer_pub)


# ---------------------------------------------------------------------------
# ChaCha20 (RFC 8439)
# ---------------------------------------------------------------------------

def _keystream_block(x0, x1, x2, x3, x4, x5, x6, x7,
                     x8, x9, x10, x11, x12, x13, x14, x15, M):
    """One ChaCha20 block (20 rounds) -> (x0..x15)+state packed as one 64-byte int.

    Straight-line: no list indexing, no function calls in the hot loop."""
    y0, y1, y2, y3 = x0, x1, x2, x3
    y4, y5, y6, y7 = x4, x5, x6, x7
    y8, y9, y10, y11 = x8, x9, x10, x11
    y12, y13, y14, y15 = x12, x13, x14, x15
    for _ in range(10):
        # column rounds
        t = (x0 + x4) & M; x12 ^= t; x12 = ((x12 << 16) & M) | (x12 >> 16)
        u = (x8 + x12) & M; x4 ^= u; x4 = ((x4 << 12) & M) | (x4 >> 20)
        t = (t + x4) & M; x12 ^= t; x12 = ((x12 << 8) & M) | (x12 >> 24)
        u = (u + x12) & M; x4 ^= u; x4 = ((x4 << 7) & M) | (x4 >> 25)
        x0, x8 = t, u

        t = (x1 + x5) & M; x13 ^= t; x13 = ((x13 << 16) & M) | (x13 >> 16)
        u = (x9 + x13) & M; x5 ^= u; x5 = ((x5 << 12) & M) | (x5 >> 20)
        t = (t + x5) & M; x13 ^= t; x13 = ((x13 << 8) & M) | (x13 >> 24)
        u = (u + x13) & M; x5 ^= u; x5 = ((x5 << 7) & M) | (x5 >> 25)
        x1, x9 = t, u

        t = (x2 + x6) & M; x14 ^= t; x14 = ((x14 << 16) & M) | (x14 >> 16)
        u = (x10 + x14) & M; x6 ^= u; x6 = ((x6 << 12) & M) | (x6 >> 20)
        t = (t + x6) & M; x14 ^= t; x14 = ((x14 << 8) & M) | (x14 >> 24)
        u = (u + x14) & M; x6 ^= u; x6 = ((x6 << 7) & M) | (x6 >> 25)
        x2, x10 = t, u

        t = (x3 + x7) & M; x15 ^= t; x15 = ((x15 << 16) & M) | (x15 >> 16)
        u = (x11 + x15) & M; x7 ^= u; x7 = ((x7 << 12) & M) | (x7 >> 20)
        t = (t + x7) & M; x15 ^= t; x15 = ((x15 << 8) & M) | (x15 >> 24)
        u = (u + x15) & M; x7 ^= u; x7 = ((x7 << 7) & M) | (x7 >> 25)
        x3, x11 = t, u

        # diagonal rounds
        t = (x0 + x5) & M; x15 ^= t; x15 = ((x15 << 16) & M) | (x15 >> 16)
        u = (x10 + x15) & M; x5 ^= u; x5 = ((x5 << 12) & M) | (x5 >> 20)
        t = (t + x5) & M; x15 ^= t; x15 = ((x15 << 8) & M) | (x15 >> 24)
        u = (u + x15) & M; x5 ^= u; x5 = ((x5 << 7) & M) | (x5 >> 25)
        x0, x10 = t, u

        t = (x1 + x6) & M; x12 ^= t; x12 = ((x12 << 16) & M) | (x12 >> 16)
        u = (x11 + x12) & M; x6 ^= u; x6 = ((x6 << 12) & M) | (x6 >> 20)
        t = (t + x6) & M; x12 ^= t; x12 = ((x12 << 8) & M) | (x12 >> 24)
        u = (u + x12) & M; x6 ^= u; x6 = ((x6 << 7) & M) | (x6 >> 25)
        x1, x11 = t, u

        t = (x2 + x7) & M; x13 ^= t; x13 = ((x13 << 16) & M) | (x13 >> 16)
        u = (x8 + x13) & M; x7 ^= u; x7 = ((x7 << 12) & M) | (x7 >> 20)
        t = (t + x7) & M; x13 ^= t; x13 = ((x13 << 8) & M) | (x13 >> 24)
        u = (u + x13) & M; x7 ^= u; x7 = ((x7 << 7) & M) | (x7 >> 25)
        x2, x8 = t, u

        t = (x3 + x4) & M; x14 ^= t; x14 = ((x14 << 16) & M) | (x14 >> 16)
        u = (x9 + x14) & M; x4 ^= u; x4 = ((x4 << 12) & M) | (x4 >> 20)
        t = (t + x4) & M; x14 ^= t; x14 = ((x14 << 8) & M) | (x14 >> 24)
        u = (u + x14) & M; x4 ^= u; x4 = ((x4 << 7) & M) | (x4 >> 25)
        x3, x9 = t, u

    return ((((x0 + y0) & M)
            | (((x1 + y1) & M) << 32) | (((x2 + y2) & M) << 64) | (((x3 + y3) & M) << 96)
            | (((x4 + y4) & M) << 128) | (((x5 + y5) & M) << 160) | (((x6 + y6) & M) << 192)
            | (((x7 + y7) & M) << 224) | (((x8 + y8) & M) << 256) | (((x9 + y9) & M) << 288)
            | (((x10 + y10) & M) << 320) | (((x11 + y11) & M) << 352) | (((x12 + y12) & M) << 384)
            | (((x13 + y13) & M) << 416) | (((x14 + y14) & M) << 448) | (((x15 + y15) & M) << 480)))


def chacha20_xor(key: bytes, counter: int, nonce: bytes, data: bytes) -> bytes:
    """RFC 8439 stream cipher; XOR keystream, 96-bit nonce, 32-bit counter."""
    if len(key) != 32:
        raise ValueError("ChaCha20 key must be 32 bytes")
    if len(nonce) != 12:
        raise ValueError("ChaCha20 nonce must be 12 bytes")
    n = len(data)
    if n == 0:
        return b""
    M = 0xFFFFFFFF
    c0, c1, c2, c3 = 0x61707865, 0x3320646E, 0x79622D32, 0x6B206574
    k0 = int.from_bytes(key[0:4], "little");  k1 = int.from_bytes(key[4:8], "little")
    k2 = int.from_bytes(key[8:12], "little"); k3 = int.from_bytes(key[12:16], "little")
    k4 = int.from_bytes(key[16:20], "little"); k5 = int.from_bytes(key[20:24], "little")
    k6 = int.from_bytes(key[24:28], "little"); k7 = int.from_bytes(key[28:32], "little")
    m0 = int.from_bytes(nonce[0:4], "little")
    m1 = int.from_bytes(nonce[4:8], "little")
    m2 = int.from_bytes(nonce[8:12], "little")

    blocks = (n + 63) >> 6
    ks = bytearray()
    for bi in range(blocks):
        x0, x1, x2, x3 = c0, c1, c2, c3
        x4, x5, x6, x7 = k0, k1, k2, k3
        x8, x9, x10, x11 = k4, k5, k6, k7
        x12 = (counter + bi) & M
        x13, x14, x15 = m0, m1, m2
        ks += _keystream_block(x0, x1, x2, x3, x4, x5, x6, x7,
                               x8, x9, x10, x11, x12, x13, x14, x15, M).to_bytes(64, "little")
    return (int.from_bytes(bytes(ks[:n]), "little")
            ^ int.from_bytes(data, "little")).to_bytes(n, "little")


# ---------------------------------------------------------------------------
# Poly1305 (RFC 8439 sec 2.5)
# ---------------------------------------------------------------------------

def poly1305(key: bytes, message: bytes) -> bytes:
    """16-byte one-time key -> 16-byte tag (RFC 8439)."""
    if len(key) != 32:
        raise ValueError("Poly1305 key must be 32 bytes")
    r = int.from_bytes(key[:16], "little") & 0x0FFFFFFC0FFFFFFC0FFFFFFC0FFFFFFF
    s = int.from_bytes(key[16:32], "little")
    p = (1 << 130) - 5
    acc = 0
    for i in range(0, len(message), 16):
        block = message[i : i + 16] + b"\x01"  # every block gets the 2^(8*len) tern appended
        acc = ((acc + int.from_bytes(block, "little")) * r) % p
    return ((acc + s) & ((1 << 128) - 1)).to_bytes(16, "little")


def _pad16(x: bytes) -> bytes:
    return b"\x00" * (-len(x) % 16)


def poly1305_key_gen(key: bytes, nonce: bytes) -> bytes:
    """RFC 8439 sec 2.6: one-time Poly1305 key = ChaCha20 block(key, 0, nonce)[:32]."""
    return chacha20_xor(key, 0, nonce, b"\x00" * 32)


def aead_chacha20_poly1305_seal(key: bytes, nonce: bytes, plaintext: bytes, aad: bytes = b"") -> bytes:
    """RFC 8439 sec 2.8 AEAD_CHACHA20_POLY1305 -> ciphertext || 16-byte tag."""
    otk = poly1305_key_gen(key, nonce)
    ct = chacha20_xor(key, 1, nonce, plaintext)
    mac_data = (
        aad + _pad16(aad)
        + ct + _pad16(ct)
        + len(aad).to_bytes(8, "little")
        + len(ct).to_bytes(8, "little")
    )
    return ct + poly1305(otk, mac_data)


def aead_chacha20_poly1305_open(key: bytes, nonce: bytes, sealed: bytes, aad: bytes = b"") -> bytes:
    if len(sealed) < 16:
        raise ValueError("sealed frame too short")
    ct, tag = sealed[:-16], sealed[-16:]
    otk = poly1305_key_gen(key, nonce)
    mac_data = (
        aad + _pad16(aad)
        + ct + _pad16(ct)
        + len(aad).to_bytes(8, "little")
        + len(ct).to_bytes(8, "little")
    )
    if not hmac.compare_digest(poly1305(otk, mac_data), tag):
        raise ValueError("AEAD tag mismatch — frame was tampered with")
    return chacha20_xor(key, 1, nonce, ct)


# ---------------------------------------------------------------------------
# HKDF-SHA256 (RFC 5869)
# ---------------------------------------------------------------------------

def hkdf_sha256(ikm: bytes, salt: bytes, info: bytes, length: int = 32) -> bytes:
    prk = hmac.new(salt or b"\x00" * 32, ikm, hashlib.sha256).digest()
    t = b""
    okm = b""
    i = 1
    while len(okm) < length:
        t = hmac.new(prk, t + info + bytes([i]), hashlib.sha256).digest()
        okm += t
        i += 1
    return okm[:length]


def session_keys(priv: bytes, peer_pub: bytes) -> tuple[bytes, bytes]:
    """X25519 -> HKDF -> (a→b 32B key, b→a 32B key)."""
    shared = x25519_shared(priv, peer_pub)
    okm = hkdf_sha256(shared, b"pipernet", b"PiperNet v0.3 session keys", 64)
    return okm[:32], okm[32:64]
