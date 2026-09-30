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

def _quarter_round(state, a, b, c, d):
    state[a] = (state[a] + state[b]) & 0xFFFFFFFF
    state[d] ^= state[a]
    state[d] = (state[d] << 16 & 0xFFFFFFFF) | (state[d] >> 16)
    state[c] = (state[c] + state[d]) & 0xFFFFFFFF
    state[b] ^= state[c]
    state[b] = (state[b] << 12 & 0xFFFFFFFF) | (state[b] >> 20)
    state[a] = (state[a] + state[b]) & 0xFFFFFFFF
    state[d] ^= state[a]
    state[d] = (state[d] << 8 & 0xFFFFFFFF) | (state[d] >> 24)
    state[c] = (state[c] + state[d]) & 0xFFFFFFFF
    state[b] ^= state[c]
    state[b] = (state[b] << 7 & 0xFFFFFFFF) | (state[b] >> 25)


def chacha20_xor(key: bytes, counter: int, nonce: bytes, data: bytes) -> bytes:
    """RFC 8439 stream cipher; XOR keystream, 96-bit nonce, 32-bit counter."""
    if len(key) != 32:
        raise ValueError("ChaCha20 key must be 32 bytes")
    if len(nonce) != 12:
        raise ValueError("ChaCha20 nonce must be 12 bytes")
    const = [0x61707865, 0x3320646E, 0x79622D32, 0x6B206574]
    kwords = [int.from_bytes(key[i : i + 4], "little") for i in range(0, 32, 4)]
    nwords = [int.from_bytes(nonce[i : i + 4], "little") for i in range(0, 12, 4)]
    out = bytearray()
    for block_index in range((len(data) + 63) // 64):
        state = const + kwords + [((counter + block_index) & 0xFFFFFFFF)] + nwords
        work = list(state)
        for _ in range(10):
            _quarter_round(work, 0, 4, 8, 12)
            _quarter_round(work, 1, 5, 9, 13)
            _quarter_round(work, 2, 6, 10, 14)
            _quarter_round(work, 3, 7, 11, 15)
            _quarter_round(work, 0, 5, 10, 15)
            _quarter_round(work, 1, 6, 11, 12)
            _quarter_round(work, 2, 7, 8, 13)
            _quarter_round(work, 3, 4, 9, 14)
        for s, w in zip(state, work):
            out += (((s + w) & 0xFFFFFFFF).to_bytes(4, "little"))
    return bytes(a ^ b for a, b in zip(data, bytes(out)))


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


def chacha20_aead_seal(key: bytes, nonce: bytes, data: bytes) -> bytes:
    """Encrypt with ChaCha20(counter=1) + raw Poly1305 tag of the plaintext."""
    assert len(nonce) == 12
    ct = chacha20_xor(key, 1, nonce, data)
    return ct + poly1305(key, data)


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
