"""ECDSA P-256 verification (pure stdlib) — PiperChat identity signatures.

Coordinates the WebCrypto output format: SHA-256 digest, DER-encoded
ECDSA-Sig-Value signature, raw uncompressed 65-byte public key.
"""

import hashlib

# NIST P-256 curve parameters
P = 2**256 - 2**224 + 2**192 + 2**96 - 1
N = 0xFFFFFFFF00000000FFFFFFFFFFFFFFFFBCE6FAADA7179E84F3B9CAC2FC632551
Gx = 0x6B17D1F2E12C4247F8BCE6E563A440F277037D812DEB33A0F4A13945D898C296
Gy = 0x4FE342E2FE1A7F9B8EE7EB4A7C0F9E162BCE33576B315ECECBB6406837BF51F5


def _inv(x, m):
    return pow(x, -1, m)


def _add(p1, p2):
    """Add two points on P-256 (points as (x, y) or None for infinity)."""
    if p1 is None:
        return p2
    if p2 is None:
        return p1
    x1, y1 = p1
    x2, y2 = p2
    if x1 == x2 and (y1 + y2) % P == 0:
        return None
    if p1 == p2:
        lam = (3 * x1 * x1 - 3) * _inv(2 * y1, P) % P  # a = -3
    else:
        lam = (y2 - y1) * _inv(x2 - x1, P) % P
    x3 = (lam * lam - x1 - x2) % P
    y3 = (lam * (x1 - x3) - y1) % P
    return (x3, y3)


def _mul(k, point):
    result = None
    addend = point
    while k:
        if k & 1:
            result = _add(result, addend)
        addend = _add(addend, addend)
        k >>= 1
    return result


def _decode_pub(raw: bytes):
    """Uncompressed 65-byte key: 0x04 || X || Y."""
    if len(raw) != 65 or raw[0] != 4:
        return None
    x, y = int.from_bytes(raw[1:33], "big"), int.from_bytes(raw[33:65], "big")
    # on-curve check: y^2 = x^3 - 3x + b
    b = 0x5AC635D8AA3A93E7B3EBBD55769886BC651D06B0CC53B0F63BCE3C3E27D2604B
    if (y * y - x * x * x - (-3 % P) * x - b) % P != 0:
        return None
    return (x, y)


def _decode_der(sig: bytes):
    """WebCrypto ECDSA signature = DER SEQUENCE of two INTEGERs."""
    try:
        assert sig[0] == 0x30
        i = 2
        assert sig[i] == 0x02
        rlen = sig[i + 1]
        r = int.from_bytes(sig[i + 2 : i + 2 + rlen], "big")
        j = i + 2 + rlen
        assert sig[j] == 0x02
        slen = sig[j + 1]
        s = int.from_bytes(sig[j + 2 : j + 2 + slen], "big")
        return r, s
    except Exception:
        return None


def p256_verify(pub_raw: bytes, message: bytes, der_sig: bytes) -> bool:
    """Verify a WebCrypto ECDSA (P-256, SHA-256) signature. Pure stdlib."""
    Q = _decode_pub(pub_raw)
    decoded = _decode_der(der_sig)
    if Q is None or decoded is None:
        return False
    r, s = decoded
    if not (0 < r < N and 0 < s < N):
        return False
    e = int.from_bytes(hashlib.sha256(message).digest(), "big")
    w = _inv(s, N)
    u1 = (e * w) % N
    u2 = (r * w) % N
    R = _add(_mul(u1, (Gx, Gy)), _mul(u2, Q))
    return R is not None and R[0] % N == r
