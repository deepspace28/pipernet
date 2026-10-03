"""Test-side ECDSA P-256 signer (pure python) — mirrors what WebCrypto
produces (SHA-256 digest + DER signature + raw uncompressed pubkey).
Used to fake-browser-test the PiperChat identity layer."""

import hashlib
import secrets

from pipernet.ecdsa import N, P, Gx, Gy, _add, _mul


def make_keypair():
    d = secrets.randbits(256) % N or 1
    pub = _mul(d, (Gx, Gy))
    pub_raw = b"\x04" + pub[0].to_bytes(32, "big") + pub[1].to_bytes(32, "big")
    return d, pub_raw


def _der(r, s):
    rb = r.to_bytes((r.bit_length() + 7) // 8 or 1, "big")
    sb = s.to_bytes((s.bit_length() + 7) // 8 or 1, "big")
    if rb[0] & 0x80:
        rb = b"\x00" + rb
    if sb[0] & 0x80:
        sb = b"\x00" + sb
    body = bytes([2, len(rb)]) + rb + bytes([2, len(sb)]) + sb
    return bytes([0x30, len(body)]) + body


def sign(d, message: bytes) -> bytes:
    e = int.from_bytes(hashlib.sha256(message).digest(), "big") % N
    while True:
        k = secrets.randbits(256) % N or 1
        R = _mul(k, (Gx, Gy))
        r = R[0] % N
        if r == 0:
            continue
        s = (pow(k, -1, N) * (e + r * d)) % N
        if s != 0:
            return _der(r, s)


def signed_envelope(name: str, text: str, ts: str, d, pub_raw: bytes) -> dict:
    payload = f"piperchat/v1\n{name}\n{ts}\n{text}".encode()
    return {
        "name": name,
        "pub": pub_raw.hex(),
        "ts": ts,
        "text": text,
        "sig": sign(d, payload).hex(),
    }
