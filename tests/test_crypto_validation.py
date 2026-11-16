"""Input-validation tests for pipernet.crypto."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest

from pipernet.crypto import (
    aead_chacha20_poly1305_open,
    aead_chacha20_poly1305_seal,
    hkdf_sha256,
    x25519,
)


def test_aead_seal_rejects_bad_key():
    with pytest.raises(ValueError, match="32 bytes"):
        aead_chacha20_poly1305_seal(b"short", b"\x00" * 12, b"pt")


def test_aead_seal_rejects_bad_nonce():
    with pytest.raises(ValueError, match="12 bytes"):
        aead_chacha20_poly1305_seal(b"\x00" * 32, b"short", b"pt")


def test_aead_open_rejects_bad_key():
    with pytest.raises(ValueError, match="32 bytes"):
        aead_chacha20_poly1305_open(b"short", b"\x00" * 12, b"\x00" * 16)


def test_aead_open_rejects_bad_nonce():
    with pytest.raises(ValueError, match="12 bytes"):
        aead_chacha20_poly1305_open(b"\x00" * 32, b"short", b"\x00" * 16)


def test_aead_open_rejects_short_sealed():
    with pytest.raises(ValueError, match="too short"):
        aead_chacha20_poly1305_open(b"\x00" * 32, b"\x00" * 12, b"\x00" * 15)


def test_hkdf_rejects_zero_length():
    with pytest.raises(ValueError, match="positive"):
        hkdf_sha256(b"ikm", b"salt", b"info", 0)


def test_hkdf_accepts_rfc_maximum_length():
    assert len(hkdf_sha256(b"ikm", b"salt", b"info", 255 * 32)) == 255 * 32


def test_hkdf_rejects_length_above_rfc_limit():
    with pytest.raises(ValueError, match="exceed"):
        hkdf_sha256(b"ikm", b"salt", b"info", 255 * 32 + 1)


def test_x25519_rejects_bad_scalar():
    with pytest.raises(ValueError):
        x25519(b"short", b"\x00" * 32)


def test_x25519_rejects_bad_u():
    with pytest.raises(ValueError):
        x25519(b"\x00" * 32, b"short")
