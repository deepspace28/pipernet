"""Regression test: __version__ must agree with pyproject.toml."""


import os
import re

import pipernet


def _read_pyproject_version():
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    with open(os.path.join(root, "pyproject.toml")) as f:
        for line in f:
            m = re.match(r'version\s*=\s*"([^"]+)"', line.strip())
            if m:
                return m.group(1)
    raise RuntimeError("version not found in pyproject.toml")


def test_version_matches_pyproject():
    assert pipernet.__version__ == _read_pyproject_version()


def test_crypto_exports_available():
    from pipernet import (
        aead_chacha20_poly1305_open,
        aead_chacha20_poly1305_seal,
        x25519_secret,
        x25519_shared,
    )
    assert callable(aead_chacha20_poly1305_open)
    assert callable(aead_chacha20_poly1305_seal)
    assert callable(x25519_secret)
    assert callable(x25519_shared)
