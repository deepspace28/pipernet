"""Persistent identity keyfile tests: load/create, roundtrip, tamper rejection."""

import asyncio
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pipernet import Node, create_identity, load_identity, load_or_create_identity


def _tmpkey():
    fd, path = tempfile.mkstemp(suffix=".key")
    os.close(fd)
    os.remove(path)
    return path


def test_create_then_load_roundtrip():
    path = _tmpkey()
    try:
        assert load_identity(path) is None
        priv, pub = create_identity(path)
        got = load_or_create_identity(path)
        assert got == (priv, pub)
        # no regeneration on a second load
        again = load_or_create_identity(path)
        assert again == (priv, pub)
    finally:
        if os.path.exists(path):
            os.remove(path)


def test_tampered_pub_is_rejected():
    path = _tmpkey()
    try:
        priv, pub = create_identity(path)
        zero_pub = (b"\x00" * 32).hex()
        with open(path, "w", encoding="utf-8") as f:
            f.write("priv " + priv.hex() + "\npub " + zero_pub + "\n")
        try:
            load_identity(path)
            raise AssertionError("forged pub accepted")
        except ValueError:
            pass
    finally:
        if os.path.exists(path):
            os.remove(path)


def test_corrupted_priv_key_derives_mismatch():
    path = _tmpkey()
    try:
        priv, pub = create_identity(path)
        with open(path, "w", encoding="utf-8") as f:
            f.write(f"priv {os.urandom(32).hex()}\npub {pub.hex()}\n")
        try:
            load_identity(path)
            raise AssertionError("mismatched priv/pub accepted")
        except ValueError:
            pass
    finally:
        if os.path.exists(path):
            os.remove(path)


def test_malformed_file_is_rejected():
    path = _tmpkey()
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write("priv abc\n")
        try:
            load_identity(path)
            raise AssertionError("malformed file accepted")
        except ValueError:
            pass
    finally:
        if os.path.exists(path):
            os.remove(path)


def test_node_persists_identity_across_restarts():
    async def t():
        path = _tmpkey()
        try:
            a = await Node(secure=True, key_path=path).start()
            pub_first = a.static_pub
            await a.stop()
            b = await Node(secure=True, key_path=path).start()
            assert b.static_pub == pub_first
            await b.stop()
        finally:
            if os.path.exists(path):
                os.remove(path)
    asyncio.run(t())


def test_identity_and_key_path_are_exclusive():
    ident = (os.urandom(32), os.urandom(32))
    try:
        Node(identity=ident, key_path="x.key")
        raise AssertionError("both params accepted")
    except ValueError:
        pass
