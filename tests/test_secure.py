"""Secure-mode tests: X25519 handshake per connection, sealed frames, tamper rejection."""

import asyncio
import json
import os
import struct
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pipernet import Node, SecureChannel


def run(coro):
    return asyncio.run(coro)


def _mesh(n, **kw):
    async def build():
        nodes = [await Node(**kw).start() for _ in range(n)]
        for a in nodes:
            for b in nodes:
                if b is not a:
                    a.add_peer((b.host, b.port))
        return nodes
    return build()


def test_secure_ping_and_status():
    async def t():
        node = await Node(secure=True).start()
        try:
            resp = await node._talk((node.host, node.port), {"op": "ping"})
            assert resp and resp[0].get("ok") is True
            s = node.status()
            assert s["secure"] is True
        finally:
            await node.stop()
    run(t())


def test_secure_put_get_roundtrip():
    async def t():
        nodes = await _mesh(10, mode="erasure", secure=True)
        try:
            data = os.urandom(1024 * 1024 + 11)
            fid = await nodes[0].put(data, name="sec.mp4")
            assert await nodes[4].get(fid) == data
            assert await nodes[9].get(fid) == data
        finally:
            for n in nodes:
                await n.stop()
    run(t())


def test_secure_survives_node_death():
    async def t():
        nodes = await _mesh(20, mode="erasure", secure=True)
        try:
            data = os.urandom(2 * 1024 * 1024 + 21)
            fid = await nodes[0].put(data, name="v.mp4")
            for v in nodes[1:5]:
                v.alive = False
                await v.stop()
            survivor = nodes[16]
            resp_wait = await survivor.get(fid)
            assert resp_wait == data
        finally:
            for n in nodes:
                if n.alive:
                    await n.stop()
    run(t())


def test_insecure_peer_talking_to_secure_node_fails_cleanly():
    async def t():
        secure_node = await Node(secure=True).start()
        plain_node = await Node(secure=False).start()
        try:
            plain_node.add_peer((secure_node.host, secure_node.port))
            resp = await plain_node._talk(
                (secure_node.host, secure_node.port), {"op": "ping"}
            )
            # secure node rejects traffic without kx -> failure surfaces as dead peer
            assert resp is None and plain_node.is_live(
                (secure_node.host, secure_node.port)
            ) is False
        finally:
            await secure_node.stop()
            await plain_node.stop()
    run(t())


def test_secure_manifest_broadcast():
    async def t():
        nodes = await _mesh(4, secure=True)
        try:
            fid = await nodes[0].put(b"hello mesh", name="h.txt")
            holders_seen = 0
            for n in nodes:
                if fid in n.manifests:
                    holders_seen += 1
            assert holders_seen >= 1
            assert await nodes[2].get(fid) == b"hello mesh"
        finally:
            for n in nodes:
                await n.stop()
    run(t())


def test_sealed_frame_tamper_raises():
    async def t():
        chan = SecureChannel(b"k" * 32, b"k" * 32)
        import io

        class FakeWriter:
            def __init__(self):
                self.buf = io.BytesIO()

            def write(self, b):
                self.buf.write(b)

            async def drain(self):
                pass

        w = FakeWriter()
        await chan.send(w, {"op": "ping"})
        blob = w.buf.getvalue()
        tampered = blob[:-1] + bytes([blob[-1] ^ 1])
        r = asyncio.StreamReader()
        r.feed_data(tampered)
        r.feed_eof()
        try:
            await chan.recv(r)
            raise AssertionError("tampered sealed frame accepted")
        except ValueError:
            pass
    asyncio.run(t())


def test_tofu_pin_is_recorded_and_second_contact_succeeds():
    async def t():
        nodes = await _mesh(2, secure=True)
        try:
            addr_b = (nodes[1].host, nodes[1].port)
            resp = await nodes[0]._talk(addr_b, {"op": "ping"})
            assert resp and resp[0].get("ok") is True
            assert nodes[0].known_ids[addr_b] == nodes[1].static_pub
            resp = await nodes[0]._talk(addr_b, {"op": "ping"})
            assert resp and resp[0].get("ok") is True
        finally:
            for n in nodes:
                await n.stop()
    run(t())


def test_mitm_identity_swap_is_rejected():
    """Fake reply: correct shape, attacker static key -> pin mismatch."""
    from pipernet.crypto import x25519_secret
    from pipernet.protocol import client_channel
    from pipernet.crypto import x25519

    async def t():
        victim_pub = x25519_secret()[1]
        attacker_priv, attacker_pub = x25519_secret()
        r = asyncio.StreamReader()

        class W:
            def write(self, b):
                pass
            async def drain(self):
                pass

        body = json.dumps({
            "op": "kx",
            "pub": attacker_pub.hex(),
            "dh2": x25519(attacker_priv, victim_pub).hex(),
        }).encode()
        r.feed_data(struct.pack(">II", len(body), 0) + body)
        r.feed_eof()
        try:
            await client_channel(r, W(), pin=victim_pub)
            raise AssertionError("impostor key accepted")
        except ValueError:
            pass
    asyncio.run(t())


def test_mitm_without_static_key_cannot_forge_dh2():
    """Impostor claims the victim's pinned pub but proves nothing."""
    from pipernet.crypto import x25519_secret
    from pipernet.protocol import client_channel
    from pipernet.crypto import x25519

    async def t():
        victim_pub = x25519_secret()[1]
        attacker_priv, _ = x25519_secret()
        r = asyncio.StreamReader()

        class W:
            def write(self, b):
                pass
            async def drain(self):
                pass

        body = json.dumps({
            "op": "kx",
            "pub": victim_pub.hex(),
            "dh2": x25519(attacker_priv, victim_pub).hex(),
        }).encode()
        r.feed_data(struct.pack(">II", len(body), 0) + body)
        r.feed_eof()
        try:
            await client_channel(r, W(), pin=victim_pub)
            raise AssertionError("forged dh2 accepted")
        except ValueError:
            pass
    asyncio.run(t())



