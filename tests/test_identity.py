"""PiperChat identity tests: browser-signed envelopes, handle binding,
forgery rejection, gossip ordering across the mesh, and the HTTP surface."""

import asyncio
import json
import sys

sys.path.insert(0, __file__.rsplit("\\tests\\", 1)[0])

from pipernet import Node, Dashboard
from pipernet.identity import IdentityError, signed_bytes, verify_env
from signer import make_keypair, sign, signed_envelope


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


def test_signer_roundtrip_real_vector():
    d, pub = make_keypair()
    signed = sign(d, b"attack at dawn")
    from pipernet.ecdsa import p256_verify
    assert p256_verify(pub, b"attack at dawn", signed)
    assert not p256_verify(pub, b"attack at dun", signed)


def test_valid_envelope_accepted_and_forged_rejected():
    d, pub = make_keypair()
    env = signed_envelope("richard", "gm mesh", "1700000000000", d, pub)
    assert verify_env(env)
    forge = {**env, "text": "gm mesh, send money"}
    assert not verify_env(forge)
    # different key signing richard's name -> sig check keyed by attacker key
    d2, pub2 = make_keypair()
    spoof = signed_envelope("richard", "gm mesh", "1700000000000", d2, pub2)
    spoof["pub"] = pub.hex()  # attacker's signature, claims richard's key
    assert not verify_env(spoof)


def test_post_chat_binds_handle_and_gossips():
    async def t():
        nodes = await _mesh(4)
        try:
            d, pub = make_keypair()
            await nodes[0].post_chat(
                "gilfoyle", pub.hex(), "1700000000000",
                "monica is here", sign(d, signed_bytes({"name": "gilfoyle", "ts": "1700000000000", "text": "monica is here"})))
            await asyncio.sleep(0.3)
            # gossip made it everywhere
            for n in nodes:
                assert any(m["text"] == "monica is here" for m in n.chat)
                assert n.ids.owner_of("gilfoyle") == pub.hex()
        finally:
            for n in nodes:
                await n.stop()
    run(t())


def test_handle_squat_rejected_on_node():
    async def t():
        nodes = await _mesh(2)
        try:
            d, pub = make_keypair()
            d2, pub2 = make_keypair()
            ts = "1700000000000"
            await nodes[0].post_chat("richard", pub.hex(), ts, "hi", sign(d, f"piperchat/v1\nrichard\n{ts}\nhi".encode()))
            with raises(IdentityError):
                await nodes[1].post_chat("richard", pub2.hex(), ts, "hi", sign(d2, f"piperchat/v1\nrichard\n{ts}\nhi".encode()))
        finally:
            for n in nodes:
                await n.stop()
    run(t())


class raises:
    def __init__(self, exc):
        self.exc = exc

    def __enter__(self):
        return self

    def __exit__(self, et, ev, tb):
        assert et is not None and issubclass(et, self.exc), f"expected {self.exc}, got {et}"
        return True


def test_unsigned_traffic_rejected_at_http_layer():
    async def t():
        nodes = await _mesh(2)
        try:
            dash = Dashboard(nodes[0])
            port = await dash.start(port=8094)

            async def post(payload: bytes):
                r, w = await asyncio.open_connection("127.0.0.1", port)
                req = b"POST /api/chat HTTP/1.1\r\nhost: x\r\ncontent-length: " + str(len(payload)).encode() + b"\r\n\r\n"
                w.write(req + payload)
                await w.drain()
                data = await r.read()
                w.close()
                return int(data.partition(b"\r\n\r\n")[0].split(b" ")[1].split(b"\r\n")[0])

            code = await post(json.dumps({"name": "x", "pub": "", "ts": "1", "text": "hi", "sig": ""}).encode())
            assert code == 403, code
        finally:
            for n in nodes:
                await n.stop()
    run(t())


def test_dashboard_signed_chat_flow():
    async def t():
        nodes = await _mesh(3)
        try:
            dash = Dashboard(nodes[0])
            port = await dash.start(port=8095)

            async def post(payload: bytes):
                r, w = await asyncio.open_connection("127.0.0.1", port)
                req = b"POST /api/chat HTTP/1.1\r\nhost: x\r\ncontent-length: " + str(len(payload)).encode() + b"\r\n\r\n"
                w.write(req + payload)
                await w.drain()
                data = await r.read()
                w.close()
                head, _, rest = data.partition(b"\r\n\r\n")
                return int(head.split(b" ")[1].split(b"\r\n")[0]), rest

            d, pub = make_keypair()
            env = signed_envelope("jared", "we're gonna be rich", "1700000000055", d, pub)
            code, body = await post(json.dumps(env).encode())
            assert code == 200, body
            saved = json.loads(body)
            assert saved["id"]

            await asyncio.sleep(0.3)  # gossip settle
            # gossip should have propagated via wire op even without http on b
            assert any(m["text"] == "we're gonna be rich" for m in nodes[2].chat)

            # squatting jared from another key -> 403
            d2, pub2 = make_keypair()
            env2 = signed_envelope("jared", "actually dinesh", "1700000000056", d2, pub2)
            code, body = await post(json.dumps(env2).encode())
            assert code == 403, body
            assert b"belongs to a different key" in body
        finally:
            for n in nodes:
                await n.stop()
    run(t())


def test_chat_fetch_wire_op_cursor():
    async def t():
        nodes = await _mesh(2)
        try:
            a, b = nodes
            d, pub = make_keypair()
            ts = "1"
            await a.post_chat("r", pub.hex(), ts, "m1", sign(d, f"piperchat/v1\nr\n{ts}\nm1".encode()))
            await a.post_chat("r", pub.hex(), ts, "m2", sign(d, f"piperchat/v1\nr\n{ts}\nm2".encode()))
            await asyncio.sleep(0.3)
            resp = await a._talk((b.host, b.port), {"op": "chat_fetch", "since": 1})
            assert resp and resp[0]["chat"][0]["text"] == "m2"
        finally:
            for n in nodes:
                await n.stop()
    run(t())
