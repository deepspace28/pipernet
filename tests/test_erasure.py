"""Erasure-coding tests: codec math + node-level survival of shard loss."""

import asyncio
import os
import random
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pipernet import DATA_SHARDS, PARITY_SHARDS, ErasureCodec, InsufficientShards, Node, cid
from pipernet.erasure import STRIP, _invert, gf_inv, gf_mul


def test_gf_arithmetic():
    for a in range(256):
        for b in (1, 3, 57, 255):
            assert gf_mul(gf_inv(b), gf_mul(a, b)) == a
    assert gf_mul(gf_inv(2), 2) == 1


def _codec():
    return ErasureCodec(DATA_SHARDS, PARITY_SHARDS)


def test_encode_shape():
    c = _codec()
    shards = c.encode(b"x" * 1000)
    assert len(shards) == c.total
    lengths = {len(s) for s in shards}
    assert len(lengths) == 1


def test_decode_identity_and_trim():
    c = _codec()
    for size in [0, 1, 17, STRIP, 4 * STRIP + 3, 1_000_003]:
        data = os.urandom(size)
        shards = c.encode(data)
        got = c.decode([s if s is not None else None for s in shards])
        assert got == data, f"size {size} roundtrip failed"
        # minimal read: exactly k shards
        k_set = random.Random(size).sample(range(c.total), c.data)
        got2 = c.decode([shards[i] if i in k_set else None for i in range(c.total)])
        assert got2 == data


def test_survives_any_four_losses():
    c = _codec()
    rng = random.Random(42)
    data = os.urandom(300_000)
    shards = c.encode(data)
    for trial in range(50):
        lost = rng.sample(range(c.total), c.parity)
        dec = c.decode([None if i in lost else s for i, s in enumerate(shards)])
        assert dec == data, f"trial {trial}: lost idx {lost}"


def test_rejects_five_losses():
    c = _codec()
    data = os.urandom(5_000)
    shards = c.encode(data)
    lost = set(range(5))
    with pytest_InsufficientShards():
        c.decode([None if i in lost else s for i, s in enumerate(shards)])


def pytest_InsufficientShards():
    import contextlib

    @contextlib.contextmanager
    def ctx():
        try:
            yield
        except InsufficientShards:
            return
        raise AssertionError("expected InsufficientShards")

    return ctx()


def test_parity_regeneration_matches():
    c = _codec()
    data = os.urandom(64_000)
    shards = c.encode(data)
    again = c.regenerate_parity(shards[: c.data])
    assert again == shards[c.data :]


def test_inverse_is_inverse():
    c = _codec()
    rows = c.matrix[: c.data]
    inv = _invert(rows)
    for i in range(c.data):
        row = [sum(gf_mul(inv[i][j], rows[j][t]) for j in range(c.data)) for t in range(c.data)]
        assert row == [1 if t == i else 0 for t in range(c.data)]


def test_upper_bound_chunk_length_roundtrip():
    c = _codec()
    data = os.urandom(1_048_576)  # exactly the deck's 1 MB chunk
    shards = c.encode(data)
    assert len(shards[0]) == len(shards[-1])
    assert c.decode(shards) == data


async def _mesh(n=20, mode="erasure"):
    nodes = [await Node(mode=mode, replication=5).start() for _ in range(n)]
    for a in nodes:
        for b in nodes:
            if b is not a:
                a.add_peer((b.host, b.port))
    return nodes


def test_erasure_put_get_roundtrip():
    async def t():
        nodes = await _mesh(14)
        try:
            data = os.urandom(1024 * 1024 + 5)
            fid = await nodes[0].put(data, name="e.bin")
            assert nodes[0].status()["codec"] == "erasure"
            for n in (nodes[7], nodes[13]):
                assert await n.get(fid) == data
        finally:
            for n in nodes:
                await n.stop()
    asyncio.run(t())


def test_erasure_survives_four_node_loss_immediately():
    async def t():
        nodes = await _mesh(20)
        try:
            data = os.urandom(2 * 1024 * 1024 + 9)
            fid = await nodes[0].put(data, name="v.mp4")
            # the first 12 shard placements go to nodes[1..12]; kill 4 of them
            for v in nodes[1:5]:
                v.alive = False
                await v.stop()
            survivor = nodes[17]  # has no local shards; pure network decode
            assert await survivor.get(fid) == data
        finally:
            for n in nodes:
                if n.alive:
                    await n.stop()
    asyncio.run(t())


def test_erasure_origin_death_still_gettable():
    async def t():
        nodes = await _mesh(20)
        try:
            data = os.urandom(1024 * 1024)
            fid = await nodes[0].put(data, name="v.mp4")
            await nodes[0].stop()
            nodes[0].alive = False
            assert await nodes[10].get(fid) == data
        finally:
            for n in nodes:
                if n.alive:
                    await n.stop()
    asyncio.run(t())


def test_erasure_repair_restores_shards():
    async def t():
        nodes = await _mesh(20)
        try:
            data = os.urandom(1024 * 1024)
            fid = await nodes[0].put(data, name="v.mp4")
            for v in nodes[1:5]:
                v.alive = False
                await v.stop()
            origin = nodes[0]
            # drive a few repair passes manually (deterministic, no timer races)
            for _ in range(3):
                await origin._repair_pass()
                holders = origin.manifests[fid]["holders"]
                if min(holders.values()) >= DATA_SHARDS + PARITY_SHARDS:
                    break
            holders = origin.manifests[fid]["holders"]
            assert min(holders.values()) >= DATA_SHARDS + PARITY_SHARDS
            assert await nodes[15].get(fid) == data
        finally:
            for n in nodes:
                if n.alive:
                    await n.stop()
    asyncio.run(t())
