"""Unit tests for PiperNet core (asyncio.run-based, no plugin needed)."""

import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pipernet import Node, chunk_data, cid


def run(coro):
    return asyncio.run(coro)


def test_chunking_roundtrip():
    data = os.urandom(5 * 1024 * 1024 + 123)  # 5 chunks + ragged tail
    chunks, cids = None, None
    chunks = chunk_data(data)
    cids = [cid(c) for c in chunks]
    assert len(chunks) == 6
    assert b"".join(chunks) == data
    assert len(set(cids)) == 6


def test_cid_is_content_address():
    assert cid(b"hello") == cid(b"hello")
    assert cid(b"hello") != cid(b"hello ")


async def _mesh(n=6, replication=3):
    nodes = [await Node(replication=replication).start() for _ in range(n)]
    for a in nodes:
        for b in nodes:
            if b is not a:
                a.add_peer((b.host, b.port))
    return nodes


def test_put_get_roundtrip_across_mesh():
    async def t():
        nodes = await _mesh(6, replication=3)
        try:
            data = os.urandom(1024 * 1024 + 7)
            fid = await nodes[0].put(data, name="x.bin")
            for n in nodes:
                assert await n.get(fid) == data
        finally:
            for n in nodes:
                await n.stop()
    run(t())


def test_stream_survives_node_death_without_repair():
    async def t():
        nodes = await _mesh(8, replication=5)
        try:
            data = os.urandom(2 * 1024 * 1024 + 3)
            fid = await nodes[0].put(data, name="v.mp4")
            # kill the origin outright
            await nodes[0].stop()
            nodes[0].alive = False
            survivor = nodes[3]
            assert await survivor.get(fid) == data
        finally:
            for n in nodes:
                if n.alive:
                    await n.stop()
    run(t())


def test_repair_restores_replication_after_failure():
    async def t():
        nodes = await _mesh(8, replication=5)
        try:
            data = os.urandom(1024 * 512)
            fid = await nodes[0].put(data, name="v.mp4")
            victims = nodes[1:4]  # take out 3 holders
            for v in victims:
                await v.stop()
                v.alive = False
            alive = [n for n in nodes if n.alive]
            # origin (nodes[0]) keeps its copies -> repair loop should top up
            # on remaining peers until replication target is met
            deadline = asyncio.get_event_loop().time() + 15
            m = nodes[0].manifests[fid]
            while asyncio.get_event_loop().time() < deadline:
                counts = [min(m["holders"].values())]
                if all(c >= 5 for c in counts):
                    break
                await asyncio.sleep(0.5)
            assert min(m["holders"].values()) >= 5
            assert await alive[0].get(fid) == data
        finally:
            for n in nodes:
                if n.alive:
                    await n.stop()
    run(t())


def test_get_unknown_file():
    async def t():
        node = await Node().start()
        try:
            assert await node.get("deadbeef") is None
        finally:
            await node.stop()
    run(t())


def test_status_shape():
    async def t():
        node = await Node().start()
        try:
            s = node.status()
            assert set(s) == {"addr", "chunks", "files", "peers", "dead_peers", "uptime"}
            assert s["chunks"] == 0
        finally:
            await node.stop()
    run(t())
