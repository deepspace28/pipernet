"""A headless PiperNet peer node.

Each node:
  - stores content-addressed chunks (its local object store)
  - accepts uploads, deconstructs them into chunks and replicates them
    onto peers (multi-peer replication)
  - serves files by fetching chunks from the network and verifying CIDs
  - runs a background repair loop: it heartbeats peers, detects failures
    and re-stores missing chunks to restore the target replica count
"""

import asyncio
import logging
import time

from .chunking import CHUNK_SIZE, chunk_data, cid
from .protocol import recv_msg, send_msg

log = logging.getLogger("pipernet")


class Node:
    def __init__(self, host: str = "127.0.0.1", port: int = 0, replication: int = 3):
        self.host = host
        self.port = port
        self.replication = replication
        self.store: dict[str, bytes] = {}
        # file_id -> {"name", "size", "chunks": [cid], "holders": {cid: n}}
        self.manifests: dict[str, dict] = {}
        self.peers: list[tuple[str, int]] = []
        self.dead_peers: set[tuple[str, int]] = set()
        self.server = None
        self._repair_task = None
        self.started_at = time.time()

    # ---- lifecycle -------------------------------------------------

    async def start(self) -> "Node":
        self.server = await asyncio.start_server(self._handle_peer, self.host, self.port)
        self.port = self.server.sockets[0].getsockname()[1]
        self._repair_task = asyncio.create_task(self._repair_loop())
        return self

    async def stop(self):
        if self._repair_task:
            self._repair_task.cancel()
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        self.alive = False

    alive = True

    def add_peer(self, addr: tuple[str, int]):
        if addr not in self.peers:
            self.peers.append(addr)

    # ---- wire handler ----------------------------------------------

    async def _handle_peer(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            while True:
                body, payload = await recv_msg(reader)
                if body is None:
                    break
                op = body.get("op")
                if op == "ping":
                    await send_msg(writer, {"ok": True})
                elif op == "store":
                    c = body["cid"]
                    self.store[c] = payload
                    await send_msg(writer, {"ok": True})
                elif op == "fetch":
                    c = body["cid"]
                    data = self.store.get(c)
                    if data is None:
                        await send_msg(writer, {"ok": False})
                    else:
                        await send_msg(writer, {"ok": True}, data)
                elif op == "manifest":
                    self.manifests.update(body["manifests"])
                    await send_msg(writer, {"ok": True})
                elif op == "has":
                    c = body["cid"]
                    await send_msg(writer, {"ok": c in self.store})
                else:
                    await send_msg(writer, {"ok": False, "err": "unknown op"})
        except Exception as e:
            log.debug("peer handler error: %s", e)
        finally:
            writer.close()


    # ---- peer IO ----------------------------------------------------

    async def _talk(self, addr, body: dict, payload: bytes = b"", timeout: float = 5.0):
        """One request/response to a peer. Returns (body, payload) or None."""
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(*addr), timeout
            )
        except (OSError, asyncio.TimeoutError):
            self.dead_peers.add(addr)
            return None
        try:
            await send_msg(writer, body, payload)
            resp = await asyncio.wait_for(recv_msg(reader), timeout)
        except (OSError, asyncio.TimeoutError, asyncio.IncompleteReadError):
            self.dead_peers.add(addr)
            return None
        finally:
            writer.close()
        if resp[0] is None:
            self.dead_peers.add(addr)
            return None
        self.dead_peers.discard(addr)
        return resp

    def _live_peers(self) -> list[tuple[str, int]]:
        return [p for p in self.peers if p not in self.dead_peers]

    def is_live(self, addr) -> bool:
        return addr not in self.dead_peers

    async def _store_on(self, addr, c: str, chunk: bytes) -> bool:
        resp = await self._talk(addr, {"op": "store", "cid": c}, chunk)
        return bool(resp and resp[0].get("ok"))

    async def _fetch_chunk(self, c: str):
        # Try local store first (origin copy), then peers.
        if c in self.store:
            return self.store[c]
        for addr in self._live_peers():
            resp = await self._talk(addr, {"op": "fetch", "cid": c})
            if resp and resp[0].get("ok"):
                data = resp[1]
                if cid(data) == c:
                    return data
        return None

    async def _chunk_holder_count(self, c: str) -> int:
        n = 0
        for addr in self._live_peers():
            resp = await self._talk(addr, {"op": "has", "cid": c})
            if resp and resp[0].get("ok"):
                n += 1
        return n

    # ---- client API -------------------------------------------------

    async def put(self, data: bytes, name: str = "file.bin") -> str:
        """Upload: chunk -> CID -> replicate onto peers (plus local origin copy)."""
        chunks = chunk_data(data)
        cids = [cid(c) for c in chunks]
        file_id = cid(name.encode() + b"\x00" + b"".join(c.encode() for c in cids))
        targets = self._live_peers()

        holders: dict[str, int] = {}
        for c, chunk in zip(cids, chunks):
            n = 0
            for addr in targets[: self.replication]:
                if await self._store_on(addr, c, chunk):
                    n += 1
            self.store[c] = chunk  # origin keeps an authoritative copy
            holders[c] = n + 1
        self.manifests[file_id] = {
            "name": name,
            "size": len(data),
            "chunks": cids,
            "holders": holders,
            "origin": (self.host, self.port),
        }
        await self._broadcast_manifests()
        log.info("put %s (%d chunks, %d bytes)", name, len(chunks), len(data))
        return file_id

    async def _broadcast_manifests(self):
        for addr in self._live_peers():
            await self._talk(addr, {"op": "manifest", "manifests": self.manifests})

    async def get(self, file_id: str):
        """Fetch + reassemble a file, verifying every chunk CID."""
        m = self.manifests.get(file_id)
        if m is None:
            return None
        parts = []
        for c in m["chunks"]:
            data = await self._fetch_chunk(c)
            if data is None:
                raise RuntimeError(f"chunk {c[:12]} unavailable on any node")
            parts.append(data)
        return b"".join(parts)

    # ---- self-healing ------------------------------------------------

    async def _repair_loop(self):
        while True:
            try:
                await self._repair_pass()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("repair pass failed")
            await asyncio.sleep(2.0)

    async def _repair_pass(self):
        """Check every ISR file's chunks; top up replicas after node loss."""
        for file_id, m in list(self.manifests.items()):
            live = self._live_peers()
            for c in m["chunks"]:
                holders = await self._chunk_holder_count(c)
                m["holders"][c] = holders + 1  # +1 local origin copy
                if holders + 1 < self.replication:
                    chunk = self.store.get(c)
                    if chunk is None:
                        continue  # cannot repair what we cannot source
                    for addr in live:
                        resp = await self._talk(addr, {"op": "has", "cid": c})
                        has = bool(resp and resp[0].get("ok"))
                        if not has:
                            if await self._store_on(addr, c, chunk):
                                m["holders"][c] += 1
                                log.info("repaired chunk %s -> %s", c[:12], addr[1])
                            if m["holders"][c] >= self.replication:
                                break

    # ---- stats -------------------------------------------------------

    def status(self) -> dict:
        return {
            "addr": f"{self.host}:{self.port}",
            "chunks": len(self.store),
            "files": {
                fid: {"name": m["name"], "size": m["size"], "chunks": len(m["chunks"])}
                for fid, m in self.manifests.items()
            },
            "peers": len(self.peers),
            "dead_peers": sorted(p[1] for p in self.dead_peers),
            "uptime": round(time.time() - self.started_at, 1),
        }
