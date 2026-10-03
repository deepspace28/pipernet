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
from .crypto import x25519_secret
from .erasure import ErasureCodec, InsufficientShards
from .identity import IdentityError, IdentityRegistry, env_id, verify_env
from .keyfile import load_or_create_identity
from .protocol import (
    client_channel,
    recv_msg,
    send_msg,
    server_channel,
)

log = logging.getLogger("pipernet")


class Node:
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 0,
        replication: int = 3,
        mode: str = "replication",
        secure: bool = False,
        identity: tuple[bytes, bytes] | None = None,
        key_path: str | None = None,
    ):
        if mode not in ("replication", "erasure"):
            raise ValueError("mode must be 'replication' or 'erasure'")
        if identity is not None and key_path is not None:
            raise ValueError("pass either identity= or key_path=, not both")
        if key_path is not None:
            identity = load_or_create_identity(key_path)
        if identity is not None and (
            len(identity) != 2
            or len(identity[0]) != 32
            or len(identity[1]) != 32
        ):
            raise ValueError("identity must be a (priv, pub) pair of 32-byte keys")
        self.host = host
        self.port = port
        self.replication = replication
        self.mode = mode
        self.secure = secure
        # long-lived node identity (static X25519 keypair). Peers pin it after
        # first contact (TOFU); later handshakes prove it via dh2 or fail.
        self.static_priv, self.static_pub = identity if identity else x25519_secret()
        self.known_ids: dict[tuple[str, int], bytes] = {}
        self.codec = ErasureCodec() if mode == "erasure" else None
        self.store: dict[str, bytes] = {}
        # erasure mode: (chunk_cid, shard_idx) -> shard bytes
        self.shards: dict[tuple[str, int], bytes] = {}
        # file_id -> {"name", "size", "chunks": [cid], "holders": {cid: n}}
        self.manifests: dict[str, dict] = {}
        self.peers: list[tuple[str, int]] = []
        self.dead_peers: set[tuple[str, int]] = set()
        self.server = None
        self._repair_task = None
        self.started_at = time.time()
        # messaging layer
        self.users: dict[str, dict] = {}      # handle -> {"addr": (host, port), "ts"}
        self.inboxes: dict[str, list] = {}    # handle -> [envelope]
        self.seen_msgs: set[str] = set()      # msg flood dedupe
        self._msg_seq = 0
        # PiperChat: signed, content-addressed message log + identity registry
        self.ids = IdentityRegistry()
        self.chat: list[dict] = []
        self._chat_seen: set[str] = set()

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
        chan = None

        async def reply(body: dict, payload: bytes = b""):
            if chan is not None:
                await chan.send(writer, body, payload)
            else:
                await send_msg(writer, body, payload)

        try:
            if self.secure:
                chan = await server_channel(reader, writer,
                                            static=(self.static_priv, self.static_pub))
            while True:
                if chan is not None:
                    body, payload = await chan.recv(reader)
                else:
                    body, payload = await recv_msg(reader)
                if body is None:
                    break
                op = body.get("op")
                if op == "ping":
                    await reply({"ok": True})
                elif op == "store":
                    c = body["cid"]
                    self.store[c] = payload
                    await reply({"ok": True})
                elif op == "fetch":
                    c = body["cid"]
                    data = self.store.get(c)
                    if data is None:
                        await reply({"ok": False})
                    else:
                        await reply({"ok": True}, data)
                elif op == "manifest":
                    self.manifests.update(body["manifests"])
                    await reply({"ok": True})
                elif op == "has":
                    c = body["cid"]
                    await reply({"ok": c in self.store})
                elif op == "store_shard":
                    key = (body["cid"], int(body["idx"]))
                    self.shards[key] = payload
                    await reply({"ok": True})
                elif op == "fetch_shard":
                    d = self.shards.get((body["cid"], int(body["idx"])))
                    await reply({"ok": False} if d is None else {"ok": True}, d or b"")
                elif op == "has_shards":
                    have = {c: sorted(i for (cc, i) in self.shards if cc == c) for c in body["cids"]}
                    await reply({"ok": True, "have": have})
                elif op == "users":
                    # gossip: merge caller's registry snapshot into ours
                    # (setdefault only: never overwrite; normalize addr -> tuple)
                    for u, r in body["users"].items():
                        self.users.setdefault(u, {"addr": tuple(r["addr"]), "ts": r["ts"]})
                    await reply({"ok": True, "users": self.users})
                elif op == "chat":
                    self._on_chat(body["env"])
                    await reply({"ok": True})
                elif op == "chat_fetch":
                    since = int(body.get("since", 0))
                    await reply({"ok": True, "chat": self.chat[since:]})
                elif op == "msg":
                    await self._on_msg(body)
                    await reply({"ok": True})
                elif op == "fetch_inbox":
                    handle = body["handle"]
                    after = int(body.get("after", 0))
                    inbox = self.inboxes.get(handle, [])
                    # store-and-forward: messages stay until claimed via cursor
                    await reply({"ok": True, "msgs": inbox[after:], "next": len(inbox)})
                else:
                    await reply({"ok": False, "err": "unknown op"})
        except Exception as e:
            log.debug("peer handler error: %s", e)
        finally:
            writer.close()


    # ---- peer IO ----------------------------------------------------

    async def _talk(self, addr, body: dict, payload: bytes = b"", timeout: float = 5.0):
        """One request/response to a peer. Returns (body, payload) or None.
        Secure mode: performs the X25519 handshake first, seals the frame."""
        chan = None
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(*addr), timeout
            )
        except (OSError, asyncio.TimeoutError):
            self.dead_peers.add(addr)
            return None
        try:
            if self.secure:
                pin = self.known_ids.get(addr)
                chan, s_pub = await asyncio.wait_for(
                    client_channel(reader, writer, pin=pin), timeout
                )
                if pin is None:
                    # first contact: trust-on-first-use pin for later connections
                    self.known_ids[addr] = s_pub
            if chan is not None:
                await chan.send(writer, body, payload)
                resp = await asyncio.wait_for(chan.recv(reader), timeout)
            else:
                await send_msg(writer, body, payload)
                resp = await asyncio.wait_for(recv_msg(reader), timeout)
        except (OSError, asyncio.TimeoutError, asyncio.IncompleteReadError, ValueError):
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
        res = await asyncio.gather(*(
            self._talk(addr, {"op": "has", "cid": c})
            for addr in self._live_peers()
        ))
        return sum(1 for r in res if r and r[0].get("ok"))

    async def _store_shard_on(self, addr, c: str, idx: int, shard: bytes) -> bool:
        resp = await self._talk(addr, {"op": "store_shard", "cid": c, "idx": idx}, shard)
        return bool(resp and resp[0].get("ok"))

    async def _fetch_shard(self, addr, c: str, idx: int):
        resp = await self._talk(addr, {"op": "fetch_shard", "cid": c, "idx": idx})
        if resp and resp[0].get("ok"):
            return resp[1]
        return None

    # ---- messaging ---------------------------------------------------

    def register_user(self, handle: str) -> dict:
        """Claim a handle on this node. Its inbox lives here store-and-forward."""
        if not handle or not handle.strip():
            raise ValueError("handle must be non-empty")
        if handle in self.users and self.users[handle]["addr"] != (self.host, self.port):
            raise ValueError(f"handle '{handle}' already registered")
        rec = {"addr": (self.host, self.port), "ts": time.time()}
        self.users[handle] = rec
        self.inboxes.setdefault(handle, [])
        return rec

    async def _gossip_users(self, snapshot: dict | None = None):
        """Push our registry to every live peer and merge theirs back."""
        snap = snapshot if snapshot is not None else {
            u: {"addr": list(r["addr"]), "ts": r["ts"]}
            for u, r in self.users.items()
            if r["addr"] == (self.host, self.port)
        }
        resp_list = await asyncio.gather(*(
            self._talk(addr, {"op": "users", "users": snap})
            for addr in self._live_peers()
        ))
        for resp in resp_list:
            if resp and resp[0].get("ok"):
                for u, r in resp[0].get("users", {}).items():
                    if tuple(r["addr"]) == (self.host, self.port):
                        continue
                    self.users.setdefault(u, {"addr": tuple(r["addr"]), "ts": r["ts"]})

    async def send(self, to: str, text: str, from_name: str = "anonymous") -> str:
        """Send a message to a handle anywhere in the mesh (flood + TTL + dedupe).

        Returns the message id. Delivery to the target node is store-and-forward:
        if the recipient's node is reachable the inbox gets it immediately;
        otherwise the flood retries on subsequent sends' gossip or fails TTL."""
        self._msg_seq += 1
        mid = cid(f"{self.host}:{self.port}:{self._msg_seq}:{to}:{text}".encode())
        if mid in self.seen_msgs:
            return mid
        self.seen_msgs.add(mid)
        env = {
            "id": mid,
            "from": from_name,
            "to": to,
            "text": text,
            "ts": time.time(),
            "ttl": 8,
        }
        await self._route_msg(env)
        return mid

    async def _route_msg(self, env: dict):
        to = env["to"]
        # delivered here?
        if to in self.users and self.users[to]["addr"] == (self.host, self.port):
            inbox = self.inboxes.setdefault(to, [])
            if env["id"] not in [m["id"] for m in inbox]:
                inbox.append({k: env[k] for k in ("id", "from", "text", "ts")})
            return
        owner = self.users.get(to)
        if owner is not None:
            # known owner elsewhere: hand off directly (one hop)
            fwd = dict(env)
            resp = await self._talk(owner["addr"], {"op": "msg", **fwd})
            if resp and resp[0].get("ok"):
                return
        if env.get("ttl", 0) <= 0:
            log.info("msg %s dropped: ttl exhausted (to=%s)", env["id"][:12], to)
            return
        # unknown / unreachable: flood to peers with decremented TTL
        fwd = dict(env)
        fwd["ttl"] = env.get("ttl", 0) - 1
        await asyncio.gather(*(
            self._talk(addr, {"op": "msg", **fwd})
            for addr in self._live_peers()
            if owner is None or addr != owner["addr"]
        ))

    async def _on_msg(self, env: dict):
        """Handle an inbound msg op: dedupe, deliver locally, relay."""
        mid = env.get("id", "")
        if not mid or mid in self.seen_msgs:
            return
        self.seen_msgs.add(mid)
        to = env.get("to", "")
        if to in self.users and self.users[to]["addr"] == (self.host, self.port):
            inbox = self.inboxes.setdefault(to, [])
            if env["id"] not in [m["id"] for m in inbox]:
                inbox.append({k: env[k] for k in ("id", "from", "text", "ts")})
            log.info("msg %s -> %s (delivered locally)", mid[:12], to)
            return
        if env.get("ttl", 0) > 0:
            relay = dict(env)
            relay["ttl"] = env["ttl"] - 1
            await self._route_msg(relay)
        else:
            log.info("msg %s died at %s (ttl 0)", mid[:12], self.port)

    def inbox(self, handle: str) -> list[dict]:
        return list(self.inboxes.get(handle, []))

    # ---- PiperChat (signed identity) ---------------------------------

    async def post_chat(self, name: str, pub: str, ts: str, text: str, sig: str) -> dict:
        """Accept a browser-signed chat envelope if the key proves the message.

        Signed + handle not bound to a *different* key => accepted, gossiped.
        Anything else raises IdentityError (403 at the dashboard)."""
        if isinstance(sig, (bytes, bytearray)):
            sig = bytes(sig).hex()
        env = {"name": name, "pub": pub, "ts": ts, "text": text, "sig": sig}
        if not verify_env(env):
            raise IdentityError("invalid signature: message rejected")
        self.ids.bind(env)
        mid = env_id(env)
        if mid not in self._chat_seen:
            self._chat_seen.add(mid)
            self.chat.append({**env, "id": mid})
            if len(self.chat) > 1000:
                self.chat = self.chat[-1000:]
            await self._gossip_chat(env)
        return {**env, "id": mid}

    async def _gossip_chat(self, env: dict):
        op = {"op": "chat", "env": env}
        for addr in self._live_peers():
            await self._talk(addr, op)

    def _on_chat(self, env: dict):
        """Inbound gossip: drop anything unsigned or forged, relay the rest."""
        if not verify_env(env) or not isinstance(env.get("ts", ""), str):
            return
        mid = env_id(env)
        if mid in self._chat_seen:
            return
        self._chat_seen.add(mid)
        try:
            self.ids.bind(env)  # publish binding globally
        except IdentityError:
            self._chat_seen.discard(mid)
            return  # forged handle — do not store or relay
        self.chat.append({**env, "id": mid})
        asyncio.get_event_loop().create_task(self._gossip_chat(env))

    # ---- client API -------------------------------------------------

    async def put(self, data: bytes, name: str = "file.bin") -> str:
        """Upload: chunk -> CID -> replicate/encode onto peers (plus local origin copy)."""
        chunks = chunk_data(data)
        cids = [cid(c) for c in chunks]
        file_id = cid(name.encode() + b"\x00" + b"".join(c.encode() for c in cids))
        targets = self._live_peers()

        holders: dict[str, int] = {}
        if self.mode == "erasure":
            codec = self.codec
            for c, chunk in zip(cids, chunks):
                shards_arr = codec.encode(chunk)
                for i, shard in enumerate(shards_arr):
                    if i < codec.data:
                        self.shards[(c, i)] = shard  # origin keeps the data blocks
                n_targets = min(len(targets), codec.total)
                res = await asyncio.gather(*(
                    self._store_shard_on(targets[i], c, i, shards_arr[i])
                    for i in range(n_targets)
                ))
                net_idx = {i for i, ok in enumerate(res) if ok}
                holders[c] = len(net_idx | set(range(codec.data)))
        else:
            for c, chunk in zip(cids, chunks):
                res = await asyncio.gather(*(
                    self._store_on(addr, c, chunk)
                    for addr in targets[: self.replication]
                ))
                self.store[c] = chunk  # origin keeps an authoritative copy
                holders[c] = sum(1 for ok in res if ok) + 1
        self.manifests[file_id] = {
            "name": name,
            "size": len(data),
            "chunks": cids,
            "holders": holders,
            "origin": (self.host, self.port),
        }
        await self._broadcast_manifests()
        log.info(
            "put %s (%d chunks, %d bytes, mode=%s)", name, len(chunks), len(data), self.mode
        )
        return file_id

    async def _broadcast_manifests(self):
        await asyncio.gather(*(
            self._talk(addr, {"op": "manifest", "manifests": self.manifests})
            for addr in self._live_peers()
        ))

    async def get(self, file_id: str):
        """Fetch + reassemble a file, verifying every chunk CID."""
        m = self.manifests.get(file_id)
        if m is None:
            return None

        async def one(c: str):
            if self.mode == "erasure":
                return await self._get_erasure_chunk(c)
            data = await self._fetch_chunk(c)
            if data is None:
                raise RuntimeError(f"chunk {c[:12]} unavailable on any node")
            return data

        parts = await asyncio.gather(*(one(c) for c in m["chunks"]))
        return b"".join(parts)

    async def _get_erasure_chunk(self, c: str) -> bytes:
        """Collect ANY codec.data surviving shards, decode, verify the CID."""
        codec = self.codec
        have: dict[int, bytes] = {}
        for i in range(codec.data):
            s = self.shards.get((c, i))
            if s is not None:
                have[i] = s
        if len(have) < codec.data:
            for addr in self._live_peers():
                if len(have) >= codec.data:
                    break
                missing = [i for i in range(codec.total) if i not in have]
                got = await asyncio.gather(*(self._fetch_shard(addr, c, i) for i in missing))
                for i, s in zip(missing, got):
                    if s is not None:
                        have[i] = s
                if len(have) >= codec.data:
                    break
        shards_list = [have.get(i) for i in range(codec.total)]
        try:
            data = codec.decode(shards_list)
        except InsufficientShards as e:
            raise RuntimeError(f"chunk {c[:12]} unrecoverable ??? {e}") from None
        if cid(data) != c:
            raise RuntimeError(f"chunk {c[:12]} decoded to wrong content (CID mismatch)")
        return data

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
        """Check every ISR file's chunks; top up replicas/re-encode shards after node loss."""
        if self.mode == "erasure":
            return await self._erasure_repair_pass()
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

    async def _erasure_repair_pass(self):
        """Origin-only repair: batch-query every live peer once per pass for
        which (chunk, shard) indices it still holds; regenerate any lost
        parity shard from the local data blocks and re-place all missing
        shards onto peers that lack them, up to the codec's total shard set."""
        codec = self.codec
        for file_id, m in list(self.manifests.items()):
            live = self._live_peers()
            if not live:
                continue
            net: dict[str, list[int]] = {}
            res_list = await asyncio.gather(*(
                self._talk(addr, {"op": "has_shards", "cids": m["chunks"]})
                for addr in live
            ))
            for resp in res_list:
                if resp and resp[0].get("ok"):
                    for c, idxs in resp[0].get("have", {}).items():
                        net.setdefault(c, []).extend(idxs)
            for c in m["chunks"]:
                local = {i for i in range(codec.data) if (c, i) in self.shards}
                if len(local) < codec.data:
                    continue  # cannot re-encode without every data block
                net_idx = set(net.get(c, []))
                missing = [i for i in range(codec.total) if i not in local and i not in net_idx]
                if not missing:
                    m["holders"][c] = codec.total
                    continue
                dshards = [self.shards[(c, i)] for i in range(codec.data)]
                parity = codec.regenerate_parity(dshards)
                shard_for = {i: (dshards[i] if i < codec.data else parity[i - codec.data]) for i in range(codec.total)}
                placed_idx: set[int] = set()
                free = list(live)
                for idx in missing:
                    if not free:
                        break
                    addr = free.pop(0)
                    if await self._store_shard_on(addr, c, idx, shard_for[idx]):
                        placed_idx.add(idx)
                        log.info("re-encoded shard %d of %s -> %s", idx, c[:12], addr[1])
                m["holders"][c] = len(local | net_idx | placed_idx)

    # ---- stats -------------------------------------------------------

    def status(self) -> dict:
        return {
            "addr": f"{self.host}:{self.port}",
            "codec": self.mode,
            "secure": self.secure,
            "chunks": len(self.store),
            "shards": len(self.shards),
            "files": {
                fid: {"name": m["name"], "size": m["size"], "chunks": len(m["chunks"])}
                for fid, m in self.manifests.items()
            },
            "peers": len(self.peers),
            "dead_peers": sorted(p[1] for p in self.dead_peers),
            "uptime": round(time.time() - self.started_at, 1),
        }
