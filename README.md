# PiperNet — Pied Piper Prototype V0.4

> *"It's the internet, completely decentralized."*

A working prototype of the Pied Piper deck: a headless P2P node,
content-addressed distributed storage, **Reed-Solomon erasure coding
(8+4)**, multi-peer replication, failure detection, automatic self-healing,
and a live web dashboard running a PiedTube-style streaming demo.

## Core scope

| Spec item | Status |
|---|---|
| Headless P2P node | done (`pipernet/node.py`) |
| **Crypto primitives (Curve25519)** | **done — RFC 7748 X25519 (Montgomery ladder, vector-verified), RFC 8439 ChaCha20+Poly1305 AEAD, RFC 5869 HKDF — all pure stdlib (`pipernet/crypto.py`); wire-level session encryption is the next slice** |
| Encrypted connections | done — per-connection X25519 handshake (RFC 7748) -> HKDF -> ChaCha20-Poly1305 sealed frames (`pipernet/protocol.py`); the handshake now proves static node identity and clients pin it (TOFU) |
| Distributed chunking & hashing | done — 1 MB chunks, SHA-256 CIDs (`pipernet/chunking.py`) |
| **Erasure coding (8 data + 4 parity)** | **done — Cauchy RS over GF(2^8), stdlib-only (`pipernet/erasure.py`); 1.5x storage overhead vs replication's 3x; any 4 shard losses per chunk are survivable** |
| Multi-peer replication | done — configurable replica factor (default mode) |
| Failure detection & auto-repair | done — heartbeat liveness + replica top-up / **shard re-encode** loop |
| Live web dashboard + PiedTube demo | done (`pipernet/dashboard.py`, serves on `:8080`) |

Non-goals (explicitly out of V0.1): ISP replacement, global data centers,
custom ASICs, speculative token markets.

## Quickstart

No third-party dependencies — Python 3.10+ standard library only.

```bash
# run the resiliency demo (the deck's failure-tolerance proof)
py demo/resiliency_demo.py

# erasure-coded mode: 8 data + 4 parity shards per chunk, 1.5x overhead
py demo/resiliency_demo.py --erasure

# ...then keep the dashboard open to upload & stream through the mesh
py demo/resiliency_demo.py --serve
# open http://127.0.0.1:8080
```

What the demo does:

1. Deploys 20 nodes
2. Uploads a multi-MB "video" -> chunked, hashed into CIDs, replicated
   (or RS 8+4 encoded into 12 shards spread across peers)
3. Streams it to a peer (full reconstruct + CID verification)
4. Kills 8 nodes simultaneously (erasure mode: 4) -> **stream continues uninterrupted**
5. Waits for the self-healing repair loop to restore targets
   (erasure mode: the origin **re-encodes** lost shards from its data blocks)
6. Kills more -> **still uninterrupted**

## Use it in code

PiperNet is a library first — the demo is just one mesh topology. Spawn
your own and put/get bytes across it:

```python
import asyncio
from pipernet import Node

async def main():
    # start nodes; port=0 picks a free ephemeral port automatically
    origin  = await Node().start()                      # replication mode, factor 3
    mirror  = await Node().start()
    client  = await Node().start()

    # full mesh wiring (peers must know each other both ways)
    for a, b in ((origin, mirror), (origin, client), (mirror, client)):
        a.add_peer((b.host, b.port))
        b.add_peer((a.host, a.port))

    file_id = await origin.put(b"middle-out compression rocks", name="note.txt")
    data = await client.get(file_id)     # fetched from any surviving holder,
                                         # CID-verified on receipt; None if
                                         # unrecoverable
    assert data == b"middle-out compression rocks"

    await client.stop(); await mirror.stop(); await origin.stop()

asyncio.run(main())
```

### Node options

| Parameter | Default | Meaning |
|---|---|---|
| `host` | `127.0.0.1` | bind address for the peer server |
| `port` | `0` | bind port; `0` = auto-pick. After `start()`, `node.port` holds the real port |
| `replication` | `3` | replication factor (replication mode) |
| `mode` | `"replication"` | `"erasure"` switches to Reed-Solomon 8+4 shards per chunk (1.5x overhead, survives any 4 shard losses per chunk) |
| `secure` | `False` | encrypts every connection: X25519 handshake (RFC 7748) -> HKDF -> ChaCha20-Poly1305 sealed frames (RFC 8439). The handshake proves the peer's static identity key; clients pin it on first contact (TOFU) and impostors are rejected thereafter. Optional `identity=(priv, pub)` restores/reuses a keypair |

### Dashboard & HTTP API

Attach a dashboard to any node to get the PiedTube-style web UI and a
plain HTTP interface to the mesh:

```python
from pipernet import Dashboard

dash = Dashboard(origin)             # mesh_status_fn defaults to origin.status()
await dash.start(port=8080)          # returns the bound port
```

| Endpoint | Method | What it does |
|---|---|---|
| `/` | GET | dashboard UI (upload, file list, PiedTube streaming) |
| `/api/status` | GET | JSON status of every node in the mesh |
| `/api/files` | GET | JSON manifest: file id, name, size, chunk count |
| `/upload?name=my-video.mp4` | POST | raw bytes body -> chunked, hashed, spread; returns `{"id", "name"}` |
| `/stream/<file_id>` | GET | reconstructs the file from the mesh, CID-verified, as raw bytes |

Uploading through the UI is the same call the demo makes — the file is
chunked into 1 MB pieces, each gets a SHA-256 content ID, and replicas
or shards are spread across peers; the self-healing repair loop runs
every 2 s and restores placement when nodes die.

## Tests

```bash
py -m pytest tests/ -v
```

## Architecture

```
Application (dashboard / any client)
        |
  PiperNet node    <- manifest of files it originated (Tier 0 origin)
  |- chunking       file -> 1 MB chunks -> SHA-256 content IDs
  |- object store   local chunks (durable origin copy)
  |- replication    chunks spread to N peers on upload
  |- wire protocol  length-prefixed JSON + payload over asyncio TCP
  |- repair loop    every 2 s: count live holders, re-store destroyed chunks
        |
   peer nodes (same software, any number, disposable)
```

Data-lives mapping (from the deck): each chunk lives on the **origin node**
(durable copy), replicated across paired **backbone/infrastructure peers**,
and is fetched on demand by **peripheral nodes** (the dashboard client).
Replication targets are restored automatically when nodes die.

## Answering "where does the data actually live?"

- `Node.manifests` = origin file index (Tier 1 · durable storage)
- Live peer set around each chunk = Tier 2 backbone replicas
- The streaming/dashboard fetch = Tier 3 edge access (serves from any
  surviving holder, reconstructs and verifies CIDs on receipt)

## Roadmap (next slice)

- QUIC transport (lsquic / `aioquic`) with mandatory end-to-end encryption
- PiedPiperCoin service accounting (proof-of-service receipts, anti-Sybil)
- Portable keypair identity persisted to disk (node identity keys are currently in-memory per process)
- Distributed origins: repair without a dedicated origin node (currently origin-only shard re-encoding)
