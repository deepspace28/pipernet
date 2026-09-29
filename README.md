# PiperNet — Pied Piper Prototype V0.1

> *"It's the internet, completely decentralized."*

A working prototype of the Pied Piper deck's V0.1 scope: a headless P2P node,
content-addressed distributed storage, multi-peer replication, failure
detection, automatic self-healing, and a live web dashboard running a
PiedTube-style streaming demo.

## V0.1 core scope (from the deck)

| Spec item | Status |
|---|---|
| Headless P2P node | done (`pipernet/node.py`) |
| Encrypted connections | wire framing in place (TCP); QUIC/TLS is a listed non-goal for this slice |
| Distributed chunking & hashing | done — 1 MB chunks, SHA-256 CIDs (`pipernet/chunking.py`) |
| Multi-peer replication | done — configurable replica factor |
| Failure detection & auto-repair | done — heartbeat-style liveness + replica top-up loop |
| Live web dashboard + PiedTube demo | done (`pipernet/dashboard.py`, serves on `:8080`) |

Non-goals (explicitly out of V0.1): ISP replacement, global data centers,
custom ASICs, speculative token markets.

## Quickstart

No third-party dependencies — Python 3.10+ standard library only.

```bash
# run the resiliency demo (the deck's failure-tolerance proof)
py demo/resiliency_demo.py

# ...then keep the dashboard open to upload & stream through the mesh
py demo/resiliency_demo.py --serve
# open http://127.0.0.1:8080
```

What the demo does:

1. Deploys 20 nodes
2. Uploads a multi-MB "video" -> chunked, hashed into CIDs, replicated
3. Streams it to a peer (full reconstruct + CID verification)
4. Kills 8 nodes simultaneously -> **stream continues uninterrupted**
5. Waits for the self-healing repair loop to restore replica targets
6. Kills 8 more -> **still uninterrupted**

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

## Roadmap (V0.2+)

- Erasure coding (8 data shards + 4 parity) replacing full-chunk replication
- QUIC transport (lsquic / `aioquic`) with mandatory end-to-end encryption
- PiedPiperCoin service accounting (proof-of-service receipts, anti-Sybil)
- Portable keypair identity (client-side keypairs, sovereign profiles)
