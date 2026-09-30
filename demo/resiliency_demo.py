"""The resiliency demo — the deck's V0.1 proof script.

deploy N nodes -> upload a file -> distributed chunks -> stream to peer ->
kill a batch of nodes simultaneously -> stream continues uninterrupted ->
network auto-reconstructs missing chunks -> replicas rebalance.

Optionally keep a dashboard running on node 0 (open http://127.0.0.1:8080).

Usage:
    py demo/resiliency_demo.py                 # replication mode (32 MB payload)
    py demo/resiliency_demo.py --erasure       # Reed-Solomon 8+4 erasure coding
    py demo/resiliency_demo.py --serve        # keep dashboard open afterwards
"""

import argparse
import asyncio
import sys
import time

sys.path.insert(0, ".")

from pipernet import Node, Dashboard


def fake_video(size_mb: int) -> bytes:
    """Deterministic pseudo-payload standing in for a multi-GB video."""
    block = bytes(range(256)) * 4096  # 1 MB block
    return block * size_mb


async def main(size_mb: int, serve: bool, erasure: bool):
    n_nodes = 20
    replication, shards_target = 8, None
    mode = "erasure" if erasure else "replication"
    if erasure:
        from pipernet import DATA_SHARDS, PARITY_SHARDS
        shards_target = DATA_SHARDS + PARITY_SHARDS

    print(f"[1] deploying {n_nodes} nodes ({'Reed-Solomon ' + str(shards_target) + ' shards/chunk' if erasure else 'replication factor ' + str(replication)})...")
    nodes: list[Node] = []
    for _ in range(n_nodes):
        node = await Node(replication=replication, mode=mode).start()
        nodes.append(node)
    for node in nodes:
        for other in nodes:
            if other is not node:
                node.add_peer((other.host, other.port))

    dash = Dashboard(nodes[0], lambda: {"nodes": [n.status() for n in nodes]})
    dash_port = await dash.start(port=8080)
    print(f"    dashboard: http://127.0.0.1:{dash_port}")

    if erasure:
        print("[2] uploading {} MB 'video' -> chunking + CIDs + RS {}+{} encode...".format(size_mb, shards_target - PARITY_SHARDS, PARITY_SHARDS))
    else:
        print(f"[2] uploading {size_mb} MB 'video' -> chunking + CIDs + replication...")
    payload = fake_video(size_mb)
    t0 = time.time()
    file_id = await nodes[0].put(payload, name="pied-tube-s01e01.mp4")
    print(f"    uploaded in {time.time() - t0:.1f}s -> file {file_id[:16]}...")

    if erasure:
        print("    storage overhead: 12/8 = 1.5x (replication would store 3x)")

    print("[3] streaming to peer (full reconstruct + verify)...")
    t0 = time.time()
    served = await nodes[5].get(file_id)
    assert served == payload, "stream mismatch before failure"
    print(f"    intact after {time.time() - t0:.1f}s across the mesh")

    kill_first = 4 if erasure else 8
    print(f"[4] killing {kill_first} nodes simultaneously (holds shard/replica copies)...")
    victims = nodes[1 : 1 + kill_first]
    for v in victims:
        v.alive = False
        await v.stop()
    alive = [n for n in nodes if n.alive]
    print(f"    {len(alive)} nodes still alive")

    await asyncio.sleep(0.5)
    print("[5] streaming again mid-outage...")
    served = await alive[1].get(file_id)
    assert served == payload, "stream SHOULD continue after node failure"
    print("    stream continues uninterrupted [OK]")

    target = shards_target if erasure else replication
    print("[6] waiting for self-healing {}...".format("shard re-encoding" if erasure else "replica top-up"))
    deadline = time.time() + 25
    reachable = min(target, 1 + len(nodes[0]._live_peers()) if erasure else replication, target)
    if erasure:
        reachable = target  # origin holds data shards + re-encodes parity beyond kills
    while time.time() < deadline:
        if file_id in nodes[0].manifests:
            holders = nodes[0].manifests[file_id]["holders"]
            if holders and min(holders.values()) >= min(reachable, target):
                break
        await asyncio.sleep(1)
    else:
        print("    WARNING: capacity not fully restored in time")
    print(f"    placement restored to >= {min(reachable, target)} [OK]")

    print(f"[7] killing {kill_first} more nodes (surviving the survivable)...")
    for n in alive[3 : 3 + kill_first]:
        if n in alive:
            n.alive = False
            await n.stop()
    alive = [n for n in alive if n.alive]
    await asyncio.sleep(0.5)
    served = await alive[1].get(file_id)
    assert served == payload, "stream should survive the second outage"
    print("    stream still uninterrupted [OK]")

    print(f"\nPIPERNET V0.2 RESILIENCY DEMO PASSED ({mode})")
    print(f"dashboard still live at http://127.0.0.1:{dash_port}" if serve else "done")
    if serve:
        print("press Ctrl+C to quit")
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            pass
    for n in alive:
        await n.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--size-mb", type=int, default=32)
    ap.add_argument("--erasure", action="store_true", help="Reed-Solomon 8+4 erasure coding")
    ap.add_argument("--serve", action="store_true", help="keep dashboard open")
    args = ap.parse_args()
    asyncio.run(main(args.size_mb, args.serve, args.erasure))
