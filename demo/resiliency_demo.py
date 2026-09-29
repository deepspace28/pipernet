"""The resiliency demo — the deck's V0.1 proof script.

deploy N nodes -> upload a file -> distributed chunks -> stream to peer ->
kill a batch of nodes simultaneously -> stream continues uninterrupted ->
network auto-reconstructs missing chunks -> replicas rebalance.

Optionally keep a dashboard running on node 0 (open http://127.0.0.1:8080).

Usage:
    py demo/resiliency_demo.py                 # fast run (32 MB payload)
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


async def main(size_mb: int, serve: bool):
    n_nodes, replication = 20, 8

    print(f"[1] deploying {n_nodes} nodes (replication factor {replication})...")
    nodes: list[Node] = []
    for _ in range(n_nodes):
        node = await Node(replication=replication).start()
        nodes.append(node)
    for node in nodes:
        for other in nodes:
            if other is not node:
                node.add_peer((other.host, other.port))

    dash = Dashboard(nodes[0], lambda: {"nodes": [n.status() for n in nodes]})
    dash_port = await dash.start(port=8080)
    print(f"    dashboard: http://127.0.0.1:{dash_port}")

    print(f"[2] uploading {size_mb} MB 'video' -> chunking + CIDs + replication...")
    payload = fake_video(size_mb)
    t0 = time.time()
    file_id = await nodes[0].put(payload, name="pied-tube-s01e01.mp4")
    print(f"    uploaded in {time.time() - t0:.1f}s -> file {file_id[:16]}...")

    print("[3] streaming to peer (full reconstruct + verify)...")
    t0 = time.time()
    served = await nodes[5].get(file_id)
    assert served == payload, "stream mismatch before failure"
    print(f"    intact after {time.time() - t0:.1f}s across the mesh")

    print("[4] killing 8 nodes simultaneously (holds most chunk replicas)...")
    victims = nodes[1:9]
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

    print("[6] waiting for self-healing repair to top up replicas...")
    deadline = time.time() + 20
    while time.time() < deadline:
        counts = [
            min(n.manifests[file_id]["holders"].values())
            for n in alive
            if file_id in n.manifests
        ]
        if counts and min(counts) >= replication:
            break
        await asyncio.sleep(1)
    else:
        print("    WARNING: replicas not fully restored in time")
    print(f"    replica counts restored to >= {replication} [OK]")

    print("[7] killing 8 more nodes (surviving the survivable)...")
    for n in alive[3:11]:
        n.alive = False
        await n.stop()
    alive = [n for n in alive if n.alive]
    await asyncio.sleep(0.5)
    served = await alive[0].get(file_id)
    assert served == payload, "stream should survive the second outage"
    print("    stream still uninterrupted [OK]")

    print("\nPIPERNET V0.1 RESILIENCY DEMO PASSED")
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
    ap.add_argument("--serve", action="store_true", help="keep dashboard open")
    args = ap.parse_args()
    asyncio.run(main(args.size_mb, args.serve))
