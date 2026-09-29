"""Length-prefixed binary framing for node-to-node messages.

Frame = 8-byte header (body_len u32, payload_len u32) + JSON body + payload.
Payload carries raw chunk bytes; body carries the operation.
"""

import asyncio
import json
import struct

_HDR = struct.Struct(">II")


async def send_msg(writer: asyncio.StreamWriter, body: dict, payload: bytes = b""):
    raw = json.dumps(body).encode()
    writer.write(_HDR.pack(len(raw), len(payload)) + raw + payload)
    await writer.drain()


async def recv_msg(reader: asyncio.StreamReader):
    """Returns (body, payload). (None, None) on clean EOF."""
    try:
        hdr = await reader.readexactly(8)
    except (asyncio.IncompleteReadError, ConnectionError):
        return None, None
    blen, plen = _HDR.unpack(hdr)
    try:
        body_raw = await reader.readexactly(blen) if blen else b"{}"
        payload = await reader.readexactly(plen) if plen else b""
    except (asyncio.IncompleteReadError, ConnectionError):
        return None, None
    return json.loads(body_raw), payload
