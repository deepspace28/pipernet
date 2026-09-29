"""Minimal async HTTP dashboard for a PiperNet node.

Routes:
  GET  /                   web UI (PiedTube demo)
  GET  /api/status         aggregated status of every node in the demo mesh
  GET  /api/files          file index on this node
  POST /upload?name=...    raw binary body -> distributed upload
  GET  /stream/<file_id>   reconstructs the file from the network and streams it
"""

import asyncio
import html
import json
import urllib.parse

from .chunking import cid

_PAGES = {}


def page(path):
    def deco(fn):
        _PAGES[path] = fn
        return fn

    return deco


class Dashboard:
    def __init__(self, node, mesh_status_fn=None):
        self.node = node
        self.mesh_status_fn = mesh_status_fn or node.status

    async def start(self, host="127.0.0.1", port=8080):
        self._server = await asyncio.start_server(self._handle, host, port)
        return self._server.sockets[0].getsockname()[1]

    async def _handle(self, reader, writer):
        try:
            request_line = (await reader.readline()).decode().strip()
            if not request_line:
                return
            method, target, _ = request_line.split(" ", 2)
            headers = {}
            while True:
                line = (await reader.readline()).decode().strip()
                if not line or line == "\r\n":
                    break
                if ":" in line:
                    k, v = line.split(":", 1)
                    headers[k.strip().lower()] = v.strip()
            n = int(headers.get("content-length", 0))
            body = await reader.readexactly(n) if n else b""

            status, ctype, payload = await self._route(method, target, body)
            head = (
                f"HTTP/1.1 {status}\r\nContent-Type: {ctype}\r\n"
                f"Content-Length: {len(payload)}\r\nConnection: close\r\n\r\n"
            )
            writer.write(head.encode() + payload)
            await writer.drain()
        finally:
            writer.close()

    async def _route(self, method, target, body):
        parsed = urllib.parse.urlparse(target)
        path = parsed.path
        qs = urllib.parse.parse_qs(parsed.query)

        if method == "GET" and path == "/":
            return 200, "text/html", PAGE.encode()
        if method == "GET" and path == "/api/status":
            return 200, "application/json", json.dumps(self.mesh_status_fn(), indent=2).encode()
        if method == "GET" and path == "/api/files":
            node = self.node
            return 200, "application/json", json.dumps(
                [
                    {
                        "id": fid,
                        "name": m["name"],
                        "size": m["size"],
                        "chunks": len(m["chunks"]),
                    }
                    for fid, m in node.manifests.items()
                ]
            ).encode()
        if method == "POST" and path == "/upload":
            name = (qs.get("name") or ["upload.bin"])[0]
            fid = await self.node.put(body, name=name) if body else None
            if not fid:
                return 400, "text/plain", b"empty body"
            return 200, "application/json", json.dumps({"id": fid, "name": name}).encode()
        if method == "GET" and path.startswith("/stream/"):
            fid = path.split("/stream/", 1)[1]
            if fid not in self.node.manifests:
                return 404, "text/plain", b"no such file on this node"
            data = await self.node.get(fid)
            if data is None:
                return 503, "text/plain", b"network cannot reconstruct file"
            name = self.node.manifests[fid]["name"]
            return (
                200,
                "application/octet-stream",
                data,
            )
        return 404, "text/plain", b"not found"


PAGE = r"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>Pied Piper — PiperNet Dashboard</title>
<style>
  :root { --blue:#1a5276; --bg:#eef3f7; }
  * { box-sizing:border-box; }
  body { font-family:system-ui,Segoe UI,sans-serif; margin:0; background:var(--bg); color:#12283a; }
  header { background:linear-gradient(120deg,#dfeaf2,#bfe0ef); padding:24px 32px; }
  header h1 { margin:0; color:var(--blue); letter-spacing:.5px; }
  main { max-width:900px; margin:24px auto; padding:0 16px; }
  section { background:#fff; border:1px solid #d5e2ec; border-radius:10px; padding:18px 20px; margin-bottom:18px; }
  h2 { margin:0 0 10px; font-size:15px; text-transform:uppercase; color:var(--blue); letter-spacing:1px; }
  table { width:100%; border-collapse:collapse; font-size:14px; }
  td, th { text-align:left; padding:6px 8px; border-bottom:1px solid #eef2f5; }
  button, input[type=file] { font-size:14px; }
  button { background:var(--blue); color:#fff; border:0; padding:8px 14px; border-radius:6px; cursor:pointer; }
  input[type=text] { padding:8px; border:1px solid #b9ccdb; border-radius:6px; width:210px; }
  .muted { color:#6c87a0; font-size:13px; }
  video { width:100%; border-radius:8px; background:#000; }
  pre { background:#0e2233; color:#bfe0ef; padding:12px; border-radius:8px; font-size:12px; overflow:auto; max-height:260px; }
</style>
</head>
<body>
<header>
  <h1>&#10004; PIED PIPER &mdash; PiperNet Dashboard</h1>
  <div class="muted">Decentralized storage &middot; content-addressed chunks &middot; self-healing replication</div>
</header>
<main>
  <section>
    <h2>Upload to the network</h2>
    <p class="muted">Choose a file (any type). It is chunked, hashed (SHA-256 CIDs) and replicated across peers.</p>
    <input type="text" id="name" placeholder="display name">
    <input type="file" id="file">
    <button onclick="upload()">Upload</button>
    <div id="upout" class="muted"></div>
  </section>

  <section>
    <h2>Files on the network</h2>
    <table id="files"><tr><th>CID (file)</th><th>Name</th><th>Size</th><th>Chunks</th><th></th></tr></table>
  </section>

  <section>
    <h2>PiedTube &mdash; stream from the mesh</h2>
    <video id="player" controls></video>
    <div id="vout" class="muted"></div>
  </section>

  <section>
    <h2>Network status</h2>
    <pre id="status">loading...</pre>
  </section>
</main>
<script>
const fmt = n => n > 1048576 ? (n/1048576).toFixed(1)+' MB' : (n/1024).toFixed(1)+' KB';
async function listFiles() {
  const r = await fetch('/api/files'); const rows = await r.json();
  const t = document.getElementById('files');
  t.innerHTML = '<tr><th>CID (file)</th><th>Name</th><th>Size</th><th>Chunks</th><th></th></tr>';
  for (const f of rows) {
    const tr = document.createElement('tr');
    tr.innerHTML = `<td class="muted">${f.id.slice(0,16)}...` +
      `<td>${f.name}</td><td>${fmt(f.size)}</td><td>${f.chunks}</td>`;
    const play = document.createElement('button');
    play.textContent = 'Stream';
    play.onclick = () => stream(f.id, f.name);
    tr.appendChild(play);
    t.appendChild(tr);
  }
}
async function upload() {
  const f = document.getElementById('file').files[0];
  if (!f) { alert('pick a file first'); return; }
  const name = document.getElementById('name').value || f.name;
  document.getElementById('upout').textContent = 'chunking + replicating...';
  const r = await fetch('/upload?name=' + encodeURIComponent(name), { method:'POST', body:f });
  const j = await r.json();
  document.getElementById('upout').textContent = 'stored as ' + j.id;
  listFiles();
}
async function stream(id, name) {
  const v = document.getElementById('player');
  const out = document.getElementById('vout');
  out.textContent = 'reconstructing from mesh...';
  const r = await fetch('/stream/' + id);
  if (!r.ok) { out.textContent = 'stream failed (' + r.status + ')'; return; }
  v.src = URL.createObjectURL(await r.blob());
  v.play();
  out.textContent = 'streaming ' + name;
}
async function status() {
  const r = await fetch('/api/status');
  document.getElementById('status').textContent = JSON.stringify(await r.json(), null, 2);
}
listFiles(); status(); setInterval(status, 2000);
</script>
</body>
</html>
"""
