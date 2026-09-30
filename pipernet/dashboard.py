"""Minimal async HTTP dashboard for a PiperNet node.

Routes:
  GET  /                   web UI (storage + PiedTube + PiperChat)
  GET  /api/status         aggregated status of every node in the demo mesh
  GET  /api/files          file index on this node
  POST /upload?name=...    raw binary body -> distributed upload
  GET  /stream/<file_id>   reconstructs the file from the network and streams it
  POST /register?name=...  claim a chat handle on this node (+ gossip)
  POST /msg                {"from","to","text"} -> flood-route across the mesh
  GET  /api/inbox?handle=&after=  fetch new chat messages for a handle
  GET  /api/users          handle registry known to this node
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
        if method == "POST" and path == "/register":
            name = (qs.get("name") or [""])[0].strip()
            if not name:
                return 400, "text/plain", b"missing handle"
            try:
                self.node.register_user(name.strip())
            except ValueError as e:
                return 409, "text/plain", str(e).encode()
            asyncio.get_event_loop().create_task(self.node._gossip_users())
            return 200, "application/json", json.dumps({"ok": True, "handle": name}).encode()
        if method == "POST" and path == "/msg":
            try:
                m = json.loads(body.decode() or "{}")
            except json.JSONDecodeError:
                return 400, "text/plain", b"bad json"
            to = (m.get("to") or "").strip()
            text = m.get("text") or ""
            from_name = (m.get("from") or "anonymous").strip()
            if not to or not text:
                return 400, "text/plain", b"missing 'to' or 'text'"
            mid = await self.node.send(to, text, from_name=from_name)
            return 200, "application/json", json.dumps({"ok": True, "id": mid}).encode()
        if method == "GET" and path == "/api/inbox":
            node = self.node
            handle = (qs.get("handle") or [""])[0]
            after = int((qs.get("after") or ["0"])[0])
            if handle in node.inboxes:
                msgs = node.inbox(handle)[after:]
                nxt = len(node.inboxes[handle])
            else:
                owner = node.users.get(handle)
                if owner is None:
                    msgs, nxt = [], after
                else:
                    resp = await node._talk(owner["addr"], {"op": "fetch_inbox", "handle": handle, "after": after})
                    if resp and resp[0].get("ok"):
                        msgs, nxt = resp[0]["msgs"], resp[0]["next"]
                    else:
                        msgs, nxt = [], after
            return 200, "application/json", json.dumps({"msgs": msgs, "next": nxt}).encode()
        if method == "GET" and path == "/api/users":
            return 200, "application/json", json.dumps(
                {
                    u: {"addr": list(r["addr"]), "ts": r["ts"], "local": r["addr"] == (self.node.host, self.node.port)}
                    for u, r in self.node.users.items()
                }
            ).encode()
        return 404, "text/plain", b"not found"


PAGE = r"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>Pied Piper — PiperNet Dashboard</title>
<style>
  :root { --bg:#0a1220; --panel:#101d33; --blue:#4db8ff; --acc:#00d4aa; --txt:#dbe9f7; --mut:#6f8bab; }
  * { box-sizing:border-box; }
  body { font-family:'Segoe UI',system-ui,sans-serif; margin:0; background:
    radial-gradient(1200px 600px at 80% -10%, #12305a 0%, transparent 60%),
    radial-gradient(900px 500px at -10% 110%, #0e2a4a 0%, transparent 55%), var(--bg);
    color:var(--txt); min-height:100vh; }
  header { padding:20px 32px; border-bottom:1px solid #16304f; background:#0c1730cc; }
  header h1 { margin:0; font-size:19px; letter-spacing:3px; color:var(--blue); }
  header .tag { color:var(--mut); font-size:12.5px; margin-top:4px; }
  main { max-width:1080px; margin:22px auto 40px; padding:0 16px; display:grid; grid-template-columns:1fr 350px; gap:18px; align-items:start; }
  @media (max-width:900px){ main { grid-template-columns:1fr; } }
  section { background:var(--panel); border:1px solid #1c3557; border-radius:12px; padding:16px 18px; margin-bottom:18px; }
  h2 { margin:0 0 10px; font-size:12.5px; text-transform:uppercase; letter-spacing:2px; color:var(--blue); }
  table { width:100%; border-collapse:collapse; font-size:13.5px; }
  td, th { text-align:left; padding:7px 8px; border-bottom:1px solid #1a2f4d; }
  th { color:var(--mut); font-weight:600; }
  button { background:#164a78; color:#eaf6ff; border:1px solid #2d69a8; padding:8px 14px; border-radius:7px; cursor:pointer; font-size:13.5px; }
  button:hover { background:#1b5c94; }
  button.acc { background:#0d5c4c; border-color:#17a085; }
  button.acc:hover { background:#127c67; }
  input[type=text] { padding:8px; background:#0b1626; border:1px solid #24466e; border-radius:7px; width:220px; color:var(--txt); }
  input[type=file] { color:var(--mut); font-size:13px; }
  .muted { color:var(--mut); font-size:12.5px; }
  video { width:100%; border-radius:9px; background:#000; }
  pre { background:#0a1626; color:#9fd4ff; padding:12px; border-radius:9px; font-size:11.5px; overflow:auto; max-height:220px; }
  #chatlog { display:flex; flex-direction:column; gap:8px; height:300px; overflow-y:auto; padding:4px 2px; }
  .msg { max-width:88%; padding:8px 11px; border-radius:10px; font-size:13.5px; line-height:1.45; word-wrap:break-word; }
  .msg .meta { font-size:10.5px; letter-spacing:.5px; opacity:.65; margin-bottom:3px; }
  .msg.in  { background:#15304f; align-self:flex-start; border-bottom-left-radius:3px; }
  .msg.out { background:#0d5c4c; align-self:flex-end; border-bottom-right-radius:3px; }
  .msg.sys { background:transparent; color:var(--mut); align-self:center; font-size:11.5px; max-width:100%; }
  #chatbar { display:flex; gap:8px; margin-top:10px; }
  #chatbar input { flex:1; width:auto; min-width:0; }
  .pill { display:inline-block; padding:2px 9px; border-radius:99px; background:#123355; color:var(--acc); font-size:11px; margin-left:6px; vertical-align:middle; }
  #users span { display:inline-block; background:#0e2440; border:1px solid #1d3c63; padding:2px 9px; border-radius:99px; margin:2px 4px 2px 0; font-size:11.5px; }
</style>
</head>
<body>
<header>
  <h1>&#10004; PIED PIPER &mdash; PIPERNET</h1>
  <div class="tag">decentralized storage &middot; erasure-coded &middot; self-healing &middot; <b style="color:var(--acc)">PiperChat</b></div>
</header>
<main>
  <div class="col">
    <section>
      <h2>Upload to the network</h2>
      <p class="muted">Chunked (1 MB), SHA-256 CIDs, replicated / RS 8+4 sharded across peers.</p>
      <input type="text" id="name" placeholder="display name">
      <input type="file" id="file">
      <button onclick="upload()">Upload</button>
      <div id="upout" class="muted"></div>
    </section>

    <section>
      <h2>Files on the network</h2>
      <table id="files"><tr><th>CID</th><th>Name</th><th>Size</th><th>Chunks</th><th></th></tr></table>
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
  </div>

  <div class="col">
    <section>
      <h2>PiperChat <span class="pill" id="chatwho">not signed in</span></h2>
      <div id="signin">
        <p class="muted">Claim a handle to chat across the mesh. Store-and-forward delivery, flood routing with dedupe &mdash; sealed end-to-end in secure mode.</p>
        <input type="text" id="handle" placeholder="e.g. gilfoyle">
        <button class="acc" onclick="reg()">Join</button>
        <span id="regout" class="muted"></span>
      </div>
      <div id="chatui" style="display:none">
        <div id="chatlog"></div>
        <div id="chatbar">
          <input type="text" id="to" placeholder="to handle">
          <input type="text" id="text" placeholder="message text" onkeydown="if(event.key==='Enter')chat()">
          <button class="acc" onclick="chat()">Send</button>
        </div>
        <div class="muted" id="chatout"></div>
        <div class="muted" style="margin-top:8px">known handles: <span id="users">&mdash;</span></div>
      </div>
    </section>
  </div>
</main>
<script>
const fmt = n => n > 1048576 ? (n/1048576).toFixed(1)+' MB' : (n/1024).toFixed(1)+' KB';

/* ---- storage / PiedTube ---- */
async function listFiles() {
  const r = await fetch('/api/files'); const rows = await r.json();
  const t = document.getElementById('files');
  t.innerHTML = '<tr><th>CID</th><th>Name</th><th>Size</th><th>Chunks</th><th></th></tr>';
  for (const f of rows) {
    const tr = document.createElement('tr');
    tr.innerHTML = `<td class="muted">${f.id.slice(0,16)}...</td>` +
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

/* ---- PiperChat ---- */
let me = localStorage.getItem('pp_handle') || '';
let cursor = 0;
async function reg() {
  const h = document.getElementById('handle').value.trim();
  if (!h) return;
  const r = await fetch('/register?name=' + encodeURIComponent(h), { method:'POST' });
  const out = document.getElementById('regout');
  if (!r.ok) { out.textContent = (await r.text()) + ' — pick another'; return; }
  me = h; localStorage.setItem('pp_handle', h);
  enterChat();
}
function enterChat() {
  document.getElementById('signin').style.display = 'none';
  document.getElementById('chatui').style.display = 'block';
  document.getElementById('chatwho').textContent = '@' + me;
  sys('joined the mesh as @' + me);
  poll(); setInterval(poll, 1500); users();
}
function esc(s) { const d = document.createElement('div'); d.textContent = s; return d.innerHTML; }
function addmsg(m) {
  const log = document.getElementById('chatlog');
  const div = document.createElement('div');
  div.className = 'msg ' + (m.from === me ? 'out' : 'in');
  div.innerHTML = `<div class="meta">${esc(m.from)} · ${new Date(m.ts*1000).toLocaleTimeString()}</div>${esc(m.text)}`;
  log.appendChild(div);
  log.scrollTop = log.scrollHeight;
}
function sys(t) {
  const log = document.getElementById('chatlog');
  const div = document.createElement('div');
  div.className = 'msg sys'; div.textContent = t;
  log.appendChild(div); log.scrollTop = log.scrollHeight;
}
async function chat() {
  const to = document.getElementById('to').value.trim();
  const text = document.getElementById('text').value.trim();
  const out = document.getElementById('chatout');
  if (!to || !text) return;
  const r = await fetch('/msg', { method:'POST',
    headers:{'Content-Type':'application/json'},
    body: JSON.stringify({from: me, to, text}) });
  if (!r.ok) { out.textContent = await r.text(); return; }
  document.getElementById('text').value = '';
  addmsg({from: me, text, ts: Date.now()/1000});
  out.textContent = 'routed to the mesh';
  setTimeout(() => out.textContent = '', 2000);
}
async function poll() {
  const r = await fetch('/api/inbox?handle=' + encodeURIComponent(me) + '&after=' + cursor);
  const j = await r.json();
  for (const m of j.msgs) addmsg(m);
  cursor = j.next;
}
async function users() {
  const r = await fetch('/api/users');
  const j = await r.json();
  const names = Object.keys(j);
  document.getElementById('users').innerHTML = names.length
    ? names.map(u => '<span>' + esc(u) + '</span>').join('')
    : '&mdash;';
}

/* ---- status ticker ---- */
async function status() {
  const r = await fetch('/api/status');
  document.getElementById('status').textContent = JSON.stringify(await r.json(), null, 2);
}
listFiles(); status(); setInterval(status, 2000);
if (me) { document.getElementById('handle').value = me; }
</script>
</body>
</html>
"""
