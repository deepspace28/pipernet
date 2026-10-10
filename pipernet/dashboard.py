"""Minimal async HTTP dashboard for a PiperNet node.

Routes:
  GET  /                   web UI (storage + PiedTube)
  GET  /chat               PiperChat - chat-only interface (no storage UI)
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

from .identity import IdentityError
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
        try:
            self._server = await asyncio.start_server(self._handle, host, port)
        except OSError:
            if port == 0:
                raise
            self._server = await asyncio.start_server(self._handle, host, 0)
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
        if method == "GET" and path == "/chat":
            return 200, "text/html", CHAT_PAGE.encode()
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
        if method == "POST" and path == "/api/chat":
            try:
                m = json.loads(body.decode() or "{}")
            except json.JSONDecodeError:
                return 400, "text/plain", b"bad json"
            try:
                saved = await self.node.post_chat(
                    str(m.get("name", "")),
                    str(m.get("pub", "")),
                    str(m.get("ts", "")),
                    str(m.get("text", "")),
                    str(m.get("sig", "")),
                )
            except IdentityError as e:
                return 403, "text/plain", str(e).encode()
            return 200, "application/json", json.dumps(saved).encode()
        if method == "GET" and path == "/api/chat":
            since = int((qs.get("since") or ["0"])[0])
            return 200, "application/json", json.dumps(
                {"chat": self.node.chat[since:], "n": len(self.node.chat)}
            ).encode()
        return 404, "text/plain", b"not found"


PAGE = r"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>Pied Piper — PiperNet Dashboard</title>
<style>
  :root {
    --bg:#202124; --panel:#2b2c30; --blue:#34a853; --acc:#34a853;
    --txt:#e8eaed; --mut:#9aa0a6; --line:rgba(255,255,255,.09);
    --font:-apple-system,BlinkMacSystemFont,'Segoe UI',system-ui,sans-serif;
  }
  * { box-sizing:border-box; }
  html { scroll-behavior:smooth; }
  body { font-family:var(--font); margin:0; color:var(--txt); min-height:100vh;
    background:
      radial-gradient(900px 400px at 90% -5%, rgba(52,168,83,.10), transparent 60%),
      var(--bg);
    -webkit-font-smoothing:antialiased; }
  header { position:sticky; top:0; z-index:10; padding:14px 28px;
    background:rgba(32,33,36,.8); -webkit-backdrop-filter:blur(20px) saturate(150%); backdrop-filter:blur(20px) saturate(150%); }
  header h1 { margin:0; font-size:16px; font-weight:600; letter-spacing:.01em; color:var(--txt); }
  header h1 b { color:var(--acc); }
  header .tag { color:var(--mut); font-size:12.5px; margin-top:2px; }
  main { max-width:920px; margin:20px auto 48px; padding:0 18px; display:grid; gap:14px; }
  section { background:var(--panel); border-radius:20px; padding:18px 20px;
    box-shadow:0 1px 2px rgba(0,0,0,.3), 0 8px 24px rgba(0,0,0,.22);
    transition:transform .18s ease, box-shadow .18s ease; }
  section:hover { transform:translateY(-2px); box-shadow:0 2px 4px rgba(0,0,0,.3), 0 14px 32px rgba(0,0,0,.3); }
  h2 { margin:0 0 12px; font-size:14px; font-weight:600; color:#c7cbcf; }
  h2::before { content:''; display:inline-block; width:8px; height:8px; border-radius:50%;
    background:var(--acc); margin-right:8px; vertical-align:1px; }
  table { width:100%; border-collapse:collapse; font-size:13.5px; }
  td, th { text-align:left; padding:8px; border-bottom:1px solid rgba(255,255,255,.06); }
  th { color:var(--mut); font-weight:500; font-size:12px; }
  tbody tr { transition:background .15s ease; }
  tbody tr:hover { background:rgba(52,168,83,.07); }
  button { background:#188038; color:#fff; border:none; padding:8px 18px; border-radius:99px;
    cursor:pointer; font-size:13.5px; font-weight:500; transition:background .15s ease, transform .1s ease, box-shadow .15s ease; }
  button:hover { background:#1e8e3e; box-shadow:0 2px 8px rgba(24,128,56,.4); }
  button:active { transform:scale(.95); }
  button.acc { background:#0b8043; }
  button.acc:hover { background:#0f9d58; }
  input[type=text] { padding:9px 14px; background:#303134; border:none; border-radius:99px;
    width:220px; color:var(--txt); font-size:13.5px; transition:box-shadow .15s ease; }
  input[type=text]:focus { outline:none; box-shadow:0 0 0 2px rgba(52,168,83,.6); }
  input[type=file] { color:var(--mut); font-size:13px; }
  video { width:100%; border-radius:16px; background:#000; box-shadow:0 6px 20px rgba(0,0,0,.4); }
  pre { background:#1a1b1e; color:#a8dab5; padding:14px; border-radius:16px; font-size:11.5px;
    overflow:auto; max-height:220px; line-height:1.6; }
  .pill { display:inline-block; padding:2px 10px; border-radius:99px; background:#3c4043; color:var(--acc);
    font-size:11px; font-weight:600; margin-left:6px; vertical-align:middle; }
  @media (prefers-reduced-motion: reduce) { * { transition:none !important; animation:none !important; } }
</style>
</head>
<body>
<header>
  <h1><b>&#10004; Pied Piper</b> &mdash; PiperNet</h1>
  <div class="tag">decentralized storage &middot; erasure-coded &middot; self-healing &middot; <a href="/chat" style="color:var(--acc);text-decoration:none"><b>PiperChat &rarr;</b></a></div>
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

/* ---- status ticker ---- */
async function status() {
  const r = await fetch('/api/status');
  document.getElementById('status').textContent = JSON.stringify(await r.json(), null, 2);
}
listFiles(); status(); setInterval(status, 2000);
</script>
</body>
</html>
"""

CHAT_PAGE = r"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>PiperChat — PiperNet</title>
<style>
  :root {
    --bg:#202124; --panel:#2b2c30; --blue:#34a853; --acc:#34a853;
    --txt:#e8eaed; --mut:#9aa0a6; --line:rgba(255,255,255,.09);
    --font:-apple-system,BlinkMacSystemFont,'Segoe UI',system-ui,sans-serif;
  }
  * { box-sizing:border-box; }
  html, body { height:100%; }
  body { font-family:var(--font); margin:0; display:flex; flex-direction:column; color:var(--txt);
    background:
      radial-gradient(900px 400px at 90% -5%, rgba(52,168,83,.10), transparent 60%),
      var(--bg);
    -webkit-font-smoothing:antialiased; }
  header { padding:14px 28px; background:rgba(32,33,36,.8);
    -webkit-backdrop-filter:blur(20px) saturate(150%); backdrop-filter:blur(20px) saturate(150%);
    display:flex; align-items:baseline; gap:14px; flex-wrap:wrap; }
  header h1 { margin:0; font-size:16px; font-weight:600; color:var(--txt); }
  header h1 b { color:var(--acc); }
  header .tag { color:var(--mut); font-size:12.5px; }
  header a { color:var(--acc); text-decoration:none; font-size:12.5px; margin-left:auto; font-weight:500; }
  header a:hover { text-decoration:underline; }
  header .pill { display:inline-block; padding:3px 11px; border-radius:99px; background:#3c4043; color:var(--acc);
    font-size:11.5px; font-weight:600; }
  main { flex:1; display:grid; grid-template-columns:250px 1fr; gap:14px; max-width:1080px; width:100%;
    margin:0 auto; padding:18px 18px 22px; min-height:0; }
  @media (max-width:800px){ main { grid-template-columns:1fr; } }
  aside { background:var(--panel); border-radius:20px; padding:16px; overflow:auto;
    box-shadow:0 1px 2px rgba(0,0,0,.3), 0 8px 24px rgba(0,0,0,.22); }
  aside h3 { margin:0 0 10px; font-size:13px; font-weight:600; color:#c7cbcf; }
  aside h3::before { content:''; display:inline-block; width:8px; height:8px; border-radius:50%;
    background:var(--acc); margin-right:8px; }
  .user { display:block; background:#303134; border:none; padding:8px 13px; border-radius:99px;
    margin-bottom:7px; font-size:12.5px; cursor:pointer; text-align:left; width:100%; color:var(--txt);
    transition:background .15s ease, transform .12s ease; }
  .user:hover { background:#3c4043; }
  .user:active { transform:scale(.97); }
  .user.me-node { background:#0b8043; color:#fff; }
  section { background:var(--panel); border-radius:20px; padding:18px 20px;
    display:flex; flex-direction:column; min-height:0; box-shadow:0 1px 2px rgba(0,0,0,.3), 0 8px 24px rgba(0,0,0,.22);
    transition:transform .18s ease, box-shadow .18s ease; }
  section:hover { transform:translateY(-2px); box-shadow:0 2px 4px rgba(0,0,0,.3), 0 14px 32px rgba(0,0,0,.3); }
  aside { transition:transform .18s ease, box-shadow .18s ease; }
  aside:hover { transform:translateY(-2px); }
  button { background:#188038; color:#fff; border:none; padding:9px 18px; border-radius:99px;
    cursor:pointer; font-size:13.5px; font-weight:500; transition:background .15s ease, transform .1s ease, box-shadow .15s ease; }
  button:hover { background:#1e8e3e; box-shadow:0 2px 8px rgba(24,128,56,.4); }
  button:active { transform:scale(.95); }
  button.acc { background:#0b8043; }
  button.acc:hover { background:#0f9d58; }
  input[type=text] { padding:10px 14px; background:#303134; border:none; border-radius:99px;
    color:var(--txt); font-size:13.5px; transition:box-shadow .15s ease; }
  input[type=text]:focus { outline:none; box-shadow:0 0 0 2px rgba(52,168,83,.6); }
  .muted { color:var(--mut); font-size:12.5px; line-height:1.55; }
  #chatlog { display:flex; flex-direction:column; gap:8px; flex:1; min-height:0; overflow-y:auto; padding:10px 6px;
    scroll-behavior:smooth; border-radius:16px;
    background:
      radial-gradient(rgba(255,255,255,.035) 1px, transparent 1px) 0 0/22px 22px,
      linear-gradient(180deg,#26282c,#212327); }
  .msgrow { display:flex; gap:8px; align-items:flex-end; max-width:100%; }
  .msgrow .msg { max-width:100%; }
  .av { width:30px; height:30px; border-radius:50%; flex:none; display:flex; align-items:center; justify-content:center;
    font-size:13px; font-weight:700; color:#fff; margin-bottom:2px; }
  .msg { max-width:72%; padding:8px 12px 6px; border-radius:16px; font-size:14.5px; line-height:1.4; word-wrap:break-word;
    animation:rise .22s ease both; box-shadow:0 1px 2px rgba(0,0,0,.35); }
  .msg .who { font-size:12px; font-weight:600; color:#a8dab5; margin-bottom:2px; }
  .msg .ts { float:right; font-size:10.5px; opacity:.6; margin:9px 0 0 10px; }
  .msg.in  { background:#33363c; align-self:flex-start; border-bottom-left-radius:5px; }
  .msg.out { background:linear-gradient(135deg,#0f9d58,#0b8043); color:#fff; align-self:flex-end; border-bottom-right-radius:5px; }
  .msg.sys { background:transparent; color:var(--mut); align-self:center; font-size:11.5px; max-width:100%; box-shadow:none; }
  #chatbar { display:flex; gap:8px; margin-top:12px; }
  #chatbar input { flex:1; min-width:0; }
  #chatlog::-webkit-scrollbar, aside::-webkit-scrollbar { width:8px; }
  #chatlog::-webkit-scrollbar-thumb, aside::-webkit-scrollbar-thumb { background:rgba(255,255,255,.15); border-radius:99px; }
  @media (prefers-reduced-motion: reduce) { * { transition:none !important; animation:none !important; } }
</style>
</head>
<body>
<header>
  <h1>&#128172; <b>PiperChat</b></h1>
  <span class="tag">flood routing &middot; store-and-forward &middot; dedupe &middot; sealed in secure mode</span>
  <a href="/">&larr; storage dashboard</a>
  <span class="pill" id="chatwho">not signed in</span>
</header>

<main>
  <aside>
    <h3>Handles on the mesh</h3>
    <div id="users" class="muted">loading&hellip;</div>
  </aside>

  <section>
    <div id="signin">
      <p class="muted">Claim a handle to chat across the mesh. Messages flood-routes with TTL and dedupe, delivered store-and-forward. This page is chat-only &mdash; storage lives on the dashboard.</p>
      <div style="display:flex; gap:8px; margin-top:12px">
        <input type="text" id="handle" placeholder="e.g. gilfoyle" style="flex:1" onkeydown="if(event.key==='Enter')reg()">
        <button class="acc" onclick="reg()">Join</button>
      </div>
      <div id="regout" class="muted" style="margin-top:8px"></div>
    </div>
    <div id="chatui" style="display:none; flex:1; min-height:0; flex-direction:column">
      <div id="chatlog"></div>
      <div id="chatbar">
        <input type="text" id="to" placeholder="to handle" style="max-width:180px">
        <input type="text" id="text" placeholder="message text" onkeydown="if(event.key==='Enter')chat()">
        <button class="acc" onclick="chat()" style="border-radius:50%; width:42px; height:42px; padding:0; flex:none" title="Send">&#10148;</button>
      </div>
      <div class="muted" id="chatout" style="margin-top:6px"></div>
    </div>
  </section>
</main>
<script>
let me = localStorage.getItem('pp_handle') || '';
let cursor = 0;

async function reg() {
  const h = document.getElementById('handle').value.trim();
  if (!h) return;
  const r = await fetch('/register?name=' + encodeURIComponent(h), { method:'POST' });
  const out = document.getElementById('regout');
  if (!r.ok) { out.textContent = await r.text(); return; }
  me = h; localStorage.setItem('pp_handle', h);
  enterChat();
}
function enterChat() {
  document.getElementById('signin').style.display = 'none';
  const ui = document.getElementById('chatui');
  ui.style.display = 'flex';
  document.getElementById('chatwho').textContent = '@' + me;
  sys('joined the mesh as @' + me);
  poll(); setInterval(poll, 1500); users(); setInterval(users, 4000);
}
function esc(s) { const d = document.createElement('div'); d.textContent = s; return d.innerHTML; }
function avColor(n) { let h=0; for (const c of n) h=(h*31+c.charCodeAt(0))%360; return 'hsl(' + h + ',45%,42%)'; }
function addmsg(m) {
  const log = document.getElementById('chatlog');
  if (m.from === me) {
    const div = document.createElement('div');
    div.className = 'msg out';
    div.innerHTML = esc(m.text) + '<span class="ts">' + new Date(m.ts*1000).toLocaleTimeString([], {hour:'2-digit',minute:'2-digit'}) + '</span>';
    log.appendChild(div);
  } else {
    const row = document.createElement('div');
    row.className = 'msgrow';
    const av = document.createElement('div');
    av.className = 'av'; av.style.background = avColor(m.from); av.textContent = m.from[0].toUpperCase();
    const div = document.createElement('div');
    div.className = 'msg in';
    div.innerHTML = '<div class="who">' + esc(m.from) + '</div>' + esc(m.text) + '<span class="ts">' + new Date(m.ts*1000).toLocaleTimeString([], {hour:'2-digit',minute:'2-digit'}) + '</span>';
    row.appendChild(av); row.appendChild(div);
    log.appendChild(row);
  }
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
  const box = document.getElementById('users');
  const names = Object.keys(j).filter(u => u !== me);
  box.innerHTML = names.length
    ? names.map(u => '<button class="user" onclick="pick(\'' + encodeURIComponent(u) + '\')"><span style="color:#34a853; margin-right:7px">&#9679;</span>' + esc(u) + '</button>').join('')
    : 'no other handles yet — open /chat in another tab or node and sign in';
}
function pick(h) {
  document.getElementById('to').value = decodeURIComponent(h);
  document.getElementById('text').focus();
}
if (me) { document.getElementById('handle').value = me; }
</script>
</body>
</html>
"""
