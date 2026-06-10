import asyncio
import os
import secrets
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse


app = FastAPI(title="Remote Control HxTend", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


API_TOKEN = os.getenv("API_TOKEN", "").strip()
CONTROL_TOKENS = tuple(
    dict.fromkeys(
        token
        for token in (
            API_TOKEN,
            os.getenv("HXTEND_CONTROL_TOKEN", "").strip(),
            os.getenv("HXTEND_API_TOKEN", "").strip(),
            os.getenv("HXTEND_PANEL_TOKEN", "").strip(),
            os.getenv("CONTROL_TOKEN", "").strip(),
            os.getenv("PANEL_TOKEN", "").strip(),
        )
        if token
    )
)
STREAM_TOKENS = tuple(
    dict.fromkeys(
        token
        for token in (
            os.getenv("HXTEND_STREAM_TOKEN", "").strip(),
            os.getenv("STREAM_TOKEN", "").strip(),
        )
        if token
    )
)
ONLINE_SECONDS = float(os.getenv("HXTEND_STREAM_ONLINE_SECONDS", "12"))


@dataclass
class FeedState:
    frame: bytes = b""
    updated_at: float = 0.0
    online: bool = False
    source_url: str = ""
    error: str = ""
    condition: asyncio.Condition = field(default_factory=asyncio.Condition)


@dataclass
class DeviceState:
    last_seen: float = 0.0
    commands: List[dict] = field(default_factory=list)


STREAMS: Dict[str, FeedState] = {
    "8001": FeedState(),
    "8002": FeedState(),
}
DEVICES: Dict[str, DeviceState] = {}


def bearer_token(authorization: Optional[str]) -> str:
    value = (authorization or "").strip()
    if value.lower().startswith("bearer "):
        return value[7:].strip()
    return value


def require_token(
    authorization: Optional[str],
    tokens: tuple[str, ...],
    missing_message: str,
    invalid_message: str,
) -> None:
    if not tokens:
        raise HTTPException(status_code=500, detail=missing_message)

    submitted = bearer_token(authorization)
    if not submitted:
        raise HTTPException(status_code=401, detail="Authorization requerido")

    if not any(secrets.compare_digest(submitted, token) for token in tokens):
        raise HTTPException(status_code=401, detail=invalid_message)


def require_control_token(authorization: Optional[str]) -> None:
    require_token(
        authorization,
        CONTROL_TOKENS,
        "API_TOKEN no está configurado en Render",
        "Token de control inválido",
    )


def require_stream_token(authorization: Optional[str]) -> None:
    require_token(
        authorization,
        STREAM_TOKENS,
        "HXTEND_STREAM_TOKEN no está configurado en Render",
        "Token de stream inválido",
    )


def feed_state(feed_id: str) -> FeedState:
    feed_id = str(feed_id)
    if feed_id not in STREAMS:
        STREAMS[feed_id] = FeedState()
    return STREAMS[feed_id]


def device_state(device_id: str) -> DeviceState:
    device_id = device_id or "procesadora-01"
    if device_id not in DEVICES:
        DEVICES[device_id] = DeviceState()
    return DEVICES[device_id]


def feed_online(feed: FeedState) -> bool:
    return bool(feed.online and feed.frame and (time.time() - feed.updated_at) <= ONLINE_SECONDS)


def stream_state_payload():
    now = time.time()
    feeds = {}
    processor_online = False
    for feed_id, feed in STREAMS.items():
        online = bool(feed.online and feed.frame and (now - feed.updated_at) <= ONLINE_SECONDS)
        processor_online = processor_online or online
        feeds[feed_id] = {
            "online": online,
            "last_seen_seconds": None if feed.updated_at <= 0 else round(now - feed.updated_at, 2),
            "source_url": feed.source_url,
            "has_frame": bool(feed.frame),
            "bytes": len(feed.frame),
            "error": feed.error,
        }
    return {"ok": True, "processor_online": processor_online, "feeds": feeds}


@app.get("/")
async def root():
    return {"ok": True, "name": "Remote Control HxTend", "panel": "/panel"}


@app.get("/health")
async def health():
    return {"ok": True}


@app.get("/isalive")
async def isalive():
    return "ok"


@app.post("/api/stream/status")
async def stream_status(request: Request, authorization: Optional[str] = Header(default=None)):
    require_stream_token(authorization)
    payload = await request.json()
    feed_id = str(payload.get("feed_id") or payload.get("port") or "8001")
    feed = feed_state(feed_id)
    feed.online = bool(payload.get("online", True))
    feed.source_url = str(payload.get("source_url") or feed.source_url or "")
    feed.error = str(payload.get("error") or "")
    feed.updated_at = time.time()

    device_id = str(payload.get("device_id") or "procesadora-01")
    device_state(device_id).last_seen = time.time()
    return {"ok": True, "feed_id": feed_id}


@app.post("/api/stream/frame/{feed_id}")
async def stream_frame(
    feed_id: str,
    request: Request,
    authorization: Optional[str] = Header(default=None),
):
    require_stream_token(authorization)
    frame = await request.body()
    if not frame.startswith(b"\xff\xd8") or not frame.endswith(b"\xff\xd9"):
        raise HTTPException(status_code=400, detail="Expected a JPEG frame")
    if len(frame) > 2_500_000:
        raise HTTPException(status_code=413, detail="Frame too large")

    feed = feed_state(feed_id)
    feed.frame = frame
    feed.online = True
    feed.error = ""
    feed.updated_at = time.time()
    device_id = str(request.query_params.get("device_id") or "procesadora-01")
    device_state(device_id).last_seen = time.time()
    async with feed.condition:
        feed.condition.notify_all()
    return {"ok": True, "feed_id": feed_id, "bytes": len(frame)}


@app.get("/api/stream/state")
async def stream_state():
    return stream_state_payload()


@app.get("/api/stream/snapshot/{feed_id}")
async def stream_snapshot(feed_id: str):
    feed = feed_state(feed_id)
    if not feed_online(feed):
        return Response(status_code=204)
    return Response(
        content=feed.frame,
        media_type="image/jpeg",
        headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"},
    )


@app.get("/api/stream/mjpeg/{feed_id}")
async def stream_mjpeg(feed_id: str):
    feed = feed_state(feed_id)
    boundary = "hxtendpreview"

    async def generate():
        last_sent_at = 0.0
        while True:
            if feed.frame and feed.updated_at != last_sent_at:
                last_sent_at = feed.updated_at
                yield (
                    f"--{boundary}\r\n"
                    "Content-Type: image/jpeg\r\n"
                    f"Content-Length: {len(feed.frame)}\r\n"
                    "Cache-Control: no-store\r\n\r\n"
                ).encode("ascii") + feed.frame + b"\r\n"
            async with feed.condition:
                try:
                    await asyncio.wait_for(feed.condition.wait(), timeout=2.5)
                except asyncio.TimeoutError:
                    pass

    return StreamingResponse(
        generate(),
        media_type=f"multipart/x-mixed-replace; boundary={boundary}",
        headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"},
    )


async def queue_command(payload: dict, authorization: Optional[str]):
    require_control_token(authorization)
    device_id = str(payload.get("device_id") or payload.get("deviceId") or "procesadora-01")
    command = str(payload.get("command") or payload.get("cmd") or payload.get("button") or "").strip()
    if not command:
        raise HTTPException(status_code=400, detail="Missing command")
    item = {"command": command, "created_at": time.time()}
    device = device_state(device_id)
    device.commands.append(item)
    device.commands = device.commands[-100:]
    return {"ok": True, "device_id": device_id, "command": command}


@app.post("/api/command")
async def api_command(request: Request, authorization: Optional[str] = Header(default=None)):
    return await queue_command(await request.json(), authorization)


@app.post("/command")
async def command(request: Request, authorization: Optional[str] = Header(default=None)):
    return await queue_command(await request.json(), authorization)


@app.post("/api/control")
async def api_control(request: Request, authorization: Optional[str] = Header(default=None)):
    return await queue_command(await request.json(), authorization)


@app.post("/control")
async def control(request: Request, authorization: Optional[str] = Header(default=None)):
    return await queue_command(await request.json(), authorization)


@app.get("/api/commands/{device_id}")
async def get_commands(device_id: str, authorization: Optional[str] = Header(default=None)):
    require_stream_token(authorization)
    device = device_state(device_id)
    commands = list(device.commands)
    device.commands.clear()
    device.last_seen = time.time()
    return {"ok": True, "device_id": device_id, "commands": commands}


@app.get("/commands/{device_id}")
async def get_commands_compat(device_id: str, authorization: Optional[str] = Header(default=None)):
    return await get_commands(device_id, authorization)


@app.post("/api/device/heartbeat")
async def device_heartbeat(request: Request, authorization: Optional[str] = Header(default=None)):
    require_stream_token(authorization)
    payload = await request.json()
    device_id = str(payload.get("device_id") or payload.get("deviceId") or "procesadora-01")
    device_state(device_id).last_seen = time.time()
    return {"ok": True, "device_id": device_id}


@app.get("/api/device/state")
async def device_state_endpoint():
    now = time.time()
    return {
        "ok": True,
        "devices": {
            device_id: {
                "online": (now - state.last_seen) <= ONLINE_SECONDS if state.last_seen else False,
                "last_seen_seconds": None if not state.last_seen else round(now - state.last_seen, 2),
                "queued_commands": len(state.commands),
            }
            for device_id, state in DEVICES.items()
        },
    }


@app.get("/api/device/test/{device_id}")
async def device_test(device_id: str, authorization: Optional[str] = Header(default=None)):
    require_control_token(authorization)
    now = time.time()
    device = DEVICES.get(device_id)
    device_online = bool(device and device.last_seen and (now - device.last_seen) <= ONLINE_SECONDS)
    streams = stream_state_payload()
    connected = bool(device_online or streams["processor_online"])
    return {
        "ok": True,
        "device_id": device_id,
        "connected": connected,
        "device_online": device_online,
        "processor_online": streams["processor_online"],
        "feeds": streams["feeds"],
        "last_seen_seconds": None if not device or not device.last_seen else round(now - device.last_seen, 2),
    }


@app.get("/panel", response_class=HTMLResponse)
async def panel():
    return HTMLResponse(PANEL_HTML)


PANEL_HTML = """
<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Remote Control HxTend</title>
  <style>
    :root { color-scheme: dark; --bg:#080b10; --panel:#101720; --line:#223043; --text:#edf4ff; --muted:#94a3b8; --ok:#49e68b; --bad:#ff5b73; --accent:#67e8f9; }
    * { box-sizing:border-box; }
    body { margin:0; font-family:Inter,ui-sans-serif,system-ui,-apple-system,"Segoe UI",sans-serif; background:radial-gradient(circle at 20% -10%,#143348 0,#080b10 35%,#05070a 100%); color:var(--text); }
    .shell { width:min(1180px,calc(100vw - 32px)); margin:0 auto; padding:28px 0 36px; }
    header { display:flex; align-items:center; justify-content:space-between; gap:18px; margin-bottom:20px; }
    h1 { margin:0; font-size:clamp(26px,4vw,44px); letter-spacing:0; }
    .subtitle { color:var(--muted); margin-top:6px; font-size:14px; }
    .status { display:flex; align-items:center; gap:10px; padding:10px 14px; border:1px solid var(--line); border-radius:999px; background:rgba(16,23,32,.82); }
    .dot { width:10px; height:10px; border-radius:50%; background:var(--bad); box-shadow:0 0 0 4px rgba(255,91,115,.12); }
    .dot.on { background:var(--ok); box-shadow:0 0 0 4px rgba(73,230,139,.12); }
    .grid { display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:16px; }
    .card { border:1px solid var(--line); background:rgba(16,23,32,.88); border-radius:18px; overflow:hidden; box-shadow:0 18px 60px rgba(0,0,0,.28); }
    .card-head { display:flex; justify-content:space-between; align-items:center; padding:13px 14px; border-bottom:1px solid var(--line); }
    .card-title { font-weight:800; }
    .badge { font-size:12px; color:var(--muted); }
    .viewer { aspect-ratio:16/9; background:#000; position:relative; display:grid; place-items:center; }
    .viewer img { width:100%; height:100%; object-fit:contain; display:block; }
    .empty { position:absolute; inset:0; display:grid; place-items:center; color:var(--muted); background:linear-gradient(135deg,rgba(255,255,255,.04),rgba(255,255,255,.01)); text-align:center; padding:22px; }
    .empty.hide { display:none; }
    .controls { margin-top:16px; border:1px solid var(--line); background:rgba(16,23,32,.76); border-radius:18px; padding:14px; }
    .controls h2 { margin:0 0 12px; font-size:16px; }
    .row { display:flex; flex-wrap:wrap; gap:8px; }
    button { border:1px solid #2c3d52; background:#172333; color:var(--text); border-radius:10px; padding:10px 12px; font-weight:750; cursor:pointer; }
    button:hover { border-color:var(--accent); }
    .log { margin-top:10px; color:var(--muted); font-size:13px; min-height:18px; }
    @media (max-width:800px){ header{align-items:flex-start; flex-direction:column;} .grid{grid-template-columns:1fr;} }
  </style>
</head>
<body>
  <main class="shell">
    <header>
      <div>
        <h1>Remote Control HxTend</h1>
        <div class="subtitle">Panel remoto con preview fuera de la red local</div>
      </div>
      <div class="status"><span id="processorDot" class="dot"></span><span id="processorText">Procesadora offline</span></div>
    </header>
    <section class="grid">
      <article class="card">
        <div class="card-head"><span class="card-title">Screen 1</span><span id="feed8001Badge" class="badge">offline</span></div>
        <div class="viewer"><img id="feed8001" src="/api/stream/mjpeg/8001" alt="Screen 1"><div id="feed8001Empty" class="empty">Esperando stream 8001</div></div>
      </article>
      <article class="card">
        <div class="card-head"><span class="card-title">Screen 2</span><span id="feed8002Badge" class="badge">offline</span></div>
        <div class="viewer"><img id="feed8002" src="/api/stream/mjpeg/8002" alt="Screen 2"><div id="feed8002Empty" class="empty">Esperando stream 8002</div></div>
      </article>
    </section>
    <section class="controls">
      <h2>Controles</h2>
      <div class="row">
        <button onclick="sendCommand('LED')">LED</button><button onclick="sendCommand('LED_PLUS')">LED +</button><button onclick="sendCommand('IN_OUT')">IN / OUT</button><button onclick="sendCommand('FRAME')">FRAME</button><button onclick="sendCommand('LED_MINUS')">LED -</button><button onclick="sendCommand('WHITE_BALANCE')">WHITE BALANCE</button>
        <button onclick="sendCommand('POWER_ON_PRESS')">Presionar ON</button><button onclick="sendCommand('POWER_ON_RELEASE')">Liberar ON</button><button onclick="sendCommand('POWER_ON_TOGGLE')">Toggle ON</button>
        <button onclick="sendCommand('POWER_OFF_PRESS')">Presionar OFF</button><button onclick="sendCommand('POWER_OFF_RELEASE')">Liberar OFF</button><button onclick="sendCommand('POWER_OFF_TOGGLE')">Toggle OFF</button>
      </div>
      <div id="log" class="log">Listo.</div>
    </section>
  </main>
  <script>
    const log = (text) => { document.getElementById('log').textContent = text; };
    async function refreshState(){
      const res = await fetch('/api/stream/state', { cache:'no-store' });
      const data = await res.json();
      const dot = document.getElementById('processorDot');
      dot.classList.toggle('on', !!data.processor_online);
      document.getElementById('processorText').textContent = data.processor_online ? 'Procesadora online' : 'Procesadora offline';
      for (const id of ['8001','8002']) {
        const feed = data.feeds[id] || {};
        document.getElementById(`feed${id}Badge`).textContent = feed.online ? `online · ${feed.last_seen_seconds}s` : 'offline';
        document.getElementById(`feed${id}Empty`).classList.toggle('hide', !!feed.online);
      }
    }
    async function sendCommand(command){
      const token = localStorage.getItem('hxtend_token') || '';
      const headers = { 'Content-Type':'application/json' };
      if (token) headers.Authorization = `Bearer ${token}`;
      const body = JSON.stringify({ command, device_id:'procesadora-01' });
      for (const endpoint of ['/api/command','/command','/api/control','/control']) {
        try {
          const res = await fetch(endpoint, { method:'POST', headers, body });
          if (res.ok) { log(`${command} enviado`); return; }
        } catch (_) {}
      }
      log(`${command}: endpoint de control no disponible`);
    }
    refreshState();
    setInterval(refreshState, 2000);
  </script>
</body>
</html>
"""


PANEL_HTML = """
<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
  <title>Remote Control HxTend</title>
  <style>
    :root {
      color-scheme: dark;
      --bg: #070a0f;
      --panel: rgba(14, 20, 30, .92);
      --panel2: rgba(20, 29, 42, .86);
      --line: rgba(148, 163, 184, .18);
      --text: #eef6ff;
      --muted: #9aa9bb;
      --ok: #50e68c;
      --bad: #ff5b73;
      --accent: #67e8f9;
      --button: #172233;
    }
    * { box-sizing: border-box; }
    html, body { min-height: 100%; }
    body {
      margin: 0;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, "Segoe UI", sans-serif;
      background:
        radial-gradient(circle at 18% -8%, rgba(33, 150, 181, .28), transparent 34%),
        radial-gradient(circle at 90% 0%, rgba(80, 230, 140, .12), transparent 30%),
        linear-gradient(180deg, #09111b, var(--bg));
      color: var(--text);
    }
    .screen {
      width: min(1180px, calc(100vw - 28px));
      margin: 0 auto;
      padding: max(18px, env(safe-area-inset-top)) 0 34px;
    }
    .auth {
      min-height: 100vh;
      display: grid;
      place-items: center;
      padding: 22px 0;
    }
    .login-card {
      width: min(460px, calc(100vw - 28px));
      border: 1px solid var(--line);
      background: linear-gradient(180deg, rgba(14,20,30,.96), rgba(9,14,22,.94));
      border-radius: 22px;
      padding: 22px;
      box-shadow: 0 24px 80px rgba(0,0,0,.38);
    }
    .brand { display:flex; align-items:center; justify-content:space-between; gap:12px; margin-bottom:18px; }
    .brand h1 { margin:0; font-size: clamp(27px, 8vw, 42px); line-height:1; letter-spacing:0; }
    .brand span { color: var(--muted); font-size: 13px; display:block; margin-top:8px; }
    .orb { width: 44px; height: 44px; border-radius: 14px; border:1px solid rgba(103,232,249,.38); background: rgba(103,232,249,.12); display:grid; place-items:center; font-weight:900; color:var(--accent); }
    label { display:block; font-size: 12px; color: var(--muted); margin: 14px 0 7px; font-weight: 750; }
    input {
      width: 100%;
      border: 1px solid var(--line);
      background: rgba(2, 6, 12, .72);
      color: var(--text);
      border-radius: 13px;
      padding: 13px 14px;
      font-size: 15px;
      outline: none;
    }
    input:focus { border-color: rgba(103,232,249,.68); box-shadow: 0 0 0 4px rgba(103,232,249,.10); }
    .login-actions { display:grid; grid-template-columns:1fr 1fr; gap:10px; margin-top:16px; }
    button {
      border: 1px solid rgba(148,163,184,.20);
      background: var(--button);
      color: var(--text);
      border-radius: 13px;
      padding: 12px 14px;
      font-weight: 800;
      cursor: pointer;
      min-height: 44px;
      transition: border-color .16s ease, transform .16s ease, background .16s ease;
    }
    button:hover { border-color: rgba(103,232,249,.72); background: #1c2a3d; }
    button:active { transform: translateY(1px); }
    button.primary { background: linear-gradient(135deg, #128da3, #176b53); border-color: rgba(103,232,249,.45); }
    button.ghost { background: rgba(255,255,255,.045); }
    .message { min-height: 22px; margin-top: 13px; color: var(--muted); font-size: 13px; }
    .message.ok { color: var(--ok); }
    .message.bad { color: var(--bad); }
    .loader {
      width: 18px; height: 18px; border-radius:50%;
      border: 2px solid rgba(255,255,255,.28); border-top-color: var(--accent);
      display:inline-block; vertical-align:-4px; margin-right:8px;
      animation: spin .7s linear infinite;
    }
    @keyframes spin { to { transform: rotate(360deg); } }
    .app { display:none; }
    .topbar {
      display:flex; align-items:center; justify-content:space-between; gap:14px;
      margin: 8px 0 16px;
    }
    .title h1 { margin:0; font-size: clamp(24px, 5vw, 40px); letter-spacing:0; }
    .title p { margin:7px 0 0; color:var(--muted); font-size:14px; }
    .status-pill {
      display:flex; align-items:center; gap:9px;
      border:1px solid var(--line); background:rgba(14,20,30,.76);
      padding:10px 13px; border-radius:999px; white-space:nowrap;
      color: var(--muted); font-size: 13px; font-weight: 750;
    }
    .dot { width:10px; height:10px; border-radius:50%; background:var(--bad); box-shadow:0 0 0 4px rgba(255,91,115,.13); }
    .dot.on { background:var(--ok); box-shadow:0 0 0 4px rgba(80,230,140,.13); }
    .preview-grid {
      display:none;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 14px;
      margin-bottom: 14px;
    }
    .preview-grid.show { display:grid; }
    .stream-card {
      overflow:hidden;
      border:1px solid var(--line);
      background: #000;
      border-radius: 18px;
      box-shadow: 0 18px 70px rgba(0,0,0,.28);
    }
    .stream-head {
      display:flex; align-items:center; justify-content:space-between;
      background: rgba(14,20,30,.94);
      border-bottom:1px solid var(--line);
      padding: 12px 13px;
    }
    .stream-title { font-weight: 850; }
    .badge { font-size: 12px; color: var(--muted); }
    .viewer { aspect-ratio: 16/9; background:#000; display:grid; place-items:center; }
    .viewer img { width:100%; height:100%; object-fit:contain; display:block; }
    .empty-state {
      border:1px solid var(--line);
      background: var(--panel);
      border-radius: 18px;
      padding: 18px;
      color: var(--muted);
      margin-bottom: 14px;
      display:block;
    }
    .empty-state.hide { display:none; }
    .control-panel {
      border:1px solid var(--line);
      background: var(--panel);
      border-radius: 18px;
      padding: 15px;
    }
    .control-head {
      display:flex; justify-content:space-between; align-items:center; gap:12px; margin-bottom:12px;
    }
    .control-head h2 { margin:0; font-size:17px; }
    .control-grid {
      display:grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap:10px;
    }
    .led-group {
      display:grid;
      grid-template-columns: 1fr 1fr 1fr;
      gap:8px;
      grid-column: span 2;
      border:1px solid var(--line);
      background: rgba(255,255,255,.035);
      border-radius:15px;
      padding:8px;
    }
    .led-group .label {
      grid-column: 1 / -1;
      color:var(--muted);
      font-size:12px;
      font-weight:800;
      padding:0 2px;
    }
    .session-actions { display:flex; gap:8px; flex-wrap:wrap; }
    .log { color:var(--muted); font-size:13px; min-height:20px; margin-top:12px; }
    @media (max-width: 900px) {
      .preview-grid { grid-template-columns: 1fr; }
      .control-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .led-group { grid-column: 1 / -1; }
      .topbar { align-items:flex-start; flex-direction:column; }
    }
    @media (max-width: 520px) {
      .screen { width: min(100vw - 18px, 1180px); padding-bottom: 22px; }
      .login-card { width: calc(100vw - 18px); padding: 18px; border-radius: 18px; }
      .login-actions { grid-template-columns:1fr; }
      .control-grid { grid-template-columns:1fr; }
      .control-panel, .empty-state, .stream-card { border-radius: 15px; }
      button { width:100%; }
      .session-actions { width:100%; }
    }
  </style>
</head>
<body>
  <section id="authScreen" class="auth">
    <div class="login-card">
      <div class="brand">
        <div>
          <h1>Remote Control HxTend</h1>
          <span>Conecta la procesadora para abrir el panel remoto</span>
        </div>
        <div class="orb">HX</div>
      </div>

      <label for="deviceIdInput">Device ID</label>
      <input id="deviceIdInput" autocomplete="username" placeholder="procesadora-01">

      <label for="tokenInput">API token</label>
      <input id="tokenInput" autocomplete="current-password" type="password" placeholder="Token privado">

      <div class="login-actions">
        <button class="ghost" onclick="saveSession()">Guardar</button>
        <button id="testButton" class="primary" onclick="testAndEnter()">Test connection</button>
      </div>
      <div id="authMessage" class="message">Los datos se guardan solo en este navegador.</div>
    </div>
  </section>

  <main id="appScreen" class="screen app">
    <div class="topbar">
      <div class="title">
        <h1>Remote Control HxTend</h1>
        <p id="sessionSubtitle">Panel remoto</p>
      </div>
      <div class="status-pill"><span id="processorDot" class="dot"></span><span id="processorText">Procesadora offline</span></div>
    </div>

    <section id="previewGrid" class="preview-grid">
      <article id="card8001" class="stream-card">
        <div class="stream-head"><span class="stream-title">Monitor 1</span><span id="feed8001Badge" class="badge">offline</span></div>
        <div class="viewer"><img id="feed8001" alt="Monitor 1"></div>
      </article>
      <article id="card8002" class="stream-card">
        <div class="stream-head"><span class="stream-title">Monitor 2</span><span id="feed8002Badge" class="badge">offline</span></div>
        <div class="viewer"><img id="feed8002" alt="Monitor 2"></div>
      </article>
    </section>

    <div id="emptyStream" class="empty-state">Procesadora conectada al panel. Esperando transmisión activa para mostrar monitores.</div>

    <section class="control-panel">
      <div class="control-head">
        <h2>Controls</h2>
        <div class="session-actions">
          <button class="ghost" onclick="testConnection()">Test</button>
          <button class="ghost" onclick="changeConnection()">Cambiar datos</button>
        </div>
      </div>
      <div class="control-grid">
        <div class="led-group">
          <div class="label">LED</div>
          <button onclick="sendCommand('LED_MINUS')">LED -</button>
          <button onclick="sendCommand('LED')">LED</button>
          <button onclick="sendCommand('LED_PLUS')">LED +</button>
        </div>
        <button onclick="sendCommand('IN_OUT')">IN / OUT</button>
        <button onclick="sendCommand('FRAME')">Frame</button>
        <button onclick="sendCommand('WHITE_BALANCE')">White balance</button>
        <button onclick="sendCommand('POWER_ON_TOGGLE')">Toggle ON</button>
        <button onclick="sendCommand('POWER_OFF_TOGGLE')">Toggle OFF</button>
      </div>
      <div id="panelLog" class="log">Listo.</div>
    </section>
  </main>

  <script>
    const STORAGE_KEY = 'hxtend_remote_session_v2';
    let session = { deviceId: 'procesadora-01', token: '' };
    let stateTimer = null;

    function $(id){ return document.getElementById(id); }
    function setAuthMessage(text, type=''){ const el=$('authMessage'); el.className='message ' + type; el.innerHTML=text; }
    function setPanelLog(text){ $('panelLog').textContent = text; }
    function authHeaders(){
      const headers = { 'Content-Type': 'application/json' };
      if (session.token) headers.Authorization = `Bearer ${session.token}`;
      return headers;
    }
    function loadSession(){
      try {
        const saved = JSON.parse(localStorage.getItem(STORAGE_KEY) || '{}');
        session.deviceId = saved.deviceId || 'procesadora-01';
        session.token = saved.token || '';
      } catch (_) {}
      $('deviceIdInput').value = session.deviceId;
      $('tokenInput').value = session.token;
    }
    function saveSession(){
      session = {
        deviceId: $('deviceIdInput').value.trim() || 'procesadora-01',
        token: $('tokenInput').value.trim()
      };
      localStorage.setItem(STORAGE_KEY, JSON.stringify(session));
      setAuthMessage('Datos guardados en este navegador.', 'ok');
    }
    async function testConnection(){
      const res = await fetch(`/api/device/test/${encodeURIComponent(session.deviceId)}`, {
        headers: session.token ? { Authorization: `Bearer ${session.token}` } : {},
        cache: 'no-store'
      });
      if (!res.ok) throw new Error(res.status === 401 ? 'Token inválido.' : 'No se pudo comprobar el dispositivo.');
      return await res.json();
    }
    async function testAndEnter(){
      saveSession();
      const btn = $('testButton');
      btn.disabled = true;
      btn.innerHTML = '<span class="loader"></span>Conectando';
      setAuthMessage('Comprobando respuesta del dispositivo...');
      try {
        const data = await testConnection();
        if (!data.connected) {
          setAuthMessage('Token correcto, pero la procesadora todavía no está enviando señal a Render.', 'bad');
          return;
        }
        setAuthMessage('Conectado. Abriendo panel...', 'ok');
        setTimeout(showPanel, 450);
      } catch (err) {
        setAuthMessage(err.message || 'No se pudo conectar.', 'bad');
      } finally {
        btn.disabled = false;
        btn.textContent = 'Test connection';
      }
    }
    function showPanel(){
      $('authScreen').style.display = 'none';
      $('appScreen').style.display = 'block';
      $('sessionSubtitle').textContent = `Device ID: ${session.deviceId}`;
      refreshState();
      if (stateTimer) clearInterval(stateTimer);
      stateTimer = setInterval(refreshState, 2000);
    }
    function changeConnection(){
      $('appScreen').style.display = 'none';
      $('authScreen').style.display = 'grid';
      if (stateTimer) clearInterval(stateTimer);
    }
    function setFeedImage(feedId, online){
      const card = $(`card${feedId}`);
      const img = $(`feed${feedId}`);
      card.style.display = online ? 'block' : 'none';
      if (online && !img.src) img.src = `/api/stream/mjpeg/${feedId}`;
      if (!online) img.removeAttribute('src');
    }
    async function refreshState(){
      try {
        const res = await fetch('/api/stream/state', { cache:'no-store' });
        const data = await res.json();
        const dot = $('processorDot');
        dot.classList.toggle('on', !!data.processor_online);
        $('processorText').textContent = data.processor_online ? 'Procesadora online' : 'Procesadora offline';

        let anyStream = false;
        for (const id of ['8001','8002']) {
          const feed = data.feeds[id] || {};
          const online = !!feed.online;
          anyStream = anyStream || online;
          $(`feed${id}Badge`).textContent = online ? `online · ${feed.last_seen_seconds}s` : 'offline';
          setFeedImage(id, online);
        }
        $('previewGrid').classList.toggle('show', anyStream);
        $('emptyStream').classList.toggle('hide', anyStream);
      } catch (err) {
        $('processorDot').classList.remove('on');
        $('processorText').textContent = 'Sin conexión con Render';
      }
    }
    async function sendCommand(command){
      try {
        const res = await fetch('/api/command', {
          method: 'POST',
          headers: authHeaders(),
          body: JSON.stringify({ command, device_id: session.deviceId })
        });
        if (!res.ok) throw new Error(res.status === 401 ? 'Token inválido.' : 'No se pudo enviar.');
        setPanelLog(`${command} enviado`);
      } catch (err) {
        setPanelLog(err.message || 'Error enviando comando');
      }
    }
    loadSession();
  </script>
</body>
</html>
"""
