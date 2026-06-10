import asyncio
import os
import time
from dataclasses import dataclass, field
from typing import Dict, Optional

from fastapi import Header, HTTPException, Request
from fastapi.responses import HTMLResponse, Response, StreamingResponse


STREAM_TOKEN = (
    os.getenv("HXTEND_STREAM_TOKEN")
    or os.getenv("API_TOKEN")
    or os.getenv("PANEL_TOKEN")
    or ""
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


STREAMS: Dict[str, FeedState] = {
    "8001": FeedState(),
    "8002": FeedState(),
}


def feed_state(feed_id: str) -> FeedState:
    feed_id = str(feed_id)
    if feed_id not in STREAMS:
        STREAMS[feed_id] = FeedState()
    return STREAMS[feed_id]


def authorize(authorization: Optional[str]) -> None:
    if not STREAM_TOKEN:
        return
    if authorization not in {STREAM_TOKEN, f"Bearer {STREAM_TOKEN}"}:
        raise HTTPException(status_code=401, detail="Invalid stream token")


def is_online(feed: FeedState) -> bool:
    return bool(feed.online and feed.frame and (time.time() - feed.updated_at) <= ONLINE_SECONDS)


def state_payload():
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


def remove_route(app, path: str) -> None:
    app.router.routes = [route for route in app.router.routes if getattr(route, "path", None) != path]


def install_remote_preview(app) -> None:
    @app.post("/api/stream/status")
    async def stream_status(request: Request, authorization: Optional[str] = Header(default=None)):
        authorize(authorization)
        payload = await request.json()
        feed_id = str(payload.get("feed_id") or payload.get("port") or "8001")
        feed = feed_state(feed_id)
        feed.online = bool(payload.get("online", True))
        feed.source_url = str(payload.get("source_url") or feed.source_url or "")
        feed.error = str(payload.get("error") or "")
        feed.updated_at = time.time()
        return {"ok": True, "feed_id": feed_id}

    @app.post("/api/stream/frame/{feed_id}")
    async def stream_frame(feed_id: str, request: Request, authorization: Optional[str] = Header(default=None)):
        authorize(authorization)
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
        async with feed.condition:
            feed.condition.notify_all()
        return {"ok": True, "feed_id": feed_id, "bytes": len(frame)}

    @app.get("/api/stream/state")
    async def stream_state():
        return state_payload()

    @app.get("/api/stream/snapshot/{feed_id}")
    async def stream_snapshot(feed_id: str):
        feed = feed_state(feed_id)
        if not is_online(feed):
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

    remove_route(app, "/panel")

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
    refreshState(); setInterval(refreshState, 2000);
  </script>
</body>
</html>
"""
