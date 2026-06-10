import asyncio
import os
import secrets
import time
from collections import defaultdict
from dataclasses import dataclass, field
from threading import Lock
from typing import Dict, List, Optional

from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, Response, StreamingResponse
from pydantic import BaseModel, Field


# ============================================================
# VARIABLES DE ENTORNO DE RENDER
# ============================================================

API_TOKEN = os.getenv("API_TOKEN", "").strip()
DEVICE_TOKEN = os.getenv("DEVICE_TOKEN", "").strip()
STREAM_TOKEN = os.getenv("HXTEND_STREAM_TOKEN", os.getenv("STREAM_TOKEN", "")).strip()
DEFAULT_DEVICE_ID = os.getenv("DEVICE_ID", "procesadora-01").strip()

COMMAND_TIMEOUT_SECONDS = int(os.getenv("COMMAND_TIMEOUT_SECONDS", "30"))
DEVICE_ONLINE_SECONDS = int(os.getenv("DEVICE_ONLINE_SECONDS", "30"))
STREAM_ONLINE_SECONDS = float(os.getenv("HXTEND_STREAM_ONLINE_SECONDS", "8"))
MAX_COMMAND_HISTORY = int(os.getenv("MAX_COMMAND_HISTORY", "1000"))
MAX_STREAM_FRAME_BYTES = int(os.getenv("HXTEND_MAX_STREAM_FRAME_BYTES", "2500000"))


# ============================================================
# ACCIONES PERMITIDAS
# ============================================================

ALLOWED_ACTIONS = {
    "led",
    "led_plus",
    "led_minus",
    "in_out",
    "frame",
    "white_balance",
    "power_on_press",
    "power_on_release",
    "power_on_toggle",
    "power_off_press",
    "power_off_release",
    "power_off_toggle",
}


# ============================================================
# FASTAPI
# ============================================================

app = FastAPI(
    title="Remote Control HxTend",
    version="2.1.0",
    description="API remota para control HxTend y preview de stream de baja latencia",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)


# ============================================================
# ALMACENAMIENTO TEMPORAL EN MEMORIA
# ============================================================

lock = Lock()

commands_by_device = defaultdict(list)
command_history = {}
device_statuses = {}


@dataclass
class FeedState:
    frame: bytes = b""
    seq: int = 0
    updated_at: float = 0.0
    online: bool = False
    source_url: str = ""
    error: str = ""
    device_id: str = DEFAULT_DEVICE_ID
    condition: asyncio.Condition = field(default_factory=asyncio.Condition)


STREAMS: Dict[str, FeedState] = {
    "8001": FeedState(),
    "8002": FeedState(),
}


# ============================================================
# MODELOS
# ============================================================

class CommandCreate(BaseModel):
    device_id: str = Field(default=DEFAULT_DEVICE_ID, min_length=1, max_length=80)
    action: Optional[str] = Field(default=None, max_length=80)
    command: Optional[str] = Field(default=None, max_length=80)


class CommandResult(BaseModel):
    device_id: str
    command_id: str
    ok: bool
    message: Optional[str] = None


class Heartbeat(BaseModel):
    device_id: str
    local_ip: Optional[str] = None
    rssi_dbm: Optional[int] = None
    signal_percent: Optional[int] = None
    quality: Optional[str] = None
    power_on_active: bool = False
    power_off_active: bool = False


# ============================================================
# UTILIDADES
# ============================================================

def current_timestamp():
    return int(time.time())


def bearer_value(authorization: Optional[str]) -> str:
    value = (authorization or "").strip()
    if value.lower().startswith("bearer "):
        return value[7:].strip()
    return value


def require_api_token(authorization):
    if not API_TOKEN:
        raise HTTPException(
            status_code=500,
            detail="API_TOKEN no está configurado en Render",
        )

    submitted = bearer_value(authorization)

    if not submitted:
        raise HTTPException(
            status_code=401,
            detail="Authorization requerido",
        )

    if not secrets.compare_digest(submitted, API_TOKEN):
        raise HTTPException(
            status_code=401,
            detail="Token de control inválido",
        )


def require_api_query_token(token: Optional[str]):
    if not API_TOKEN:
        raise HTTPException(
            status_code=500,
            detail="API_TOKEN no está configurado en Render",
        )

    if not token or not secrets.compare_digest(token.strip(), API_TOKEN):
        raise HTTPException(
            status_code=401,
            detail="Token de control inválido",
        )


def require_device_token(token):
    if not DEVICE_TOKEN:
        raise HTTPException(
            status_code=500,
            detail="DEVICE_TOKEN no está configurado en Render",
        )

    if not token:
        raise HTTPException(
            status_code=401,
            detail="Token del dispositivo requerido",
        )

    if not secrets.compare_digest(token.strip(), DEVICE_TOKEN):
        raise HTTPException(
            status_code=401,
            detail="Token del dispositivo inválido",
        )


def require_stream_token(authorization):
    if not STREAM_TOKEN:
        raise HTTPException(
            status_code=500,
            detail="HXTEND_STREAM_TOKEN no está configurado en Render",
        )

    submitted = bearer_value(authorization)

    if not submitted:
        raise HTTPException(
            status_code=401,
            detail="Authorization de stream requerido",
        )

    if not secrets.compare_digest(submitted, STREAM_TOKEN):
        raise HTTPException(
            status_code=401,
            detail="Token de stream inválido",
        )


def recover_stale_commands_locked(device_id):
    recovered = 0
    now = current_timestamp()

    queue = commands_by_device[device_id]

    for command in queue:
        if (
            command["status"] == "delivered"
            and command["delivered_at"] is not None
            and now - command["delivered_at"] >= COMMAND_TIMEOUT_SECONDS
        ):
            command["status"] = "pending"
            command["delivered_at"] = None
            recovered += 1

    return recovered


def trim_command_history_locked():
    while len(command_history) > MAX_COMMAND_HISTORY:
        oldest_id = next(iter(command_history))
        command_history.pop(oldest_id, None)


def feed_state(feed_id: str) -> FeedState:
    feed_id = str(feed_id or "8001")
    if feed_id not in STREAMS:
        STREAMS[feed_id] = FeedState()
    return STREAMS[feed_id]


def feed_is_online(feed: FeedState, now: Optional[float] = None) -> bool:
    now = now or time.time()
    return bool(feed.online and feed.frame and (now - feed.updated_at) <= STREAM_ONLINE_SECONDS)


def stream_state_payload():
    now = time.time()
    feeds = {}
    processor_online = False

    for feed_id, feed in STREAMS.items():
        online = feed_is_online(feed, now)
        processor_online = processor_online or online
        feeds[feed_id] = {
            "online": online,
            "last_seen_seconds": None if not feed.updated_at else round(now - feed.updated_at, 2),
            "source_url": feed.source_url,
            "has_frame": bool(feed.frame),
            "bytes": len(feed.frame),
            "seq": feed.seq,
            "error": feed.error,
            "device_id": feed.device_id,
        }

    return {
        "ok": True,
        "processor_online": processor_online,
        "feeds": feeds,
    }


def device_status_payload(device_id: str):
    with lock:
        status = device_statuses.get(device_id)

    if not status:
        return {
            "ok": True,
            "online": False,
            "device_id": device_id,
            "message": "Todavía no se recibió heartbeat",
        }

    seconds_ago = max(0, current_timestamp() - status["last_seen"])

    return {
        "ok": True,
        "online": seconds_ago <= DEVICE_ONLINE_SECONDS,
        "seconds_ago": seconds_ago,
        **status,
    }


# ============================================================
# RUTAS PÚBLICAS
# ============================================================

@app.get("/")
def root():
    return {
        "ok": True,
        "service": "Remote Control HxTend",
        "version": "2.1.0",
        "device_id": DEFAULT_DEVICE_ID,
        "panel": "/panel",
        "docs": "/docs",
    }


@app.get("/health")
def health():
    return {
        "ok": True,
        "server_time": current_timestamp(),
    }


@app.get("/isalive")
def isalive():
    return "ok"


# ============================================================
# RUTAS PARA PANEL, UNITY O CELULAR
# ============================================================

@app.get("/api/actions")
def get_actions(
    authorization: Optional[str] = Header(default=None),
):
    require_api_token(authorization)

    return {
        "ok": True,
        "actions": sorted(ALLOWED_ACTIONS),
    }


@app.post("/api/command")
def create_command(
    request: CommandCreate,
    authorization: Optional[str] = Header(default=None),
):
    require_api_token(authorization)

    device_id = request.device_id.strip()
    action = (request.action or request.command or "").strip().lower()

    if not device_id:
        raise HTTPException(
            status_code=400,
            detail="device_id obligatorio",
        )

    if action not in ALLOWED_ACTIONS:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "Acción inválida",
                "allowed_actions": sorted(ALLOWED_ACTIONS),
            },
        )

    command_id = secrets.token_hex(8)

    command = {
        "id": command_id,
        "device_id": device_id,
        "action": action,
        "status": "pending",
        "created_at": current_timestamp(),
        "delivered_at": None,
        "completed_at": None,
        "result": None,
    }

    with lock:
        commands_by_device[device_id].append(command)
        command_history[command_id] = command
        trim_command_history_locked()

    return {
        "ok": True,
        "message": "Comando agregado",
        "command": command,
    }


@app.post("/command")
def create_command_compat(
    request: CommandCreate,
    authorization: Optional[str] = Header(default=None),
):
    return create_command(request, authorization)


@app.post("/api/control")
def create_control_compat(
    request: CommandCreate,
    authorization: Optional[str] = Header(default=None),
):
    return create_command(request, authorization)


@app.get("/api/command/{command_id}")
def get_command_status(
    command_id: str,
    authorization: Optional[str] = Header(default=None),
):
    require_api_token(authorization)

    with lock:
        command = command_history.get(command_id)

        if not command:
            raise HTTPException(
                status_code=404,
                detail="Comando no encontrado",
            )

        return {
            "ok": True,
            "command": dict(command),
        }


@app.get("/api/device/{device_id}/status")
def get_device_status(
    device_id: str,
    authorization: Optional[str] = Header(default=None),
):
    require_api_token(authorization)
    return device_status_payload(device_id)


@app.get("/api/device/test/{device_id}")
def test_device_connection(
    device_id: str,
    authorization: Optional[str] = Header(default=None),
):
    require_api_token(authorization)

    status = device_status_payload(device_id)
    streams = stream_state_payload()
    connected = bool(status.get("online") or streams["processor_online"])

    return {
        "ok": True,
        "device_id": device_id,
        "connected": connected,
        "device_online": bool(status.get("online")),
        "processor_online": streams["processor_online"],
        "status": status,
        "feeds": streams["feeds"],
    }


# ============================================================
# RUTAS USADAS POR EL PICO W
# ============================================================

@app.get("/api/device/next")
def get_next_command(
    device_id: str = Query(..., min_length=1),
    token: str = Query(..., min_length=1),
):
    require_device_token(token)

    with lock:
        recover_stale_commands_locked(device_id)

        queue = commands_by_device[device_id]

        for command in queue:
            if command["status"] == "pending":
                command["status"] = "delivered"
                command["delivered_at"] = current_timestamp()

                return {
                    "ok": True,
                    "has_command": True,
                    "command": {
                        "id": command["id"],
                        "action": command["action"],
                    },
                }

    return {
        "ok": True,
        "has_command": False,
    }


@app.post("/api/device/result")
def save_command_result(
    result: CommandResult,
    token: str = Query(..., min_length=1),
):
    require_device_token(token)

    with lock:
        command = command_history.get(result.command_id)

        if not command:
            raise HTTPException(
                status_code=404,
                detail="Comando no encontrado",
            )

        if command["device_id"] != result.device_id:
            raise HTTPException(
                status_code=400,
                detail="El comando no pertenece a este dispositivo",
            )

        command["status"] = "completed" if result.ok else "failed"
        command["completed_at"] = current_timestamp()
        command["result"] = {
            "ok": result.ok,
            "message": result.message,
        }

    return {
        "ok": True,
        "message": "Resultado registrado",
    }


@app.post("/api/device/heartbeat")
def heartbeat(
    data: Heartbeat,
    token: str = Query(..., min_length=1),
):
    require_device_token(token)

    status = data.model_dump()
    status["last_seen"] = current_timestamp()

    with lock:
        device_statuses[data.device_id] = status

    return {
        "ok": True,
        "server_time": current_timestamp(),
    }


@app.post("/api/device/recover")
def recover_commands(
    device_id: str = Query(..., min_length=1),
    token: str = Query(..., min_length=1),
):
    require_device_token(token)

    with lock:
        recovered = recover_stale_commands_locked(device_id)

    return {
        "ok": True,
        "recovered": recovered,
    }


# ============================================================
# RUTAS DEL STREAM REMOTO
# ============================================================

@app.post("/api/stream/status")
async def receive_stream_status(
    request: Request,
    authorization: Optional[str] = Header(default=None),
):
    require_stream_token(authorization)

    payload = await request.json()
    feed_id = str(payload.get("feed_id") or payload.get("port") or "8001")
    feed = feed_state(feed_id)

    feed.online = bool(payload.get("online", True))
    feed.source_url = str(payload.get("source_url") or feed.source_url or "")
    feed.error = str(payload.get("error") or "")
    feed.device_id = str(payload.get("device_id") or feed.device_id or DEFAULT_DEVICE_ID)
    feed.updated_at = time.time()

    async with feed.condition:
        feed.condition.notify_all()

    return {
        "ok": True,
        "feed_id": feed_id,
    }


@app.post("/api/stream/frame/{feed_id}")
async def receive_stream_frame(
    feed_id: str,
    request: Request,
    authorization: Optional[str] = Header(default=None),
):
    require_stream_token(authorization)

    frame = await request.body()

    if len(frame) > MAX_STREAM_FRAME_BYTES:
        raise HTTPException(status_code=413, detail="Frame demasiado grande")

    if not frame.startswith(b"\xff\xd8") or not frame.endswith(b"\xff\xd9"):
        raise HTTPException(status_code=400, detail="Se esperaba un frame JPEG")

    feed = feed_state(feed_id)
    feed.frame = frame
    feed.seq += 1
    feed.online = True
    feed.error = ""
    feed.updated_at = time.time()
    feed.device_id = str(request.query_params.get("device_id") or feed.device_id or DEFAULT_DEVICE_ID)

    async with feed.condition:
        feed.condition.notify_all()

    return {
        "ok": True,
        "feed_id": feed_id,
        "bytes": len(frame),
        "seq": feed.seq,
    }


@app.get("/api/stream/state")
def get_stream_state(
    authorization: Optional[str] = Header(default=None),
):
    require_api_token(authorization)
    return stream_state_payload()


@app.get("/api/stream/snapshot/{feed_id}")
def get_stream_snapshot(
    feed_id: str,
    authorization: Optional[str] = Header(default=None),
    token: Optional[str] = Query(default=None),
):
    if authorization:
        require_api_token(authorization)
    else:
        require_api_query_token(token)

    feed = feed_state(feed_id)

    if not feed_is_online(feed):
        return Response(status_code=204)

    return Response(
        content=feed.frame,
        media_type="image/jpeg",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/api/stream/mjpeg/{feed_id}")
async def get_stream_mjpeg(
    feed_id: str,
    token: Optional[str] = Query(default=None),
):
    require_api_query_token(token)

    feed = feed_state(feed_id)
    boundary = "hxtendpreview"

    async def generate():
        last_seq = -1

        while True:
            if feed_is_online(feed) and feed.frame and feed.seq != last_seq:
                last_seq = feed.seq
                frame = feed.frame
                yield (
                    f"--{boundary}\r\n"
                    "Content-Type: image/jpeg\r\n"
                    f"Content-Length: {len(frame)}\r\n"
                    "Cache-Control: no-store\r\n\r\n"
                ).encode("ascii") + frame + b"\r\n"

            async with feed.condition:
                try:
                    await asyncio.wait_for(feed.condition.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    pass

    return StreamingResponse(
        generate(),
        media_type=f"multipart/x-mixed-replace; boundary={boundary}",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
            "X-Accel-Buffering": "no",
        },
    )


# ============================================================
# PANEL WEB
# ============================================================

PANEL_HTML = """
<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Remote Control HxTend</title>
  <style>
    :root {
      color-scheme: dark;
      --bg: #080a0d;
      --surface: #10151b;
      --surface-2: #141b22;
      --line: #27323d;
      --text: #f2f7fb;
      --muted: #a7b2bd;
      --green: #42d392;
      --red: #ff5f6d;
      --cyan: #5dd7ff;
      --amber: #ffca6a;
      --button: #1d2a35;
      --button-hover: #243746;
    }

    * { box-sizing: border-box; }

    body {
      margin: 0;
      min-height: 100vh;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, "Segoe UI", sans-serif;
      background: var(--bg);
      color: var(--text);
    }

    button, input {
      font: inherit;
    }

    button {
      min-height: 44px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--button);
      color: var(--text);
      font-weight: 750;
      cursor: pointer;
      transition: border-color .15s ease, background .15s ease, transform .15s ease;
    }

    button:hover { background: var(--button-hover); border-color: var(--cyan); }
    button:active { transform: translateY(1px); }
    button:disabled { cursor: not-allowed; opacity: .62; }

    input {
      width: 100%;
      min-height: 46px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #0b1015;
      color: var(--text);
      padding: 0 12px;
      outline: none;
    }

    input:focus { border-color: var(--cyan); }

    .app {
      width: min(1180px, calc(100vw - 28px));
      margin: 0 auto;
      padding: 24px 0 34px;
    }

    .login {
      min-height: 100vh;
      display: grid;
      place-items: center;
      padding: 24px;
    }

    .login-card {
      width: min(460px, 100%);
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--surface);
      padding: 22px;
      box-shadow: 0 24px 70px rgba(0,0,0,.36);
    }

    .brand {
      display: flex;
      align-items: center;
      gap: 12px;
      margin-bottom: 18px;
    }

    .mark {
      width: 38px;
      height: 38px;
      border-radius: 8px;
      display: grid;
      place-items: center;
      background: #122632;
      border: 1px solid #244250;
      color: var(--cyan);
      font-weight: 900;
    }

    h1, h2, h3, p { margin: 0; }

    h1 { font-size: clamp(24px, 5vw, 36px); letter-spacing: 0; }
    h2 { font-size: 18px; letter-spacing: 0; }
    h3 { font-size: 14px; letter-spacing: 0; }

    .muted { color: var(--muted); font-size: 14px; line-height: 1.45; }
    .field { display: grid; gap: 7px; margin: 12px 0; }
    .field label { color: var(--muted); font-size: 13px; font-weight: 700; }

    .primary {
      width: 100%;
      margin-top: 12px;
      background: #115a6f;
      border-color: #227e98;
    }

    .primary:hover { background: #146d86; }

    .login-message {
      min-height: 22px;
      margin-top: 12px;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.35;
    }

    .loader {
      width: 18px;
      height: 18px;
      border: 2px solid rgba(255,255,255,.24);
      border-top-color: #fff;
      border-radius: 50%;
      display: inline-block;
      vertical-align: -4px;
      margin-right: 8px;
      animation: spin .8s linear infinite;
    }

    @keyframes spin { to { transform: rotate(360deg); } }

    .hidden { display: none !important; }

    .topbar {
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      gap: 16px;
      margin-bottom: 18px;
    }

    .title-stack { display: grid; gap: 4px; }

    .status-pill {
      display: inline-flex;
      align-items: center;
      gap: 9px;
      min-height: 38px;
      padding: 0 12px;
      border: 1px solid var(--line);
      border-radius: 999px;
      background: var(--surface);
      white-space: nowrap;
      color: var(--muted);
      font-size: 14px;
      font-weight: 700;
    }

    .dot {
      width: 10px;
      height: 10px;
      border-radius: 50%;
      background: var(--red);
      box-shadow: 0 0 0 4px rgba(255,95,109,.14);
    }

    .dot.on {
      background: var(--green);
      box-shadow: 0 0 0 4px rgba(66,211,146,.14);
    }

    .layout {
      display: grid;
      grid-template-columns: minmax(0, 1fr) 330px;
      gap: 16px;
      align-items: start;
    }

    .panel, .stream-card, .empty-stream {
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--surface);
    }

    .panel { padding: 14px; }

    .panel-head {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 10px;
      margin-bottom: 12px;
    }

    .mini {
      color: var(--muted);
      font-size: 12px;
      font-weight: 700;
    }

    .controls {
      display: grid;
      gap: 10px;
    }

    .control-grid {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 8px;
    }

    .wide-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 8px;
    }

    .danger { border-color: #65303a; background: #31161c; }
    .danger:hover { border-color: var(--red); background: #421d25; }
    .accent { border-color: #2a5364; background: #102936; }
    .accent:hover { border-color: var(--cyan); background: #143747; }

    .preview-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
    }

    .stream-card {
      overflow: hidden;
      min-width: 0;
    }

    .stream-head {
      height: 42px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      padding: 0 12px;
      border-bottom: 1px solid var(--line);
      background: var(--surface-2);
    }

    .viewer {
      aspect-ratio: 16 / 9;
      background: #020304;
      display: grid;
      place-items: center;
      position: relative;
      overflow: hidden;
    }

    .viewer img {
      width: 100%;
      height: 100%;
      object-fit: contain;
      display: block;
    }

    .latency {
      color: var(--amber);
      font-size: 12px;
      font-weight: 750;
    }

    .empty-stream {
      min-height: 260px;
      display: grid;
      place-items: center;
      text-align: center;
      padding: 26px;
      color: var(--muted);
    }

    .log {
      min-height: 38px;
      padding: 10px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #0b1015;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.35;
    }

    .session-row {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      margin-top: 10px;
    }

    .ghost {
      min-height: 36px;
      padding: 0 10px;
      font-size: 13px;
      background: transparent;
    }

    @media (max-width: 920px) {
      .layout { grid-template-columns: 1fr; }
      .panel { order: -1; }
    }

    @media (max-width: 680px) {
      .app { width: min(100vw - 18px, 1180px); padding-top: 14px; }
      .topbar { flex-direction: column; }
      .preview-grid { grid-template-columns: 1fr; }
      .control-grid, .wide-grid { grid-template-columns: 1fr; }
      .status-pill { width: 100%; justify-content: center; }
    }
  </style>
</head>
<body>
  <section id="loginView" class="login">
    <div class="login-card">
      <div class="brand">
        <div class="mark">HX</div>
        <div>
          <h1>Remote Control HxTend</h1>
          <p class="muted">Acceso remoto del controlador y preview en vivo.</p>
        </div>
      </div>

      <div class="field">
        <label for="deviceInput">Device ID</label>
        <input id="deviceInput" autocomplete="username" value="procesadora-01">
      </div>

      <div class="field">
        <label for="tokenInput">API token</label>
        <input id="tokenInput" type="password" autocomplete="current-password" placeholder="API_TOKEN configurado en Render">
      </div>

      <button id="connectButton" class="primary" onclick="connectPanel()">Conectar</button>
      <div id="loginMessage" class="login-message">El token se guarda solo en este navegador.</div>
    </div>
  </section>

  <main id="panelView" class="app hidden">
    <div class="topbar">
      <div class="title-stack">
        <h1>Remote Control HxTend</h1>
        <p id="deviceLabel" class="muted">procesadora-01</p>
      </div>
      <div class="status-pill">
        <span id="processorDot" class="dot"></span>
        <span id="processorText">Procesadora offline</span>
      </div>
    </div>

    <div class="layout">
      <section>
        <div id="emptyStream" class="empty-stream">
          <div>
            <h2>Esperando transmisión activa</h2>
            <p class="muted">Cuando la caja envíe video, el monitor aparece automáticamente.</p>
          </div>
        </div>

        <div id="previewGrid" class="preview-grid hidden">
          <article id="card8001" class="stream-card hidden">
            <div class="stream-head">
              <h3>Monitor 8001</h3>
              <span id="feed8001Info" class="latency">offline</span>
            </div>
            <div class="viewer">
              <img id="feed8001" alt="Monitor 8001">
            </div>
          </article>

          <article id="card8002" class="stream-card hidden">
            <div class="stream-head">
              <h3>Monitor 8002</h3>
              <span id="feed8002Info" class="latency">offline</span>
            </div>
            <div class="viewer">
              <img id="feed8002" alt="Monitor 8002">
            </div>
          </article>
        </div>
      </section>

      <aside class="panel">
        <div class="panel-head">
          <h2>Controles</h2>
          <span id="controlStatus" class="mini">Listo</span>
        </div>

        <div class="controls">
          <div class="control-grid">
            <button onclick="sendAction('led_minus')">LED -</button>
            <button class="accent" onclick="sendAction('led')">LED</button>
            <button onclick="sendAction('led_plus')">LED +</button>
          </div>

          <div class="wide-grid">
            <button onclick="sendAction('in_out')">IN / OUT</button>
            <button onclick="sendAction('frame')">Frame</button>
          </div>

          <button onclick="sendAction('white_balance')">White balance</button>

          <div class="wide-grid">
            <button class="accent" onclick="sendAction('power_on_toggle')">Toggle ON</button>
            <button class="danger" onclick="sendAction('power_off_toggle')">Toggle OFF</button>
          </div>

          <div id="panelLog" class="log">Panel conectado.</div>

          <div class="session-row">
            <span id="lastUpdate" class="mini">Sin datos todavía</span>
            <button class="ghost" onclick="logoutPanel()">Cambiar token</button>
          </div>
        </div>
      </aside>
    </div>
  </main>

  <script>
    const STORAGE_KEY = "hxtend_remote_session_v3";
    const PANEL_FPS = 12;
    const SNAPSHOT_INTERVAL_MS = Math.max(70, Math.round(1000 / PANEL_FPS));

    const feeds = {
      "8001": { online: false, seq: -1, inFlight: false, objectUrl: "", lastPull: 0 },
      "8002": { online: false, seq: -1, inFlight: false, objectUrl: "", lastPull: 0 }
    };

    let session = {
      deviceId: "procesadora-01",
      token: ""
    };

    let stateTimer = null;
    let snapshotTimer = null;

    function $(id) {
      return document.getElementById(id);
    }

    function authHeaders() {
      return {
        "Authorization": "Bearer " + session.token,
        "Content-Type": "application/json"
      };
    }

    function loadStoredSession() {
      try {
        const raw = localStorage.getItem(STORAGE_KEY);
        if (raw) {
          session = Object.assign(session, JSON.parse(raw));
        } else {
          session.token = localStorage.getItem("hxtend_token") || "";
          session.deviceId = localStorage.getItem("hxtend_device") || session.deviceId;
        }
      } catch {
        session = { deviceId: "procesadora-01", token: "" };
      }

      $("deviceInput").value = session.deviceId || "procesadora-01";
      $("tokenInput").value = session.token || "";
    }

    function saveSession() {
      session.deviceId = $("deviceInput").value.trim() || "procesadora-01";
      session.token = $("tokenInput").value.trim();
      localStorage.setItem(STORAGE_KEY, JSON.stringify(session));
      localStorage.setItem("hxtend_token", session.token);
      localStorage.setItem("hxtend_device", session.deviceId);
    }

    function setLoginMessage(message, loading = false) {
      $("loginMessage").innerHTML = loading
        ? `<span class="loader"></span>${message}`
        : message;
    }

    function showPanel() {
      $("loginView").classList.add("hidden");
      $("panelView").classList.remove("hidden");
      $("deviceLabel").textContent = session.deviceId;
      startLoops();
    }

    function showLogin() {
      stopLoops();
      $("panelView").classList.add("hidden");
      $("loginView").classList.remove("hidden");
    }

    async function connectPanel() {
      saveSession();

      if (!session.token) {
        setLoginMessage("Ingresa el API_TOKEN de Render.");
        return;
      }

      $("connectButton").disabled = true;
      setLoginMessage("Comprobando conexión...", true);

      try {
        const response = await fetch(`/api/device/test/${encodeURIComponent(session.deviceId)}`, {
          cache: "no-store",
          headers: authHeaders()
        });

        const data = await response.json().catch(() => ({}));

        if (!response.ok) {
          throw new Error(data.detail || "Token inválido");
        }

        setLoginMessage(data.connected ? "Conectado." : "Token correcto. Procesadora en espera.");
        showPanel();
      } catch (error) {
        setLoginMessage(error.message || "No se pudo conectar.");
      } finally {
        $("connectButton").disabled = false;
      }
    }

    function logoutPanel() {
      localStorage.removeItem(STORAGE_KEY);
      showLogin();
    }

    function setProcessorOnline(online) {
      $("processorDot").classList.toggle("on", online);
      $("processorText").textContent = online ? "Procesadora online" : "Procesadora offline";
    }

    async function refreshState() {
      try {
        const response = await fetch("/api/stream/state", {
          cache: "no-store",
          headers: authHeaders()
        });

        if (!response.ok) {
          throw new Error("No se pudo leer el estado");
        }

        const data = await response.json();
        const processorOnline = !!data.processor_online;
        let anyStream = false;

        setProcessorOnline(processorOnline);

        for (const id of Object.keys(feeds)) {
          const info = data.feeds[id] || {};
          const online = !!info.online;
          feeds[id].online = online;
          feeds[id].seq = info.seq || feeds[id].seq;
          anyStream = anyStream || online;

          $("card" + id).classList.toggle("hidden", !online);
          $("feed" + id + "Info").textContent = online
            ? `${info.last_seen_seconds}s`
            : "offline";
        }

        $("previewGrid").classList.toggle("hidden", !anyStream);
        $("emptyStream").classList.toggle("hidden", anyStream);
        $("lastUpdate").textContent = "Actualizado " + new Date().toLocaleTimeString();
      } catch {
        setProcessorOnline(false);
        $("controlStatus").textContent = "Sin estado";
      }
    }

    async function pullSnapshot(id) {
      const feed = feeds[id];

      if (!feed.online || feed.inFlight) {
        return;
      }

      feed.inFlight = true;

      try {
        const url = `/api/stream/snapshot/${id}?t=${Date.now()}`;
        const response = await fetch(url, {
          cache: "no-store",
          headers: authHeaders()
        });

        if (response.status === 204) {
          return;
        }

        if (!response.ok) {
          throw new Error("snapshot failed");
        }

        const blob = await response.blob();
        const objectUrl = URL.createObjectURL(blob);
        const image = $("feed" + id);
        const previous = feed.objectUrl;

        image.src = objectUrl;
        feed.objectUrl = objectUrl;

        if (previous) {
          setTimeout(() => URL.revokeObjectURL(previous), 500);
        }
      } catch {
        feed.online = false;
      } finally {
        feed.inFlight = false;
      }
    }

    function pullSnapshots() {
      const now = performance.now();

      for (const id of Object.keys(feeds)) {
        if (now - feeds[id].lastPull >= SNAPSHOT_INTERVAL_MS) {
          feeds[id].lastPull = now;
          pullSnapshot(id);
        }
      }
    }

    async function sendAction(action) {
      $("controlStatus").textContent = "Enviando";
      $("panelLog").textContent = "Enviando " + action + "...";

      try {
        const response = await fetch("/api/command", {
          method: "POST",
          headers: authHeaders(),
          body: JSON.stringify({
            device_id: session.deviceId,
            action
          })
        });

        const data = await response.json().catch(() => ({}));

        if (!response.ok) {
          throw new Error(data.detail || "No se pudo enviar el comando");
        }

        $("controlStatus").textContent = "Listo";
        $("panelLog").textContent = `${action} enviado. ID: ${data.command.id}`;
      } catch (error) {
        $("controlStatus").textContent = "Error";
        $("panelLog").textContent = error.message || "Error enviando comando";
      }
    }

    function startLoops() {
      stopLoops();
      refreshState();
      stateTimer = setInterval(refreshState, 900);
      snapshotTimer = setInterval(pullSnapshots, 70);
    }

    function stopLoops() {
      if (stateTimer) clearInterval(stateTimer);
      if (snapshotTimer) clearInterval(snapshotTimer);
      stateTimer = null;
      snapshotTimer = null;

      for (const feed of Object.values(feeds)) {
        if (feed.objectUrl) {
          URL.revokeObjectURL(feed.objectUrl);
          feed.objectUrl = "";
        }
        feed.inFlight = false;
      }
    }

    loadStoredSession();

    if (session.token) {
      connectPanel();
    }
  </script>
</body>
</html>
"""


@app.get(
    "/panel",
    response_class=HTMLResponse,
)
def panel():
    return PANEL_HTML
