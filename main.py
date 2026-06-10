import os
import time
import secrets
from collections import defaultdict
from threading import Lock
from typing import Optional

from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field


# ============================================================
# VARIABLES DE ENTORNO DE RENDER
# ============================================================

API_TOKEN = os.getenv("API_TOKEN", "")
DEVICE_TOKEN = os.getenv("DEVICE_TOKEN", "")
DEFAULT_DEVICE_ID = os.getenv("DEVICE_ID", "procesadora-01")

COMMAND_TIMEOUT_SECONDS = int(
    os.getenv("COMMAND_TIMEOUT_SECONDS", "30")
)


# ============================================================
# ACCIONES PERMITIDAS
# ============================================================

ALLOWED_ACTIONS = {
    "led",
    "led_plus",
    "in_out",
    "frame",
    "led_minus",
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
    title="HxTend Remote Controller",
    version="1.0.0",
    description="API para controlar un Pico W desde Internet"
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


# ============================================================
# MODELOS
# ============================================================

class CommandCreate(BaseModel):
    device_id: str = Field(
        default=DEFAULT_DEVICE_ID,
        min_length=1,
        max_length=80
    )
    action: str = Field(
        min_length=1,
        max_length=80
    )


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


def require_api_token(authorization):
    if not API_TOKEN:
        raise HTTPException(
            status_code=500,
            detail="API_TOKEN no está configurado en Render"
        )

    expected = "Bearer " + API_TOKEN

    if not authorization:
        raise HTTPException(
            status_code=401,
            detail="Authorization requerido"
        )

    if not secrets.compare_digest(authorization, expected):
        raise HTTPException(
            status_code=401,
            detail="Token de control inválido"
        )


def require_device_token(token):
    if not DEVICE_TOKEN:
        raise HTTPException(
            status_code=500,
            detail="DEVICE_TOKEN no está configurado en Render"
        )

    if not token:
        raise HTTPException(
            status_code=401,
            detail="Token del dispositivo requerido"
        )

    if not secrets.compare_digest(token, DEVICE_TOKEN):
        raise HTTPException(
            status_code=401,
            detail="Token del dispositivo inválido"
        )


def recover_stale_commands_locked(device_id):
    recovered = 0
    now = current_timestamp()

    queue = commands_by_device[device_id]

    for command in queue:
        if (
            command["status"] == "delivered"
            and command["delivered_at"] is not None
            and now - command["delivered_at"]
            >= COMMAND_TIMEOUT_SECONDS
        ):
            command["status"] = "pending"
            command["delivered_at"] = None
            recovered += 1

    return recovered


# ============================================================
# RUTAS PÚBLICAS
# ============================================================

@app.get("/")
def root():
    return {
        "ok": True,
        "service": "HxTend Remote Controller",
        "version": "1.0.0",
        "device_id": DEFAULT_DEVICE_ID,
        "panel": "/panel",
        "docs": "/docs"
    }


@app.get("/health")
def health():
    return {
        "ok": True,
        "server_time": current_timestamp()
    }


# ============================================================
# RUTAS PARA PANEL, UNITY O CELULAR
# ============================================================

@app.get("/api/actions")
def get_actions(
    authorization: Optional[str] = Header(default=None)
):
    require_api_token(authorization)

    return {
        "ok": True,
        "actions": sorted(ALLOWED_ACTIONS)
    }


@app.post("/api/command")
def create_command(
    request: CommandCreate,
    authorization: Optional[str] = Header(default=None)
):
    require_api_token(authorization)

    device_id = request.device_id.strip()
    action = request.action.strip().lower()

    if not device_id:
        raise HTTPException(
            status_code=400,
            detail="device_id obligatorio"
        )

    if action not in ALLOWED_ACTIONS:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "Acción inválida",
                "allowed_actions": sorted(ALLOWED_ACTIONS)
            }
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
        "result": None
    }

    with lock:
        commands_by_device[device_id].append(command)
        command_history[command_id] = command

    return {
        "ok": True,
        "message": "Comando agregado",
        "command": command
    }


@app.get("/api/command/{command_id}")
def get_command_status(
    command_id: str,
    authorization: Optional[str] = Header(default=None)
):
    require_api_token(authorization)

    with lock:
        command = command_history.get(command_id)

        if not command:
            raise HTTPException(
                status_code=404,
                detail="Comando no encontrado"
            )

        return {
            "ok": True,
            "command": dict(command)
        }


@app.get("/api/device/{device_id}/status")
def get_device_status(
    device_id: str,
    authorization: Optional[str] = Header(default=None)
):
    require_api_token(authorization)

    with lock:
        status = device_statuses.get(device_id)

    if not status:
        return {
            "ok": True,
            "online": False,
            "device_id": device_id,
            "message": "Todavía no se recibió heartbeat"
        }

    seconds_ago = max(
        0,
        current_timestamp() - status["last_seen"]
    )

    return {
        "ok": True,
        "online": seconds_ago <= 30,
        "seconds_ago": seconds_ago,
        **status
    }


# ============================================================
# RUTAS USADAS POR EL PICO W
# ============================================================

@app.get("/api/device/next")
def get_next_command(
    device_id: str = Query(..., min_length=1),
    token: str = Query(..., min_length=1)
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
                        "action": command["action"]
                    }
                }

    return {
        "ok": True,
        "has_command": False
    }


@app.post("/api/device/result")
def save_command_result(
    result: CommandResult,
    token: str = Query(..., min_length=1)
):
    require_device_token(token)

    with lock:
        command = command_history.get(result.command_id)

        if not command:
            raise HTTPException(
                status_code=404,
                detail="Comando no encontrado"
            )

        if command["device_id"] != result.device_id:
            raise HTTPException(
                status_code=400,
                detail="El comando no pertenece a este dispositivo"
            )

        command["status"] = (
            "completed"
            if result.ok
            else "failed"
        )

        command["completed_at"] = current_timestamp()

        command["result"] = {
            "ok": result.ok,
            "message": result.message
        }

    return {
        "ok": True,
        "message": "Resultado registrado"
    }


@app.post("/api/device/heartbeat")
def heartbeat(
    data: Heartbeat,
    token: str = Query(..., min_length=1)
):
    require_device_token(token)

    status = data.model_dump()
    status["last_seen"] = current_timestamp()

    with lock:
        device_statuses[data.device_id] = status

    return {
        "ok": True,
        "server_time": current_timestamp()
    }


@app.post("/api/device/recover")
def recover_commands(
    device_id: str = Query(..., min_length=1),
    token: str = Query(..., min_length=1)
):
    require_device_token(token)

    with lock:
        recovered = recover_stale_commands_locked(
            device_id
        )

    return {
        "ok": True,
        "recovered": recovered
    }


# ============================================================
# PANEL WEB
# ============================================================

PANEL_HTML = """
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">

    <meta
        name="viewport"
        content="width=device-width, initial-scale=1"
    >

    <title>HxTend Remote Control</title>

    <style>
        body {
            font-family: Arial, sans-serif;
            background: #0b1220;
            color: white;
            margin: 0;
            padding: 20px;
        }

        main {
            max-width: 850px;
            margin: auto;
        }

        .card {
            background: #111b2e;
            border: 1px solid #26344f;
            border-radius: 16px;
            padding: 18px;
            margin-bottom: 16px;
        }

        input {
            width: 100%;
            box-sizing: border-box;
            padding: 12px;
            margin: 6px 0 14px 0;
            border-radius: 9px;
            border: 1px solid #40506e;
        }

        .grid {
            display: grid;
            grid-template-columns:
                repeat(auto-fit, minmax(150px, 1fr));
            gap: 10px;
        }

        button {
            padding: 13px;
            border: 0;
            border-radius: 10px;
            background: #2563eb;
            color: white;
            font-weight: bold;
            cursor: pointer;
        }

        button.secondary {
            background: #475569;
        }

        button.danger {
            background: #dc2626;
        }

        pre {
            white-space: pre-wrap;
            background: #050a13;
            padding: 14px;
            border-radius: 10px;
            min-height: 70px;
        }

        small {
            color: #a8b3c7;
        }
    </style>
</head>

<body>
<main>

    <h1>HxTend Remote Control</h1>

    <div class="card">
        <label>Device ID</label>

        <input
            id="device"
            value="procesadora-01"
        >

        <label>API token</label>

        <input
            id="token"
            type="password"
            placeholder="API_TOKEN configurado en Render"
        >

        <small>
            El token se guarda únicamente en este navegador.
        </small>
    </div>

    <div class="card">
        <h2>Pulsos</h2>

        <div class="grid">
            <button onclick="sendAction('led')">
                LED
            </button>

            <button onclick="sendAction('led_plus')">
                LED +
            </button>

            <button onclick="sendAction('in_out')">
                IN / OUT
            </button>

            <button onclick="sendAction('frame')">
                FRAME
            </button>

            <button onclick="sendAction('led_minus')">
                LED -
            </button>

            <button onclick="sendAction('white_balance')">
                WHITE BALANCE
            </button>
        </div>
    </div>

    <div class="card">
        <h2>Power ON</h2>

        <div class="grid">
            <button
                onclick="sendAction('power_on_press')"
            >
                Presionar ON
            </button>

            <button
                class="secondary"
                onclick="sendAction('power_on_release')"
            >
                Liberar ON
            </button>

            <button
                onclick="sendAction('power_on_toggle')"
            >
                Toggle ON
            </button>
        </div>
    </div>

    <div class="card">
        <h2>Power OFF</h2>

        <div class="grid">
            <button
                class="danger"
                onclick="sendAction('power_off_press')"
            >
                Presionar OFF
            </button>

            <button
                class="secondary"
                onclick="sendAction('power_off_release')"
            >
                Liberar OFF
            </button>

            <button
                class="danger"
                onclick="sendAction('power_off_toggle')"
            >
                Toggle OFF
            </button>
        </div>
    </div>

    <div class="card">
        <div class="grid">
            <button onclick="getStatus()">
                Actualizar estado
            </button>

            <button
                class="secondary"
                onclick="saveSettings()"
            >
                Guardar datos
            </button>
        </div>

        <pre id="output">Listo.</pre>
    </div>

</main>

<script>
    const tokenElement =
        document.getElementById("token");

    const deviceElement =
        document.getElementById("device");

    const outputElement =
        document.getElementById("output");

    tokenElement.value =
        localStorage.getItem("hxtend_token") || "";

    deviceElement.value =
        localStorage.getItem("hxtend_device")
        || "procesadora-01";


    function saveSettings() {
        localStorage.setItem(
            "hxtend_token",
            tokenElement.value
        );

        localStorage.setItem(
            "hxtend_device",
            deviceElement.value
        );

        outputElement.textContent =
            "Datos guardados en este navegador.";
    }


    async function apiRequest(path, options = {}) {
        saveSettings();

        options.headers = Object.assign(
            {
                "Authorization":
                    "Bearer " + tokenElement.value,

                "Content-Type":
                    "application/json"
            },
            options.headers || {}
        );

        const response = await fetch(
            path,
            options
        );

        const text = await response.text();

        let data;

        try {
            data = JSON.parse(text);
        } catch {
            data = {
                raw: text
            };
        }

        if (!response.ok) {
            throw new Error(
                JSON.stringify(data, null, 2)
            );
        }

        return data;
    }


    async function sendAction(action) {
        outputElement.textContent =
            "Enviando " + action + "...";

        try {
            const data = await apiRequest(
                "/api/command",
                {
                    method: "POST",

                    body: JSON.stringify({
                        device_id:
                            deviceElement.value,

                        action: action
                    })
                }
            );

            outputElement.textContent =
                JSON.stringify(data, null, 2);

        } catch (error) {
            outputElement.textContent =
                "ERROR\\n" + error.message;
        }
    }


    async function getStatus() {
        outputElement.textContent =
            "Consultando estado...";

        try {
            const path =
                "/api/device/"
                + encodeURIComponent(
                    deviceElement.value
                )
                + "/status";

            const data = await apiRequest(path);

            outputElement.textContent =
                JSON.stringify(data, null, 2);

        } catch (error) {
            outputElement.textContent =
                "ERROR\\n" + error.message;
        }
    }
</script>

</body>
</html>
"""


@app.get(
    "/panel",
    response_class=HTMLResponse
)
def panel():
    return PANEL_HTML
import fastapi as _hxtend_fastapi_module

_HXTEND_DEFERRED_ROUTES = []
_HXTEND_ORIGINAL_FASTAPI = _hxtend_fastapi_module.FastAPI


class _HxtendDeferredApp:
    def _route(self, method, path, **kwargs):
        def decorator(fn):
            _HXTEND_DEFERRED_ROUTES.append((method, path, kwargs, fn))
            return fn
        return decorator

    def get(self, path, **kwargs):
        return self._route("get", path, **kwargs)

    def post(self, path, **kwargs):
        return self._route("post", path, **kwargs)


def _hxtend_install_deferred_routes(real_app):
    for method, path, kwargs, fn in _HXTEND_DEFERRED_ROUTES:
        getattr(real_app, method)(path, **kwargs)(fn)


class _HxtendFastAPI(_HXTEND_ORIGINAL_FASTAPI):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        _hxtend_install_deferred_routes(self)


_hxtend_fastapi_module.FastAPI = _HxtendFastAPI
app = _HxtendDeferredApp()

#
# ---------------------------------------------------------------------------
# Remote stream preview relay
# ---------------------------------------------------------------------------
# Render cannot access private LAN URLs such as 192.168.8.221:8001/feed.
# The processor runs hxtend_stream_agent.py, reads the local feeds, and pushes
# the latest JPEG frames here over HTTPS. The public panel then previews those
# frames from Render.

import asyncio as _hxtend_asyncio
import os as _hxtend_os
import time as _hxtend_time
from dataclasses import dataclass as _hxtend_dataclass, field as _hxtend_field
from typing import Dict as _HxtendDict, Optional as _HxtendOptional

from fastapi import Header as _HxtendHeader, HTTPException as _HxtendHTTPException, Request as _HxtendRequest
from fastapi.responses import HTMLResponse as _HxtendHTMLResponse
from fastapi.responses import JSONResponse as _HxtendJSONResponse
from fastapi.responses import Response as _HxtendResponse
from fastapi.responses import StreamingResponse as _HxtendStreamingResponse


_HXTEND_STREAM_TOKEN = (
    _hxtend_os.getenv("HXTEND_STREAM_TOKEN")
    or _hxtend_os.getenv("API_TOKEN")
    or _hxtend_os.getenv("PANEL_TOKEN")
    or ""
)
_HXTEND_STREAM_ONLINE_SECONDS = float(_hxtend_os.getenv("HXTEND_STREAM_ONLINE_SECONDS", "12"))


@_hxtend_dataclass
class _HxtendFeedState:
    frame: bytes = b""
    updated_at: float = 0.0
    online: bool = False
    source_url: str = ""
    error: str = ""
    condition: _hxtend_asyncio.Condition = _hxtend_field(default_factory=_hxtend_asyncio.Condition)


_HXTEND_STREAMS: _HxtendDict[str, _HxtendFeedState] = {
    "8001": _HxtendFeedState(),
    "8002": _HxtendFeedState(),
}


def _hxtend_feed(feed_id: str) -> _HxtendFeedState:
    feed_id = str(feed_id)
    if feed_id not in _HXTEND_STREAMS:
        _HXTEND_STREAMS[feed_id] = _HxtendFeedState()
    return _HXTEND_STREAMS[feed_id]


def _hxtend_authorize(authorization: _HxtendOptional[str]) -> None:
    if not _HXTEND_STREAM_TOKEN:
        return
    expected = f"Bearer {_HXTEND_STREAM_TOKEN}"
    if authorization != expected and authorization != _HXTEND_STREAM_TOKEN:
        raise _HxtendHTTPException(status_code=401, detail="Invalid stream token")


def _hxtend_is_online(feed: _HxtendFeedState) -> bool:
    return bool(feed.online and feed.frame and (_hxtend_time.time() - feed.updated_at) <= _HXTEND_STREAM_ONLINE_SECONDS)


def _hxtend_stream_state_payload():
    now = _hxtend_time.time()
    feeds = {}
    any_online = False
    for feed_id, feed in _HXTEND_STREAMS.items():
        online = bool(feed.online and feed.frame and (now - feed.updated_at) <= _HXTEND_STREAM_ONLINE_SECONDS)
        any_online = any_online or online
        feeds[feed_id] = {
            "online": online,
            "last_seen_seconds": None if feed.updated_at <= 0 else round(now - feed.updated_at, 2),
            "source_url": feed.source_url,
            "has_frame": bool(feed.frame),
            "bytes": len(feed.frame),
            "error": feed.error,
        }
    return {"ok": True, "processor_online": any_online, "feeds": feeds}


def _hxtend_remove_existing_route(path: str) -> None:
    try:
        app.router.routes = [route for route in app.router.routes if getattr(route, "path", None) != path]
    except Exception:
        pass


@app.post("/api/stream/status")
async def hxtend_stream_status(
    request: _HxtendRequest,
    authorization: _HxtendOptional[str] = _HxtendHeader(default=None),
):
    _hxtend_authorize(authorization)
    payload = await request.json()
    feed_id = str(payload.get("feed_id") or payload.get("port") or "8001")
    feed = _hxtend_feed(feed_id)
    feed.online = bool(payload.get("online", True))
    feed.source_url = str(payload.get("source_url") or feed.source_url or "")
    feed.error = str(payload.get("error") or "")
    feed.updated_at = _hxtend_time.time()
    return {"ok": True, "feed_id": feed_id}


@app.post("/api/stream/frame/{feed_id}")
async def hxtend_stream_frame(
    feed_id: str,
    request: _HxtendRequest,
    authorization: _HxtendOptional[str] = _HxtendHeader(default=None),
):
    _hxtend_authorize(authorization)
    frame = await request.body()
    if not frame.startswith(b"\xff\xd8") or not frame.endswith(b"\xff\xd9"):
        raise _HxtendHTTPException(status_code=400, detail="Expected a JPEG frame")
    if len(frame) > 2_500_000:
        raise _HxtendHTTPException(status_code=413, detail="Frame too large")

    feed = _hxtend_feed(feed_id)
    feed.frame = frame
    feed.online = True
    feed.error = ""
    feed.updated_at = _hxtend_time.time()
    async with feed.condition:
        feed.condition.notify_all()
    return {"ok": True, "feed_id": feed_id, "bytes": len(frame)}


@app.get("/api/stream/state")
async def hxtend_stream_state():
    return _hxtend_stream_state_payload()


@app.get("/api/stream/snapshot/{feed_id}")
async def hxtend_stream_snapshot(feed_id: str):
    feed = _hxtend_feed(feed_id)
    if not _hxtend_is_online(feed):
        return _HxtendResponse(status_code=204)
    return _HxtendResponse(
        content=feed.frame,
        media_type="image/jpeg",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
        },
    )


@app.get("/api/stream/mjpeg/{feed_id}")
async def hxtend_stream_mjpeg(feed_id: str):
    feed = _hxtend_feed(feed_id)
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
                    await _hxtend_asyncio.wait_for(feed.condition.wait(), timeout=2.5)
                except _hxtend_asyncio.TimeoutError:
                    pass

    return _HxtendStreamingResponse(
        generate(),
        media_type=f"multipart/x-mixed-replace; boundary={boundary}",
        headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"},
    )


_hxtend_remove_existing_route("/panel")


@app.get("/panel", response_class=_HxtendHTMLResponse)
async def hxtend_remote_control_panel():
    return _HxtendHTMLResponse(
        """
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
        <button onclick="sendCommand('LED')">LED</button>
        <button onclick="sendCommand('LED_PLUS')">LED +</button>
        <button onclick="sendCommand('IN_OUT')">IN / OUT</button>
        <button onclick="sendCommand('FRAME')">FRAME</button>
        <button onclick="sendCommand('LED_MINUS')">LED -</button>
        <button onclick="sendCommand('WHITE_BALANCE')">WHITE BALANCE</button>
        <button onclick="sendCommand('POWER_ON_PRESS')">Presionar ON</button>
        <button onclick="sendCommand('POWER_ON_RELEASE')">Liberar ON</button>
        <button onclick="sendCommand('POWER_ON_TOGGLE')">Toggle ON</button>
        <button onclick="sendCommand('POWER_OFF_PRESS')">Presionar OFF</button>
        <button onclick="sendCommand('POWER_OFF_RELEASE')">Liberar OFF</button>
        <button onclick="sendCommand('POWER_OFF_TOGGLE')">Toggle OFF</button>
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
      const body = JSON.stringify({ command, device_id:'procesadora-01' });
      const headers = { 'Content-Type':'application/json' };
      if (token) headers.Authorization = `Bearer ${token}`;
      const endpoints = ['/api/command','/command','/api/control','/control'];
      for (const endpoint of endpoints) {
        try {
          const res = await fetch(endpoint, { method:'POST', headers, body });
          if (res.ok) { log(`${command} enviado`); return; }
        } catch (_) {}
      }
      log(`${command}: no encontré endpoint de control compatible`);
    }
    refreshState();
    setInterval(refreshState, 2000);
  </script>
</body>
</html>
        """
    )
