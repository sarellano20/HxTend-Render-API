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