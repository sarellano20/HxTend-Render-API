#!/usr/bin/env bash
set -Eeuo pipefail

SERVICE_NAME="hxtend-render-stream-agent.service"
INSTALL_DIR="${HXTEND_RELAY_INSTALL_DIR:-/opt/hxtend-render-relay}"
ENV_FILE="${HXTEND_RELAY_ENV_FILE:-/etc/hxtend-render-relay.env}"
AGENT_FILE="$INSTALL_DIR/hxtend_stream_agent.py"
VENV_DIR="$INSTALL_DIR/.venv"

RENDER_URL="${HXTEND_RENDER_URL:-https://hxtend-controller.onrender.com}"
DEVICE_ID="${HXTEND_DEVICE_ID:-procesadora-01}"
FEED1_URL="${HXTEND_FEED1_URL:-http://127.0.0.1:8001/feed}"
FEED2_URL="${HXTEND_FEED2_URL:-http://127.0.0.1:8002/feed}"
PREVIEW_FPS="${HXTEND_REMOTE_PREVIEW_FPS:-8}"
STREAM_TOKEN="${HXTEND_STREAM_TOKEN:-}"

usage() {
  cat <<USAGE
HxTend Render relay installer for the Linux box.

Usage:
  sudo bash INSTALL_CAJA_AUTOSTART_HXTEND_RENDER.sh --token TOKEN [options]

Options:
  --token TOKEN          Token configured in Render as HXTEND_STREAM_TOKEN.
  --render-url URL      Render server URL. Default: $RENDER_URL
  --device-id ID        Device id shown in the panel. Default: $DEVICE_ID
  --feed1-url URL       Local feed 1 URL. Default: $FEED1_URL
  --feed2-url URL       Local feed 2 URL. Default: $FEED2_URL
  --fps FPS             Remote preview FPS. Default: $PREVIEW_FPS
  --help                Show this help.

Example:
  sudo bash INSTALL_CAJA_AUTOSTART_HXTEND_RENDER.sh \\
    --token 'CHANGE_ME_LONG_TOKEN' \\
    --device-id 'procesadora-01' \\
    --render-url 'https://hxtend-controller.onrender.com' \\
    --feed1-url 'http://127.0.0.1:8001/feed' \\
    --feed2-url 'http://127.0.0.1:8002/feed'
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --token)
      STREAM_TOKEN="${2:-}"
      shift 2
      ;;
    --render-url)
      RENDER_URL="${2:-}"
      shift 2
      ;;
    --device-id)
      DEVICE_ID="${2:-}"
      shift 2
      ;;
    --feed1-url)
      FEED1_URL="${2:-}"
      shift 2
      ;;
    --feed2-url)
      FEED2_URL="${2:-}"
      shift 2
      ;;
    --fps)
      PREVIEW_FPS="${2:-}"
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "[ERROR] Unknown option: $1" >&2
      usage
      exit 2
      ;;
  esac
done

if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
  echo "[ERROR] Run this installer with sudo." >&2
  echo "Example: sudo bash $0 --token 'TOKEN_DE_RENDER'" >&2
  exit 1
fi

if [[ -z "$STREAM_TOKEN" && -t 0 ]]; then
  read -r -s -p "Enter HXTEND_STREAM_TOKEN from Render: " STREAM_TOKEN
  echo
fi

if [[ -z "$STREAM_TOKEN" ]]; then
  echo "[ERROR] Missing token. Pass --token or set HXTEND_STREAM_TOKEN." >&2
  echo "Example: sudo bash $0 --token 'TOKEN_DE_RENDER'" >&2
  exit 1
fi

if ! command -v systemctl >/dev/null 2>&1; then
  echo "[ERROR] This installer needs systemd/systemctl." >&2
  exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
  if command -v apt-get >/dev/null 2>&1; then
    apt-get update
    apt-get install -y python3 python3-venv python3-pip ca-certificates
  else
    echo "[ERROR] python3 is not installed. Install python3 and run again." >&2
    exit 1
  fi
fi

mkdir -p "$INSTALL_DIR"
chmod 755 "$INSTALL_DIR"

cat > "$AGENT_FILE" <<'PY'
#!/usr/bin/env python3
from __future__ import annotations

import os
import signal
import sys
import threading
import time
from typing import Optional
from urllib.parse import quote

import requests


SERVER_URL = os.getenv("HXTEND_RENDER_URL", "https://hxtend-controller.onrender.com").rstrip("/")
DEVICE_ID = os.getenv("HXTEND_DEVICE_ID", "procesadora-01")
TOKEN = os.getenv("HXTEND_STREAM_TOKEN", "")
PREVIEW_FPS = max(1.0, float(os.getenv("HXTEND_REMOTE_PREVIEW_FPS", "8")))
STATUS_EVERY_SECONDS = max(3.0, float(os.getenv("HXTEND_STATUS_EVERY_SECONDS", "5")))
RECONNECT_SECONDS = max(1.0, float(os.getenv("HXTEND_RECONNECT_SECONDS", "2")))
MAX_FRAME_BYTES = int(os.getenv("HXTEND_MAX_FRAME_BYTES", str(2_500_000)))
REQUEST_TIMEOUT = (
    float(os.getenv("HXTEND_CONNECT_TIMEOUT_SECONDS", "4")),
    float(os.getenv("HXTEND_READ_TIMEOUT_SECONDS", "12")),
)

FEEDS = [
    ("8001", os.getenv("HXTEND_FEED1_URL", "http://127.0.0.1:8001/feed")),
    ("8002", os.getenv("HXTEND_FEED2_URL", "http://127.0.0.1:8002/feed")),
]

STOP = threading.Event()
SESSION = requests.Session()


def headers(content_type: Optional[str] = None) -> dict[str, str]:
    value = {
        "User-Agent": "hxtend-render-relay/1.0",
    }
    if TOKEN:
        value["Authorization"] = f"Bearer {TOKEN}"
    if content_type:
        value["Content-Type"] = content_type
    return value


def post_json(path: str, payload: dict) -> bool:
    try:
        response = SESSION.post(
            f"{SERVER_URL}{path}",
            json=payload,
            headers=headers(),
            timeout=REQUEST_TIMEOUT,
        )
        if response.status_code >= 400:
            print(f"[WARN] POST {path} -> {response.status_code}: {response.text[:160]}", flush=True)
            return False
        return True
    except Exception as exc:
        print(f"[WARN] POST {path} failed: {exc}", flush=True)
        return False


def push_status(feed_id: str, online: bool, source_url: str, error: str = "") -> None:
    post_json(
        "/api/stream/status",
        {
            "device_id": DEVICE_ID,
            "feed_id": feed_id,
            "online": online,
            "source_url": source_url,
            "error": error[:240],
        },
    )


def push_heartbeat() -> None:
    post_json(
        "/api/device/heartbeat",
        {
            "device_id": DEVICE_ID,
            "online": True,
            "updated_at": time.time(),
        },
    )


def push_frame(feed_id: str, frame: bytes) -> bool:
    if len(frame) < 128 or len(frame) > MAX_FRAME_BYTES:
        return False

    endpoint = f"/api/stream/frame/{quote(feed_id)}?device_id={quote(DEVICE_ID)}"
    try:
        response = SESSION.post(
            f"{SERVER_URL}{endpoint}",
            data=frame,
            headers=headers("image/jpeg"),
            timeout=REQUEST_TIMEOUT,
        )
        if response.status_code >= 400:
            print(f"[WARN] frame {feed_id} -> {response.status_code}: {response.text[:160]}", flush=True)
            return False
        return True
    except Exception as exc:
        print(f"[WARN] frame {feed_id} failed: {exc}", flush=True)
        return False


def frames_from_mjpeg(response: requests.Response):
    buffer = bytearray()
    for chunk in response.iter_content(chunk_size=65536):
        if STOP.is_set():
            return
        if not chunk:
            continue
        buffer.extend(chunk)

        if len(buffer) > MAX_FRAME_BYTES * 3:
            marker = buffer.rfind(b"\xff\xd8")
            if marker > 0:
                del buffer[:marker]
            else:
                del buffer[:-2]

        while True:
            start = buffer.find(b"\xff\xd8")
            if start < 0:
                if len(buffer) > 2:
                    del buffer[:-2]
                break

            end = buffer.find(b"\xff\xd9", start + 2)
            if end < 0:
                if start > 0:
                    del buffer[:start]
                break

            frame = bytes(buffer[start : end + 2])
            del buffer[: end + 2]
            yield frame


def feed_worker(feed_id: str, source_url: str) -> None:
    interval = 1.0 / PREVIEW_FPS
    next_push = 0.0
    last_status = 0.0
    online = False

    while not STOP.is_set():
        try:
            with SESSION.get(
                source_url,
                stream=True,
                timeout=REQUEST_TIMEOUT,
                headers={"User-Agent": "hxtend-render-relay/1.0"},
            ) as response:
                response.raise_for_status()

                for frame in frames_from_mjpeg(response):
                    if STOP.is_set():
                        break

                    now = time.monotonic()
                    if now < next_push:
                        continue

                    if push_frame(feed_id, frame):
                        next_push = now + interval
                        if not online or now - last_status >= STATUS_EVERY_SECONDS:
                            push_status(feed_id, True, source_url)
                            online = True
                            last_status = now

        except Exception as exc:
            message = str(exc)
            if online:
                print(f"[INFO] feed {feed_id} offline: {message}", flush=True)
            else:
                print(f"[INFO] waiting for feed {feed_id}: {message}", flush=True)
            push_status(feed_id, False, source_url, message)
            online = False

        STOP.wait(RECONNECT_SECONDS)


def heartbeat_worker() -> None:
    while not STOP.is_set():
        push_heartbeat()
        STOP.wait(STATUS_EVERY_SECONDS)


def handle_stop(signum, frame) -> None:
    STOP.set()


def main() -> int:
    if not TOKEN:
        print("[ERROR] HXTEND_STREAM_TOKEN is required.", file=sys.stderr)
        return 2

    signal.signal(signal.SIGTERM, handle_stop)
    signal.signal(signal.SIGINT, handle_stop)

    print(f"[INFO] HxTend Render relay started for device {DEVICE_ID}", flush=True)
    print(f"[INFO] Server: {SERVER_URL}", flush=True)
    for feed_id, source_url in FEEDS:
        print(f"[INFO] feed {feed_id}: {source_url}", flush=True)

    threads = [threading.Thread(target=heartbeat_worker, daemon=True)]
    threads.extend(
        threading.Thread(target=feed_worker, args=(feed_id, source_url), daemon=True)
        for feed_id, source_url in FEEDS
    )

    for thread in threads:
        thread.start()

    while not STOP.is_set():
        time.sleep(0.5)

    for feed_id, source_url in FEEDS:
        push_status(feed_id, False, source_url, "agent stopped")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
PY

chmod 755 "$AGENT_FILE"

if ! python3 -m venv "$VENV_DIR" >/dev/null 2>&1; then
  if command -v apt-get >/dev/null 2>&1; then
    apt-get update
    apt-get install -y python3-venv python3-pip ca-certificates
    python3 -m venv "$VENV_DIR"
  else
    echo "[ERROR] Could not create Python venv. Install python3-venv and run again." >&2
    exit 1
  fi
fi

"$VENV_DIR/bin/python" -m pip install --upgrade pip setuptools wheel
"$VENV_DIR/bin/python" -m pip install --upgrade requests

cat > "$ENV_FILE" <<ENV
HXTEND_RENDER_URL=$RENDER_URL
HXTEND_DEVICE_ID=$DEVICE_ID
HXTEND_STREAM_TOKEN=$STREAM_TOKEN
HXTEND_REMOTE_PREVIEW_FPS=$PREVIEW_FPS
HXTEND_FEED1_URL=$FEED1_URL
HXTEND_FEED2_URL=$FEED2_URL
PYTHONDONTWRITEBYTECODE=1
PYTHONUNBUFFERED=1
ENV

chmod 600 "$ENV_FILE"

cat > "/etc/systemd/system/$SERVICE_NAME" <<SERVICE
[Unit]
Description=HxTend Render remote preview relay
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
EnvironmentFile=$ENV_FILE
WorkingDirectory=$INSTALL_DIR
ExecStartPre=/bin/sleep 6
ExecStart=$VENV_DIR/bin/python $AGENT_FILE
Restart=always
RestartSec=2
StartLimitIntervalSec=0
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=full

[Install]
WantedBy=multi-user.target
SERVICE

systemctl daemon-reload
systemctl enable "$SERVICE_NAME"
systemctl restart "$SERVICE_NAME"

echo
echo "[OK] HxTend Render relay installed."
echo "[OK] Service: $SERVICE_NAME"
echo "[OK] It will start automatically after every reboot."
echo "[OK] It only manages this relay service; existing HxTend services were not modified."
echo
echo "Check status:"
echo "  sudo systemctl status $SERVICE_NAME"
echo
echo "Live logs:"
echo "  sudo journalctl -u $SERVICE_NAME -f"
echo
echo "Panel:"
echo "  $RENDER_URL/panel"
