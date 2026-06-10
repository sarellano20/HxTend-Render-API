#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${HXTEND_AGENT_DIR:-/home/hxtend/Hxtend/render-relay}"
SERVICE_NAME="hxtend-render-stream-agent.service"
RENDER_URL="${HXTEND_RENDER_URL:-https://hxtend-controller.onrender.com}"
DEVICE_ID="${HXTEND_DEVICE_ID:-procesadora-01}"
FEED1_URL="${HXTEND_FEED1_URL:-http://127.0.0.1:8001/feed}"
FEED2_URL="${HXTEND_FEED2_URL:-http://127.0.0.1:8002/feed}"
PREVIEW_FPS="${HXTEND_REMOTE_PREVIEW_FPS:-8}"

if [ "$(id -u)" -ne 0 ]; then
  echo "Ejecuta con sudo:"
  echo "sudo HXTEND_STREAM_TOKEN=TU_TOKEN ./install_hxtend_stream_agent.sh"
  exit 1
fi

if [ -z "${HXTEND_STREAM_TOKEN:-}" ]; then
  echo "Falta HXTEND_STREAM_TOKEN. Debe ser el mismo token configurado en Render."
  exit 1
fi

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
mkdir -p "$APP_DIR"
install -m 755 "$BASE_DIR/hxtend_stream_agent.py" "$APP_DIR/hxtend_stream_agent.py"

python3 -m venv "$APP_DIR/venv"
"$APP_DIR/venv/bin/python" -m pip install --upgrade pip
"$APP_DIR/venv/bin/python" -m pip install requests

cat > "/etc/systemd/system/$SERVICE_NAME" <<SERVICE
[Unit]
Description=HxTend Render remote preview stream agent
After=network-online.target hxtend-stream.service
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=$APP_DIR
ExecStart=$APP_DIR/venv/bin/python $APP_DIR/hxtend_stream_agent.py
Restart=always
RestartSec=2
Environment=HXTEND_RENDER_URL=$RENDER_URL
Environment=HXTEND_DEVICE_ID=$DEVICE_ID
Environment=HXTEND_STREAM_TOKEN=$HXTEND_STREAM_TOKEN
Environment=HXTEND_REMOTE_PREVIEW_FPS=$PREVIEW_FPS
Environment=HXTEND_FEED1_URL=$FEED1_URL
Environment=HXTEND_FEED2_URL=$FEED2_URL

[Install]
WantedBy=multi-user.target
SERVICE

systemctl daemon-reload
systemctl enable "$SERVICE_NAME"
systemctl restart "$SERVICE_NAME"

echo "Agente instalado."
echo "Estado: sudo systemctl status $SERVICE_NAME"
echo "Logs: sudo journalctl -u $SERVICE_NAME -f"
