#!/usr/bin/env python3
"""
HxTend remote preview agent.

Runs on the local processor box. It reads the local MJPEG feeds and pushes the
latest JPEG frames to the Render server, so the public /panel can show previews
without being on the same LAN.
"""

import argparse
import os
import threading
import time
from typing import Optional

import requests


BOUNDARY_PREFIX = b"--"


def iter_mjpeg_frames(url: str, timeout=(3, 15)):
    with requests.get(url, stream=True, timeout=timeout) as response:
        response.raise_for_status()
        content_type = response.headers.get("content-type", "")
        boundary = None
        for part in content_type.split(";"):
            part = part.strip()
            if part.startswith("boundary="):
                boundary = part.split("=", 1)[1].strip().strip('"').encode()
                break
        if not boundary:
            boundary = b"nadjiebmjpegstreamer"
        boundary_line = BOUNDARY_PREFIX + boundary

        buffer = b""
        for chunk in response.iter_content(chunk_size=8192):
            if not chunk:
                continue
            buffer += chunk
            while True:
                start = buffer.find(b"\xff\xd8")
                end = buffer.find(b"\xff\xd9", start + 2 if start >= 0 else 0)
                if start >= 0 and end >= 0:
                    frame = buffer[start:end + 2]
                    buffer = buffer[end + 2:]
                    yield frame
                    continue

                marker = buffer.find(boundary_line, 1)
                if marker > 0:
                    buffer = buffer[marker:]
                    continue

                if len(buffer) > 2_000_000:
                    buffer = buffer[-200_000:]
                break


class FeedPusher(threading.Thread):
    def __init__(
        self,
        feed_id: str,
        source_url: str,
        server_url: str,
        device_id: str,
        token: str,
        fps: float,
    ):
        super().__init__(daemon=True)
        self.feed_id = feed_id
        self.source_url = source_url
        self.server_url = server_url.rstrip("/")
        self.device_id = device_id
        self.token = token
        self.min_interval = 1.0 / max(0.2, fps)
        self.session = requests.Session()
        self.last_push = 0.0

    def push_status(self, online: bool, error: Optional[str] = None):
        try:
            self.session.post(
                f"{self.server_url}/api/stream/status",
                json={
                    "device_id": self.device_id,
                    "feed_id": self.feed_id,
                    "online": online,
                    "source_url": self.source_url,
                    "error": error or "",
                },
                headers={"Authorization": f"Bearer {self.token}"},
                timeout=(3, 8),
            )
        except Exception:
            pass

    def push_frame(self, frame: bytes):
        self.session.post(
            f"{self.server_url}/api/stream/frame/{self.feed_id}",
            params={"device_id": self.device_id},
            data=frame,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "image/jpeg",
                "Cache-Control": "no-store",
            },
            timeout=(3, 8),
        )

    def run(self):
        while True:
            try:
                print(f"[{self.feed_id}] connecting {self.source_url}", flush=True)
                self.push_status(True)
                for frame in iter_mjpeg_frames(self.source_url):
                    now = time.monotonic()
                    if now - self.last_push < self.min_interval:
                        continue
                    self.last_push = now
                    self.push_frame(frame)
            except Exception as exc:
                print(f"[{self.feed_id}] offline: {exc}", flush=True)
                self.push_status(False, str(exc))
                time.sleep(1.5)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", default=os.getenv("HXTEND_RENDER_URL", "https://hxtend-controller.onrender.com"))
    parser.add_argument("--device-id", default=os.getenv("HXTEND_DEVICE_ID", "procesadora-01"))
    parser.add_argument("--token", default=os.getenv("HXTEND_STREAM_TOKEN", ""))
    parser.add_argument("--fps", type=float, default=float(os.getenv("HXTEND_REMOTE_PREVIEW_FPS", "8")))
    parser.add_argument("--feed1", default=os.getenv("HXTEND_FEED1_URL", "http://127.0.0.1:8001/feed"))
    parser.add_argument("--feed2", default=os.getenv("HXTEND_FEED2_URL", "http://127.0.0.1:8002/feed"))
    args = parser.parse_args()

    if not args.token:
        raise SystemExit("Set HXTEND_STREAM_TOKEN with the same token configured in Render.")

    threads = [
        FeedPusher("8001", args.feed1, args.server, args.device_id, args.token, args.fps),
        FeedPusher("8002", args.feed2, args.server, args.device_id, args.token, args.fps),
    ]
    for thread in threads:
        thread.start()
    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()
