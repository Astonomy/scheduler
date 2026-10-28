"""Read account quota without starting a model turn or consuming model tokens."""
from datetime import datetime, timezone
import json
import math
import os
import queue
import subprocess
import threading
import time

from .dispatch import executable, terminate


def timestamp(value):
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("timestamps must include a timezone")
    return result.astimezone(timezone.utc)


def fetch(task):
    process = subprocess.Popen([*executable(task), "app-server"], stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                               start_new_session=os.name != 'nt')
    messages = queue.Queue()

    def consume():
        for line in iter(process.stdout.readline, b""):
            try:
                messages.put(json.loads(line))
            except ValueError:
                pass
        messages.put(None)

    reader = threading.Thread(target=consume, daemon=True)
    reader.start()
    deadline = time.monotonic() + 30

    def send(message):
        process.stdin.write((json.dumps(message) + "\n").encode())
        process.stdin.flush()

    def response(identifier):
        while True:
            try:
                message = messages.get(timeout=max(0.01, deadline - time.monotonic()))
            except queue.Empty:
                raise ValueError("quota query timed out") from None
            if message is None:
                raise ValueError("quota app-server closed")
            if message.get("id") == identifier:
                if "error" in message:
                    raise ValueError("quota query failed: " + str(message["error"].get("message")))
                return message["result"]
            if time.monotonic() >= deadline:
                raise ValueError("quota query timed out")

    try:
        send(dict(id=1, method="initialize", params=dict(clientInfo=dict(name="reqflow_scheduler", version="0.2.0"))))
        response(1)
        send(dict(method="initialized"))
        send(dict(id=2, method="account/rateLimits/read"))
        return response(2)
    finally:
        terminate(process)
        process.stdin.close()
        reader.join(timeout=5)
        if not reader.is_alive():
            process.stdout.close()


def normalize(result, now, limit_id="codex"):
    buckets = result.get("rateLimitsByLimitId") or {}
    bucket = buckets.get(limit_id)
    if bucket is None:
        bucket = result.get("rateLimits")
        if bucket and bucket.get("limitId") not in (None, limit_id):
            bucket = None
    if not bucket:
        raise ValueError("account quota unavailable for " + limit_id)
    windows = [bucket.get(name) for name in ("primary", "secondary") if bucket.get(name)]
    five = next((w for w in windows if w.get("windowDurationMins") == 300), None)
    if five is None:
        raise ValueError("account did not report a five-hour quota window")
    blocked = []
    for window in windows:
        used = window.get("usedPercent")
        if isinstance(used, bool) or not isinstance(used, (float, int)) or not math.isfinite(used) or not 0 <= used <= 100:
            raise ValueError("invalid quota usage")
        reset = datetime.fromtimestamp(window["resetsAt"], timezone.utc)
        if reset <= now:
            raise ValueError("account returned stale quota; retry later")
        if used >= 100:
            blocked.append(reset)
    reset = datetime.fromtimestamp(five["resetsAt"], timezone.utc)
    if bucket.get("rateLimitReachedType") and not blocked:
        raise ValueError("account reports a reached limit without an exhausted window")
    return dict(source="codex-app-server", observed_at=now.isoformat(),
                remaining_percent=100 - five["usedPercent"], resets_at=reset.isoformat(),
                blocked_until=max(blocked).isoformat() if blocked else None)


def query(task, now):
    return normalize(fetch(task), now, task.get("quota_limit_id", "codex"))
