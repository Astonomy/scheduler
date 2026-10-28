"""Atomic checkpoints and OS-backed locks (automatically released on process exit)."""
import contextlib
import hashlib
import json
import os
from pathlib import Path
import tempfile
import time


class Busy(RuntimeError):
    pass


@contextlib.contextmanager
def lock(key):
    root = Path(tempfile.gettempdir()) / "reqflow-scheduler-locks"
    root.mkdir(exist_ok=True)
    path = root / (hashlib.sha256(key.encode()).hexdigest() + ".lock")
    try:
        with path.open("xb") as initial:
            initial.write(b"0")
    except FileExistsError:
        pass
    with path.open("r+b") as stream:
        stream.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise Busy("another scheduler holds the lock") from exc
        try:
            yield
        finally:
            stream.seek(0)
            if os.name == "nt":
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(stream, fcntl.LOCK_UN)


def read(path, default=None):
    return json.loads(path.read_text(encoding="utf-8-sig")) if path.exists() else default


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(dir=path.parent, prefix=".checkpoint-", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        # Windows readers briefly deny rename while status polling has the
        # checkpoint open. Retain atomic replacement; retry that sharing error.
        for attempt in range(10):
            try:
                os.replace(temporary, path)
                break
            except PermissionError as exc:
                if getattr(exc, "winerror", None) not in {5, 32} or attempt == 9:
                    raise
                time.sleep(0.01 * (attempt + 1))
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
