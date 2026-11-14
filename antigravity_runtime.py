"""Runtime status persistence shared by the AG daemon, supervisor, and dashboard."""
from __future__ import annotations

import json
import os
import tempfile
import ctypes
import time
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
DAEMON_HEALTH_PATH = ROOT / "antigravity_daemon_health.json"
SUPERVISOR_STATUS_PATH = ROOT / "antigravity_supervisor_status.json"
SUPERVISOR_STATE_PATH = ROOT / "antigravity_supervisor_state.json"


def read_json(path: Path) -> dict[str, Any]:
    for attempt in range(3):
        try:
            with path.open("r", encoding="utf-8") as source:
                value = json.load(source)
            return value if isinstance(value, dict) else {}
        except FileNotFoundError:
            return {}
        except (OSError, json.JSONDecodeError):
            if attempt < 2:
                time.sleep(0.05 * (attempt + 1))
    return {}


def atomic_write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temp_name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as output:
            json.dump(value, output, indent=2, ensure_ascii=False)
            output.flush()
            os.fsync(output.fileno())
                                                                               
                                                                                
                                                                             
        attempts = 9 if os.name == "nt" else 1
        for attempt in range(attempts):
            try:
                os.replace(temp_name, path)
                break
            except PermissionError:
                if attempt + 1 >= attempts:
                    raise
                time.sleep(min(0.05 * (2 ** attempt), 1.0))
    except Exception:
        try:
            os.unlink(temp_name)
        except OSError:
            pass
        raise


def pid_is_alive(pid: int) -> bool:
    """Portable PID liveness check; os.kill(pid, 0) is unreliable on Windows."""
    try:
        pid = int(pid)
    except (TypeError, ValueError, OverflowError):
        return False
    if pid <= 0:
        return False
    if os.name != "nt":
        try:
            os.kill(pid, 0)
            return True
        except PermissionError:
            return True
        except (OSError, ProcessLookupError):
            return False

    from ctypes import wintypes
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.OpenProcess.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
    kernel32.OpenProcess.restype = wintypes.HANDLE
    kernel32.GetExitCodeProcess.argtypes = (wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD))
    kernel32.GetExitCodeProcess.restype = wintypes.BOOL
    kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
    kernel32.CloseHandle.restype = wintypes.BOOL
    ctypes.set_last_error(0)
    handle = kernel32.OpenProcess(0x1000, False, pid)                                     
    if not handle:
        return ctypes.get_last_error() == 5                                       
    try:
        exit_code = wintypes.DWORD()
        return bool(kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code))
                    and exit_code.value == 259)                
    finally:
        kernel32.CloseHandle(handle)
