from __future__ import annotations

import os
import queue
import subprocess
import sys
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from aiteams.plugins.manifest import compute_plugin_fingerprint
from aiteams.utils import json_dumps, json_loads, make_id


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass(slots=True)
class PluginSandbox:
    plugin_id: str
    manifest: dict[str, Any]
    root_path: Path
    stdout_timeout: float = 10.0
    process: subprocess.Popen[str] | None = None
    started_at: str | None = None
    last_error: str | None = None
    last_descriptor: dict[str, Any] | None = None
    _fingerprint: str | None = None
    _stderr_lines: list[str] = field(default_factory=list)
    _lock: threading.RLock = field(default_factory=threading.RLock)
    _stderr_thread: threading.Thread | None = None
    _stdout_thread: threading.Thread | None = None
    _stdout_queue: queue.Queue[str] = field(default_factory=queue.Queue)

    def start(self) -> None:
        with self._lock:
            if self.process and self.process.poll() is None:
                return
            env = {
                "PYTHONIOENCODING": "utf-8",
                "AITEAMS_PLUGIN_ID": self.plugin_id,
                "AITEAMS_PLUGIN_ROOT": str(self.root_path),
            }
            project_root = str(Path(__file__).resolve().parents[2])
            existing_pythonpath = os.environ.get("PYTHONPATH", "")
            env["PYTHONPATH"] = project_root if not existing_pythonpath else f"{project_root}{os.pathsep}{existing_pythonpath}"
            self.process = subprocess.Popen(
                [sys.executable, "-u", "-m", "aiteams.plugins.worker", str(self.root_path)],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                cwd=str(self.root_path),
                env={**os.environ, **env},
            )
            self.started_at = _utcnow()
            self.last_error = None
            self._stderr_lines = []
            self._stdout_queue = queue.Queue()
            self._fingerprint = compute_plugin_fingerprint(self.root_path)
            self._stdout_thread = threading.Thread(target=self._capture_stdout, daemon=True)
            self._stdout_thread.start()
            self._stderr_thread = threading.Thread(target=self._capture_stderr, daemon=True)
            self._stderr_thread.start()

    def stop(self) -> None:
        with self._lock:
            if not self.process:
                return
            process = self.process
            try:
                if process.poll() is None:
                    try:
                        self._send({"id": make_id("rpc"), "method": "shutdown"})
                    except Exception:
                        pass
                    process.terminate()
                    process.wait(timeout=2)
            except Exception:
                try:
                    process.kill()
                    process.wait(timeout=2)
                except Exception:
                    pass
            finally:
                for stream_name in ("stdin", "stdout", "stderr"):
                    stream = getattr(process, stream_name, None)
                    try:
                        if stream:
                            stream.close()
                    except Exception:
                        pass
                self.process = None

    def restart(self) -> None:
        with self._lock:
            self.stop()
            self.start()

    def health(self) -> dict[str, Any]:
        return self.request("health")

    def describe(self) -> dict[str, Any]:
        return self.request("describe")

    def invoke(self, action: str, payload: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
        return self.request("invoke", action=action, payload=payload, context=context)

    def request(self, method: str, **params: Any) -> dict[str, Any]:
        with self._lock:
            self._maybe_hot_reload()
            self.start()
            assert self.process is not None
            request_id = make_id("rpc")
            self._send({"id": request_id, "method": method, **params})
            try:
                line = self._stdout_queue.get(timeout=self.stdout_timeout)
            except queue.Empty as exc:
                self.last_error = f"Plugin sandbox request `{method}` timed out after {self.stdout_timeout:.1f}s."
                raise RuntimeError(self.last_error) from exc
            if not line:
                self.last_error = self._compose_crash_error()
                raise RuntimeError(self.last_error)
            payload = json_loads(line.strip(), {})
            if str(payload.get("id") or "") != request_id:
                raise RuntimeError("Plugin sandbox protocol mismatch.")
            if not payload.get("ok", False):
                error = str(payload.get("error") or "Plugin sandbox request failed.")
                self.last_error = error
                raise RuntimeError(error)
            result = dict(payload.get("result") or {})
            if method == "describe":
                self.last_descriptor = result
            return result

    def snapshot(self) -> dict[str, Any]:
        running = bool(self.process and self.process.poll() is None)
        return {
            "plugin_id": self.plugin_id,
            "running": running,
            "pid": self.process.pid if running and self.process else None,
            "started_at": self.started_at,
            "last_error": self.last_error,
            "descriptor": self.last_descriptor or {},
            "fingerprint": self._fingerprint,
            "stderr_tail": self._stderr_lines[-12:],
        }

    def _maybe_hot_reload(self) -> None:
        hot_reload = bool(self.manifest.get("hot_reload", True))
        if not hot_reload:
            return
        current = compute_plugin_fingerprint(self.root_path)
        if self._fingerprint is None:
            self._fingerprint = current
            return
        if current != self._fingerprint:
            self._fingerprint = current
            self.restart()

    def _send(self, payload: dict[str, Any]) -> None:
        if not self.process or not self.process.stdin:
            raise RuntimeError("Plugin sandbox is not running.")
        self.process.stdin.write(f"{json_dumps(payload)}\n")
        self.process.stdin.flush()

    def _capture_stderr(self) -> None:
        if not self.process or not self.process.stderr:
            return
        for line in self.process.stderr:
            text = line.rstrip()
            if text:
                self._stderr_lines.append(text)
                if len(self._stderr_lines) > 100:
                    self._stderr_lines = self._stderr_lines[-100:]

    def _capture_stdout(self) -> None:
        if not self.process or not self.process.stdout:
            return
        for line in self.process.stdout:
            self._stdout_queue.put(line.rstrip("\n"))

    def _compose_crash_error(self) -> str:
        stderr = "\n".join(self._stderr_lines[-6:])
        exit_code = self.process.poll() if self.process else None
        if stderr:
            return f"Plugin sandbox exited unexpectedly (code={exit_code}). {stderr}"
        return f"Plugin sandbox exited unexpectedly (code={exit_code})."
