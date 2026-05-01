import os
import re
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable

from app.core.settings import get_settings
from app.services.admin_config import get_microcap_env
from app.services.storage import (
    PRIVATE,
    engine_paths,
    ensure_user_microcap_workspace,
    user_microcap_paths,
)

settings = get_settings()


class ManagedMicrocapProcess:
    def __init__(self, workdir_source: Path | Callable[[], Path], name: str = "public") -> None:
        self._workdir_source = workdir_source
        self._name = name
        self._lock = threading.Lock()
        self._proc: subprocess.Popen | None = None
        self._started_at: float | None = None
        self._desired_mode: str = settings.MICROCAP_PUBLIC_MODE
        self._last_error: str = ""
        self._last_exit_code: int | None = None
        self._log_fp = None
        self._session_exists: bool = False

    @property
    def workdir(self) -> Path:
        if callable(self._workdir_source):
            return self._workdir_source()
        return self._workdir_source

    @property
    def log_path(self) -> Path:
        return self.workdir / "microcap_engine.log"

    def _close_log(self) -> None:
        try:
            if self._log_fp:
                self._log_fp.close()
        except Exception:
            pass
        self._log_fp = None

    def _tail_log(self, chars: int = 8000) -> str:
        try:
            if not self.log_path.exists():
                return ""
            return self.log_path.read_text(encoding="utf-8", errors="replace")[-chars:]
        except Exception:
            return ""

    def _status_unlocked(self) -> dict[str, Any]:
        running = self._proc is not None and self._proc.poll() is None
        if self._proc is not None and not running:
            self._last_exit_code = self._proc.poll()

        return {
            "running": running,
            "pid": self._proc.pid if self._proc else None,
            "started_at_epoch": self._started_at,
            "mode": self._desired_mode,
            "uptime_sec": (time.time() - self._started_at) if (running and self._started_at) else 0,
            "live_enabled": settings.MICROCAP_LIVE_ENABLED,
            "exit_code": None if running else self._last_exit_code,
            "last_error": self._last_error,
            "log_tail": self._tail_log(),
            "scope": self._name,
            "session_exists": self._session_exists,
        }

    def status(self) -> dict[str, Any]:
        with self._lock:
            return self._status_unlocked()

    def has_session(self) -> bool:
        with self._lock:
            return self._session_exists

    def _force_safe_webservice_config(self) -> None:
        config_path = self.workdir / "config.yaml"
        if not config_path.exists():
            return

        txt = config_path.read_text(encoding="utf-8")
        original = txt

        desired_mode = (self._desired_mode or settings.MICROCAP_PUBLIC_MODE or "paper").strip().lower()
        if desired_mode not in {"paper", "live"}:
            desired_mode = "paper"

        if re.search(r"(?mi)^mode\s*:", txt):
            txt = re.sub(r"(?mi)^mode\s*:\s*\S+\s*$", f"mode: {desired_mode}", txt)
        else:
            txt = f"mode: {desired_mode}\n{txt}"

        if re.search(r"(?mi)^metrics_enabled\s*:", txt):
            txt = re.sub(r"(?mi)^metrics_enabled\s*:\s*\S+\s*$", "metrics_enabled: false", txt)
        else:
            txt = txt.rstrip() + "\nmetrics_enabled: false\n"

        if txt != original:
            config_path.write_text(txt, encoding="utf-8")

    def _spawn(self) -> None:
        engine = engine_paths()["microcap"]
        if not engine.exists():
            raise RuntimeError(f"Microcap engine non trovato: {engine}")

        self.workdir.mkdir(parents=True, exist_ok=True)
        self.log_path.parent.mkdir(parents=True, exist_ok=True)

        with self.log_path.open("a", encoding="utf-8") as f:
            f.write(f"\n\n===== MICROCAP START {time.strftime('%Y-%m-%d %H:%M:%S')} [{self._name}] =====\n")

        env = os.environ.copy()
        env.update({k: str(v) for k, v in get_microcap_env(masked=False).items() if v not in (None, "")})
        env["PYTHONUNBUFFERED"] = "1"
        env["BTTFUSION_DISABLE_PROMETHEUS"] = "1"

        self._force_safe_webservice_config()
        self._close_log()
        self._log_fp = self.log_path.open("ab", buffering=0)

        self._proc = subprocess.Popen(
            [sys.executable, str(engine)],
            cwd=str(self.workdir),
            stdout=self._log_fp,
            stderr=subprocess.STDOUT,
            env=env,
        )
        self._started_at = time.time()
        self._last_error = ""
        self._last_exit_code = None
        self._session_exists = True

    def start(self, mode: str | None = None) -> dict[str, Any]:
        with self._lock:
            if mode:
                self._desired_mode = mode.strip().lower()

            self._session_exists = True

            if self._proc and self._proc.poll() is None:
                return self._status_unlocked()

            try:
                self._spawn()
                time.sleep(2.0)

                if self._proc is not None and self._proc.poll() is not None:
                    self._last_exit_code = self._proc.poll()
                    tail = self._tail_log(3000).strip()
                    self._last_error = tail or f"Process exited immediately with code {self._last_exit_code}"
                    self._proc = None
                    self._started_at = None
            except Exception as exc:
                self._last_error = f"{type(exc).__name__}: {exc}"
                self._last_exit_code = None
                self._proc = None
                self._started_at = None

            return self._status_unlocked()

    def stop(self) -> dict[str, Any]:
        with self._lock:
            self._session_exists = True

            if self._proc and self._proc.poll() is None:
                try:
                    self._proc.terminate()
                    self._proc.wait(timeout=8)
                except Exception:
                    try:
                        self._proc.kill()
                    except Exception:
                        pass

            if self._proc is not None and self._proc.poll() is not None:
                self._last_exit_code = self._proc.poll()

            self._proc = None
            self._started_at = None
            self._close_log()
            return self._status_unlocked()

    def restart(self, mode: str | None = None) -> dict[str, Any]:
        self.stop()
        time.sleep(0.6)
        return self.start(mode=mode)


class UserMicrocapSessionManager:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._sessions: dict[int, ManagedMicrocapProcess] = {}

    def _session_for(self, user_id: int) -> ManagedMicrocapProcess:
        with self._lock:
            if user_id not in self._sessions:
                ensure_user_microcap_workspace(user_id)
                self._sessions[user_id] = ManagedMicrocapProcess(
                    lambda uid=user_id: user_microcap_paths(uid)["workdir"],
                    name=f"user_{user_id}",
                )
            return self._sessions[user_id]

    def status(self, user_id: int, create: bool = False) -> dict[str, Any] | None:
        with self._lock:
            if user_id not in self._sessions:
                if not create:
                    return None
                ensure_user_microcap_workspace(user_id)
                self._sessions[user_id] = ManagedMicrocapProcess(
                    lambda uid=user_id: user_microcap_paths(uid)["workdir"],
                    name=f"user_{user_id}",
                )

            session = self._sessions[user_id]

        return session.status()

    def restart(self, user_id: int, mode: str | None = None) -> dict[str, Any]:
        ensure_user_microcap_workspace(user_id)
        return self._session_for(user_id).restart(mode=mode)

    def stop(self, user_id: int) -> dict[str, Any]:
        return self._session_for(user_id).stop()


microcap_manager = ManagedMicrocapProcess(PRIVATE / "microcap", name="public")
user_microcap_manager = UserMicrocapSessionManager()