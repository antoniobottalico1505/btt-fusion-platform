import os
import re
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

from app.core.settings import get_settings
from app.services.admin_config import get_microcap_env
from app.services.storage import PRIVATE, engine_paths

settings = get_settings()


class MicrocapProcessManager:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._proc: subprocess.Popen | None = None
        self._started_at: float | None = None
        self._desired_mode: str = settings.MICROCAP_PUBLIC_MODE

    @property
    def workdir(self) -> Path:
        return PRIVATE / 'microcap'

    def status(self) -> dict[str, Any]:
        with self._lock:
            running = self._proc is not None and self._proc.poll() is None
            return {
                'running': running,
                'pid': self._proc.pid if self._proc else None,
                'started_at_epoch': self._started_at,
                'mode': self._desired_mode,
                'uptime_sec': (time.time() - self._started_at) if (running and self._started_at) else 0,
                'live_enabled': settings.MICROCAP_LIVE_ENABLED,
            }

    def _force_safe_webservice_config(self) -> None:
        config_path = self.workdir / 'config.yaml'
        if not config_path.exists():
            return

        txt = config_path.read_text(encoding='utf-8')
        original = txt

        if self._desired_mode == 'paper':
            if re.search(r'(?mi)^mode\s*:', txt):
                txt = re.sub(r'(?mi)^mode\s*:\s*live\s*$', 'mode: paper', txt)
            else:
                txt = f"mode: paper\n{txt}"

        if re.search(r'(?mi)^metrics_enabled\s*:', txt):
            txt = re.sub(r'(?mi)^metrics_enabled\s*:\s*true\s*$', 'metrics_enabled: false', txt)
        else:
            txt = txt.rstrip() + '\nmetrics_enabled: false\n'

        if txt != original:
            config_path.write_text(txt, encoding='utf-8')

    def _spawn(self) -> None:
        engine = engine_paths()['microcap']
        env = os.environ.copy()
        env.update({k: str(v) for k, v in get_microcap_env(masked=False).items() if v not in (None, '')})
        env['PYTHONUNBUFFERED'] = '1'
        env['BTTFUSION_DISABLE_PROMETHEUS'] = '1'

        self._force_safe_webservice_config()

        self._proc = subprocess.Popen(
            [sys.executable, str(engine)],
            cwd=str(self.workdir),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=env,
        )
        self._started_at = time.time()

    def start(self, mode: str | None = None) -> dict[str, Any]:
        with self._lock:
            if mode:
                self._desired_mode = mode.strip().lower()
            if self._proc and self._proc.poll() is None:
                return self.status()
            self._spawn()
            return self.status()

    def stop(self) -> dict[str, Any]:
        with self._lock:
            if self._proc and self._proc.poll() is None:
                try:
                    self._proc.terminate()
                    self._proc.wait(timeout=8)
                except Exception:
                    try:
                        self._proc.kill()
                    except Exception:
                        pass
            self._proc = None
            self._started_at = None
            return self.status()

    def restart(self, mode: str | None = None) -> dict[str, Any]:
        self.stop()
        time.sleep(0.6)
        return self.start(mode=mode)


microcap_manager = MicrocapProcessManager()