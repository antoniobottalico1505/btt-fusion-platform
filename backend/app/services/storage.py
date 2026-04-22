import json
import shutil
from pathlib import Path
from typing import Any

import yaml

from app.core.settings import get_settings

settings = get_settings()
ROOT = Path(settings.STORAGE_ROOT).resolve()
PRIVATE = ROOT / 'private'
PUBLIC = ROOT / 'public'
SEED = Path(__file__).resolve().parents[2] / 'seed'
ENGINES = Path(__file__).resolve().parents[1] / 'engines'


def ensure_storage() -> None:
    for path in [ROOT, PRIVATE, PUBLIC, PRIVATE / 'microcap', PRIVATE / 'btt', PRIVATE / 'btt_runs', PUBLIC / 'exports']:
        path.mkdir(parents=True, exist_ok=True)

    seed_targets = [
        (SEED / 'microcap_config.yaml', PRIVATE / 'microcap' / 'config.yaml'),
        (SEED / 'microcap_seed_bot.db', PRIVATE / 'microcap' / 'bot.db'),
        (SEED / 'btt_preset.json', PRIVATE / 'btt' / 'preset.json'),
        (SEED / 'microcap_env.json', PRIVATE / 'microcap' / 'runtime_env.json'),
    ]
    for src, dst in seed_targets:
        if src.exists() and not dst.exists():
            shutil.copyfile(src, dst)


def read_text(path: Path, default: str = '') -> str:
    if not path.exists():
        return default
    return path.read_text(encoding='utf-8')


def write_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding='utf-8')


def read_json(path: Path, default: Any):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return default


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False), encoding='utf-8')


def engine_paths() -> dict[str, Path]:
    return {
        'microcap': ENGINES / 'microcap_bot_v4.py',
        'btt': ENGINES / 'btt_capital_bomb_final.py',
        'viewer': ENGINES / 'viewer_dashboard.py',
    }
