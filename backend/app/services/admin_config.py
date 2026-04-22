from pathlib import Path
from typing import Any

from app.services.storage import PRIVATE, read_json, write_json, read_text, write_text

MICROCAP_CONFIG = PRIVATE / 'microcap' / 'config.yaml'
MICROCAP_ENV = PRIVATE / 'microcap' / 'runtime_env.json'
BTT_PRESET = PRIVATE / 'btt' / 'preset.json'

MASK_KEYS = {
    'ZEROEX_API_KEY',
    'X_BEARER_TOKEN',
    'TELEGRAM_BOT_TOKEN',
    'TELEGRAM_CHAT_ID',
    'DISCORD_WEBHOOK_URL',
    'PRIVATE_KEY',
    'MODE_LIVE_RPC_URLS',
}


def get_microcap_config_text() -> str:
    return read_text(MICROCAP_CONFIG, '')


def set_microcap_config_text(value: str) -> None:
    write_text(MICROCAP_CONFIG, value)


def get_microcap_env(masked: bool = True) -> dict[str, Any]:
    data = read_json(MICROCAP_ENV, {})
    if not masked:
        return data
    return {k: ('********' if (k in MASK_KEYS and v) else v) for k, v in data.items()}


def set_microcap_env(value: dict[str, Any]) -> None:
    current = read_json(MICROCAP_ENV, {})
    merged = dict(current)
    for k, v in value.items():
        if isinstance(v, str) and v.strip() == '********':
            continue
        merged[k] = v
    write_json(MICROCAP_ENV, merged)


def get_btt_preset() -> dict[str, Any]:
    return read_json(BTT_PRESET, {
        'countries': 'united states,canada,united kingdom,germany,france,italy,spain,netherlands,sweden,switzerland,japan,south korea,hong kong,singapore,australia,india,brazil',
        'all_countries': False,
        'max_per_country': 30,
        'shortlist_multiplier': 4,
        'workers': 6,
        'top': 50,
        'portfolio_size': 12,
        'emerging_only': False,
        'technical_refine': True,
    })


def set_btt_preset(value: dict[str, Any]) -> None:
    write_json(BTT_PRESET, value)
