import json
import sqlite3
from pathlib import Path
from typing import Any

import yaml

from app.core.settings import get_settings

settings = get_settings()

ROOT = Path(settings.STORAGE_ROOT).resolve()
PRIVATE = ROOT / "private"
PUBLIC = ROOT / "public"
SEED = Path(__file__).resolve().parents[2] / "seed"
ENGINES = Path(__file__).resolve().parents[1] / "engines"


def _ensure_microcap_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()

        cur.execute("""
        CREATE TABLE IF NOT EXISTS state (
            k TEXT PRIMARY KEY,
            v TEXT NOT NULL
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            mode TEXT NOT NULL,
            chain TEXT NOT NULL,
            token TEXT NOT NULL,
            pair TEXT,
            side TEXT NOT NULL,
            px_usd REAL NOT NULL,
            qty REAL NOT NULL,
            usd_value REAL NOT NULL,
            reason TEXT
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS positions (
            key TEXT PRIMARY KEY,
            chain TEXT NOT NULL,
            token TEXT NOT NULL,
            pair TEXT,
            entry_px REAL NOT NULL,
            qty REAL NOT NULL,
            entry_ts INTEGER NOT NULL,
            peak_px REAL NOT NULL,
            avg_px REAL NOT NULL,
            pyramids_done INTEGER NOT NULL DEFAULT 0,
            tp1_done INTEGER NOT NULL DEFAULT 0,
            trail_armed_ts INTEGER NOT NULL DEFAULT 0,
            trail_step_n INTEGER NOT NULL DEFAULT 0,
            trail_stop_px REAL NOT NULL DEFAULT 0,
            trail_breach_n INTEGER NOT NULL DEFAULT 0
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS watchlist (
            key TEXT PRIMARY KEY,
            chain TEXT NOT NULL,
            token TEXT NOT NULL,
            added_ts INTEGER NOT NULL,
            pair TEXT,
            score REAL,
            cooldown_until INTEGER
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS snapshots (
            ts INTEGER NOT NULL,
            key TEXT NOT NULL,
            chain TEXT NOT NULL,
            token TEXT NOT NULL,
            price_usd REAL NOT NULL,
            liq_usd REAL,
            vol_m5 REAL,
            txns_m5 INTEGER,
            fdv REAL,
            score REAL,
            PRIMARY KEY (ts, key)
        )
        """)

        cur.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_key_ts ON snapshots(key, ts)")
        cur.execute("INSERT OR IGNORE INTO state (k, v) VALUES (?, ?)", ("cash", json.dumps(200.0)))
        cur.execute("INSERT OR IGNORE INTO state (k, v) VALUES (?, ?)", ("peak_equity", json.dumps(200.0)))
        conn.commit()
    finally:
        conn.close()


def _sqlite_has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    rows = cur.fetchall()
    return any(str(row[1]) == column for row in rows)


def _migrate_existing_microcap_db(db_path: Path) -> None:
    if not db_path.exists():
        return

    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()

        cur.execute("""
        CREATE TABLE IF NOT EXISTS state (
            k TEXT PRIMARY KEY,
            v TEXT NOT NULL
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            mode TEXT NOT NULL,
            chain TEXT NOT NULL,
            token TEXT NOT NULL,
            pair TEXT,
            side TEXT NOT NULL,
            px_usd REAL NOT NULL,
            qty REAL NOT NULL,
            usd_value REAL NOT NULL,
            reason TEXT
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS positions (
            key TEXT PRIMARY KEY,
            chain TEXT NOT NULL,
            token TEXT NOT NULL,
            pair TEXT,
            entry_px REAL NOT NULL,
            qty REAL NOT NULL,
            entry_ts INTEGER NOT NULL,
            peak_px REAL NOT NULL,
            avg_px REAL NOT NULL,
            pyramids_done INTEGER NOT NULL DEFAULT 0,
            tp1_done INTEGER NOT NULL DEFAULT 0
        )
        """)

        if not _sqlite_has_column(conn, "positions", "trail_armed_ts"):
            cur.execute("ALTER TABLE positions ADD COLUMN trail_armed_ts INTEGER NOT NULL DEFAULT 0")
        if not _sqlite_has_column(conn, "positions", "trail_step_n"):
            cur.execute("ALTER TABLE positions ADD COLUMN trail_step_n INTEGER NOT NULL DEFAULT 0")
        if not _sqlite_has_column(conn, "positions", "trail_stop_px"):
            cur.execute("ALTER TABLE positions ADD COLUMN trail_stop_px REAL NOT NULL DEFAULT 0")
        if not _sqlite_has_column(conn, "positions", "trail_breach_n"):
            cur.execute("ALTER TABLE positions ADD COLUMN trail_breach_n INTEGER NOT NULL DEFAULT 0")

        cur.execute("""
        CREATE TABLE IF NOT EXISTS watchlist (
            key TEXT PRIMARY KEY,
            chain TEXT NOT NULL,
            token TEXT NOT NULL,
            added_ts INTEGER NOT NULL,
            pair TEXT,
            score REAL,
            cooldown_until INTEGER
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS snapshots (
            ts INTEGER NOT NULL,
            key TEXT NOT NULL,
            chain TEXT NOT NULL,
            token TEXT NOT NULL,
            price_usd REAL NOT NULL,
            liq_usd REAL,
            vol_m5 REAL,
            txns_m5 INTEGER,
            fdv REAL,
            score REAL,
            PRIMARY KEY (ts, key)
        )
        """)

        cur.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_key_ts ON snapshots(key, ts)")
        cur.execute("INSERT OR IGNORE INTO state (k, v) VALUES (?, ?)", ("cash", json.dumps(200.0)))
        cur.execute("INSERT OR IGNORE INTO state (k, v) VALUES (?, ?)", ("peak_equity", json.dumps(200.0)))

        conn.commit()
    finally:
        conn.close()


def ensure_storage() -> None:
    for path in [
        ROOT,
        PRIVATE,
        PUBLIC,
        PRIVATE / "microcap",
        PRIVATE / "btt",
        PRIVATE / "btt_runs",
        PUBLIC / "exports",
    ]:
        path.mkdir(parents=True, exist_ok=True)

    small_seed_targets = [
        (SEED / "microcap_config.yaml", PRIVATE / "microcap" / "config.yaml"),
        (SEED / "btt_preset.json", PRIVATE / "btt" / "preset.json"),
        (SEED / "microcap_env.json", PRIVATE / "microcap" / "runtime_env.json"),
    ]

    for src, dst in small_seed_targets:
        if src.exists() and not dst.exists():
            dst.write_bytes(src.read_bytes())

    db_path = PRIVATE / "microcap" / "bot.db"
    if not db_path.exists():
        _ensure_microcap_db(db_path)
    _migrate_existing_microcap_db(db_path)

def read_text(path: Path, default: str = "") -> str:
    if not path.exists():
        return default
    return path.read_text(encoding="utf-8")


def write_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def read_json(path: Path, default: Any):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False), encoding="utf-8")


def engine_paths() -> dict[str, Path]:
    return {
        "microcap": ENGINES / "microcap_bot_v4.py",
        "btt": ENGINES / "btt_capital_bomb_final.py",
        "viewer": ENGINES / "viewer_dashboard.py",
    }