import asyncio
import dataclasses
import json
import logging
import math
import os
import random
import re
import sqlite3
import time
from datetime import datetime
from collections import defaultdict, deque
from typing import Any, Dict, List, Optional, Tuple

import httpx
import yaml
from dotenv import load_dotenv

# LIVE deps (servono anche importate, ma userai "paper" finché non installi tutto)
from eth_account import Account
from web3 import Web3

# Prometheus
from prometheus_client import Counter, Gauge, Histogram, start_http_server


# -------------------- utils --------------------

def now_ts() -> int:
    return int(time.time())

def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None

def is_evm_address(addr: str) -> bool:
    return isinstance(addr, str) and bool(re.fullmatch(r"0x[a-fA-F0-9]{40}", str(addr).strip()))

def is_evm_pair_id(addr: str) -> bool:
    return isinstance(addr, str) and bool(re.fullmatch(r"0x[a-fA-F0-9]{64}", str(addr).strip()))

def is_base58_address(addr: str, *, min_len: int = 32, max_len: int = 64) -> bool:
    s = str(addr or "").strip()
    return bool(re.fullmatch(rf"[1-9A-HJ-NP-Za-km-z]{{{min_len},{max_len}}}", s))

def is_aptos_hex_address(addr: str) -> bool:
    return isinstance(addr, str) and bool(re.fullmatch(r"0x[a-fA-F0-9]{64}", str(addr).strip()))

def is_sui_hex_address(addr: str) -> bool:
    return isinstance(addr, str) and bool(re.fullmatch(r"0x[a-fA-F0-9]{64}", str(addr).strip()))

def is_supported_token_ref(addr: str) -> bool:
    s = str(addr or "").strip()
    if not s or len(s) < 24 or len(s) > 96:
        return False
    if any(ch.isspace() for ch in s):
        return False
    if is_evm_address(s) or is_evm_pair_id(s) or is_aptos_hex_address(s) or is_sui_hex_address(s) or is_base58_address(s):
        return True
    return bool(re.fullmatch(r"[A-Za-z0-9_-]{24,96}", s))

def is_dexscreener_id(x: str) -> bool:
    return is_supported_token_ref(x)

def normalize_chain_id(ch: Any, default: str = "base") -> str:
    """
    Normalizza chain proveniente da DexScreener:
    - accetta nomi ('base', 'ethereum', ...)
    - accetta chainId numerici e li mappa a nomi quando noti
    """
    if ch is None:
        return default

    s = str(ch).strip().lower()
    if s == "":
        return default
    if not s.isdigit():
        return s

    cid = int(s)
    id_map = {
        1: "ethereum",
        10: "optimism",
        25: "cronos",
        56: "bsc",
        100: "gnosis",
        130: "unichain",
        137: "polygon",
        146: "sonic",
        169: "manta",
        250: "fantom",
        324: "zksync",
        1088: "metis",
        1101: "polygon-zkevm",
        1284: "moonbeam",
        5000: "mantle",
        8453: "base",
        34443: "mode",
        42161: "arbitrum",
        43114: "avalanche",
        59144: "linea",
        81457: "blast",
        167000: "taiko",
        534352: "scroll",
        7777777: "zora",
    }
    return id_map.get(cid, str(cid))

def wants_all_chains(values: Any) -> bool:
    if values is None:
        return False
    if isinstance(values, str):
        values = [values]
    for v in values:
        s = str(v or "").strip().lower()
        if s in {"all", "*", "any"}:
            return True
    return False

def normalize_allowed_chains(values: Any, known: Optional[Dict[str, Any]] = None) -> List[str]:
    if values is None:
        values = []
    if isinstance(values, str):
        values = [values]

    out: List[str] = []
    for v in values:
        s = str(v or "").strip().lower()
        if not s:
            continue
        if s in {"all", "*", "any"}:
            return ["all"]
        s = normalize_chain_id(s, default="")
        if s and s not in out:
            out.append(s)

    if not out and known:
        return list(known.keys())

    return out

def chain_is_allowed(chain: str, allowed_values: Any) -> bool:
    ch = normalize_chain_id(chain, default="")
    if not ch:
        return False
    if wants_all_chains(allowed_values):
        return True
    allowed = {
        normalize_chain_id(x, default="")
        for x in (allowed_values or [])
        if str(x or "").strip()
    }
    return ch in allowed

def parse_dexscreener_url(url: Any) -> Tuple[str, str]:
    s = str(url or "").strip()
    if not s:
        return "", ""

    m = re.search(
        r"dexscreener\.com/([a-zA-Z0-9_-]+)/([A-Za-z0-9_-]{24,96})",
        s,
        re.IGNORECASE,
    )
    if not m:
        return "", ""

    chain = normalize_chain_id(m.group(1), default="base")
    ident = canonical_token_ref(chain, (m.group(2) or "").strip())
    return chain, ident


def parse_birdeye_url(url: Any) -> Tuple[str, str]:
    s = str(url or "").strip()
    if not s:
        return "", ""

    m = re.search(
        r"(?:birdeye\.so|beta\.birdeye\.so)/token/([1-9A-HJ-NP-Za-km-z]{32,44})(?:\?[^\s#]*?(?:[?&]chain=([a-zA-Z0-9_-]+)))?",
        s,
        re.IGNORECASE,
    )
    if not m:
        return "", ""

    chain = normalize_chain_id(m.group(2) or "solana", default="solana")
    ident = canonical_token_ref(chain, (m.group(1) or "").strip())
    return chain, ident


def canonical_token_ref(chain: Any, token: Any) -> str:
    ch = normalize_chain_id(chain, default="")
    s = str(token or "").strip()
    if not s:
        return ""
    if ch == "solana":
        return s
    if is_evm_address(s) or is_evm_pair_id(s) or is_aptos_hex_address(s) or is_sui_hex_address(s):
        return s.lower()
    return s.lower()


def make_token_key(chain: Any, token: Any) -> str:
    ch = normalize_chain_id(chain, default="base")
    tok = canonical_token_ref(ch, token)
    return f"{ch}:{tok}" if tok else f"{ch}:"


def extract_plain_social_addresses(blob: Any) -> List[str]:
    s = str(blob or "")
    out: List[str] = []
    seen = set()

    for a in re.findall(r"0x[a-fA-F0-9]{40}", s):
        aa = str(a or "").strip().lower()
        if aa and aa not in seen:
            seen.add(aa)
            out.append(aa)

    sol_re = re.compile(r"(?<![A-Za-z0-9])[1-9A-HJ-NP-Za-km-z]{32,44}(?![A-Za-z0-9])")
    for mm in sol_re.finditer(s):
        aa = str(mm.group(0) or "").strip()
        if not is_base58_address(aa, min_len=32, max_len=44):
            continue
        if aa and aa not in seen:
            seen.add(aa)
            out.append(aa)

    return out


def extract_social_url_hits(blob: Any) -> List[Dict[str, str]]:
    s = str(blob or "")
    hits: List[Dict[str, str]] = []
    seen = set()

    dex_re = re.compile(r"dexscreener\.com/([a-zA-Z0-9_-]+)/([A-Za-z0-9_-]{24,96})", re.IGNORECASE)
    for mm in dex_re.finditer(s):
        chain = normalize_chain_id(mm.group(1), default="base")
        addr = canonical_token_ref(chain, mm.group(2) or "")
        if not chain or not addr or not is_supported_token_ref(addr):
            continue
        tup = (chain, addr)
        if tup in seen:
            continue
        seen.add(tup)
        hits.append({"chain": chain, "address": addr})

    birdeye_re = re.compile(
        r"(?:birdeye\.so|beta\.birdeye\.so)/token/([1-9A-HJ-NP-Za-km-z]{32,44})(?:\?[^\s#]*?(?:[?&]chain=([a-zA-Z0-9_-]+)))?",
        re.IGNORECASE,
    )
    for mm in birdeye_re.finditer(s):
        chain = normalize_chain_id(mm.group(2) or "solana", default="solana")
        addr = canonical_token_ref(chain, mm.group(1) or "")
        if not chain or not addr or not is_supported_token_ref(addr):
            continue
        tup = (chain, addr)
        if tup in seen:
            continue
        seen.add(tup)
        hits.append({"chain": chain, "address": addr})

    return hits


def infer_social_chain_for_plain_address(
    addr: Any,
    *,
    default_chain: str = "base",
    default_evm_chain: Optional[str] = None,
    default_solana_chain: str = "solana",
) -> Tuple[str, str]:
    a = str(addr or "").strip()
    if not a:
        return "", ""

    if is_evm_address(a):
        chain = normalize_chain_id(default_evm_chain or default_chain or "base", default="base")
        return chain, canonical_token_ref(chain, a)

    if is_base58_address(a, min_len=32, max_len=44):
        chain = normalize_chain_id(default_solana_chain or "solana", default="solana")
        return chain, canonical_token_ref(chain, a)

    return "", ""

def jitter(base: float, pct: float = 0.15) -> float:

    return base * (1.0 + random.uniform(-pct, pct))

def fmt_money(x: float) -> str:
    s = f"{x:+.2f}"
    return s

def fmt_pct(x: float) -> str:
    return f"{x*100:+.2f}%"

SOLANA_USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

def _raw_usdc_6(amount_usdc: float) -> int:
    return max(1, int(round(float(amount_usdc) * 1_000_000)))

# -------------------- config --------------------

@dataclasses.dataclass
class ChainCfg:
    chain_id: int
    usdc: str
    rpc_urls: List[str] = dataclasses.field(default_factory=list)

@dataclasses.dataclass
class Config:
    strategy: Dict[str, Any] = dataclasses.field(default_factory=dict)

    mode: str = "paper"

    tick_interval_sec: int = 1
    scan_interval_sec: int = 10
    refresh_watchlist_sec: int = 5

    allowed_chains: List[str] = dataclasses.field(default_factory=lambda: ["base"])

    min_liquidity_usd: float = 100.0
    min_volume_5m_usd: float = 8000.0
    min_txns_5m: int = 40
    max_fdv_usd: Optional[float] = 30000000.0

    entry_return_60s: float = 0.02
    min_volatility_60s: float = 0.01

    stop_loss_pct: float = 0.07
    tp1_pct: float = 0.08
    tp1_fraction: float = 0.60
    tp2_pct: float = 0.16
    trailing_stop_pct: float = 0.06
    max_hold_sec: int = 600

    pyramid_step_pct: float = 0.10
    pyramid_fraction: float = 0.20
    max_pyramids: int = 1

    max_positions: int = 3
    max_watchlist: int = 450
    cooldown_sec: int = 60

    start_equity_usd: float = 100.0
    base_risk_pct: float = 0.18
    scale_exponent: float = 0.85
    max_risk_pct: float = 0.30

    max_total_exposure_pct: float = 0.60
    max_drawdown_from_peak: float = 0.25
    max_daily_loss_pct: float = 0.12

    zeroex_slippage_bps: int = 600
    max_slippage_bps: int = 1200
    dynamic_slippage: bool = True
    max_buy_tax_bps: int = 100
    max_sell_tax_bps: int = 100
    precheck_usdc: float = 10.0

    metrics_enabled: bool = True
    metrics_port: int = 9108

    snapshot_enabled: bool = True
    snapshot_store_top_n: int = 400
    snapshot_retention_days: int = 14
    refresh_include_recent_sec: int = 900
    refresh_include_recent_max: int = 80

    max_consecutive_failures: int = 5
    pause_sec_after_failures: int = 120

    chains: Dict[str, ChainCfg] = dataclasses.field(default_factory=dict)

def _as_float(v):
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        s = v.strip().replace(",", ".")
        if s == "":
            return None
        return float(s)
    return float(v)

def _as_int(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, int):
        return int(v)
    if isinstance(v, float):
        return int(v)
    if isinstance(v, str):
        s = v.strip()
        if s == "":
            return None
        s = s.replace(",", ".")
        return int(float(s))
    return int(v)

def _as_bool(v):
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        s = v.strip().lower()
        return s in ("1", "true", "yes", "y", "on")
    return bool(v)

def normalize_cfg_types(cfg):
    # int
    for k in [
        "tick_interval_sec","scan_interval_sec","refresh_watchlist_sec",
        "min_txns_5m","max_hold_sec","max_pyramids","max_positions","max_watchlist","cooldown_sec",
        "zeroex_slippage_bps","max_slippage_bps","max_buy_tax_bps","max_sell_tax_bps",
        "metrics_port","snapshot_store_top_n","snapshot_retention_days",
        "refresh_include_recent_sec","refresh_include_recent_max",
        "max_consecutive_failures","pause_sec_after_failures"
    ]:
        setattr(cfg, k, _as_int(getattr(cfg, k)))

    # float
    for k in [
        "min_liquidity_usd","min_volume_5m_usd","max_fdv_usd",
        "entry_return_60s","min_volatility_60s",
        "stop_loss_pct","tp1_pct","tp1_fraction","tp2_pct","trailing_stop_pct",
        "pyramid_step_pct","pyramid_fraction",
        "start_equity_usd","base_risk_pct","scale_exponent","max_risk_pct",
        "max_total_exposure_pct","max_drawdown_from_peak","max_daily_loss_pct",
        "precheck_usdc"
    ]:
        val = getattr(cfg, k)
        setattr(cfg, k, _as_float(val))

    # bool
    for k in ["dynamic_slippage","metrics_enabled","snapshot_enabled"]:
        setattr(cfg, k, _as_bool(getattr(cfg, k)))

    # optional max_fdv_usd
    if cfg.max_fdv_usd is not None:
        cfg.max_fdv_usd = float(cfg.max_fdv_usd)

    # --- strategy (nested dict) ---
    # We keep it as a plain dict so config.yaml can define any strategy params.
    if isinstance(getattr(cfg, "strategy", None), dict):
        s = dict(cfg.strategy)

        # mode
        if "mode" in s and isinstance(s["mode"], str):
            s["mode"] = s["mode"].strip().lower()

        # ints
        for kk in ["dip_window_sec", "short_window_sec", "min_seconds_after_trail_activation", "target_entries_per_day", "daily_entry_cap", "max_new_positions_per_tick"]:
            if kk in s and s[kk] is not None:
                try:
                    s[kk] = int(float(str(s[kk]).strip().replace(",", ".")))
                except Exception:
                    pass

        # floats (all in "decimal" form, e.g. -0.012 = -1.2%)
        float_keys = [
            "min_dip_pct_60s", "rebound_retrace_pct", "min_rebound_pct_before_trail",
            "min_dip_pct_lookback",
            "max_dump_speed_pct_per_sec",
            "min_rebound_speed_pct_per_sec",
            "min_micro_rebound_pct",
            "trail_step_pct",
            "trail_floor_profit_pct",
            "exit_if_dump_speed_again_pct_per_sec",
            "exit_pump_slow_speed_pct_per_sec",
            "exit_if_pump_speed_below_pct_per_sec",
        ]
        for kk in float_keys:
            if kk in s and s[kk] is not None:
                try:
                    s[kk] = float(str(s[kk]).strip().replace(",", "."))
                except Exception:
                    pass

        cfg.strategy = s

    return cfg

def load_config(path: str) -> Config:
    raw = yaml.safe_load(open(path, "r", encoding="utf-8")) or {}
    chains_raw = raw.get("chains", {}) or {}
    chains: Dict[str, ChainCfg] = {}
    for name, obj in chains_raw.items():
        rpc_urls = obj.get("rpc_urls", []) if isinstance(obj, dict) else []
        if isinstance(rpc_urls, str):
            rpc_urls = [u.strip() for u in rpc_urls.split(",") if u.strip()]
        elif not isinstance(rpc_urls, list):
            rpc_urls = []
        chains[name.lower()] = ChainCfg(
            chain_id=int(obj["chain_id"]),
            usdc=str(obj["usdc"]),
            rpc_urls=[str(u).strip() for u in rpc_urls if str(u).strip()],
        )
    raw["chains"] = chains
    cfg = Config(**raw)
    cfg = normalize_cfg_types(cfg)
    cfg.mode = cfg.mode.lower().strip()
    normalized_allowed = normalize_allowed_chains(cfg.allowed_chains, known=cfg.chains)
    if wants_all_chains(normalized_allowed):
        cfg.allowed_chains = ["all"]
    else:
        cfg.allowed_chains = [c for c in normalized_allowed if (not cfg.chains or c in cfg.chains)]
        if not cfg.allowed_chains:
            cfg.allowed_chains = list(cfg.chains.keys()) if cfg.chains else ["all"]
    cfg.tp1_fraction = clamp(cfg.tp1_fraction, 0.0, 1.0)
    cfg.max_total_exposure_pct = clamp(cfg.max_total_exposure_pct, 0.0, 1.0)
    return cfg


# -------------------- metrics --------------------

M_SCAN = Counter("bot_scan_total", "Numero scan discovery")
M_REFRESH = Counter("bot_refresh_total", "Numero refresh batch")
M_API_ERR = Counter("bot_api_errors_total", "Errori API/RPC/tx", ["where"])
M_TRADES = Counter("bot_trades_total", "Trades eseguiti", ["side", "reason"])
M_EQUITY = Gauge("bot_equity_total_usd", "Equity totale stimata USD")
M_CASH = Gauge("bot_cash_usd", "Cash USD (paper) / USDC balance (live)")
M_EXPOSURE = Gauge("bot_exposure_usd", "Esposizione stimata USD")
M_POS = Gauge("bot_positions", "Numero posizioni aperte")
M_WATCH = Gauge("bot_watchlist", "Numero watchlist")
M_RPC_INDEX = Gauge("bot_rpc_index", "Indice RPC attivo")
M_LAT = Histogram("bot_latency_seconds", "Latenze operazioni", ["op"])


# -------------------- persistence --------------------

class Store:
    def __init__(self, path: str = "bot.db"):
        self.conn = sqlite3.connect(path, timeout=30)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self.conn.execute("PRAGMA busy_timeout=5000;")
        self._init()

    def _init(self) -> None:
        cur = self.conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS state (k TEXT PRIMARY KEY, v TEXT NOT NULL);
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
        );
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
            pyramids_done INTEGER NOT NULL,
            tp1_done INTEGER NOT NULL
        );
        """)
        # --- migration (safe): add columns for stepped trailing (if missing) ---
        try:
            cur.execute("ALTER TABLE positions ADD COLUMN trail_armed_ts INTEGER NOT NULL DEFAULT 0")
        except Exception:
            pass
        try:
            cur.execute("ALTER TABLE positions ADD COLUMN trail_step_n INTEGER NOT NULL DEFAULT 0")
        except Exception:
            pass
        try:
            cur.execute("ALTER TABLE positions ADD COLUMN trail_stop_px REAL NOT NULL DEFAULT 0")
        except Exception:
            pass
        try:
            cur.execute("ALTER TABLE positions ADD COLUMN trail_breach_n INTEGER NOT NULL DEFAULT 0")
        except Exception:
            pass

        cur.execute("""
        CREATE TABLE IF NOT EXISTS watchlist (
            key TEXT PRIMARY KEY,
            chain TEXT NOT NULL,
            token TEXT NOT NULL,
            added_ts INTEGER NOT NULL,
            pair TEXT,
            score REAL,
            cooldown_until INTEGER
        );
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
        );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_key_ts ON snapshots(key, ts);")

               # --- Narrative / hype events ---
        cur.execute("""
        CREATE TABLE IF NOT EXISTS hype_events (
            ts INTEGER NOT NULL,
            key TEXT NOT NULL,
            source TEXT NOT NULL,
            event TEXT NOT NULL,
            value REAL,
            PRIMARY KEY (ts, key, source, event)
        );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_hype_events_key_ts ON hype_events(key, ts);")

        cur.execute("""
        CREATE TABLE IF NOT EXISTS social_events (
            ts INTEGER NOT NULL,
            key TEXT NOT NULL,
            source TEXT NOT NULL,
            channel TEXT,
            mentions INTEGER NOT NULL,
            PRIMARY KEY (ts, key, source, channel)
        );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_social_events_key_ts ON social_events(key, ts);")

        cur.execute("""
        CREATE TABLE IF NOT EXISTS audit_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            event TEXT NOT NULL,
            key TEXT,
            payload TEXT
        );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_audit_events_ts ON audit_events(ts);")

        self.conn.commit()

    def set_state(self, k: str, v: Any) -> None:
        cur = self.conn.cursor()
        cur.execute("INSERT OR REPLACE INTO state (k, v) VALUES (?, ?)", (k, json.dumps(v)))
        self.conn.commit()

    def get_state(self, k: str, default: Any = None) -> Any:
        cur = self.conn.cursor()
        cur.execute("SELECT v FROM state WHERE k=?", (k,))
        row = cur.fetchone()
        if not row:
            return default
        try:
            return json.loads(row[0])
        except Exception:
            return default

    def log_trade(self, *, ts: int, mode: str, chain: str, token: str, pair: str,
                  side: str, px_usd: float, qty: float, usd_value: float, reason: str) -> None:
        cur = self.conn.cursor()
        cur.execute("""
        INSERT INTO trades (ts, mode, chain, token, pair, side, px_usd, qty, usd_value, reason)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (ts, mode, chain, token, pair, side, px_usd, qty, usd_value, reason))
        self.conn.commit()

    def log_audit(self, *, ts: int, event: str, key: str = "", payload: Any = None) -> None:
        cur = self.conn.cursor()
        try:
            payload_txt = json.dumps(payload if payload is not None else {}, ensure_ascii=False)
        except Exception:
            payload_txt = json.dumps({"repr": str(payload)}, ensure_ascii=False)
        cur.execute(
            "INSERT INTO audit_events (ts, event, key, payload) VALUES (?, ?, ?, ?)",
            (int(ts), str(event), str(key or ""), payload_txt),
        )
        self.conn.commit()

    def retention_audit(self, retention_days: int = 14) -> None:
        cutoff = now_ts() - int(retention_days * 86400)
        cur = self.conn.cursor()
        cur.execute("DELETE FROM audit_events WHERE ts < ?", (cutoff,))
        self.conn.commit()

    def save_watchlist(self, items: Dict[str, Dict[str, Any]]) -> None:
        cur = self.conn.cursor()
        cur.execute("DELETE FROM watchlist")
        for k, info in items.items():
            cur.execute("""
            INSERT INTO watchlist (key, chain, token, added_ts, pair, score, cooldown_until)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                k, info["chain"], info["token"], info["added_ts"],
                info.get("pair",""), info.get("score"), int(info.get("cooldown_until") or 0)
            ))
        self.conn.commit()

    def load_watchlist(self) -> Dict[str, Dict[str, Any]]:
        cur = self.conn.cursor()
        cur.execute("SELECT key, chain, token, added_ts, pair, score, cooldown_until FROM watchlist")
        out: Dict[str, Dict[str, Any]] = {}
        for row in cur.fetchall():
            k, chain, token, added_ts, pair, score, cooldown_until = row
            out[k] = {
                "chain": chain, "token": token, "added_ts": int(added_ts),
                "pair": pair or "", "score": score, "cooldown_until": int(cooldown_until or 0),
                "last_feat": None
            }
        return out

    def save_positions(self, pos: Dict[str, "Position"]) -> None:
        cur = self.conn.cursor()
        cur.execute("DELETE FROM positions")
        for k, p in pos.items():
            cur.execute("""
            INSERT INTO positions (key, chain, token, pair, entry_px, qty, entry_ts, peak_px, avg_px, pyramids_done, tp1_done, trail_armed_ts, trail_step_n, trail_stop_px, trail_breach_n)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                k, p.chain, p.token, p.pair,
                float(p.entry_px), float(p.qty), int(p.entry_ts),
                float(p.peak_px), float(p.avg_px),
                int(p.pyramids_done), int(bool(p.tp1_done)),
                int(getattr(p, "trail_armed_ts", 0) or 0),
                int(getattr(p, "trail_step_n", 0) or 0),
                float(getattr(p, "trail_stop_px", 0.0) or 0.0),
                int(getattr(p, "trail_breach_n", 0) or 0),
            ))
        self.conn.commit()

    def load_positions(self) -> Dict[str, "Position"]:
        cur = self.conn.cursor()
        try:
            cur.execute("""
            SELECT key, chain, token, pair, entry_px, qty, entry_ts, peak_px, avg_px, pyramids_done, tp1_done, trail_armed_ts, trail_step_n, trail_stop_px, trail_breach_n
            FROM positions
            """)
            rows = cur.fetchall()
            out: Dict[str, Position] = {}
            for row in rows:
                 (k, chain, token, pair, entry_px, qty, entry_ts, peak_px, avg_px, pyramids_done, tp1_done,
                 trail_armed_ts, trail_step_n, trail_stop_px, trail_breach_n) = row
                 out[k] = Position(
                    chain=chain, token=token, pair=pair or "",
                    entry_px=float(entry_px), qty=float(qty), entry_ts=int(entry_ts),
                    peak_px=float(peak_px), avg_px=float(avg_px),
                    pyramids_done=int(pyramids_done), tp1_done=bool(tp1_done),
                    trail_armed_ts=int(trail_armed_ts or 0),
                    trail_step_n=int(trail_step_n or 0),
                    trail_stop_px=float(trail_stop_px or 0.0),
                    trail_breach_n=int(trail_breach_n or 0),
                 )
            return out
        except Exception:
            cur.execute("""
            SELECT key, chain, token, pair, entry_px, qty, entry_ts, peak_px, avg_px, pyramids_done, tp1_done
            FROM positions
            """)
            out: Dict[str, Position] = {}
            for row in cur.fetchall():
                k, chain, token, pair, entry_px, qty, entry_ts, peak_px, avg_px, pyramids_done, tp1_done = row
                out[k] = Position(
                    chain=chain, token=token, pair=pair or "",
                    entry_px=float(entry_px), qty=float(qty), entry_ts=int(entry_ts),
                    peak_px=float(peak_px), avg_px=float(avg_px),
                    pyramids_done=int(pyramids_done), tp1_done=bool(tp1_done),
                )
            return out

    def insert_snapshots(self, rows: List[Tuple]) -> None:
        if not rows:
            return
        cur = self.conn.cursor()
        cur.executemany("""
        INSERT OR IGNORE INTO snapshots (ts, key, chain, token, price_usd, liq_usd, vol_m5, txns_m5, fdv, score)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
        self.conn.commit()

    def retention_snapshots(self, retention_days: int) -> None:
        cutoff = now_ts() - int(retention_days * 86400)
        cur = self.conn.cursor()
        cur.execute("DELETE FROM snapshots WHERE ts < ?", (cutoff,))
        self.conn.commit()

    # --- snapshot helpers (used to keep ENTRY logic stable across restarts) ---
    def _snap_latest(self, key: str) -> Optional[Tuple[int, float]]:
        cur = self.conn.cursor()
        cur.execute(
            "SELECT ts, price_usd FROM snapshots WHERE key=? AND price_usd>0 ORDER BY ts DESC LIMIT 1",
            (key,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return int(row[0]), float(row[1])

    def _snap_price_at_or_before(self, key: str, ts: int) -> Optional[Tuple[int, float]]:
        cur = self.conn.cursor()
        cur.execute(
            "SELECT ts, price_usd FROM snapshots WHERE key=? AND ts<=? AND price_usd>0 ORDER BY ts DESC LIMIT 1",
            (key, int(ts)),
        )
        row = cur.fetchone()
        if not row:
            return None
        return int(row[0]), float(row[1])

    def _snap_low_since(self, key: str, ts_from: int) -> Optional[float]:
        cur = self.conn.cursor()
        cur.execute(
            "SELECT MIN(price_usd) FROM snapshots WHERE key=? AND ts>=? AND price_usd>0",
            (key, int(ts_from)),
        )
        row = cur.fetchone()
        if not row or row[0] is None:
            return None
        return float(row[0])

    def snapshot_dip_rebound_features(
        self,
        key: str,
        *,
        dip_window_sec: int,
        short_window_sec: int,
        speed5_window_sec: int,
        px_override: Optional[float] = None,
    ) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
        """
        Compute (dip, speed, micro_reb_from_low, speed5) from persisted snapshots.

        This is a fallback used when the in-memory cache doesn't have enough history
        (common after bot restarts). It matches the dashboard math closely.
        """
        latest = self._snap_latest(key)
        if not latest:
            return None, None, None, None
        ts_now, px_now_db = latest
        px_now = float(px_override) if (px_override is not None and px_override > 0) else float(px_now_db)

        # dip
        old = self._snap_price_at_or_before(key, ts_now - int(dip_window_sec))
        dip = None
        if old and old[1] > 0:
            dip = (px_now / float(old[1])) - 1.0

        # speed over short window
        sh = self._snap_price_at_or_before(key, ts_now - int(short_window_sec))
        speed = None
        if sh and sh[1] > 0:
            dt = max(1, int(ts_now - int(sh[0])))
            speed = (px_now / float(sh[1]) - 1.0) / dt

        # micro rebound from recent low
        low = self._snap_low_since(key, ts_now - int(short_window_sec))
        micro = None
        if low and low > 0:
            micro = (px_now / float(low)) - 1.0

        # speed5
        s5 = self._snap_price_at_or_before(key, ts_now - int(speed5_window_sec))
        speed5 = None
        if s5 and s5[1] > 0:
            dt5 = max(1, int(ts_now - int(s5[0])))
            speed5 = (px_now / float(s5[1]) - 1.0) / dt5

        return dip, speed, micro, speed5




# --- health helpers ---
def snapshot_liq_range_pct(self, key: str, *, window_sec: int) -> float | None:
    """Return (max_liq-min_liq)/max_liq in last window."""
    cur = self.conn.cursor()
    t0 = int(now_ts() - int(window_sec))
    cur.execute(
        "SELECT MIN(liq_usd), MAX(liq_usd) FROM snapshots WHERE key=? AND ts>=? AND liq_usd IS NOT NULL",
        (key, t0),
    )
    row = cur.fetchone()
    if not row or row[0] is None or row[1] is None:
        return None
    mn, mx = float(row[0]), float(row[1])
    if mx <= 0:
        return None
    return max(0.0, (mx - mn) / mx)

def snapshot_recent_drawdown_from_peak_pct(self, key: str, *, window_sec: int, px_override: float | None = None) -> float | None:
    """Drawdown from peak in last window: (peak - current)/peak."""
    cur = self.conn.cursor()
    t0 = int(now_ts() - int(window_sec))
    cur.execute(
        "SELECT MAX(price_usd) FROM snapshots WHERE key=? AND ts>=? AND price_usd>0",
        (key, t0),
    )
    row = cur.fetchone()
    if not row or row[0] is None:
        return None
    peak = float(row[0])
    if peak <= 0:
        return None
    # current
    if px_override is not None and px_override > 0:
        cur_px = float(px_override)
    else:
        latest = self._snap_latest(key)
        if not latest:
            return None
        cur_px = float(latest[1])
    return max(0.0, (peak - cur_px) / peak)

# --- narrative helpers ---
def log_hype_event(self, *, ts: int, key: str, source: str, event: str, value: float | None = None) -> None:
    cur = self.conn.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO hype_events (ts, key, source, event, value) VALUES (?, ?, ?, ?, ?)",
        (int(ts), key, source, event, value),
    )
    self.conn.commit()

def hype_count(self, key: str, *, window_sec: int) -> int:
    cur = self.conn.cursor()
    t0 = int(now_ts() - int(window_sec))
    cur.execute("SELECT COUNT(*) FROM hype_events WHERE key=? AND ts>=?", (key, t0))
    row = cur.fetchone()
    return int(row[0] or 0) if row else 0

def log_social_mentions(self, *, ts: int, key: str, source: str, channel: str | None, mentions: int) -> None:
    cur = self.conn.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO social_events (ts, key, source, channel, mentions) VALUES (?, ?, ?, ?, ?)",
        (int(ts), key, source, channel, int(mentions)),
    )
    self.conn.commit()

def social_mentions_sum(self, key: str, *, window_sec: int, source: str | None = None) -> int:
    cur = self.conn.cursor()
    t0 = int(now_ts() - int(window_sec))
    if source:
        cur.execute(
            "SELECT COALESCE(SUM(mentions),0) FROM social_events WHERE key=? AND ts>=? AND source=?",
            (key, t0, source),
        )
    else:
        cur.execute(
            "SELECT COALESCE(SUM(mentions),0) FROM social_events WHERE key=? AND ts>=?",
            (key, t0),
        )
    row = cur.fetchone()
    return int(row[0] or 0) if row else 0

def attention_trend(self, key: str, *, window_sec: int = 1800) -> dict:
    now = int(now_ts())
    half = int(window_sec)
    t1 = now
    t0 = now - half
    tprev1 = t0
    tprev0 = t0 - half
    cur = self.conn.cursor()
    def _avg(t_from, t_to):
        cur.execute(
            """SELECT AVG(COALESCE(vol_m5,0)), AVG(COALESCE(txns_m5,0))
               FROM snapshots WHERE key=? AND ts>=? AND ts<?""",
            (key, int(t_from), int(t_to)),
        )
        row = cur.fetchone() or (0.0, 0.0)
        return float(row[0] or 0.0), float(row[1] or 0.0)
    v_cur, x_cur = _avg(t0, t1)
    v_prev, x_prev = _avg(tprev0, tprev1)
    return {"vol_cur": v_cur, "vol_prev": v_prev, "txns_cur": x_cur, "txns_prev": x_prev}

def get_watchlist_rows(self, limit: int = 50) -> list[dict]:
    cur = self.conn.cursor()
    cur.execute("SELECT key, chain, token, pair, score FROM watchlist ORDER BY COALESCE(score,0) DESC LIMIT ?", (int(limit),))
    out=[]
    for row in cur.fetchall():
        k, chain, token, pair, score = row
        out.append({"key": k, "chain": chain, "token": token, "pair": pair or '', "score": score})
    return out
# -------------------- rate limiting --------------------

class AsyncRateLimiter:
    def __init__(self, capacity: int, period_sec: float):
        self.capacity = capacity
        self.period_sec = period_sec
        self.tokens = float(capacity)
        self.last = time.monotonic()
        self.lock = asyncio.Lock()

    async def acquire(self, n: int = 1) -> None:
        async with self.lock:
            while True:
                now = time.monotonic()
                elapsed = now - self.last
                self.last = now
                refill = (elapsed / self.period_sec) * self.capacity
                self.tokens = min(float(self.capacity), self.tokens + refill)

                if self.tokens >= n:
                    self.tokens -= n
                    return

                missing = n - self.tokens
                wait = (missing / self.capacity) * self.period_sec
                await asyncio.sleep(max(0.01, wait))




# -------------------- narrative sources --------------------

class TelegramNarrativeClient:
    """Telegram narrative collector using Telethon (client API).

    Reads recent messages from configured channels and counts mentions of contract addresses.
    """

    def __init__(self, *, api_id: int, api_hash: str, session: str = "microcap_bot", channels: list[str] | None = None):
        self.api_id = int(api_id)
        self.api_hash = str(api_hash)
        self.session = str(session)
        self.channels = channels or []
        self._client = None
        self._last_ids: dict[str, int] = {}

    async def connect(self):
        try:
            from telethon import TelegramClient
        except Exception as e:
            raise RuntimeError("Telethon non installato. Esegui: pip install telethon") from e
        self._client = TelegramClient(self.session, self.api_id, self.api_hash)
        await self._client.start()
        # quick coverage check
        for ch in self.channels:
            try:
                await self._client.get_entity(ch)
            except Exception as e:
                logging.getLogger("microcap_bot_v4").warning("telegram entity NOT FOUND channel=%s err=%s", ch, str(e))

    async def close(self):
        if self._client:
            await self._client.disconnect()
            self._client = None

    async def fetch_mentions(self, *, since_sec: int = 300, max_msgs_per_channel: int = 60) -> list[dict]:
        if not self._client:
            await self.connect()
        out=[]
        url_re = re.compile(r"https?://\S+")
        now = now_ts()
        for ch in self.channels:
            try:
                entity = await self._client.get_entity(ch)
                last_id = self._last_ids.get(ch, 0)
                msgs = await self._client.get_messages(entity, limit=max_msgs_per_channel)
                max_seen = last_id
                addrs=[]
                dex=[]
                for m in msgs:
                    mid = getattr(m,'id',0) or 0
                    if mid <= last_id:
                        continue
                    dt = getattr(m,'date',None)
                    if not dt:
                        continue
                    ts = int(dt.timestamp())
                    if ts < now - int(since_sec):
                        continue
                    txt = (getattr(m, "message", "") or "")

                    # --- collect URLs from message text + Telegram entities (clickable links) ---
                    urls = set()

                    # 1) URLs already in plain text
                    for u in url_re.findall(txt):
                        urls.add(u)

                    # 2) URLs hidden in entities (e.g., "DexScreener" clickable)
                    ents = getattr(m, "entities", None) or []
                    for ent in ents:
                        # MessageEntityTextUrl: has .url (hidden URL)
                        u = getattr(ent, "url", None)
                        if u:
                            urls.add(str(u))
                            continue

                        # MessageEntityUrl: URL is in text slice at offset/length
                        off = getattr(ent, "offset", None)
                        ln = getattr(ent, "length", None)
                        if off is not None and ln is not None:
                            try:
                                frag = txt[int(off): int(off) + int(ln)]
                                if frag.startswith("http://") or frag.startswith("https://"):
                                    urls.add(frag)
                            except Exception:
                                pass

                    # Build a blob: message + all URLs
                    blob = txt
                    if urls:
                        blob += " " + " ".join(sorted(urls))

                    # Extract plain contract addresses (EVM + Solana) from text+urls
                    found = extract_plain_social_addresses(blob)
                    if found:
                        addrs.extend(found)

                    # Extract chain-aware token links (DexScreener + Birdeye)
                    dex.extend(extract_social_url_hits(blob))

                    if mid > max_seen:
                        max_seen = mid

                if max_seen > last_id:
                    self._last_ids[ch] = max_seen

                if addrs or dex:
                    # de-dupe
                    addrs = list(dict.fromkeys([a for a in addrs if a]))
                    seen = set()
                    dex2 = []
                    for h in dex:
                        chain = normalize_chain_id(h.get("chain"), default="")
                        addr = canonical_token_ref(chain, h.get("address") or "")
                        if not chain or not addr:
                            continue
                        tup = (chain, addr)
                        if tup in seen:
                            continue
                        seen.add(tup)
                        dex2.append({"chain": chain, "address": addr})

                    out.append({"channel": ch, "ts": now, "addrs": addrs, "dex": dex2})

            except Exception as e:
                logging.getLogger("microcap_bot_v4").warning(
                    "telegram fetch failed channel=%s err=%s", ch, str(e)
                )
                continue

        return out

class NewsApiClient:
    def __init__(self, *, api_key: str, base_url: str = "https://newsapi.org/v2"):
        self.api_key = str(api_key)
        self.base_url = base_url.rstrip('/')
        self._session = None

    async def close(self):
        if self._session:
            await self._session.close()
            self._session = None

    async def _get(self, url: str, params: dict):
        import aiohttp
        if not self._session:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=12))
        headers = {"X-Api-Key": self.api_key}
        async with self._session.get(url, params=params, headers=headers) as r:
            txt = await r.text()
            if r.status != 200:
                raise RuntimeError(f"NewsAPI {r.status}: {txt[:200]}")
            return json.loads(txt)

    async def count_mentions(self, query: str, *, lookback_hours: int = 24, page_size: int = 20) -> int:
        from datetime import datetime, timezone

        since_ts = int(now_ts() - int(lookback_hours) * 3600)
        from_dt = datetime.fromtimestamp(since_ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        url = f"{self.base_url}/everything"
        data = await self._get(
            url,
            {
                "q": query,
                "from": from_dt,
                "sortBy": "publishedAt",
                "pageSize": int(page_size),
                "language": "en",
            },
        )
        return int((data or {}).get("totalResults") or 0)

    async def search_articles(self, query: str, *, lookback_hours: int = 24, page_size: int = 20) -> List[Dict[str, Any]]:
        """Return list of NewsAPI articles for a query."""
        from datetime import datetime, timezone

        since_ts = int(now_ts() - int(lookback_hours) * 3600)
        from_dt = datetime.fromtimestamp(since_ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        url = f"{self.base_url}/everything"
        data = await self._get(
            url,
            {
                "q": query,
                "from": from_dt,
                "sortBy": "publishedAt",
                "pageSize": int(page_size),
                "language": "en",
            },
        )
        arts = (data or {}).get("articles") if isinstance(data, dict) else None
        return arts if isinstance(arts, list) else []

class XApiClient:
    def __init__(self, *, bearer_token: str, base_url: str = "https://api.x.com/2"):
        token = str(bearer_token or "").strip()
        if not token:
            raise ValueError("X bearer token mancante")
        self.bearer_token = token
        self.base_url = base_url.rstrip("/")
        self.http = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=12.0,
            headers={"Authorization": f"Bearer {self.bearer_token}", "Accept": "application/json"},
        )
        self._user_cache: Dict[str, str] = {}

    async def close(self):
        await self.http.aclose()

    async def _get(self, url: str, params: dict | None = None) -> Dict[str, Any]:
        r = await self.http.get(url, params=params or {})
        txt = r.text
        if r.status_code != 200:
            raise RuntimeError(f"X API {r.status_code}: {txt[:400]}")
        data = r.json()
        return data if isinstance(data, dict) else {}

    async def _user_id(self, username: str) -> str:
        u = str(username or "").strip().lstrip("@")
        if not u:
            return ""
        if u in self._user_cache:
            return self._user_cache[u]
        data = await self._get(f"/users/by/username/{u}", params={"user.fields": "id,username"})
        uid = str(((data.get("data") or {}).get("id") or "")).strip()
        if uid:
            self._user_cache[u] = uid
        return uid

    def _extract_hits_from_post(self, post: Dict[str, Any]) -> Tuple[List[str], List[Dict[str, str]]]:
        text = str(post.get("text") or "")

        urls = set()
        ents = (post.get("entities") or {}) if isinstance(post.get("entities"), dict) else {}
        for u in ents.get("urls") or []:
            if not isinstance(u, dict):
                continue
            for kk in ("expanded_url", "unwound_url", "url", "display_url"):
                vv = str(u.get(kk) or "").strip()
                if vv:
                    urls.add(vv)

        blob = text
        if urls:
            blob += " " + " ".join(sorted(urls))

        addrs = extract_plain_social_addresses(blob)
        dex = extract_social_url_hits(blob)

        return list(dict.fromkeys(addrs)), dex

    async def fetch_mentions(
        self,
        *,
        queries: List[str] | None = None,
        usernames: List[str] | None = None,
        since_sec: int = 900,
        max_results_per_call: int = 25,
    ) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        now = now_ts()
        max_results = max(10, min(100, int(max_results_per_call or 25)))

        for q in list(queries or []):
            q = str(q or "").strip()
            if not q:
                continue
            data = await self._get(
                "/tweets/search/recent",
                params={
                    "query": q,
                    "max_results": max_results,
                    "tweet.fields": "created_at,entities,text,author_id",
                },
            )
            posts = data.get("data") if isinstance(data.get("data"), list) else []
            addrs: List[str] = []
            dex: List[Dict[str, str]] = []
            for post in posts:
                if not isinstance(post, dict):
                    continue
                created_at = str(post.get("created_at") or "").strip()
                try:
                    ts = int(datetime.fromisoformat(created_at.replace("Z", "+00:00")).timestamp()) if created_at else now
                except Exception:
                    ts = now
                if ts < now - int(since_sec):
                    continue
                a, d = self._extract_hits_from_post(post)
                addrs.extend(a)
                dex.extend(d)
            if addrs or dex:
                out.append({
                    "channel": f"x_search:{q}",
                    "ts": now,
                    "addrs": list(dict.fromkeys(addrs)),
                    "dex": dex,
                })

        for username in list(usernames or []):
            uname = str(username or "").strip().lstrip("@")
            if not uname:
                continue
            uid = await self._user_id(uname)
            if not uid:
                continue
            data = await self._get(
                f"/users/{uid}/tweets",
                params={
                    "max_results": max_results,
                    "tweet.fields": "created_at,entities,text",
                    "exclude": "replies",
                },
            )
            posts = data.get("data") if isinstance(data.get("data"), list) else []
            addrs: List[str] = []
            dex: List[Dict[str, str]] = []
            for post in posts:
                if not isinstance(post, dict):
                    continue
                created_at = str(post.get("created_at") or "").strip()
                try:
                    ts = int(datetime.fromisoformat(created_at.replace("Z", "+00:00")).timestamp()) if created_at else now
                except Exception:
                    ts = now
                if ts < now - int(since_sec):
                    continue
                a, d = self._extract_hits_from_post(post)
                addrs.extend(a)
                dex.extend(d)
            if addrs or dex:
                out.append({
                    "channel": f"x_user:@{uname}",
                    "ts": now,
                    "addrs": list(dict.fromkeys(addrs)),
                    "dex": dex,
                })

        return out

# -------------------- notifiers --------------------

class Notifier:
    def __init__(self):
        self.tg_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        self.tg_chat = os.getenv("TELEGRAM_CHAT_ID", "").strip()
        self.discord_webhook = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
        self.http = httpx.AsyncClient(timeout=10.0)

    async def close(self):
        await self.http.aclose()

    async def _post_with_retry(self, url: str, payload: dict, where: str, ok_codes: set):
        # prova 2 volte se 429 (rate limit)
        for attempt in range(2):
            r = await self.http.post(url, json=payload)
            if r.status_code in ok_codes:
                return True

            if r.status_code == 429:
                # Telegram/Discord di solito includono retry_after nel JSON
                try:
                    j = r.json()
                    retry = float(j.get("parameters", {}).get("retry_after", j.get("retry_after", 1.0)))
                except Exception:
                    retry = 1.0
                await asyncio.sleep(max(1.0, retry))
                continue

            # altri errori: stampo e stoppo
            print(f"[Notifier:{where}] HTTP {r.status_code} -> {r.text[:200]}")
            return False

        print(f"[Notifier:{where}] HTTP 429 (rate limit) persistente, messaggio scartato")
        return False

    async def send(self, text: str):
        # Telegram
        if self.tg_token and self.tg_chat:
            try:
                url = f"https://api.telegram.org/bot{self.tg_token}/sendMessage"
                ok = await self._post_with_retry(url, {"chat_id": self.tg_chat, "text": text}, "telegram", {200})
                if not ok:
                    M_API_ERR.labels(where="telegram").inc()
            except Exception as e:
                print("[Notifier:telegram] exception:", e)
                M_API_ERR.labels(where="telegram").inc()

        # Discord webhook
        if self.discord_webhook:
            try:
                ok = await self._post_with_retry(self.discord_webhook, {"content": text}, "discord", {204, 200})
                if not ok:
                    M_API_ERR.labels(where="discord").inc()
            except Exception as e:
                print("[Notifier:discord] exception:", e)
                M_API_ERR.labels(where="discord").inc()

# -------------------- DexScreener client --------------------

class DexScreenerClient:
    def __init__(self):
        self.http = httpx.AsyncClient(
            base_url="https://api.dexscreener.com",
            timeout=12.0,
            headers={"Accept": "application/json"},
        )
        # DexScreener docs: token-profiles 60/min, others 300/min
        self.lim_profiles = AsyncRateLimiter(capacity=60, period_sec=60.0)
        self.lim_market = AsyncRateLimiter(capacity=300, period_sec=60.0)

    async def close(self) -> None:
        await self.http.aclose()

    def _rows_from_feed_payload(self, data: Any) -> List[Dict[str, Any]]:
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
        if isinstance(data, dict):
            rows = data.get("data")
            if isinstance(rows, list):
                return [x for x in rows if isinstance(x, dict)]
            return [data]
        return []

    async def latest_token_profiles(self) -> List[Dict[str, Any]]:
        await self.lim_profiles.acquire(1)
        r = await self.http.get("/token-profiles/latest/v1")
        r.raise_for_status()
        return self._rows_from_feed_payload(r.json())

    async def latest_token_community_takeovers(self) -> List[Dict[str, Any]]:
        await self.lim_profiles.acquire(1)
        r = await self.http.get("/community-takeovers/latest/v1")
        r.raise_for_status()
        return self._rows_from_feed_payload(r.json())

    async def latest_token_boosts(self) -> List[Dict[str, Any]]:
        await self.lim_profiles.acquire(1)
        r = await self.http.get("/token-boosts/latest/v1")
        r.raise_for_status()
        return self._rows_from_feed_payload(r.json())

    async def top_token_boosts(self) -> List[Dict[str, Any]]:
        await self.lim_profiles.acquire(1)
        r = await self.http.get("/token-boosts/top/v1")
        r.raise_for_status()
        return self._rows_from_feed_payload(r.json())

    async def tokens_batch(self, chain: str, tokens: List[str]) -> List[Dict[str, Any]]:
        await self.lim_market.acquire(1)
        token_csv = ",".join([str(t).strip() for t in tokens[:30] if str(t).strip()])
        if not token_csv:
            return []
        r = await self.http.get(f"/tokens/v1/{chain}/{token_csv}")
        r.raise_for_status()
        data = r.json()
        return data if isinstance(data, list) else []

    async def token_exists(self, chain: str, token: str) -> bool:
        t = (token or "").strip()
        if not is_supported_token_ref(t):
            return False
        try:
            pairs = await self.tokens_batch(chain, [t])
            return bool(pairs)
        except Exception:
            return False

    async def pair_by_address(self, chain: str, pair_address: str) -> Optional[Dict[str, Any]]:
        await self.lim_market.acquire(1)
        r = await self.http.get(f"/latest/dex/pairs/{chain}/{pair_address}")
        r.raise_for_status()
        data = r.json() or {}
        pairs = (data or {}).get("pairs") if isinstance(data, dict) else None
        if isinstance(pairs, list) and pairs:
            p = pairs[0] or {}
            return p if isinstance(p, dict) else None
        return None

    async def resolve_token_address(self, chain: str, addr: str) -> str:
        a = (addr or "").strip()
        if not a:
            return ""
        try:
            pairs = await self.tokens_batch(chain, [a])
            if pairs:
                return a
        except Exception:
            pass
        try:
            p = await self.pair_by_address(chain, a)
            if isinstance(p, dict):
                base = ((p.get("baseToken") or {}).get("address") or "").strip()
                if base:
                    return base
        except Exception:
            pass
        return a

# -------------------- 0x client v2 --------------------

class ZeroXClient:
    def __init__(
        self,
        api_key: str,
        *,
        rps: float = 4.0,
        max_attempts: int = 4,
        base_sleep_sec: float = 0.35,
    ):
        if not api_key:
            raise ValueError("ZEROEX_API_KEY mancante")

        self.api_key = api_key
        self.http = httpx.AsyncClient(base_url="https://api.0x.org", timeout=12.0)

        self.rps = max(0.5, float(rps))
        self.max_attempts = max(1, int(max_attempts))
        self.base_sleep_sec = max(0.05, float(base_sleep_sec))

        # pacing regolare senza cooldown globale del bot
        self.lim = AsyncRateLimiter(capacity=1, period_sec=max(0.05, 1.0 / self.rps))

    async def close(self):
        await self.http.aclose()

    def _headers(self) -> Dict[str, str]:
        return {"0x-api-key": self.api_key, "0x-version": "v2"}

    async def _get_json(self, path: str, *, params: Dict[str, Any], label: str) -> Dict[str, Any]:
        last_text = ""
        last_exc = None

        for attempt in range(self.max_attempts):
            await self.lim.acquire(1)

            try:
                r = await self.http.get(path, params=params, headers=self._headers())
            except Exception as e:
                last_exc = e
                if attempt >= self.max_attempts - 1:
                    break
                sleep_s = self.base_sleep_sec * (2 ** attempt) * random.uniform(0.90, 1.25)
                await asyncio.sleep(sleep_s)
                continue

            txt = (r.text or "")[:2000]

            if r.status_code == 200:
                return r.json()

            if r.status_code in (429, 500, 502, 503, 504):
                last_text = txt
                if attempt >= self.max_attempts - 1:
                    break

                retry_after = 0.0
                try:
                    retry_after = float(r.headers.get("retry-after") or 0.0)
                except Exception:
                    retry_after = 0.0

                sleep_s = max(retry_after, self.base_sleep_sec * (2 ** attempt))
                sleep_s *= random.uniform(0.95, 1.25)
                await asyncio.sleep(sleep_s)
                continue

            raise RuntimeError(f"0x {label} {r.status_code}: {txt}")

        if last_exc is not None and not last_text:
            raise RuntimeError(f"0x {label} network_error: {last_exc}")

        raise RuntimeError(f"0x {label} failed_after_retries: {last_text or 'unknown_error'}")

    async def price_allowance_holder(
        self,
        *,
        chain_id: int,
        buy_token: str,
        sell_token: str,
        sell_amount: int,
        taker: Optional[str] = None,
        slippage_bps: int,
    ) -> Dict[str, Any]:
        params = {
            "chainId": chain_id,
            "buyToken": buy_token,
            "sellToken": sell_token,
            "sellAmount": str(sell_amount),
            "slippageBps": int(slippage_bps),
        }
        if taker:
            params["taker"] = taker

        return await self._get_json(
            "/swap/allowance-holder/price",
            params=params,
            label="allowance-holder/price",
        )

    async def price_v1(
        self,
        *,
        chain_id: int,
        buy_token: str,
        sell_token: str,
        sell_amount: int,
        slippage_bps: int,
    ) -> Dict[str, Any]:
        params = {
            "chainId": chain_id,
            "buyToken": buy_token,
            "sellToken": sell_token,
            "sellAmount": str(sell_amount),
            "slippageBps": int(slippage_bps),
        }
        return await self._get_json(
            "/swap/v1/price",
            params=params,
            label="swap/v1/price",
        )

    async def quote_allowance_holder(
        self,
        *,
        chain_id: int,
        buy_token: str,
        sell_token: str,
        sell_amount: int,
        taker: str,
        slippage_bps: int,
    ) -> Dict[str, Any]:
        params = {
            "chainId": chain_id,
            "buyToken": buy_token,
            "sellToken": sell_token,
            "sellAmount": str(sell_amount),
            "taker": taker,
            "slippageBps": int(slippage_bps),
        }
        return await self._get_json(
            "/swap/allowance-holder/quote",
            params=params,
            label="allowance-holder/quote",
        )

# -------------------- RPC failover --------------------

class RPCManager:
    def __init__(self, rpc_urls: List[str]):
        self.urls = [u.strip() for u in rpc_urls if u.strip()]
        if not self.urls:
            raise ValueError("Nessun RPC URL fornito")
        self.idx = 0
        self.w3 = Web3(Web3.HTTPProvider(self.urls[self.idx], request_kwargs={"timeout": 15}))

    def current_url(self) -> str:
        return self.urls[self.idx]

    def switch_next(self):
        self.idx = (self.idx + 1) % len(self.urls)
        self.w3 = Web3(Web3.HTTPProvider(self.urls[self.idx], request_kwargs={"timeout": 15}))
        M_RPC_INDEX.set(self.idx)

    async def healthcheck(self) -> bool:
        try:
            ok = await asyncio.to_thread(self.w3.is_connected)
            if not ok:
                return False
            _ = await asyncio.to_thread(lambda: self.w3.eth.block_number)
            return True
        except Exception:
            return False

class MultiChainRPCManager:
    def __init__(self, chain_cfgs: Dict[str, ChainCfg], fallback_urls: Optional[List[str]] = None):
        self.managers: Dict[str, RPCManager] = {}
        self.fallback_urls = [u.strip() for u in (fallback_urls or []) if str(u).strip()]
        for chain, cfg in (chain_cfgs or {}).items():
            urls = [u.strip() for u in (getattr(cfg, "rpc_urls", []) or []) if str(u).strip()]
            if not urls:
                env_key = f"MODE_LIVE_RPC_URLS_{str(chain).upper()}"
                env_urls = os.getenv(env_key, "").strip()
                if env_urls:
                    urls = [u.strip() for u in env_urls.split(",") if u.strip()]
            if not urls and self.fallback_urls and len(chain_cfgs or {}) == 1:
                urls = list(self.fallback_urls)
            if urls:
                self.managers[str(chain).lower()] = RPCManager(urls)

    def has_chain(self, chain: str) -> bool:
        return str(chain).lower() in self.managers

    def get(self, chain: str) -> Optional[RPCManager]:
        return self.managers.get(str(chain).lower())

    def current_url(self, chain: str) -> str:
        mgr = self.get(chain)
        return mgr.current_url() if mgr else ""

    def switch_next(self, chain: str):
        mgr = self.get(chain)
        if mgr:
            mgr.switch_next()

    async def healthcheck(self, chain: str) -> bool:
        mgr = self.get(chain)
        if not mgr:
            return False
        return await mgr.healthcheck()

    @property
    def w3(self) -> Web3:
        if not self.managers:
            raise RuntimeError("Nessun RPC manager disponibile")
        return next(iter(self.managers.values())).w3


# -------------------- ERC20 ABI --------------------


ERC20_ABI = json.loads("""
[
  {"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"type":"function"},
  {"constant":true,"inputs":[{"name":"owner","type":"address"},{"name":"spender","type":"address"}],"name":"allowance","outputs":[{"name":"","type":"uint256"}],"type":"function"},
  {"constant":false,"inputs":[{"name":"spender","type":"address"},{"name":"amount","type":"uint256"}],"name":"approve","outputs":[{"name":"","type":"bool"}],"type":"function"},
  {"constant":true,"inputs":[{"name":"account","type":"address"}],"name":"balanceOf","outputs":[{"name":"","type":"uint256"}],"type":"function"}
]
""")


# -------------------- market features --------------------

def best_pair_by_liquidity(pairs: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    best = None
    best_liq = -1.0
    for p in pairs:
        liq = safe_float(((p.get("liquidity") or {}).get("usd")))
        if liq is None:
            continue
        if liq > best_liq:
            best_liq = liq
            best = p
    return best

def features_from_pair(pair: Dict[str, Any]) -> Dict[str, Any]:
    liq = safe_float(((pair.get("liquidity") or {}).get("usd")))
    fdv = safe_float(pair.get("fdv"))
    px = safe_float(pair.get("priceUsd"))
    vol5 = safe_float(((pair.get("volume") or {}).get("m5")))
    tx5o = ((pair.get("txns") or {}).get("m5")) or {}
    buys = int(tx5o.get("buys") or 0)
    sells = int(tx5o.get("sells") or 0)
    tx5 = buys + sells
    pair_addr = pair.get("pairAddress") or pair.get("pairId") or ""
    return {
        "pair_address": pair_addr,
        "liq_usd": liq,
        "fdv": fdv,
        "price_usd": px,
        "vol_m5": vol5,
        "txns_m5": tx5,
        "buys_m5": buys,
        "sells_m5": sells,
    }

def passes_filters(cfg: Config, f: Dict[str, Any]) -> bool:
    if f["price_usd"] is None or f["price_usd"] <= 0:
        return False
    if f["liq_usd"] is None or f["liq_usd"] < cfg.min_liquidity_usd:
        return False
    if f["vol_m5"] is None or f["vol_m5"] < cfg.min_volume_5m_usd:
        return False
    if f["txns_m5"] < cfg.min_txns_5m:
        return False
    if cfg.max_fdv_usd is not None:
        if f["fdv"] is None or f["fdv"] > cfg.max_fdv_usd:
            return False
    return True

def score_pair(f: Dict[str, Any]) -> float:
    liq = max(f["liq_usd"] or 1.0, 1.0)
    vol = max(f["vol_m5"] or 1.0, 1.0)
    tx = max(f["txns_m5"], 1)
    fdv = max(f["fdv"] or 1.0, 1.0)
    return math.log10(liq)*1.7 + math.log10(vol)*1.5 + math.log10(tx)*1.2 - math.log10(fdv)*0.7


# -------------------- price cache --------------------

class PriceCache:
    def __init__(self, maxlen: int = 1800):
        self.data: Dict[str, deque] = {}
        self.maxlen = maxlen

    def push(self, key: str, ts: int, price: float):
        q = self.data.setdefault(key, deque(maxlen=self.maxlen))
        q.append((ts, price))

    def last(self, key: str) -> Optional[float]:
        q = self.data.get(key)
        return q[-1][1] if q else None

    def ret_over(self, key: str, now: int, lookback_sec: int) -> Optional[float]:
        q = self.data.get(key)
        if not q or len(q) < 2:
            return None
        target = now - lookback_sec
        old = None
        new = q[-1][1]
        for t, p in reversed(q):
            if t <= target:
                old = p
                break
        if old is None or old <= 0:
            return None
        return (new / old) - 1.0

    def volatility_over(self, key: str, now: int, lookback_sec: int) -> Optional[float]:
        q = self.data.get(key)
        if not q or len(q) < 10:
            return None
        target = now - lookback_sec
        pts = [(t, p) for (t, p) in q if t >= target]
        if len(pts) < 10:
            return None
        rets = []
        for i in range(1, len(pts)):
            p0 = pts[i-1][1]
            p1 = pts[i][1]
            if p0 > 0 and p1 > 0:
                rets.append((p1 / p0) - 1.0)
        if len(rets) < 5:
            return None
        mean = sum(rets)/len(rets)
        var = sum((r-mean)**2 for r in rets)/max(1, (len(rets)-1))
        return math.sqrt(var)


    def speed_pct_per_sec(self, key: str, now: int, lookback_sec: int) -> Optional[float]:
        """Return recent %/sec over lookback_sec: (p_now/p_old - 1) / dt."""
        q = self.data.get(key)
        if not q or len(q) < 2:
            return None
        target = now - lookback_sec
        old = None
        old_t = None
        new_t, new_p = q[-1]
        for t, p in reversed(q):
            if t <= target:
                old = p
                old_t = t
                break
        if old is None or old_t is None or old <= 0 or new_p <= 0:
            return None
        dt = max(1, int(new_t - old_t))
        return (new_p / old - 1.0) / dt


# -------------------- positions --------------------

@dataclasses.dataclass
class Position:
    chain: str
    token: str
    pair: str
    entry_px: float
    qty: float
    entry_ts: int
    peak_px: float
    avg_px: float
    pyramids_done: int = 0
    tp1_done: bool = False

    # Dip/Rebound trailing (stepped)
    trail_armed_ts: int = 0           # quando attiva il trailing
    trail_step_n: int = 0             # step corrente del trailing
    trail_stop_px: float = 0.0        # stop price corrente (ratcheting, non scende mai)
    trail_breach_n: int = 0

# -------------------- executors --------------------

class Executor:
    def _paper_mev_bps(self, *, tax_bps: float, slip_bps: int) -> float:
        strat = self.cfg.strategy if isinstance(getattr(self.cfg, "strategy", None), dict) else {}
        if str(getattr(self.cfg, "mode", "paper")).lower() != "paper":
            return 0.0
        if not _as_bool(strat.get("paper_mev_enabled", False)):
            return 0.0

        base = float(strat.get("paper_mev_base_bps", 0) or 0)
        cap = float(strat.get("paper_mev_cap_bps", 0) or 0)
        tax_ref = float(strat.get("paper_mev_tax_ref_bps", 0) or 0)
        per_tax = float(strat.get("paper_mev_per_tax_bps", 0) or 0)
        per_slip = float(strat.get("paper_mev_per_slip_bps", 0) or 0)
        mode_min = float(strat.get("paper_mev_mode_min", 0.35) or 0.35)
        mode_max = float(strat.get("paper_mev_mode_max", 0.95) or 0.95)

        tax_excess = max(0.0, float(tax_bps) - float(tax_ref))
        mev = base + (per_tax * tax_excess) + (per_slip * float(slip_bps))
        if cap > 0:
            mev = min(mev, cap)
        mev *= random.uniform(mode_min, mode_max)
        return float(max(0.0, mev))

    async def buy(self, *, chain: str, token: str, pair: str, px_usd: float, usd_amount: float, slippage_bps: int) -> Tuple[float, float, float]:
        k = make_token_key(chain, token)
        meta = self.store.get_state("token_meta_cache", {}) or {}
        rec = (meta.get(k) or {}) if isinstance(meta, dict) else {}

        buy_tax = float(rec.get("buy_tax_bps") or 0.0)
        sell_tax = float(rec.get("sell_tax_bps") or 0.0)
        tax_bps = max(buy_tax, sell_tax)

        mev_bps = self._paper_mev_bps(tax_bps=tax_bps, slip_bps=int(slippage_bps))
        eff_slip = float(slippage_bps) + float(mev_bps)

        slip = random.triangular(0, eff_slip, eff_slip * 0.35) / 10000.0
        fill_px = float(px_usd) * (1.0 + slip)

        qty = float(usd_amount) / max(float(fill_px), 1e-18)

        # apply buy tax as fewer tokens received
        if buy_tax > 0:
            qty *= max(0.0, 1.0 - (buy_tax / 10000.0))

        spent = float(usd_amount)

        self.store.log_trade(
            ts=now_ts(),
            mode=self.cfg.mode,
            chain=normalize_chain_id(chain, default="base"),
            token=canonical_token_ref(chain, token),
            pair=pair,
            side="BUY",
            px_usd=float(fill_px),
            qty=float(qty),
            usd_value=float(spent),
            reason="entry_paper",
        )
        M_TRADES.labels(side="BUY", reason="entry").inc()
        return float(qty), float(fill_px), float(spent)

    async def sell(self, *, position: "Position", px_usd: float, qty: float, reason: str, slippage_bps: int) -> Tuple[float, float]:
        k = make_token_key(position.chain, position.token)
        meta = self.store.get_state("token_meta_cache", {}) or {}
        rec = (meta.get(k) or {}) if isinstance(meta, dict) else {}

        buy_tax = float(rec.get("buy_tax_bps") or 0.0)
        sell_tax = float(rec.get("sell_tax_bps") or 0.0)
        tax_bps = max(buy_tax, sell_tax)

        mev_bps = self._paper_mev_bps(tax_bps=tax_bps, slip_bps=int(slippage_bps))
        eff_slip = float(slippage_bps) + float(mev_bps)

        slip = random.triangular(0, eff_slip, eff_slip * 0.35) / 10000.0
        fill_px = float(px_usd) * (1.0 - slip)

        usd_got = float(qty) * max(float(fill_px), 1e-18)

        # apply sell tax as less USDC received (approx)
        if sell_tax > 0:
            usd_got *= max(0.0, 1.0 - (sell_tax / 10000.0))

        self.store.log_trade(
            ts=now_ts(),
            mode=self.cfg.mode,
            chain=normalize_chain_id(position.chain, default="base"),
            token=canonical_token_ref(position.chain, position.token),
            pair=position.pair,
            side="SELL",
            px_usd=float(fill_px),
            qty=float(qty),
            usd_value=float(usd_got),
            reason=reason,
        )
        M_TRADES.labels(side="SELL", reason=reason).inc()
        return float(usd_got), float(fill_px)

class PaperExecutor(Executor):
    def __init__(self, cfg: Config, store: Store):
        self.cfg = cfg
        self.store = store

    async def buy(self, *, chain: str, token: str, pair: str, px_usd: float, usd_amount: float, slippage_bps: int) -> Tuple[float, float, float]:
        max_slip = max(0.0, slippage_bps / 10000.0)

        # slippage "possibile": spesso piccolo, a volte vicino al max (come in live entro tolleranza)
        slip = random.triangular(0.0, max_slip, max_slip * 0.25)

        fill_px = px_usd * (1 + slip)
        usd_spent = float(usd_amount)
        qty = usd_spent / max(fill_px, 1e-18)

        self.store.log_trade(ts=now_ts(), mode=self.cfg.mode, chain=chain, token=token, pair=pair,
                             side="BUY", px_usd=float(fill_px), qty=float(qty), usd_value=float(usd_spent), reason="entry_paper")
        M_TRADES.labels(side="BUY", reason="entry").inc()
        return float(qty), float(fill_px), float(usd_spent)

    async def sell(self, *, position: Position, px_usd: float, qty: float, reason: str, slippage_bps: int) -> Tuple[float, float]:
        max_slip = max(0.0, slippage_bps / 10000.0)
        slip = random.triangular(0.0, max_slip, max_slip * 0.25)
        fill_px = px_usd * (1 - slip)
        usd_got = float(qty) * max(fill_px, 1e-18)

        self.store.log_trade(ts=now_ts(), mode=self.cfg.mode, chain=position.chain, token=position.token, pair=position.pair,
                             side="SELL", px_usd=float(fill_px), qty=float(qty), usd_value=float(usd_got), reason=reason)
        M_TRADES.labels(side="SELL", reason=reason).inc()
        return float(usd_got), float(fill_px)

class LiveZeroXExecutor(Executor):
    def __init__(self, cfg: Config, store: Store, zerox: ZeroXClient, rpcm: MultiChainRPCManager, account: Account):
        self.cfg = cfg
        self.store = store
        self.zerox = zerox
        self.rpcm = rpcm
        self.account = account
        self.addr = account.address
        self._dec_cache: Dict[str, int] = {}

    def w3(self, chain: str) -> Web3:
        mgr = self.rpcm.get(chain)
        if mgr is None:
            raise RuntimeError(f"unsupported_chain_rpc:{chain}")
        return mgr.w3

    async def _to_thread(self, fn, *args, **kwargs):
        return await asyncio.to_thread(fn, *args, **kwargs)

    async def _decimals(self, chain: str, token: str) -> int:
        t = make_token_key(chain, token)
        if t in self._dec_cache:
            return self._dec_cache[t]
        c = self.w3(chain).eth.contract(address=Web3.to_checksum_address(token), abi=ERC20_ABI)
        d = int(await self._to_thread(lambda: c.functions.decimals().call()))
        self._dec_cache[t] = d
        return d

    async def _balance(self, chain: str, token: str) -> int:
        c = self.w3(chain).eth.contract(address=Web3.to_checksum_address(token), abi=ERC20_ABI)
        return int(await self._to_thread(lambda: c.functions.balanceOf(self.addr).call()))

    async def _allowance(self, chain: str, token: str, spender: str) -> int:
        c = self.w3(chain).eth.contract(address=Web3.to_checksum_address(token), abi=ERC20_ABI)
        return int(await self._to_thread(lambda: c.functions.allowance(self.addr, Web3.to_checksum_address(spender)).call()))

    async def _approve_if_needed(self, chain: str, token: str, spender: str, amount: int) -> None:
        cur = await self._allowance(chain, token, spender)
        if cur >= amount:
            return
        c = self.w3(chain).eth.contract(address=Web3.to_checksum_address(token), abi=ERC20_ABI)
        tx = c.functions.approve(Web3.to_checksum_address(spender), int(amount)).build_transaction({
            "from": self.addr,
            "nonce": await self._to_thread(lambda: self.w3(chain).eth.get_transaction_count(self.addr, "pending")),
            "gas": 120000,
            "maxFeePerGas": await self._to_thread(lambda: self.w3(chain).eth.gas_price * 2),
            "maxPriorityFeePerGas": await self._to_thread(lambda: max(1, int(self.w3(chain).eth.gas_price * 0.1))),
            "chainId": int(self.cfg.chains[chain].chain_id),
        })
        await self._send_tx_and_wait(chain, tx, op="approve")

    async def _send_tx_and_wait(self, chain: str, tx: Dict[str, Any], op: str) -> None:
        w3 = self.w3(chain)
        tx = dict(tx)
        tx.setdefault("from", self.addr)
        tx.setdefault("nonce", await self._to_thread(lambda: w3.eth.get_transaction_count(self.addr, "pending")))
        tx.setdefault("chainId", int(self.cfg.chains[chain].chain_id))
        gp = await self._to_thread(lambda: w3.eth.gas_price)
        tx.setdefault("maxFeePerGas", int(gp * 2))
        tx.setdefault("maxPriorityFeePerGas", max(1, int(gp * 0.1)))
        if "gas" not in tx:
            try:
                est = await self._to_thread(lambda: w3.eth.estimate_gas(tx))
                tx["gas"] = int(est * 1.20)
            except Exception:
                tx["gas"] = 500000
        await self._to_thread(lambda: w3.eth.call(tx, "latest"))
        signed = self.account.sign_transaction(tx)
        raw = getattr(signed, "rawTransaction", None) or getattr(signed, "raw_transaction", None)
        if raw is None:
            raise RuntimeError("SignedTransaction senza raw tx (eth_account mismatch)")
        with M_LAT.labels(op=op).time():
            txh = await self._to_thread(lambda: w3.eth.send_raw_transaction(raw))
            rec = await self._to_thread(lambda: w3.eth.wait_for_transaction_receipt(txh, timeout=180))
        if int(rec.get("status", 0)) != 1:
            raise RuntimeError(f"{op} failed")

    async def buy(self, *, chain: str, token: str, pair: str, px_usd: float, usd_amount: float, slippage_bps: int) -> Tuple[float, float, float]:
        ch = self.cfg.chains.get(chain)
        if ch is None:
            raise RuntimeError(f"unsupported_chain_for_live_buy:{chain}")
        usdc = ch.usdc
        usdc_dec = await self._decimals(chain, usdc)
        sell_amount = int(usd_amount * (10 ** usdc_dec))
        q = await self.zerox.quote_allowance_holder(
            chain_id=ch.chain_id, buy_token=token, sell_token=usdc,
            sell_amount=sell_amount, taker=self.addr, slippage_bps=slippage_bps
        )
        spender = (((q.get("issues") or {}).get("allowance") or {}).get("spender")) or q.get("allowanceTarget")
        if not spender:
            raise RuntimeError("spender mancante")
        await self._approve_if_needed(chain, usdc, spender, sell_amount)
        txo = q.get("transaction") if isinstance(q.get("transaction"), dict) else q
        to = txo.get("to"); data = txo.get("data"); value = int(txo.get("value") or 0)
        if not to or not data:
            raise RuntimeError("quote senza to/data")
        tx = {"to": Web3.to_checksum_address(to), "data": data, "value": value}
        usdc_before = await self._balance(chain, usdc)
        tok_before = await self._balance(chain, token)
        await self._send_tx_and_wait(chain, tx, op="swap_buy")
        usdc_after = await self._balance(chain, usdc)
        tok_after = await self._balance(chain, token)
        spent = max(0, usdc_before - usdc_after)
        got = max(0, tok_after - tok_before)
        if got <= 0:
            raise RuntimeError("BUY got=0 (blacklist/tax?)")
        spent_usd = spent / (10 ** usdc_dec)
        tok_dec = await self._decimals(chain, token)
        qty = float(got) / (10 ** tok_dec)
        fill_px = spent_usd / max(qty, 1e-18)
        self.store.log_trade(ts=now_ts(), mode=self.cfg.mode, chain=chain, token=token, pair=pair,
                             side="BUY", px_usd=float(fill_px), qty=float(qty), usd_value=float(spent_usd), reason="entry_live")
        M_TRADES.labels(side="BUY", reason="entry").inc()
        return float(qty), float(fill_px), float(spent_usd)

    async def sell(self, *, position: Position, px_usd: float, qty: float, reason: str, slippage_bps: int) -> Tuple[float, float]:
        ch = self.cfg.chains.get(position.chain)
        if ch is None:
            raise RuntimeError(f"unsupported_chain_for_live_sell:{position.chain}")
        usdc = ch.usdc
        token = position.token
        chain = position.chain
        tok_dec = await self._decimals(chain, token)
        sell_amount = int(qty * (10 ** tok_dec))
        q = await self.zerox.quote_allowance_holder(
            chain_id=ch.chain_id, buy_token=usdc, sell_token=token,
            sell_amount=sell_amount, taker=self.addr, slippage_bps=slippage_bps
        )
        spender = (((q.get("issues") or {}).get("allowance") or {}).get("spender")) or q.get("allowanceTarget")
        if not spender:
            raise RuntimeError("spender mancante")
        await self._approve_if_needed(chain, token, spender, sell_amount)
        txo = q.get("transaction") if isinstance(q.get("transaction"), dict) else q
        to = txo.get("to"); data = txo.get("data"); value = int(txo.get("value") or 0)
        if not to or not data:
            raise RuntimeError("quote senza to/data")
        tx = {"to": Web3.to_checksum_address(to), "data": data, "value": value}
        tok_before = await self._balance(chain, token)
        usdc_before = await self._balance(chain, usdc)
        await self._send_tx_and_wait(chain, tx, op="swap_sell")
        tok_after = await self._balance(chain, token)
        usdc_after = await self._balance(chain, usdc)
        sold = max(0, tok_before - tok_after)
        got = max(0, usdc_after - usdc_before)
        if sold <= 0:
            raise RuntimeError("SELL sold=0")
        usdc_dec = await self._decimals(chain, usdc)
        tok_dec = await self._decimals(chain, token)
        usd_got = float(got) / (10 ** usdc_dec)
        qty_sold = float(sold) / (10 ** tok_dec)
        fill_px = usd_got / max(qty_sold, 1e-18)
        self.store.log_trade(ts=now_ts(), mode=self.cfg.mode, chain=position.chain, token=position.token, pair=position.pair,
                             side="SELL", px_usd=float(fill_px), qty=float(qty_sold), usd_value=float(usd_got), reason=reason)
        M_TRADES.labels(side="SELL", reason=reason).inc()
        return float(usd_got), float(fill_px)

class MicrocapBot:

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.log = logging.getLogger("microcap_bot_v4")

        self.store = Store("bot.db")

        # --- FIRST TRADE OUTCOME / optional first-loss ban (config aware) ---
        self.first_trade_outcome = self.store.get_state("first_trade_outcome", {}) or {}
        if not isinstance(self.first_trade_outcome, dict):
            self.first_trade_outcome = {}

        strat0 = self.cfg.strategy if isinstance(getattr(self.cfg, "strategy", None), dict) else {}
        self.first_trade_loss_ban_enabled = _as_bool(strat0.get("ban_token_after_first_trade_loss", False))
        self.first_trade_loss_ban_min_pnl_pct = float(
            safe_float(strat0.get("ban_token_after_first_trade_loss_min_pnl_pct", -0.05)) or -0.05
        )

        self.bad_tokens = set()
        if self.first_trade_loss_ban_enabled:
            for kk, vv in self.first_trade_outcome.items():
                if not isinstance(vv, dict):
                    continue
                if str(vv.get("status")) != "loss":
                    continue
                pnl_pct0 = float(safe_float(vv.get("pnl_pct")) or 0.0)
                if pnl_pct0 <= float(self.first_trade_loss_ban_min_pnl_pct):
                    self.bad_tokens.add(kk)
        
        self.dex = DexScreenerClient()
        self.cache = PriceCache()
        self.notify = Notifier()

        # --- Narrative sources (Telegram client + NewsAPI + X) ---
        self.tg: TelegramNarrativeClient | None = None
        self.news: NewsApiClient | None = None
        self.x: XApiClient | None = None
        try:
            strat = self.cfg.strategy if isinstance(getattr(self.cfg, 'strategy', None), dict) else {}
            h = (strat or {}).get('health') or {}
            social = (h or {}).get('social') or {}
            if isinstance(social, dict):
                tg_cfg = social.get('telegram') or {}
                if _as_bool(tg_cfg.get('enabled', False)):
                    self.tg = TelegramNarrativeClient(
                        api_id=int(tg_cfg.get('api_id')),
                        api_hash=str(tg_cfg.get('api_hash')),
                        session=str(tg_cfg.get('session', 'microcap_bot')),
                        channels=list(tg_cfg.get('channels') or []),
                    )

                news_cfg = social.get('newsapi') or {}
                if _as_bool(news_cfg.get('enabled', False)) and str(news_cfg.get('api_key', '')).strip():
                    self.news = NewsApiClient(api_key=str(news_cfg.get('api_key')).strip())

                x_cfg = social.get('x') or {}
                x_token = str(x_cfg.get('bearer_token') or os.getenv('X_BEARER_TOKEN', '')).strip()
                if _as_bool(x_cfg.get('enabled', False)) and x_token:
                    self.x = XApiClient(bearer_token=x_token)
        except Exception as e:
            self.log.warning('Narrative init skipped: %s', e)

        self.watchlist = self.store.load_watchlist()
        self.positions = self.store.load_positions()
        # --- TOKEN PnL + BANS (persistenti su bot.db) ---
        self.token_pnl_usd = self.store.get_state("token_pnl_usd", {}) or {}
        if not isinstance(self.token_pnl_usd, dict):
            self.token_pnl_usd = {}

        self.banned_tokens = self.store.get_state("banned_tokens", {}) or {}
        if not isinstance(self.banned_tokens, dict):
            self.banned_tokens = {}

        self.banned_set = set(self.banned_tokens.keys())

        # --- new persistent states ---
        self.loss_streak = self.store.get_state("loss_streak", {}) or {}
        if not isinstance(self.loss_streak, dict):
            self.loss_streak = {}

        self.token_meta_cache = self.store.get_state("token_meta_cache", {}) or {}
        if not isinstance(self.token_meta_cache, dict):
            self.token_meta_cache = {}

        # prune expired bans at boot
        try:
            self._prune_expired_bans()
        except Exception:
            pass

        self.fail_count = 0
        self.pause_until = 0
        self._newsapi_pause_until = 0
        self._newsapi_backoff_sec = 0
        self._newsapi_last_429_notify_ts = 0
        self.day_key = self._day_key()
        self.day_start_equity = float(self.store.get_state("day_start_equity", cfg.start_equity_usd))
        self.peak_equity = float(self.store.get_state("peak_equity", cfg.start_equity_usd))
        self.day_entries = int(self.store.get_state("day_entries", 0))
        self.day_exits = int(self.store.get_state("day_exits", 0))

        # LIVE
        self.mode = cfg.mode
        self.zerox: Optional[ZeroXClient] = None
        self.rpcm: Optional[MultiChainRPCManager] = None
        self.account: Optional[Account] = None

        self.solana_precheck_rps = max(
                0.2,
                float(strat0.get("solana_precheck_rps", 0.5) or 0.5),
        )
        self.solana_precheck_lim = AsyncRateLimiter(
                capacity=1,
                period_sec=max(0.05, 1.0 / self.solana_precheck_rps),
        )

        # cash: paper = USD virtuale; live = USDC reale letto periodicamente
        self.cash = float(self.store.get_state("cash", cfg.start_equity_usd))
        if self.mode == "live":
            fallback_rpc_urls = os.getenv("MODE_LIVE_RPC_URLS", "").strip()
            pk = os.getenv("PRIVATE_KEY", "").strip()
            zkey = os.getenv("ZEROEX_API_KEY", "").strip()
            if not (pk and zkey):
                raise RuntimeError("LIVE: servono PRIVATE_KEY e ZEROEX_API_KEY nel .env")
            fallback_list = [u.strip() for u in fallback_rpc_urls.split(",") if u.strip()] if fallback_rpc_urls else []
            self.rpcm = MultiChainRPCManager(cfg.chains, fallback_urls=fallback_list)
            if not self.rpcm.managers:
                raise RuntimeError("LIVE: nessun RPC configurato. Aggiungi rpc_urls per chain nel config oppure MODE_LIVE_RPC_URLS[_CHAIN] nel .env")
            self.account = Account.from_key(pk)
            self.zerox = ZeroXClient(zkey)
            self.exec: Executor = LiveZeroXExecutor(cfg, self.store, self.zerox, self.rpcm, self.account)
        else:
            self.exec = PaperExecutor(cfg, self.store)

        self._lock = asyncio.Lock()
        
        # Bootstrap in-memory price history from DB snapshots (helps ENTRY decisions right after restarts)
        self.bootstrap_cache_from_snapshots()

    def _day_key(self) -> str:
        return time.strftime("%Y-%m-%d", time.localtime())

    def key(self, chain: str, token: str) -> str:
        return make_token_key(chain, token)


    def bootstrap_cache_from_snapshots(self) -> None:
        """Prefill PriceCache from snapshots so dip/speed computations work immediately after restarts."""
        try:
            if not getattr(self.cfg, "snapshot_enabled", False):
                return

            strat = (self.cfg.strategy or {}) if isinstance(getattr(self.cfg, "strategy", None), dict) else {}
            dip_w = int(strat.get("dip_window_sec", 90) or 90)
            sh_w = int(strat.get("short_window_sec", 15) or 15)
            speed5_w = max(5, sh_w // 3)

            # enough history to cover dip + some buffer
            bootstrap_sec = int(strat.get("bootstrap_history_sec", max(180, dip_w * 4, sh_w * 12, 300)) or max(180, dip_w * 4, sh_w * 12, 300))

            now = now_ts()
            since = now - int(bootstrap_sec)

            keys = list(dict.fromkeys(list(self.watchlist.keys()) + list(self.positions.keys())))
            if not keys:
                return

            total = 0
            # SQLite has a variable limit; chunk keys
            CHUNK = 900
            for i in range(0, len(keys), CHUNK):
                chunk = keys[i:i+CHUNK]
                qm = ",".join(["?"] * len(chunk))
                cur = self.store.conn.cursor()
                cur.execute(
                    f"SELECT key, ts, price_usd FROM snapshots WHERE key IN ({qm}) AND ts>=? AND price_usd>0 ORDER BY key, ts",
                    (*chunk, int(since)),
                )
                for k, ts, px in cur.fetchall():
                    self.cache.push(str(k), int(ts), float(px))
                    total += 1

            if total:
                self.log.info("Bootstrap cache: loaded %d price points (%d keys, last %ds)", total, len(keys), bootstrap_sec)
        except Exception as e:
            self.log.warning("Bootstrap cache failed: %s", e)
    def equity_total_est(self) -> float:
        return self.cash + self.total_exposure_usd()

    def total_exposure_usd(self) -> float:
        exp = 0.0
        for k, p in self.positions.items():
            px = self.cache.last(k)
            if px:
                exp += p.qty * px
        return exp

    def position_size_usd(self) -> float:
        eq = self.equity_total_est()
        ratio = max(eq / max(self.cfg.start_equity_usd, 1e-9), 0.0)
        pct = self.cfg.base_risk_pct * (ratio ** self.cfg.scale_exponent)
        pct = min(pct, self.cfg.max_risk_pct)
        return eq * pct

    def position_size_usd_dynamic(
        self,
        *,
        k: str,
        base_usd: float,
        slip_pre_bps: int,
        pre_meta: Dict[str, Any],
        health: Dict[str, Any],
        signal_score: Optional[float] = None,
    ) -> Tuple[float, Dict[str, Any]]:
        strat = self.cfg.strategy if isinstance(getattr(self.cfg, "strategy", None), dict) else {}

        min_mult = float(strat.get("pos_sizing_min_mult", 0.35) or 0.35)
        max_mult = float(strat.get("pos_sizing_max_mult", 1.35) or 1.35)

        tax_knee = float(strat.get("pos_sizing_tax_knee_bps", 200) or 200)
        rt_knee = float(strat.get("pos_sizing_rt_knee_pct", 0.02) or 0.02)
        slip_knee = float(strat.get("pos_sizing_slip_knee_bps", 800) or 800)

        boost_max = float(strat.get("pos_sizing_boost_max", 0.30) or 0.30)
        flow_thr = float(strat.get("pos_sizing_flow_thr", 0.70) or 0.70)
        liq_thr = float(strat.get("pos_sizing_liq_thr", 0.70) or 0.70)

        loss_mul = float(strat.get("pos_sizing_loss_streak_mul", 0.70) or 0.70)
        signal_boost_max = float(strat.get("entry_signal_boost_max", 0.30) or 0.30)

        buy_tax = float((pre_meta or {}).get("buy_tax_bps") or 0.0)
        sell_tax = float((pre_meta or {}).get("sell_tax_bps") or 0.0)
        tax_bps = max(buy_tax, sell_tax)

        rt_loss = float((pre_meta or {}).get("roundtrip_loss_pct") or 0.0)

        # --- cost penalty ---
        pen = 1.0
        if tax_knee > 0:
            over = max(0.0, tax_bps - tax_knee)
            pen *= 1.0 / (1.0 + (over / tax_knee) * 1.25)

        if rt_knee > 0:
            over = max(0.0, rt_loss - rt_knee)
            pen *= 1.0 / (1.0 + (over / rt_knee) * 1.50)

        if slip_knee > 0:
            over = max(0.0, float(slip_pre_bps) - slip_knee)
            pen *= 1.0 / (1.0 + (over / slip_knee) * 1.00)

        # --- quality boost ---
        score_flow = float((health or {}).get("score_flow") or 0.0)
        score_liq = float((health or {}).get("score_liq_stability") or (health or {}).get("score_liquidity") or 0.0)

        q = min(score_flow, score_liq)
        boost = 1.0
        if q >= flow_thr and score_liq >= liq_thr:
            thr = max(flow_thr, liq_thr)
            span = max(1e-9, 1.0 - thr)
            t = clamp((q - thr) / span, 0.0, 1.0)
            boost = 1.0 + boost_max * t

        # --- loss streak multiplier (robust vs old int-based state) ---
        ls_raw = (self.loss_streak or {}).get(k)
        if isinstance(ls_raw, dict):
            n_ls = int(ls_raw.get("n") or 0)
        else:
            n_ls = int(ls_raw or 0)

        streak_mult = (loss_mul ** n_ls) if n_ls > 0 else 1.0

        # --- entry signal multiplier ---
        signal_mult = 1.0
        if signal_score is not None:
            s = clamp(float(signal_score), 0.0, 1.0)
            signal_mult = clamp(0.85 + (s * signal_boost_max), 0.70, 1.0 + signal_boost_max)

        score_total = float((health or {}).get("score_total") or 0.0)
        att_used = float((health or {}).get("factor_attention_used") or 1.0)
        hype_used = float((health or {}).get("factor_hype_used") or 1.0)
        soc_used = float((health or {}).get("factor_social_used") or 1.0)

        health_mult = 1.0
        if score_total > 0:
            health_mult *= clamp(0.82 + (score_total * 0.40), 0.75, 1.18)

        social_combo = max(1e-9, att_used * hype_used * soc_used) ** (1.0 / 3.0)
        health_mult *= clamp(social_combo, 0.85, 1.10)

        mult = clamp(pen * boost * streak_mult * signal_mult * health_mult, min_mult, max_mult)

        usd = float(base_usd) * float(mult)
        meta = {
            "base_usd": float(base_usd),
            "mult": float(mult),
            "pen": float(pen),
            "boost": float(boost),
            "streak_mult": float(streak_mult),
            "signal_mult": float(signal_mult),
            "signal_score": None if signal_score is None else float(signal_score),
            "tax_bps": float(tax_bps),
            "rt_loss_pct": float(rt_loss),
            "slip_pre_bps": int(slip_pre_bps),
            "score_flow": float(score_flow),
            "score_liq": float(score_liq),
            "loss_streak_n": int(n_ls),
        }
        return float(usd), meta

    def can_trade_today(self) -> bool:
        dk = self._day_key()
        if dk != self.day_key:
            self.day_key = dk
            self.day_start_equity = self.equity_total_est()
            self.store.set_state("day_start_equity", self.day_start_equity)
            self.store.set_state("day_key", dk)
            # reset daily trade counters
            self.day_entries = 0
            self.day_exits = 0
            self.store.set_state("day_entries", 0)
            self.store.set_state("day_exits", 0)

        dd_day = 1.0 - (self.equity_total_est() / max(self.day_start_equity, 1e-9))
        return dd_day < self.cfg.max_daily_loss_pct

    def can_open_new(self) -> bool:
        eq = self.equity_total_est()
        self.peak_equity = max(self.peak_equity, eq)
        dd_peak = 1.0 - (eq / max(self.peak_equity, 1e-9))
        if dd_peak >= self.cfg.max_drawdown_from_peak:
            return False
        if not self.can_trade_today():
            return False
        if self.total_exposure_usd() >= eq * self.cfg.max_total_exposure_pct:
            return False
        return True

    def _prune_expired_bans(self) -> None:
        now = now_ts()
        bt = self.banned_tokens if isinstance(getattr(self, "banned_tokens", None), dict) else {}
        changed = False
        for k in list(bt.keys()):
            rec = bt.get(k) or {}
            until = int(rec.get("until") or 0)
            if until > 0 and now >= until:
                bt.pop(k, None)
                changed = True
        if changed:
            self.banned_tokens = bt
            self.banned_set = set(bt.keys())
            self.store.set_state("banned_tokens", self.banned_tokens)

    def is_banned(self, k: str) -> bool:
        # returns True if banned; auto-unbans if expired (until>0)
        rec = (self.banned_tokens or {}).get(k)
        if not rec:
            return False
        until = int((rec or {}).get("until") or 0)
        if until > 0 and now_ts() >= until:
            # expired ban -> remove
            try:
                self.banned_tokens.pop(k, None)
                self.banned_set.discard(k)
                self.store.set_state("banned_tokens", self.banned_tokens)
            except Exception:
                pass
            return False
        return True

    def _cache_token_meta(self, k: str, meta: Dict[str, Any]) -> None:
        if not isinstance(meta, dict):
            return
        rec = dict(meta)
        rec["ts"] = now_ts()
        self.token_meta_cache[k] = rec
        # persist
        self.store.set_state("token_meta_cache", self.token_meta_cache)

    def dynamic_slippage_bps(self, feat: Dict[str, Any], k: str) -> int:
        base = self.cfg.zeroex_slippage_bps
        if not self.cfg.dynamic_slippage:
            return int(clamp(base, 25, self.cfg.max_slippage_bps))

        liq = float(feat.get("liq_usd") or 0.0)
        vol = self.cache.volatility_over(k, now_ts(), 60) or 0.0
        liq_factor = clamp((80000.0 / liq), 0.0, 2.5) if liq > 0 else 2.5
        vol_factor = clamp(vol / 0.03, 0.0, 2.0)
        s = base * (1.0 + 0.6 * liq_factor + 0.6 * vol_factor)
        return int(clamp(s, 25, self.cfg.max_slippage_bps))

    def paper_fill_slippage_bps(self, feat: Dict[str, Any], k: str) -> int:
        """Slippage SOLO per PaperExecutor (in PAPER è costo certo).
        Separato dalla tolleranza 0x usata nel precheck.
        Params via config.strategy:
          - paper_fill_slippage_bps (default 120)
          - paper_fill_max_slippage_bps (default 600)
          - paper_fill_dynamic_slippage (default false)
        """
        strat = (self.cfg.strategy or {}) if isinstance(getattr(self.cfg, "strategy", None), dict) else {}
        base = _as_int(strat.get("paper_fill_slippage_bps", 120)) or 120
        maxb = _as_int(strat.get("paper_fill_max_slippage_bps", 600)) or 600
        dyn = _as_bool(strat.get("paper_fill_dynamic_slippage", False))

        if not dyn:
            return int(clamp(float(base), 25, float(maxb)))

        liq = float(feat.get("liq_usd") or 0.0)
        vol = self.cache.volatility_over(k, now_ts(), 60) or 0.0
        liq_factor = clamp((80000.0 / liq), 0.0, 2.5) if liq > 0 else 2.5
        vol_factor = clamp(vol / 0.03, 0.0, 2.0)
        s = float(base) * (1.0 + 0.6 * liq_factor + 0.6 * vol_factor)
        return int(clamp(s, 25, float(maxb)))

    # -------------------- health gating (microcap quality) --------------------
    def _health_cfg(self) -> Dict[str, Any]:
        strat = (self.cfg.strategy or {}) if isinstance(getattr(self.cfg, "strategy", None), dict) else {}
        h = strat.get("health") or {}
        return h if isinstance(h, dict) else {}

    def microcap_health_score(
        self,
        *,
        k: str,
        feat: Dict[str, Any],
        px: float,
        precheck_meta: Optional[Dict[str, Any]] = None,
    ) -> Tuple[float, Dict[str, Any]]:
        """Return (score 0..1, details).

        Score is designed as a conservative gate: anything clearly bad drives score down fast.
        """
        h = self._health_cfg()
        details: Dict[str, Any] = {}

        # (1) roundtrip cost / tradability
        rt_loss = None
        if precheck_meta and isinstance(precheck_meta, dict):
            rt_loss = safe_float(precheck_meta.get("roundtrip_loss_pct"))
        details["roundtrip_loss_pct"] = rt_loss
        max_rt_loss = safe_float(h.get("max_roundtrip_loss_pct", 0.07))
        if max_rt_loss is None:
            max_rt_loss = 0.07
        s_rt = 1.0
        if rt_loss is not None:
            s_rt = clamp(1.0 - (rt_loss / max(1e-9, float(max_rt_loss))), 0.0, 1.0)
        details["score_roundtrip"] = s_rt

        # (2) liquidity stability
        liq_win = _as_int(h.get("liq_stability_window_sec", 600)) or 600
        liq_rng = self.store.snapshot_liq_range_pct(k, window_sec=int(liq_win))
        details["liq_range_pct"] = liq_rng
        max_liq_rng = safe_float(h.get("max_liq_range_pct", 0.35))
        if max_liq_rng is None:
            max_liq_rng = 0.35
        s_liq = 1.0
        if liq_rng is not None:
            s_liq = clamp(1.0 - (liq_rng / max(1e-9, float(max_liq_rng))), 0.0, 1.0)
        details["score_liq_stability"] = s_liq

        # (3) txn density + buy pressure
        buys = int(feat.get("buys_m5") or 0)
        sells = int(feat.get("sells_m5") or 0)
        tx = max(1, int(feat.get("txns_m5") or 0))
        buy_ratio = buys / max(1, (buys + sells))
        details["buys_m5"] = buys
        details["sells_m5"] = sells
        details["buy_ratio_m5"] = buy_ratio

        min_buys = _as_int(h.get("min_buys_m5", 25)) or 25
        min_buy_ratio = safe_float(h.get("min_buy_ratio_m5", 0.55))
        if min_buy_ratio is None:
            min_buy_ratio = 0.55

        s_buys = clamp(buys / max(1.0, float(min_buys)), 0.0, 1.0)
        # buy ratio: 0.5 is neutral, higher is better
        s_ratio = clamp((buy_ratio - 0.5) / max(1e-9, (float(min_buy_ratio) - 0.5)), 0.0, 1.0) if buy_ratio >= 0.5 else 0.0
        s_flow = clamp(0.5 * s_buys + 0.5 * s_ratio, 0.0, 1.0)
        details["score_flow"] = s_flow

        # (4) absorption / avoid collapsing from recent peak
        abs_win = _as_int(h.get("absorption_window_sec", 90)) or 90
        dd = self.store.snapshot_recent_drawdown_from_peak_pct(k, window_sec=int(abs_win), px_override=float(px))
        details["recent_drawdown_from_peak_pct"] = dd
        max_dd = safe_float(h.get("max_recent_drawdown_from_peak_pct", 0.10))
        if max_dd is None:
            max_dd = 0.10
        s_abs = 1.0
        if dd is not None:
            s_abs = clamp(1.0 - (dd / max(1e-9, float(max_dd))), 0.0, 1.0)
        details["score_absorption"] = s_abs

        # (5) token rules / tax (already checked in precheck, but we score it too)
        buy_tax = safe_float((precheck_meta or {}).get("buy_tax_bps")) if precheck_meta else None
        sell_tax = safe_float((precheck_meta or {}).get("sell_tax_bps")) if precheck_meta else None
        details["buy_tax_bps"] = buy_tax
        details["sell_tax_bps"] = sell_tax
        max_buy_tax = safe_float(h.get("max_buy_tax_bps", self.cfg.max_buy_tax_bps))
        max_sell_tax = safe_float(h.get("max_sell_tax_bps", self.cfg.max_sell_tax_bps))
        max_buy_tax = float(max_buy_tax) if max_buy_tax is not None else float(self.cfg.max_buy_tax_bps)
        max_sell_tax = float(max_sell_tax) if max_sell_tax is not None else float(self.cfg.max_sell_tax_bps)
        s_tax = 1.0
        if buy_tax is not None:
            s_tax = min(s_tax, clamp(1.0 - (buy_tax / max(1e-9, max_buy_tax)), 0.0, 1.0))
        if sell_tax is not None:
            s_tax = min(s_tax, clamp(1.0 - (sell_tax / max(1e-9, max_sell_tax)), 0.0, 1.0))
        details["score_tax"] = s_tax

        

                # (6) narrative attention trend (proxy: volume/txns sustained)
        nar = (h.get("narrative") or {}) if isinstance(h.get("narrative"), dict) else {}
        enabled_nar = _as_bool(nar.get("enabled", True))
        s_att = 1.0
        if enabled_nar:
            win = _as_int(nar.get("attention_window_sec", 1800)) or 1800
            min_tx = float(safe_float(nar.get("min_txns_avg", 20.0)) or 20.0)
            min_vol = float(safe_float(nar.get("min_vol_avg", 2000.0)) or 2000.0)
            allowed_drop = float(safe_float(nar.get("max_drop_pct", 0.25)) or 0.25)

            trend = self.store.attention_trend(k, window_sec=int(win))
            details["attention"] = trend
            tx_cur = float(trend.get("txns_cur") or 0.0)
            vol_cur = float(trend.get("vol_cur") or 0.0)
            tx_prev = float(trend.get("txns_prev") or 0.0)
            vol_prev = float(trend.get("vol_prev") or 0.0)

            base_ok = (tx_cur >= min_tx) and (vol_cur >= min_vol)

            trend_ok = True
            if tx_prev > 0:
                trend_ok = trend_ok and (tx_cur >= tx_prev * (1.0 - allowed_drop))
            if vol_prev > 0:
                trend_ok = trend_ok and (vol_cur >= vol_prev * (1.0 - allowed_drop))

            s_att = 1.0 if (base_ok and trend_ok) else 0.0
        details["score_attention"] = s_att

        # (7) hype proxy (DexScreener boosts)
        hype = (h.get("hype") or {}) if isinstance(h.get("hype"), dict) else {}
        enabled_hype = _as_bool(hype.get("enabled", True))
        s_hype = 1.0
        if enabled_hype:
            win = _as_int(hype.get("window_sec", 3600)) or 3600
            need = _as_int(hype.get("min_events", 1))
            if need is None:
                need = 1
            need = max(0, int(need))
            cnt = self.store.hype_count(k, window_sec=int(win))
            details["hype_events"] = cnt
            s_hype = 1.0 if cnt >= need else 0.0
        details["score_hype"] = s_hype

        # (8) social mentions (Telegram + NewsAPI)
        social = (h.get("social") or {}) if isinstance(h.get("social"), dict) else {}
        enabled_soc = _as_bool(social.get("enabled", False))
        s_soc = 1.0
        if enabled_soc:
            win = _as_int(social.get("window_sec", 3600)) or 3600
            need = _as_int(social.get("min_mentions", 3)) or 3
            m = self.store.social_mentions_sum(k, window_sec=int(win))
            details["social_mentions"] = m
            s_soc = clamp(m / max(1.0, float(need)), 0.0, 1.0)
        details["score_social"] = s_soc

        # Combine: multiplicative is intentionally harsh.
        # Combine: multiplicative.
        # Se hard_reject è FALSE, i segnali "narrative/hype/social" NON devono poter azzerare lo score:
        # li usiamo come downweight con un pavimento (soft_floor).
        nar_hard = _as_bool(nar.get("hard_reject", False))
        hype_hard = _as_bool(hype.get("hard_reject", False))
        soc_hard = _as_bool(social.get("hard_reject", False))

        nar_floor = float(safe_float(nar.get("soft_floor", 0.75)) or 0.75)
        hype_floor = float(safe_float(hype.get("soft_floor", 0.85)) or 0.85)
        soc_floor = float(safe_float(social.get("soft_floor", 0.70)) or 0.70)

        # clamp floors
        nar_floor = clamp(nar_floor, 0.0, 1.0)
        hype_floor = clamp(hype_floor, 0.0, 1.0)
        soc_floor = clamp(soc_floor, 0.0, 1.0)

        att_factor = float(s_att) if nar_hard else max(nar_floor, float(s_att))
        hype_factor = float(s_hype) if hype_hard else max(hype_floor, float(s_hype))
        soc_factor = float(s_soc) if soc_hard else max(soc_floor, float(s_soc))

        details["factor_attention_used"] = att_factor
        details["factor_hype_used"] = hype_factor
        details["factor_social_used"] = soc_factor

        score = float(s_rt * s_liq * s_flow * s_abs * s_tax * att_factor * hype_factor * soc_factor)
        details["score_total"] = score
        return score, details

    async def health_gate(
        self,
        *,
        k: str,
        chain: str,
        token: str,
        feat: Dict[str, Any],
        px: float,
        slip_bps: int,
        precheck_meta: Optional[Dict[str, Any]] = None,
        precheck_quote_usdc: Optional[float] = None,
    ) -> Tuple[bool, str, Dict[str, Any]]:
        """Apply microcap health gates. Returns (ok, reason, details)."""
        h = self._health_cfg()
        enabled = _as_bool(h.get("enabled", False))
        if not enabled:
            return True, "health_disabled", {}

        min_score = safe_float(h.get("min_health_score", 0.70))
        min_score = float(min_score) if min_score is not None else 0.70

        # Optional: ensure we have precheck meta (roundtrip cost / taxes / routeability). If missing and requested, run a dedicated precheck.
        use_zerox = _as_bool(h.get("use_zerox_precheck", True))
        meta = precheck_meta
        if use_zerox and (meta is None or not isinstance(meta, dict) or meta.get("precheck_ok") is not True):
            ok0, msg0, meta0 = await self.precheck_dispatch(
                chain=chain,
                token=token,
                slippage_bps=int(slip_bps),
                quote_usdc=precheck_quote_usdc,
            )
            if not ok0:
                return False, f"health_precheck:{msg0}", {"precheck": meta0}
            meta = meta0

        score, details = self.microcap_health_score(k=k, feat=feat, px=float(px), precheck_meta=meta)
        details["min_health_score"] = min_score

        # Hard rejects
        max_rt_loss = safe_float(h.get("max_roundtrip_loss_pct", 0.07))
        if max_rt_loss is None:
            max_rt_loss = 0.07
        rt_loss = safe_float(details.get("roundtrip_loss_pct"))
        if rt_loss is not None and rt_loss > float(max_rt_loss):
            return False, "health_roundtrip_too_costly", details

        max_dd = safe_float(h.get("max_recent_drawdown_from_peak_pct", 0.10))
        if max_dd is None:
            max_dd = 0.10
        dd = safe_float(details.get("recent_drawdown_from_peak_pct"))
        if dd is not None and dd > float(max_dd):
            return False, "health_collapsing_from_peak", details

        max_liq_rng = safe_float(h.get("max_liq_range_pct", 0.35))
        if max_liq_rng is None:
            max_liq_rng = 0.35
        liq_rng = safe_float(details.get("liq_range_pct"))
        if liq_rng is not None and liq_rng > float(max_liq_rng):
            return False, "health_liq_unstable", details

        min_buys = _as_int(h.get("min_buys_m5", 25)) or 25
        min_buy_ratio = safe_float(h.get("min_buy_ratio_m5", 0.55))
        min_buy_ratio = float(min_buy_ratio) if min_buy_ratio is not None else 0.55
        if int(details.get("buys_m5") or 0) < int(min_buys):
            return False, "health_low_buys", details
        if float(details.get("buy_ratio_m5") or 0.0) < float(min_buy_ratio):
            return False, "health_sell_dominant", details

        nar = (h.get("narrative") or {}) if isinstance(h.get("narrative"), dict) else {}
        if _as_bool(nar.get("enabled", True)) and _as_bool(nar.get("hard_reject", True)):
            if float(details.get("score_attention") or 0.0) <= 0.0:
                return False, "health_attention_not_sustained", details

        hype = (h.get("hype") or {}) if isinstance(h.get("hype"), dict) else {}
        if _as_bool(hype.get("enabled", True)) and _as_bool(hype.get("hard_reject", False)):
            if float(details.get("score_hype") or 0.0) <= 0.0:
                return False, "health_no_hype_proxy", details

        social_cfg = (h.get("social") or {}) if isinstance(h.get("social"), dict) else {}
        if _as_bool(social_cfg.get("enabled", False)) and _as_bool(social_cfg.get("hard_reject", False)):
            min_sc = float(safe_float(social_cfg.get("min_score", 0.5)) or 0.5)
            if float(details.get("score_social") or 0.0) < min_sc:
                return False, "health_low_social", details

        if score < min_score:
            return False, "health_score_low", details

        return True, "health_ok", details

    async def close(self):
        """Chiude risorse HTTP/RPC/notify in modo sicuro."""
        try:
            await self.dex.close()
        except Exception:
            pass
        try:
            if self.zerox:
                await self.zerox.close()
        except Exception:
            pass
        try:
            if self.tg is not None:
                await self.tg.close()
        except Exception:
            pass
        try:
            if self.news is not None:
                await self.news.close()
        except Exception:
            pass
        try:
            if self.x is not None:
                await self.x.close()
        except Exception:
            pass
        try:
            await self.notify.close()
        except Exception:
            pass

    async def circuit_fail(self, where: str, err: Exception):
        M_API_ERR.labels(where=where).inc()
        self.fail_count += 1
        self.log.warning(
            "%s error (%d/%d): %s",
            where,
            self.fail_count,
            self.cfg.max_consecutive_failures,
            err,
        )
        if self.fail_count >= self.cfg.max_consecutive_failures:
            self.pause_until = now_ts() + self.cfg.pause_sec_after_failures
            self.fail_count = 0

    async def task_rpc_health(self):
        if self.mode != "live" or not self.rpcm:
            return
        while True:
            await asyncio.sleep(10)
            try:
                for chain in list((self.cfg.chains or {}).keys()):
                    if not self.rpcm.has_chain(chain):
                        continue
                    ok = await self.rpcm.healthcheck(chain)
                    if not ok:
                        await self.notify.send(f"⚠️ RPC DOWN [{chain}]: switch da {self.rpcm.current_url(chain)}")
                        self.rpcm.switch_next(chain)
                        await self.notify.send(f"✅ RPC NOW [{chain}]: {self.rpcm.current_url(chain)}")
            except Exception as e:
                await self.circuit_fail("rpc_health", e)

    async def task_sync_live_cash(self):
        if self.mode != "live" or not self.rpcm or not self.account:
            return
        while True:
            await asyncio.sleep(15)
            try:
                total_usdc = 0.0
                for chain, ch in (self.cfg.chains or {}).items():
                    mgr = self.rpcm.get(chain)
                    if mgr is None:
                        continue
                    w3 = mgr.w3
                    usdc = ch.usdc
                    c = w3.eth.contract(address=Web3.to_checksum_address(usdc), abi=ERC20_ABI)
                    dec = int(await asyncio.to_thread(lambda: c.functions.decimals().call()))
                    bal = int(await asyncio.to_thread(lambda: c.functions.balanceOf(self.account.address).call()))
                    total_usdc += bal / (10 ** dec)

                async with self._lock:
                    self.cash = total_usdc
                    self.store.set_state("cash", self.cash)
            except Exception as e:
                await self.circuit_fail("live_cash_sync", e)

    async def task_discovery(self):
        while True:
            await asyncio.sleep(self.cfg.scan_interval_sec)
            if now_ts() < self.pause_until:
                continue
            try:
                with M_LAT.labels(op="scan").time():
                    prof = await self.dex.latest_token_profiles()
                    cto = await self.dex.latest_token_community_takeovers()
                    boost_latest = await self.dex.latest_token_boosts()
                    boost_top = await self.dex.top_token_boosts()
                    feed = (prof or []) + (cto or []) + (boost_latest or []) + (boost_top or [])

                M_SCAN.inc()
                now0 = now_ts()
                parsed_candidates: List[Tuple[str, str]] = []
                parsed_seen: set[str] = set()
                added_keys: List[str] = []
                rejects = defaultdict(int)

                try:
                    for it in (boost_latest or []) + (boost_top or []) + (cto or []):
                        if not isinstance(it, dict):
                            continue
                        ch = normalize_chain_id((it.get("chainId") or it.get("chain") or "").strip(), default="base")
                        taddr = (
                            (it.get("tokenAddress") or it.get("token") or it.get("address") or "").strip()
                            or str(((it.get("baseToken") or {}).get("address") or "")).strip()
                        )
                        if (not taddr) and it.get("url"):
                            url_chain, url_ident = parse_dexscreener_url(it.get("url"))
                            if url_ident:
                                ch = normalize_chain_id(url_chain or ch, default="base")
                                taddr = url_ident
                        if is_dexscreener_id(taddr):
                            try:
                                taddr = (await self.dex.resolve_token_address(ch, taddr) or taddr).strip()
                            except Exception:
                                pass
                        if is_supported_token_ref(taddr):
                            self.store.log_hype_event(ts=now0, key=self.key(ch, taddr), source="dexscreener", event="boost", value=safe_float(it.get("amount")))
                except Exception:
                    pass

                for p in feed:
                    if not isinstance(p, dict):
                        rejects["non_dict"] += 1
                        continue
                    chain = normalize_chain_id((p.get("chainId") or p.get("chain") or "").strip(), default="base")
                    token_or_pair = (
                        (p.get("tokenAddress") or p.get("token") or p.get("address") or "").strip()
                        or str(((p.get("baseToken") or {}).get("address") or "")).strip()
                    )
                    if not token_or_pair:
                        url_chain, url_ident = parse_dexscreener_url(p.get("url"))
                        if url_ident:
                            chain = normalize_chain_id(url_chain or chain, default="base")
                            token_or_pair = url_ident
                    token_or_pair = (token_or_pair or "").strip()
                    if not token_or_pair:
                        rejects["empty_token"] += 1
                        continue
                    if not chain_is_allowed(chain, self.cfg.allowed_chains):
                        rejects[f"chain_filtered:{chain or 'unknown'}"] += 1
                        continue
                    if is_dexscreener_id(token_or_pair):
                        try:
                            token_or_pair = (await self.dex.resolve_token_address(chain, token_or_pair) or token_or_pair).strip()
                        except Exception:
                            pass
                    if not is_supported_token_ref(token_or_pair):
                        rejects["unsupported_token_ref"] += 1
                        continue
                    k = self.key(chain, token_or_pair)
                    if k in parsed_seen:
                        rejects["dup_in_feed"] += 1
                        continue
                    parsed_seen.add(k)
                    parsed_candidates.append((chain, token_or_pair))

                added = 0
                async with self._lock:
                    for chain, token in parsed_candidates:
                        k = self.key(chain, token)
                        if self.is_banned(k):
                            rejects["banned"] += 1
                            continue
                        if k in self.watchlist or k in self.positions:
                            rejects["already_known"] += 1
                            continue
                        self.watchlist[k] = {"chain": chain, "token": token, "added_ts": now0, "pair": "", "score": None, "cooldown_until": 0, "last_feat": None}
                        added += 1
                        added_keys.append(k)
                        if len(self.watchlist) > self.cfg.max_watchlist:
                            oldest = min(self.watchlist.items(), key=lambda kv: kv[1]["added_ts"])[0]
                            self.watchlist.pop(oldest, None)
                    if added:
                        self.store.save_watchlist(self.watchlist)

                if added:
                    self.log.info("DEX DISCOVERY added=%d keys=%s", added, ", ".join(added_keys[:20]))
                else:
                    last_log = int(getattr(self, "_dex_discovery_last_diag_ts", 0) or 0)
                    if (now0 - last_log) >= 60:
                        self.log.info("DEX DISCOVERY added=0 allowed=%s feed=%d rejects=%s", ",".join(self.cfg.allowed_chains or []), len(feed), dict(sorted(rejects.items())))
                        self._dex_discovery_last_diag_ts = now0
            except Exception as e:
                await self.circuit_fail("dex_scan", e)

    async def task_refresh(self):
        while True:
            await asyncio.sleep(self.cfg.refresh_watchlist_sec)
            if now_ts() < self.pause_until:
                continue
            try:
                strat = self.cfg.strategy if isinstance(getattr(self.cfg, "strategy", None), dict) else {}
                prune_on = _as_bool(strat.get("prune_no_data_enabled", True))
                prune_after_n = int(strat.get("prune_no_data_after_n", 3) or 3)
                prune_min_age = int(strat.get("prune_no_data_min_age_sec", 120) or 120)
                async with self._lock:
                    items = list(self.watchlist.items())
                    items.sort(key=lambda kv: (kv[1].get("score") is not None, kv[1].get("score") or -1), reverse=True)

                    base_top = items[: self.cfg.snapshot_store_top_n] if self.cfg.snapshot_enabled else items[:300]

                    # IMPORTANT: include also "recently added" tokens, otherwise social/discovery tokens
                    # can end up never being refreshed (and therefore never traded) when watchlist is large.
                    now0 = now_ts()
                    recent_sec = int(getattr(self.cfg, "refresh_include_recent_sec", 900) or 900)  # 15 min
                    recent_max = int(getattr(self.cfg, "refresh_include_recent_max", 80) or 80)

                    recent = []
                    if recent_sec > 0 and recent_max != 0:
                        for k, info in items:
                            if int(info.get("added_ts") or 0) <= 0:
                                continue
                            if (now0 - int(info.get("added_ts") or 0)) <= int(recent_sec):
                                recent.append((k, info))
                                if recent_max > 0 and len(recent) >= recent_max:
                                    break

                    top_items = []
                    seen = set()
                    for k, info in (base_top + recent):
                        if k in seen:
                            continue
                        seen.add(k)
                        top_items.append((k, info))

                    tokens_by_chain: Dict[str, List[str]] = defaultdict(list)
                    for k, info in top_items:
                        ch = normalize_chain_id(info.get("chain"), default="base")
                        tok = canonical_token_ref(ch, info.get("token"))
                        if tok:
                            tokens_by_chain[ch].append(tok)

                    for k, pos in self.positions.items():
                        ch = normalize_chain_id(getattr(pos, "chain", ""), default="base")
                        tok = canonical_token_ref(ch, getattr(pos, "token", ""))
                        if tok:
                            tokens_by_chain[ch].append(tok)

                rows_to_snapshot: List[Tuple] = []

                for chain, toks in tokens_by_chain.items():
                    uniq = list(
                        dict.fromkeys(
                            [
                                canonical_token_ref(chain, t)
                                for t in toks
                                if is_supported_token_ref(canonical_token_ref(chain, t))
                            ]
                        )
                    )
                    for i in range(0, len(uniq), 30):
                        chunk = uniq[i:i+30]
                        with M_LAT.labels(op="refresh").time():
                            pairs = await self.dex.tokens_batch(chain, chunk)
                        M_REFRESH.inc()

                        grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
                        for pr in pairs:
                            base = canonical_token_ref(chain, ((pr.get("baseToken") or {}).get("address") or ""))
                            quote = canonical_token_ref(chain, ((pr.get("quoteToken") or {}).get("address") or ""))
                            if base:
                                grouped[base].append(pr)
                            if quote:
                                grouped[quote].append(pr)

                        ts = now_ts()
                        async with self._lock:
                            for t in chunk:
                                k = self.key(chain, t)
                                best = best_pair_by_liquidity(grouped.get(t, []))
                                if not best:
                                    # No pairs returned for this token on this chain.
                                    # Most common cause: a wrong-chain address got into the watchlist (Telegram/NewsAPI).
                                    if prune_on and k in self.watchlist:
                                        info = self.watchlist.get(k) or {}
                                        age = int(ts) - int(info.get("added_ts") or ts)
                                        if int(prune_min_age) <= 0 or age >= int(prune_min_age):
                                            nmiss = int(info.get("no_data_n") or 0) + 1
                                            info["no_data_n"] = int(nmiss)
                                            self.watchlist[k] = info
                                            if int(prune_after_n) > 0 and nmiss >= int(prune_after_n):
                                                self.watchlist.pop(k, None)
                                    continue
                                feat = features_from_pair(best)
                                px = feat.get("price_usd")
                                if px:
                                    self.cache.push(k, ts, float(px))
                                sc = score_pair(feat) if feat.get("price_usd") else None

                                if k in self.watchlist:
                                    # got data again -> clear miss counter
                                    self.watchlist[k].pop("no_data_n", None)
                                    self.watchlist[k]["pair"] = feat.get("pair_address") or ""
                                    self.watchlist[k]["score"] = sc
                                    self.watchlist[k]["last_feat"] = feat

                                if self.cfg.snapshot_enabled and px:
                                    rows_to_snapshot.append((
                                        ts, k, chain, t, float(px),
                                        feat.get("liq_usd"), feat.get("vol_m5"),
                                        int(feat.get("txns_m5") or 0), feat.get("fdv"), sc
                                    ))

                        if self.cfg.snapshot_enabled:
                            self.store.insert_snapshots(rows_to_snapshot)
                            rows_to_snapshot.clear()

                async with self._lock:
                    self.store.save_watchlist(self.watchlist)

                if self.cfg.snapshot_enabled:
                    self.store.retention_snapshots(self.cfg.snapshot_retention_days)
                try:
                    self.store.retention_audit(self.cfg.snapshot_retention_days)
                except Exception:
                    pass

            except Exception as e:
                await self.circuit_fail("dex_refresh", e)

    async def solana_precheck(self, *, chain: str, token: str, slippage_bps: int, quote_usdc: Optional[float] = None) -> Tuple[bool, str, Dict[str, Any]]:
        strat = self.cfg.strategy if isinstance(getattr(self.cfg, "strategy", None), dict) else {}
        hcfg = self._health_cfg() if hasattr(self, "_health_cfg") else {}

        if not is_base58_address(token):
            return False, "solana_bad_token", {
                "precheck_ok": False,
                "precheck_source": "solana_quote",
                "precheck_reason": "solana_bad_token",
                "unsupported_chain": chain,
                "unsupported_token": token,
            }

        base_url = str(strat.get("solana_quote_base_url", "https://lite-api.jup.ag") or "https://lite-api.jup.ag").rstrip("/")
        timeout_sec = float(strat.get("solana_precheck_timeout_sec", 10.0) or 10.0)
        quote_usdc = float(
            quote_usdc
            if quote_usdc is not None
            else (strat.get("solana_precheck_usdc", self.cfg.precheck_usdc) or self.cfg.precheck_usdc)
        )
        max_pi_bps = int(float(strat.get("solana_max_price_impact_bps", 250) or 250))
        max_rt_loss = float(hcfg.get("max_roundtrip_loss_pct", 0.035) or 0.035)

        amt_in = _raw_usdc_6(quote_usdc)

        try:
            async with httpx.AsyncClient(base_url=base_url, timeout=timeout_sec) as http:
                async def _sol_get(path: str, *, params: Dict[str, Any]) -> httpx.Response:
                    last_resp: Optional[httpx.Response] = None

                    for attempt in range(6):
                        await self.solana_precheck_lim.acquire(1)
                        resp = await http.get(path, params=params)
                        last_resp = resp

                        if resp.status_code == 200:
                            return resp

                        if resp.status_code not in (429, 500, 502, 503, 504):
                            return resp

                        if attempt >= 5:
                            return resp

                        retry_after = 0.0
                        try:
                            retry_after = float(resp.headers.get("retry-after") or 0.0)
                        except Exception:
                            retry_after = 0.0

                        sleep_s = max(retry_after, 1.20 * (2 ** attempt))
                        sleep_s *= random.uniform(0.95, 1.25)
                        await asyncio.sleep(sleep_s)

                    return last_resp

                r1 = await _sol_get(
                    "/swap/v1/quote",
                    params={
                        "inputMint": SOLANA_USDC_MINT,
                        "outputMint": str(token).strip(),
                        "amount": str(amt_in),
                        "slippageBps": str(int(slippage_bps)),
                        "swapMode": "ExactIn",
                    },
                )
                if r1.status_code != 200:
                    return False, f"sol_quote_buy_http_{r1.status_code}", {
                        "precheck_ok": False,
                        "precheck_source": "solana_quote",
                        "precheck_reason": f"sol_quote_buy_http_{r1.status_code}",
                        "http_body": (r1.text or "")[:300],
                    }
                q1 = r1.json() or {}
                out_amount = int((q1 or {}).get("outAmount") or 0)
                if out_amount <= 0:
                    return False, "sol_quote_buy_empty", {
                        "precheck_ok": False,
                        "precheck_source": "solana_quote",
                        "precheck_reason": "sol_quote_buy_empty",
                    }

                r2 = await _sol_get(
                    "/swap/v1/quote",
                    params={
                        "inputMint": str(token).strip(),
                        "outputMint": SOLANA_USDC_MINT,
                        "amount": str(out_amount),
                        "slippageBps": str(int(slippage_bps)),
                        "swapMode": "ExactIn",
                    },
                )
                if r2.status_code != 200:
                    return False, f"sol_quote_sell_http_{r2.status_code}", {
                        "precheck_ok": False,
                        "precheck_source": "solana_quote",
                        "precheck_reason": f"sol_quote_sell_http_{r2.status_code}",
                        "http_body": (r2.text or "")[:300],
                    }
                q2 = r2.json() or {}
                back_amount = int((q2 or {}).get("outAmount") or 0)
                if back_amount <= 0:
                    return False, "sol_quote_sell_empty", {
                        "precheck_ok": False,
                        "precheck_source": "solana_quote",
                        "precheck_reason": "sol_quote_sell_empty",
                    }

                pi_buy = int(round(max(0.0, float(safe_float((q1 or {}).get("priceImpactPct")) or 0.0)) * 10000.0))
                pi_sell = int(round(max(0.0, float(safe_float((q2 or {}).get("priceImpactPct")) or 0.0)) * 10000.0))
                pi_max = max(pi_buy, pi_sell)
                rt_loss = max(0.0, 1.0 - (float(back_amount) / max(float(amt_in), 1.0)))

                meta = {
                    "precheck_ok": True,
                    "precheck_source": "solana_quote",
                    "precheck_reason": "solana_ok",
                    "chain": str(chain).lower(),
                    "buy_tax_bps": None,
                    "sell_tax_bps": None,
                    "roundtrip_return": float((float(back_amount) / max(float(amt_in), 1.0)) - 1.0),
                    "roundtrip_loss_pct": float(rt_loss),
                    "price_impact_buy_bps": int(pi_buy),
                    "price_impact_sell_bps": int(pi_sell),
                    "price_impact_max_bps": int(pi_max),
                    "quote_in_usdc": float(quote_usdc),
                    "quote_back_usdc": float(back_amount) / 1_000_000.0,
                }

                if pi_max > int(max_pi_bps):
                    meta["precheck_ok"] = False
                    meta["precheck_reason"] = f"sol_price_impact:{pi_max}bps"
                    return False, meta["precheck_reason"], meta

                if rt_loss > float(max_rt_loss):
                    meta["precheck_ok"] = False
                    meta["precheck_reason"] = f"sol_rt_loss:{rt_loss:.4f}"
                    return False, meta["precheck_reason"], meta

                return True, "solana_ok", meta

        except Exception as e:
            return False, f"sol_precheck_err:{type(e).__name__}", {
                "precheck_ok": False,
                "precheck_source": "solana_quote",
                "precheck_reason": f"sol_precheck_err:{type(e).__name__}",
                "err": str(e)[:300],
            }

    async def precheck_dispatch(self, *, chain: str, token: str, slippage_bps: int, quote_usdc: Optional[float] = None) -> Tuple[bool, str, Dict[str, Any]]:
        strat = self.cfg.strategy if isinstance(getattr(self.cfg, "strategy", None), dict) else {}
        strict = _as_bool(strat.get("strict_precheck_required", True))
        ch = normalize_chain_id(chain, default="")

        if ch == "solana":
            if not _as_bool(strat.get("solana_precheck_enabled", True)):
                meta = {
                    "precheck_ok": False,
                    "precheck_source": "solana_quote",
                    "precheck_reason": "solana_precheck_disabled",
                    "skipped": True,
                    "unsupported_chain": ch,
                }
                return (False, "solana_precheck_disabled", meta) if strict else (True, "solana_precheck_disabled", meta)
            return await self.solana_precheck(
                chain=ch,
                token=token,
                slippage_bps=int(slippage_bps),
                quote_usdc=quote_usdc,
            )

        ok, msg, meta = await self.zerox_precheck(
            chain=ch,
            token=token,
            slippage_bps=int(slippage_bps),
            quote_usdc=quote_usdc,
        )
        if not isinstance(meta, dict):
            meta = {}

        if meta.get("skipped") is True:
            meta.setdefault("precheck_ok", False)
            meta.setdefault("precheck_source", "0x")
            meta.setdefault("precheck_reason", msg or f"unsupported_precheck:{ch}")
            return (False, meta["precheck_reason"], meta) if strict else (True, meta["precheck_reason"], meta)

        if ok:
            meta.setdefault("precheck_ok", True)
            meta.setdefault("precheck_source", "0x")
            meta.setdefault("precheck_reason", msg or "ok")
            return True, msg, meta

        meta.setdefault("precheck_ok", False)
        meta.setdefault("precheck_source", "0x")
        meta.setdefault("precheck_reason", msg or "precheck_failed")
        return False, msg, meta

    async def zerox_precheck(self, *, chain: str, token: str, slippage_bps: int, quote_usdc: Optional[float] = None) -> Tuple[bool, str, Dict[str, Any]]:
        zkey = os.getenv("ZEROEX_API_KEY", "").strip()
        if not zkey:
            return False, "skip_no_zerox_key", {"skipped": True, "precheck_ok": False, "precheck_source": "0x", "precheck_reason": "skip_no_zerox_key"}

        strat = self.cfg.strategy if isinstance(getattr(self.cfg, "strategy", None), dict) else {}

        if not self.zerox:
            self.zerox = ZeroXClient(
                zkey,
                rps=float(strat.get("zerox_client_rps", 4.0) or 4.0),
                max_attempts=int(strat.get("zerox_retry_max_attempts", 4) or 4),
                base_sleep_sec=float(strat.get("zerox_retry_base_sleep_sec", 0.35) or 0.35),
            )
    
        try:
            ch = self.cfg.chains.get(chain)
            is_evm_like = is_evm_address(token)
            if ch is None or not is_evm_like:
                if self.mode == "paper":
                    return True, f"paper_skip_precheck:{chain}", {"skipped": True, "unsupported_chain": chain, "unsupported_token": token}
                return False, f"skip_unsupported_chain:{chain}", {"skipped": True, "unsupported_chain": chain, "unsupported_token": token}

            usdc = ch.usdc
            taker = self.account.address if (self.mode == "live" and self.account) else None

            usdc_dec = 6
            if self.mode == "live" and self.rpcm:
                mgr = self.rpcm.get(chain)
                if mgr is None:
                    return False, f"skip_unsupported_chain_rpc:{chain}", {"skipped": True, "unsupported_chain": chain}
                c = mgr.w3.eth.contract(address=Web3.to_checksum_address(usdc), abi=ERC20_ABI)
                usdc_dec = int(await asyncio.to_thread(lambda: c.functions.decimals().call()))

            quote_usdc = float(self.cfg.precheck_usdc if quote_usdc is None else quote_usdc)
            sell_amount = int(quote_usdc * (10 ** usdc_dec))

            with M_LAT.labels(op="zerox_price_buy").time():
                buy_p = await self.zerox.price_allowance_holder(
                    chain_id=ch.chain_id,
                    buy_token=token,
                    sell_token=usdc,
                    sell_amount=sell_amount,
                    taker=taker,
                    slippage_bps=slippage_bps,
                )

            if buy_p.get("liquidityAvailable") is False:
                return False, "no_liquidity", {"buy": buy_p}

            issues = buy_p.get("issues") or {}
            if issues.get("simulationIncomplete") is True:
                return False, "sim_incomplete", {"buy": buy_p}

            md = (buy_p.get("tokenMetadata") or {}).get("buyToken") or {}
            buy_tax = int(md.get("buyTaxBps") or 0)
            sell_tax = int(md.get("sellTaxBps") or 0)

            if buy_tax > self.cfg.max_buy_tax_bps or sell_tax > self.cfg.max_sell_tax_bps:
                return (
                    False,
                    f"tax_high buy={buy_tax} sell={sell_tax}",
                    {
                        "buy_tax_bps": buy_tax,
                        "sell_tax_bps": sell_tax,
                        "buy": buy_p,
                    },
                )

            buy_amount = int(buy_p.get("buyAmount") or 0)
            if buy_amount <= 0:
                return False, "buyAmount_zero", {"buy": buy_p}

            with M_LAT.labels(op="zerox_price_sell").time():
                sell_p = await self.zerox.price_allowance_holder(
                    chain_id=ch.chain_id,
                    buy_token=usdc,
                    sell_token=token,
                    sell_amount=buy_amount,
                    taker=taker,
                    slippage_bps=slippage_bps,
                )

            if sell_p.get("liquidityAvailable") is False:
                return False, "cannot_sell", {"buy": buy_p, "sell": sell_p}

            issues2 = sell_p.get("issues") or {}
            if issues2.get("simulationIncomplete") is True:
                return False, "sell_sim_incomplete", {"buy": buy_p, "sell": sell_p}

            usdc_out = int(sell_p.get("buyAmount") or 0)
            roundtrip_return = (usdc_out / max(1.0, float(sell_amount))) - 1.0

            meta = {
                "precheck_ok": True,
                "precheck_source": "0x",
                "precheck_reason": "ok",
                "buy_tax_bps": buy_tax,
                "sell_tax_bps": sell_tax,
                "roundtrip_return": float(roundtrip_return),
                "roundtrip_loss_pct": float(max(0.0, -roundtrip_return)),
            }
            return True, "ok", meta

        except Exception as e:
            M_API_ERR.labels(where="zerox").inc()
            return False, f"precheck_err:{str(e)}", {"err": str(e)}

    async def manage_positions(self):
        """Gestione posizioni più conservativa e più adatta a reversal microcap.

        Migliorie:
        - fail-fast se il rebound non parte
        - breakeven stop dopo primo profitto
        - TP1 parziale reale
        - trailing stepped mantenuto, ma meno nervoso
        """
        if not self.positions:
            return

        strat = self.cfg.strategy if isinstance(getattr(self.cfg, "strategy", None), dict) else {}
        strat_mode = str(strat.get("mode") or "momentum").lower().strip()
        now = now_ts()

        # --- trailing stepped ---
        activate_pct = float(strat.get("min_rebound_pct_before_trail", 0.08) or 0.08)
        step_pct = float(strat.get("trail_step_pct", 0.10) or 0.10)
        giveback_pct = float(strat.get("rebound_retrace_pct", 0.06) or 0.06)
        floor_profit = float(strat.get("trail_floor_profit_pct", 0.0) or 0.0)
        min_after_arm = int(strat.get("min_seconds_after_trail_activation", 0) or 0)
        min_step_to_exit = int(strat.get("trail_min_step_to_exit", 1) or 1)
        exit_confirm_ticks = max(1, int(strat.get("trail_exit_confirm_ticks", 2) or 2))

        # --- exits intrabar / quality exits ---
        sh_w = int(strat.get("short_window_sec", 15) or 15)
        exit_dump_again = strat.get("exit_if_dump_speed_again_pct_per_sec", None)
        exit_dump_again = float(exit_dump_again) if exit_dump_again is not None else None
        exit_pump_slow = strat.get("exit_pump_slow_speed_pct_per_sec", None)
        exit_pump_slow = float(exit_pump_slow) if exit_pump_slow is not None else None

        # --- new protections ---
        fail_fast_sec = int(strat.get("rebound_fail_fast_sec", 45) or 45)
        fail_fast_pnl = float(strat.get("rebound_fail_fast_pnl_pct", -0.02) or -0.02)
        fail_fast_peak = float(strat.get("rebound_fail_fast_peak_pnl_pct", 0.01) or 0.01)

        breakeven_arm = float(strat.get("breakeven_arm_pct", 0.018) or 0.018)
        breakeven_off = float(strat.get("breakeven_offset_pct", 0.003) or 0.003)

        tp1_take_pct = float(
            strat.get("tp1_take_pct", max(float(getattr(self.cfg, "tp1_pct", 0.08) or 0.08), breakeven_arm * 2.0))
            or max(float(getattr(self.cfg, "tp1_pct", 0.08) or 0.08), breakeven_arm * 2.0)
        )
        tp1_frac_raw = strat.get("tp1_fraction", None)
        if tp1_frac_raw is None:
            tp1_frac_raw = getattr(self.cfg, "tp1_fraction", 0.60)
        tp1_frac = clamp(float(tp1_frac_raw), 0.0, 0.95)
        tp1_move_stop_to_be = _as_bool(strat.get("tp1_move_stop_to_be", True))

        red_after_green_peak = float(strat.get("max_red_after_green_peak_pct", 0.03) or 0.03)
        red_after_green_floor = float(strat.get("max_red_after_green_floor_pct", 0.005) or 0.005)

        # Hard SL
        hard_sl = abs(float(getattr(self.cfg, "stop_loss_pct", 0.12) or 0.12))

        def fill_slip(feat: Dict[str, Any], key: str) -> int:
            return int(
                self.paper_fill_slippage_bps(feat, key)
                if self.mode == "paper"
                else self.dynamic_slippage_bps(feat, key)
            )

        for k, pos in list(self.positions.items()):
            try:
                info = self.watchlist.get(k) or {}
                feat = info.get("last_feat") or {}

                px = feat.get("price_usd") or self.cache.last(k) or pos.entry_px
                if not px:
                    continue
                px = float(px)

                if px > float(pos.peak_px):
                    pos.peak_px = float(px)

                avg_px = float(getattr(pos, "avg_px", 0.0) or 0.0)
                if avg_px <= 0:
                    continue

                age = int(now - int(pos.entry_ts))
                pnl = (px / max(avg_px, 1e-18)) - 1.0
                peak_pnl = (float(pos.peak_px) / max(avg_px, 1e-18)) - 1.0

                # --- hard SL ---
                if pnl <= -hard_sl:
                    slip = fill_slip(feat, k)
                    await self.exit_full(k, pos, px, reason="stop_loss", slip=int(slip))
                    continue

                # --- fail fast: rebound non parte davvero ---
                if age >= int(fail_fast_sec) and peak_pnl < float(fail_fast_peak) and pnl <= float(fail_fast_pnl):
                    slip = fill_slip(feat, k)
                    await self.exit_full(k, pos, px, reason="failed_rebound", slip=int(slip))
                    continue

                # --- gave back green: è andato verde serio e ha restituito quasi tutto ---
                if peak_pnl >= float(red_after_green_peak) and pnl <= float(red_after_green_floor):
                    slip = fill_slip(feat, k)
                    await self.exit_full(k, pos, px, reason="gave_back_green", slip=int(slip))
                    continue

                # --- time stop ---
                if age >= int(self.cfg.max_hold_sec):
                    slip = fill_slip(feat, k)
                    await self.exit_full(k, pos, px, reason="time_stop", slip=int(slip))
                    continue

                if strat_mode == "dip_rebound":
                    speed = self.cache.speed_pct_per_sec(k, now, sh_w)

                    # --- TP1 / BE / stepped trailing: un solo stop protetto, un solo breach counter per tick ---
                    raw_tp1_px = float(avg_px) * (1.0 + float(tp1_take_pct))
                    be_px_candidate = float(avg_px) * (1.0 + float(breakeven_off))
                    tp1_stop_target_px = float(be_px_candidate) if tp1_move_stop_to_be else float(raw_tp1_px)

                    # TP1 si "arma" quando il target è stato superato; NON vende subito.
                    # Da qui in poi uscirà solo in discesa, con conferma su 2 tick.
                    if (not bool(pos.tp1_done)) and peak_pnl >= float(tp1_take_pct) and float(pos.qty) > 0.0:
                        async with self._lock:
                            pos.tp1_done = True

                            if int(getattr(pos, "trail_armed_ts", 0) or 0) == 0:
                                pos.trail_armed_ts = int(now)

                            # quando si arma un nuovo livello, resettiamo la conferma breach
                            pos.trail_breach_n = 0

                            self.positions[k] = pos
                            self.store.save_positions(self.positions)

                        await self.notify.send(
                            f"🟡 TP1 {k}\n"
                            f"armed_only stop_px={float(tp1_stop_target_px):.10f}\n"
                            f"qty_left={float(getattr(pos, 'qty', 0.0) or 0.0):.6f} cash={float(self.cash):.2f}$"
                        )

                    # --- knife continuation exit, ma con guard rail minimi ---
                    dump_min_hold = max(20, int(strat.get("dump_speed_again_min_hold_sec", 90) or 90))
                    dump_min_pnl = min(-0.01, float(strat.get("dump_speed_again_min_pnl", -0.06) or -0.06))
                    dump_disable_when_trailing = _as_bool(strat.get("dump_speed_again_disable_when_trailing_armed", True))

                    hold_ok = age >= int(dump_min_hold)
                    pnl_ok = float(pnl) <= float(dump_min_pnl)
                    trail_ok = (not dump_disable_when_trailing) or int(getattr(pos, "trail_armed_ts", 0) or 0) == 0

                    if (
                        exit_dump_again is not None
                        and speed is not None
                        and hold_ok
                        and pnl_ok
                        and trail_ok
                        and float(speed) < float(exit_dump_again)
                    ):
                        slip = fill_slip(feat, k)
                        await self.exit_full(k, pos, px, reason="dump_speed_again", slip=int(slip))
                        continue

                    # --- pump slow only if already green ---
                    if exit_pump_slow is not None and speed is not None and pnl > 0 and float(speed) < float(exit_pump_slow):
                        slip = fill_slip(feat, k)
                        await self.exit_full(k, pos, px, reason="pump_slow", slip=int(slip))
                        continue

                    # --- armo il trailing una sola volta quando viene raggiunta l'attivazione ---
                    if int(getattr(pos, "trail_armed_ts", 0) or 0) == 0 and peak_pnl >= float(activate_pct):
                        pos.trail_armed_ts = int(now)
                        pos.trail_step_n = 0
                        pos.trail_breach_n = 0

                    # --- costruisco tutti i livelli protetti candidati ---
                    be_px = None
                    if peak_pnl >= float(breakeven_arm):
                        be_px = float(be_px_candidate)

                    tp1_stop_px = float(tp1_stop_target_px) if bool(pos.tp1_done) else None

                    step_stop_px = None
                    if int(getattr(pos, "trail_armed_ts", 0) or 0) > 0:
                        step_for_calc = max(float(step_pct), 1e-12)

                        if peak_pnl >= float(activate_pct):
                            step_n = int(math.floor(((float(peak_pnl) - float(activate_pct)) + 1e-12) / step_for_calc))
                        else:
                            step_n = 0

                        step_n = max(0, int(step_n))

                        if step_n > int(getattr(pos, "trail_step_n", 0) or 0):
                            pos.trail_step_n = int(step_n)
                            pos.trail_breach_n = 0

                        step_level = float(activate_pct) + (int(getattr(pos, "trail_step_n", 0) or 0) * float(step_pct))

                        # rebound_retrace_pct ora viene rispettato davvero:
                        # 0.0 = ritocco esatto dello step
                        # >0  = consenti giveback sotto lo step
                        lock_profit = max(
                            float(floor_profit),
                            float(step_level) - max(0.0, float(giveback_pct)),
                        )

                        step_stop_px = float(avg_px) * (1.0 + float(lock_profit))

                    # --- scelgo UN solo stop finale per questo tick: il più alto ---
                    stop_candidates = []
                    if be_px is not None:
                        stop_candidates.append(("breakeven_stop", float(be_px)))
                    if tp1_stop_px is not None:
                        stop_candidates.append(("tp1_stop", float(tp1_stop_px)))
                    if step_stop_px is not None:
                        stop_candidates.append((f"trail_step_{int(getattr(pos, 'trail_step_n', 0) or 0)}", float(step_stop_px)))

                    if stop_candidates:
                        exit_reason, calc_stop_px = max(stop_candidates, key=lambda item: item[1])

                        pos.trail_stop_px = max(
                            float(getattr(pos, "trail_stop_px", 0.0) or 0.0),
                            float(calc_stop_px),
                        )

                        # QUI si conta il breach UNA SOLA VOLTA per tick
                        if px <= float(pos.trail_stop_px):
                            pos.trail_breach_n = int(getattr(pos, "trail_breach_n", 0) or 0) + 1
                        else:
                            pos.trail_breach_n = 0

                        breach_ok = int(getattr(pos, "trail_breach_n", 0) or 0) >= int(exit_confirm_ticks)

                        if str(exit_reason).startswith("trail_step_"):
                            exit_allowed = (
                                (now - int(getattr(pos, "trail_armed_ts", 0) or 0)) >= int(min_after_arm)
                                and int(getattr(pos, "trail_step_n", 0) or 0) >= int(min_step_to_exit)
                            )
                        else:
                            exit_allowed = (
                                (now - int(getattr(pos, "trail_armed_ts", 0) or 0)) >= int(min_after_arm)
                            )

                        if breach_ok and exit_allowed:
                            slip = fill_slip(feat, k)
                            await self.exit_full(k, pos, px, reason=str(exit_reason), slip=int(slip))
                            continue

                    # --- knife continuation exit, ma con guard rail minimi ---
                    dump_min_hold = max(20, int(strat.get("dump_speed_again_min_hold_sec", 90) or 90))
                    dump_min_pnl = min(-0.01, float(strat.get("dump_speed_again_min_pnl", -0.06) or -0.06))
                    dump_disable_when_trailing = _as_bool(strat.get("dump_speed_again_disable_when_trailing_armed", True))

                    hold_ok = age >= int(dump_min_hold)
                    pnl_ok = float(pnl) <= float(dump_min_pnl)
                    trail_ok = (not dump_disable_when_trailing) or int(getattr(pos, "trail_armed_ts", 0) or 0) == 0

                    if (
                        exit_dump_again is not None
                        and speed is not None
                        and hold_ok
                        and pnl_ok
                        and trail_ok
                        and float(speed) < float(exit_dump_again)
                    ):
                        slip = fill_slip(feat, k)
                        await self.exit_full(k, pos, px, reason="dump_speed_again", slip=int(slip))
                        continue

                    # --- pump slow only if already green ---
                    if exit_pump_slow is not None and speed is not None and pnl > 0 and float(speed) < float(exit_pump_slow):
                        slip = fill_slip(feat, k)
                        await self.exit_full(k, pos, px, reason="pump_slow", slip=int(slip))
                        continue

                    # --- stepped trailing ---
                    if int(getattr(pos, "trail_armed_ts", 0) or 0) == 0 and pnl >= float(activate_pct):
                        pos.trail_armed_ts = int(now)
                        pos.trail_step_n = 0
                        pos.trail_breach_n = 0

                    if int(getattr(pos, "trail_armed_ts", 0) or 0) > 0:
                        if step_pct <= 0:
                            step_pct = 1e9

                        step_n = int((peak_pnl - float(activate_pct)) // float(step_pct)) if peak_pnl >= float(activate_pct) else 0
                        if step_n > int(getattr(pos, "trail_step_n", 0) or 0):
                            pos.trail_step_n = int(step_n)

                        step_level = float(activate_pct) + (int(getattr(pos, "trail_step_n", 0) or 0) * float(step_pct))
                        lock_profit = max(float(floor_profit), float(step_level))
                        new_stop_px = float(avg_px) * (1.0 + float(lock_profit))

                        cur_stop = float(getattr(pos, "trail_stop_px", 0.0) or 0.0)
                        if cur_stop <= 0.0:
                            pos.trail_stop_px = float(new_stop_px)
                        else:
                            pos.trail_stop_px = max(float(cur_stop), float(new_stop_px))

                        if px <= float(pos.trail_stop_px):
                            pos.trail_breach_n = int(getattr(pos, "trail_breach_n", 0) or 0) + 1
                        else:
                            pos.trail_breach_n = 0

                        breach_ok = int(getattr(pos, "trail_breach_n", 0) or 0) >= int(exit_confirm_ticks)

                        if (
                            (now - int(pos.trail_armed_ts)) >= int(min_after_arm)
                            and int(getattr(pos, "trail_step_n", 0) or 0) >= int(min_step_to_exit)
                            and breach_ok
                        ):
                            slip = fill_slip(feat, k)
                            await self.exit_full(
                                k,
                                pos,
                                px,
                                reason=f"trail_step_{int(getattr(pos, 'trail_step_n', 0) or 0)}",
                                slip=int(slip),
                            )
                            continue

                self.positions[k] = pos

            except Exception as e:
                await self.circuit_fail("manage_positions", e)

    

    async def task_narrative_sources(self):
        """Collect narrative signals from Telegram (Telethon) and NewsAPI."""
        h = self._health_cfg()
        social = (h.get("social") or {}) if isinstance(h.get("social"), dict) else {}

        tg_cfg = (social.get("telegram") or {}) if isinstance(social.get("telegram"), dict) else {}
        news_cfg = (social.get("newsapi") or {}) if isinstance(social.get("newsapi"), dict) else {}
        x_cfg = (social.get("x") or {}) if isinstance(social.get("x"), dict) else {}

        # Collect anche se social.enabled è FALSE (così paper prepara il live)
        tg_on = _as_bool(tg_cfg.get("enabled", False))
        news_on = _as_bool(news_cfg.get("enabled", False))
        x_on = _as_bool(x_cfg.get("enabled", False))
        if not (tg_on or news_on or x_on):
            return

        poll = _as_int(social.get("poll_sec", 45)) or 45
        tg_since = _as_int(tg_cfg.get("since_sec", 300)) or 300
        news_lb = _as_int(news_cfg.get("lookback_hours", 24)) or 24
        x_since = _as_int(x_cfg.get("since_sec", 900)) or 900        
        
        while True:
            try:
                # key -> channel -> mentions (sempre definito, così non esplode se tg è off/None)
                counts = defaultdict(lambda: defaultdict(int))

                if self.tg is not None and tg_on:
                    evs = await self.tg.fetch_mentions(
                        since_sec=int(tg_since),
                        max_msgs_per_channel=int(_as_int(tg_cfg.get("max_msgs_per_channel", 200)) or 200),
                    )
                    default_chain = normalize_chain_id(tg_cfg.get("default_chain", "base"), default="base")
                    default_evm_chain = normalize_chain_id(tg_cfg.get("default_evm_chain", default_chain), default=default_chain)
                    default_solana_chain = normalize_chain_id(tg_cfg.get("default_solana_chain", "solana"), default="solana")
                    allowed = None if wants_all_chains(self.cfg.allowed_chains) else set(
                        normalize_chain_id(c, default=default_chain)
                        for c in (self.cfg.allowed_chains or [default_chain])
                    )
                    resolved: Dict[Tuple[str, str], str] = {}
                    accept_plain = _as_bool(tg_cfg.get("accept_plain_addrs", False))

                    for ev in evs:
                        ch = (ev.get("channel") or "unknown").strip()

                        # (1) plain 0x... addresses (chain unknown -> default_chain)
                        # Default: IGNORE (unsafe on multi-chain Telegram). Enable with social.telegram.accept_plain_addrs=true
                        if accept_plain:
                            for a in (ev.get("addrs") or []):
                                raw_chain, raw_addr = infer_social_chain_for_plain_address(
                                    a,
                                    default_chain=default_chain,
                                    default_evm_chain=default_evm_chain,
                                    default_solana_chain=default_solana_chain,
                                )
                                if not raw_chain or not raw_addr:
                                    continue
                                if allowed and raw_chain not in allowed:
                                    continue
                                key = make_token_key(raw_chain, raw_addr)
                                counts[key][ch] += 1

                        # (2) DexScreener links (chain-aware; resolves pair->token)
                        for hit in (ev.get("dex") or []):
                            chain = normalize_chain_id(hit.get("chain"), default=default_chain)
                            if allowed and chain not in allowed:
                                continue

                            addr = canonical_token_ref(chain, hit.get("address") or "")
                            if not is_dexscreener_id(addr):
                                continue

                            tup = (chain, addr)
                            if tup not in resolved:
                                resolved[tup] = await self.dex.resolve_token_address(chain, addr)
                            token_addr = resolved[tup]

                            if token_addr:
                                key = make_token_key(chain, token_addr)
                                counts[key][ch] += 1

                    for key, by_ch in counts.items():
                        for ch, c in by_ch.items():
                            self.store.log_social_mentions(
                                ts=now_ts(),
                                key=key,
                                source="telegram",
                                channel=ch,
                                mentions=int(c),
                            )

                # --- OPTIONAL: add tokens repeated on Telegram into watchlist ---
                # Idea: se un contratto viene menzionato >= N volte su Telegram in una finestra (window_sec),
                # lo mettiamo in watchlist anche se non è ancora apparso nei feed DexScreener.
                try:
                    wl_on = _as_bool(tg_cfg.get("watchlist_on_repeats", True))
                    wl_min = _as_int(tg_cfg.get("watchlist_min_mentions", 3)) or 3
                    wl_win = _as_int(tg_cfg.get("watchlist_window_sec", social.get("window_sec", 21600))) or 21600
                    wl_max = _as_int(tg_cfg.get("watchlist_max_add_per_poll", 5))
                    # wl_max: 0 = unlimited; <0 = disable
                    if wl_max is None:
                        wl_max = 5
                    # If TRUE, we only add to watchlist tokens that DexScreener can actually resolve on this chain.
                    # This prevents multi-chain Telegram channels from poisoning the watchlist with non-Base addresses.
                    require_dex = _as_bool(tg_cfg.get("watchlist_require_dexscreener", True))

                    if wl_on and int(wl_min) > 0 and int(wl_max) >= 0:
                        added = 0
                        for key in list(counts.keys()):
                            if int(wl_max) > 0 and added >= int(wl_max):
                                break

                            msum = self.store.social_mentions_sum(key, window_sec=int(wl_win), source="telegram")
                            if int(msum or 0) < int(wl_min):
                                continue

                            # canonicalize key parts
                            chain = key.split(":", 1)[0] if ":" in key else default_chain
                            token_addr = key.split(":", 1)[1] if ":" in key else key
                            chain = normalize_chain_id(chain, default=default_chain)
                            token_addr = canonical_token_ref(chain, token_addr)
                            key2 = make_token_key(chain, token_addr)

                            # safety: do not let Telegram/News poison the watchlist with non-allowed chains
                            if allowed and chain not in allowed:
                                continue

                            # safety: only add tokens that DexScreener can actually see on this chain
                            if require_dex and (not await self.dex.token_exists(chain, token_addr)):
                                continue

                            async with self._lock:
                                if key2 in self.positions or key2 in self.watchlist:
                                    continue
                                if hasattr(self, "banned_set") and isinstance(getattr(self, "banned_set"), set) and key2 in self.banned_set:
                                    continue
                                if hasattr(self, "bad_tokens") and isinstance(getattr(self, "bad_tokens"), set) and key2 in self.bad_tokens:
                                    continue

                                self.watchlist[key2] = {
                                    "chain": chain,
                                    "token": canonical_token_ref(chain, token_addr),
                                    "added_ts": now_ts(),
                                    "pair": "",
                                    "score": float(msum) * 0.001,  # score piccolino, verrà ricalcolato appena arrivano snapshot
                                    "cooldown_until": 0,
                                    "last_feat": None,
                                }
                                self.store.save_watchlist(self.watchlist)
                                self.store.set_state(
                                    "watchlist_last_added",
                                    {"ts": now_ts(), "key": key2, "src": "telegram", "mentions": int(msum)},
                                )
                            added += 1
                except Exception:
                    pass

                if self.x is not None and x_on:
                    x_queries = [str(q).strip() for q in list(x_cfg.get("search_queries") or []) if str(q).strip()]
                    x_users = [str(u).strip() for u in list(x_cfg.get("usernames") or []) if str(u).strip()]
                    x_max_results = _as_int(x_cfg.get("max_results_per_call", 25)) or 25

                    x_default_chain = normalize_chain_id(x_cfg.get("default_chain", "base"), default="base")
                    x_default_evm_chain = normalize_chain_id(x_cfg.get("default_evm_chain", x_default_chain), default=x_default_chain)
                    x_default_solana_chain = normalize_chain_id(x_cfg.get("default_solana_chain", "solana"), default="solana")
                    x_allowed = None if wants_all_chains(self.cfg.allowed_chains) else set(
                        normalize_chain_id(c, default=x_default_chain)
                        for c in (self.cfg.allowed_chains or [x_default_chain])
                    )
                    x_accept_plain = _as_bool(x_cfg.get("accept_plain_addrs", False))
                    x_counts = defaultdict(lambda: defaultdict(int))
                    x_resolved: Dict[Tuple[str, str], str] = {}

                    evs = await self.x.fetch_mentions(
                        queries=x_queries,
                        usernames=x_users,
                        since_sec=int(x_since),
                        max_results_per_call=int(x_max_results),
                    )

                    for ev in evs:
                        ch = (ev.get("channel") or "x").strip()

                        if x_accept_plain:
                            for a in (ev.get("addrs") or []):
                                raw_chain, raw_addr = infer_social_chain_for_plain_address(
                                    a,
                                    default_chain=x_default_chain,
                                    default_evm_chain=x_default_evm_chain,
                                    default_solana_chain=x_default_solana_chain,
                                )
                                if not raw_chain or not raw_addr:
                                    continue
                                if x_allowed and raw_chain not in x_allowed:
                                    continue
                                key = make_token_key(raw_chain, raw_addr)
                                x_counts[key][ch] += 1

                        for hit in (ev.get("dex") or []):
                            chain = normalize_chain_id(hit.get("chain"), default=x_default_chain)
                            if x_allowed and chain not in x_allowed:
                                continue

                            addr = canonical_token_ref(chain, hit.get("address") or "")
                            if not is_dexscreener_id(addr):
                                continue

                            tup = (chain, addr)
                            if tup not in x_resolved:
                                x_resolved[tup] = await self.dex.resolve_token_address(chain, addr)
                            token_addr = x_resolved[tup]

                            if token_addr:
                                key = make_token_key(chain, token_addr)
                                x_counts[key][ch] += 1

                    for key, by_ch in x_counts.items():
                        for ch, c in by_ch.items():
                            self.store.log_social_mentions(
                                ts=now_ts(),
                                key=key,
                                source="x",
                                channel=ch,
                                mentions=int(c),
                            )

                    try:
                        wl_on = _as_bool(x_cfg.get("watchlist_on_repeats", True))
                        wl_min = _as_int(x_cfg.get("watchlist_min_mentions", 2)) or 2
                        wl_win = _as_int(x_cfg.get("watchlist_window_sec", social.get("window_sec", 21600))) or 21600
                        wl_max = _as_int(x_cfg.get("watchlist_max_add_per_poll", 10))
                        if wl_max is None:
                            wl_max = 10
                        require_dex = _as_bool(x_cfg.get("watchlist_require_dexscreener", True))

                        if wl_on and int(wl_min) > 0 and int(wl_max) >= 0:
                            added = 0
                            for key in list(x_counts.keys()):
                                if int(wl_max) > 0 and added >= int(wl_max):
                                    break

                                msum = self.store.social_mentions_sum(key, window_sec=int(wl_win), source="x")
                                if int(msum or 0) < int(wl_min):
                                    continue

                                chain = key.split(":", 1)[0] if ":" in key else x_default_chain
                                token_addr = key.split(":", 1)[1] if ":" in key else key
                                chain = normalize_chain_id(chain, default=x_default_chain)
                                token_addr = canonical_token_ref(chain, token_addr)
                                key2 = make_token_key(chain, token_addr)

                                if x_allowed and chain not in x_allowed:
                                    continue

                                if require_dex and (not await self.dex.token_exists(chain, token_addr)):
                                    continue

                                async with self._lock:
                                    if key2 in self.positions or key2 in self.watchlist:
                                        continue
                                    if hasattr(self, "banned_set") and isinstance(getattr(self, "banned_set"), set) and key2 in self.banned_set:
                                        continue
                                    if hasattr(self, "bad_tokens") and isinstance(getattr(self, "bad_tokens"), set) and key2 in self.bad_tokens:
                                        continue

                                    self.watchlist[key2] = {
                                        "chain": chain,
                                        "token": canonical_token_ref(chain, token_addr),
                                        "added_ts": now_ts(),
                                        "pair": "",
                                        "score": float(msum) * 0.001,
                                        "cooldown_until": 0,
                                        "last_feat": None,
                                    }
                                    self.store.save_watchlist(self.watchlist)
                                    self.store.set_state(
                                        "watchlist_last_added",
                                        {"ts": now_ts(), "key": key2, "src": "x", "mentions": int(msum)},
                                    )
                                added += 1
                    except Exception:
                        pass

                if self.news is not None and news_on:
                    # --- NewsAPI: mentions + discovery (no backoff se pause_on_429_sec=0) ---
                    now = int(now_ts())

                    pause_cfg = int(news_cfg.get("pause_on_429_sec", 0) or 0)  # 0 = NO backoff interno
                    if pause_cfg > 0 and int(getattr(self, "_newsapi_pause_until", 0) or 0) > now:
                        # in backoff: salta tutto NewsAPI per questo giro
                        pass
                    else:
                        # --------------------
                        # (1) MENTIONS su token già in watchlist
                        # --------------------
                        wl_limit = int(news_cfg.get("watchlist_query_limit", 80) or 80)
                        wl = self.store.get_watchlist_rows(limit=int(wl_limit))

                        max_q = int(news_cfg.get("max_queries_per_poll", 0) or 0)  # 0 = unlimited
                        min_int = int(news_cfg.get("min_interval_sec", 0) or 0)    # 0 = disabled
                        sent_q = 0

                        for w in wl:
                            if max_q > 0 and sent_q >= max_q:
                                break

                            key = (w.get("key") or "").strip()
                            if not key:
                                continue

                            last = int(self.store.get_state(f"news_last:{key}", 0) or 0)
                            if min_int > 0 and (now - last) < min_int:
                                continue

                            chain = normalize_chain_id((w.get("chain") or (key.split(":", 1)[0] if ":" in key else "")).strip(), default="base")
                            token_addr = canonical_token_ref(chain, (w.get("token") or (key.split(":", 1)[1] if ":" in key else key)).strip())
                            pair = (w.get("pair") or "").strip()

                            # query: se ho symbol, uso quello; altrimenti uso address
                            sym = ""
                            if "/" in pair:
                                sym = pair.split("/")[0].strip()
                            query = f"{sym} crypto" if sym else str(token_addr)

                            sent_q += 1
                            try:
                                n = await self.news.count_mentions(query=query, lookback_hours=int(news_lb))
                                # se una call riesce, reset backoff
                                self._newsapi_backoff_sec = 0
                                if pause_cfg <= 0:
                                    self._newsapi_pause_until = 0

                            except Exception as e:
                                msg = str(e)

                                # --- 429: backoff SOLO se pause_on_429_sec>0 ---
                                if ("NewsAPI 429" in msg) or ("rateLimited" in msg):
                                    now2 = int(now_ts())

                                    if pause_cfg <= 0:
                                        # backoff disabilitato: notifichiamo (max 1/20min) e continuiamo
                                        last_nt = int(getattr(self, "_newsapi_last_429_notify_ts", 0) or 0)
                                        if (now2 - last_nt) >= 1200:
                                            await self.notify.send("🟠 NewsAPI 429 rate-limited (backoff DISABLED)")
                                            self._newsapi_last_429_notify_ts = int(now2)
                                        continue

                                    cur = int(getattr(self, "_newsapi_backoff_sec", 0) or 0)
                                    if cur <= 0:
                                        cur = 300
                                    else:
                                        cur = min(cur * 2, 7200)

                                    cur = min(int(cur), int(pause_cfg))
                                    self._newsapi_backoff_sec = int(cur)
                                    self._newsapi_pause_until = int(now2 + cur)

                                    last_nt = int(getattr(self, "_newsapi_last_429_notify_ts", 0) or 0)
                                    if (now2 - last_nt) >= 1200:
                                        await self.notify.send(f"🟠 NewsAPI 429 rate-limited: backoff {cur}s")
                                        self._newsapi_last_429_notify_ts = int(now2)

                                    continue

                                # altri errori: non fermiamo il bot, log e avanti
                                logging.getLogger("microcap_bot_v4").warning("NewsAPI error key=%s err=%s", key, msg)
                                continue

                            # log (cap a 50)
                            self.store.log_social_mentions(
                                ts=now_ts(),
                                key=key,
                                source="newsapi",
                                channel="news",
                                mentions=int(min(int(n or 0), 50)),
                            )
                            self.store.set_state(f"news_last:{key}", now_ts())

                        # --------------------
                        # (2) DISCOVERY: cerca articoli e tira fuori 0x... / link DexScreener
                        # --------------------
                        try:
                            disc_on = _as_bool(news_cfg.get("discovery_enabled", False))
                            if disc_on:
                                disc_queries = news_cfg.get("discovery_queries", [])
                                if isinstance(disc_queries, str):
                                    disc_queries = [disc_queries]
                                if not isinstance(disc_queries, list):
                                    disc_queries = []

                                disc_queries = [str(q).strip() for q in disc_queries if str(q).strip()]
                                disc_lb = _as_int(news_cfg.get("discovery_lookback_hours", news_lb)) or int(news_lb)
                                disc_page = _as_int(news_cfg.get("discovery_page_size", 20)) or 20
                                disc_max_articles = _as_int(news_cfg.get("discovery_max_articles", 40))
                                if disc_max_articles is None:
                                    disc_max_articles = 40

                                add_immediate = _as_bool(news_cfg.get("discovery_add_immediately", True))
                                add_max = _as_int(news_cfg.get("discovery_max_add_per_poll", 5))
                                if add_max is None:
                                    add_max = 5

                                # If TRUE, we only add discovery tokens that DexScreener actually sees on this chain.
                                # This prevents discovery from adding non-Base (or non-existent) contracts into the watchlist.
                                require_dex = _as_bool(news_cfg.get(
                                    "discovery_require_dexscreener",
                                    news_cfg.get("watchlist_require_dexscreener", True),
                                ))

                                default_chain = normalize_chain_id(news_cfg.get("discovery_default_chain", "base"), default="base")
                                default_evm_chain = normalize_chain_id(news_cfg.get("discovery_default_evm_chain", default_chain), default=default_chain)
                                default_solana_chain = normalize_chain_id(news_cfg.get("discovery_default_solana_chain", "solana"), default="solana")
                                allowed = None if wants_all_chains(self.cfg.allowed_chains) else set(
                                    normalize_chain_id(c, default=default_chain)
                                    for c in (self.cfg.allowed_chains or [default_chain])
                                )

                                async def _resolve_token(chain: str, addr: str) -> str:
                                    """addr può essere token o pair; prova token endpoint, altrimenti /latest/dex/pairs -> baseToken.address."""
                                    a = canonical_token_ref(chain, addr or "")
                                    if not a:
                                        return ""
                                    # 1) try as token
                                    try:
                                        pairs = await self.dex.tokens_batch(chain, [a])
                                        if pairs:
                                            return a
                                    except Exception:
                                        pass
                                    # 2) try as pair
                                    try:
                                        await self.dex.lim_market.acquire(1)
                                        r = await self.dex.http.get(f"/latest/dex/pairs/{chain}/{a}")
                                        if r.status_code == 200:
                                            data = r.json() or {}
                                            pairs = data.get("pairs") if isinstance(data, dict) else None
                                            if isinstance(pairs, list) and pairs:
                                                p0 = pairs[0] or {}
                                                base = ((p0.get("baseToken") or {}).get("address") or "")
                                                base = canonical_token_ref(chain, str(base).strip())
                                                if base:
                                                    return base
                                    except Exception:
                                        pass
                                    return a

                                added = 0
                                seen_keys = set()

                                for q in disc_queries:
                                    if int(add_max) > 0 and added >= int(add_max):
                                        break

                                    # chiamata NewsAPI
                                    arts = await self.news.search_articles(query=q, lookback_hours=int(disc_lb), page_size=int(disc_page))
                                    if int(disc_max_articles) > 0:
                                        arts = arts[: int(disc_max_articles)]

                                    for art in (arts or []):
                                        if int(add_max) > 0 and added >= int(add_max):
                                            break

                                        if not isinstance(art, dict):
                                            continue

                                        blob = " ".join(
                                            x for x in [
                                                str(art.get("title") or ""),
                                                str(art.get("description") or ""),
                                                str(art.get("content") or ""),
                                                str(art.get("url") or ""),
                                            ] if x
                                        )

                                        # (a) chain-aware token links (DexScreener + Birdeye)
                                        for hit in extract_social_url_hits(blob):
                                            chain = normalize_chain_id(hit.get("chain"), default=default_chain)
                                            if allowed and chain not in allowed:
                                                continue

                                            addr = canonical_token_ref(chain, hit.get("address") or "")
                                            if not is_dexscreener_id(addr):
                                                continue

                                            tok = await _resolve_token(chain, addr)
                                            if not is_supported_token_ref(tok):
                                                continue

                                            key = make_token_key(chain, tok)
                                            if key in seen_keys:
                                                continue
                                            seen_keys.add(key)

                                            # log discovery event
                                            self.store.log_social_mentions(
                                                ts=now_ts(),
                                                key=key,
                                                source="newsapi",
                                                channel="discovery",
                                                mentions=1,
                                            )

                                            if add_immediate:
                                                if require_dex and (not await self.dex.token_exists(chain, tok)):
                                                    continue
                                                async with self._lock:
                                                    if key in self.positions or key in self.watchlist:
                                                        continue
                                                    if hasattr(self, "banned_set") and isinstance(getattr(self, "banned_set"), set) and key in self.banned_set:
                                                        continue
                                                    if hasattr(self, "bad_tokens") and isinstance(getattr(self, "bad_tokens"), set) and key in self.bad_tokens:
                                                        continue

                                                    self.watchlist[key] = {
                                                        "chain": chain,
                                                        "token": canonical_token_ref(chain, tok),
                                                        "added_ts": now_ts(),
                                                        "pair": "",
                                                        "score": 0.001,
                                                        "cooldown_until": 0,
                                                        "last_feat": None,
                                                    }
                                                    self.store.save_watchlist(self.watchlist)
                                                    self.store.set_state(
                                                        "watchlist_last_added",
                                                        {"ts": now_ts(), "key": key, "src": "newsapi_discovery", "q": q[:80]},
                                                    )
                                                added += 1

                                        # (b) raw plain addresses (EVM + Solana)
                                        for a in extract_plain_social_addresses(blob):
                                            if int(add_max) > 0 and added >= int(add_max):
                                                break

                                            chain, raw_addr = infer_social_chain_for_plain_address(
                                                a,
                                                default_chain=default_chain,
                                                default_evm_chain=default_evm_chain,
                                                default_solana_chain=default_solana_chain,
                                            )
                                            if not chain or not raw_addr:
                                                continue
                                            if not is_supported_token_ref(raw_addr):
                                                continue

                                            if allowed and chain not in allowed:
                                                continue

                                            key = make_token_key(chain, raw_addr)
                                            if key in seen_keys:
                                                continue
                                            seen_keys.add(key)

                                            self.store.log_social_mentions(
                                                ts=now_ts(),
                                                key=key,
                                                source="newsapi",
                                                channel="discovery",
                                                mentions=1,
                                            )

                                            if add_immediate:
                                                if require_dex and (not await self.dex.token_exists(chain, raw_addr)):
                                                    continue
                                                async with self._lock:
                                                    if key in self.positions or key in self.watchlist:
                                                        continue
                                                    if hasattr(self, "banned_set") and isinstance(getattr(self, "banned_set"), set) and key in self.banned_set:
                                                        continue
                                                    if hasattr(self, "bad_tokens") and isinstance(getattr(self, "bad_tokens"), set) and key in self.bad_tokens:
                                                        continue

                                                    self.watchlist[key] = {
                                                        "chain": chain,
                                                        "token": canonical_token_ref(chain, raw_addr),
                                                        "added_ts": now_ts(),
                                                        "pair": "",
                                                        "score": 0.001,
                                                        "cooldown_until": 0,
                                                        "last_feat": None,
                                                    }
                                                    self.store.save_watchlist(self.watchlist)
                                                    self.store.set_state(
                                                        "watchlist_last_added",
                                                        {"ts": now_ts(), "key": key, "src": "newsapi_discovery", "q": q[:80]},
                                                    )
                                                added += 1
                        except Exception:
                            pass

                        # --------------------
                        # (3) OPTIONAL: add tokens repeated on NewsAPI into watchlist (anche senza add_immediate)
                        # --------------------
                        try:
                            wl_on = _as_bool(news_cfg.get("watchlist_on_repeats", False))
                            wl_min = _as_int(news_cfg.get("watchlist_min_mentions", 1)) or 1
                            wl_win = _as_int(news_cfg.get("watchlist_window_sec", social.get("window_sec", 21600))) or 21600
                            wl_max = _as_int(news_cfg.get("watchlist_max_add_per_poll", 3))
                            if wl_max is None:
                                wl_max = 3
                            # If TRUE, we only add watchlist tokens that DexScreener can actually see on this chain.
                            require_dex_wl = _as_bool(news_cfg.get("watchlist_require_dexscreener", True))

                            if wl_on and int(wl_min) > 0 and int(wl_max) >= 0:
                                added = 0
                                cur = self.store.conn.cursor()
                                t0 = int(now_ts() - int(wl_win))
                                cur.execute(
                                    """
                                    SELECT key, COALESCE(SUM(mentions),0) AS m
                                    FROM social_events
                                    WHERE ts>=? AND source='newsapi'
                                    GROUP BY key
                                    ORDER BY m DESC
                                    LIMIT 400
                                    """,
                                    (t0,),
                                )
                                rows = cur.fetchall()

                                for key, msum in rows:
                                    if int(wl_max) > 0 and added >= int(wl_max):
                                        break

                                    key = str(key)
                                    msum = int(msum or 0)
                                    if msum < int(wl_min):
                                        continue

                                    chain = key.split(":", 1)[0] if ":" in key else "base"
                                    token_addr = key.split(":", 1)[1] if ":" in key else key
                                    chain = normalize_chain_id(chain, default="base")
                                    token_addr = canonical_token_ref(chain, token_addr)
                                    key2 = make_token_key(chain, token_addr)

                                    # safety: do not add non-allowed chains (e.g., polluted keys)
                                    if (not wants_all_chains(self.cfg.allowed_chains)) and self.cfg.allowed_chains and chain not in set(self.cfg.allowed_chains):
                                        continue

                                    # safety: only add tokens that DexScreener can actually see on this chain
                                    if require_dex_wl and (not await self.dex.token_exists(chain, token_addr)):
                                        continue

                                    async with self._lock:
                                        if key2 in self.positions or key2 in self.watchlist:
                                            continue
                                        if hasattr(self, "banned_set") and isinstance(getattr(self, "banned_set"), set) and key2 in self.banned_set:
                                            continue
                                        if hasattr(self, "bad_tokens") and isinstance(getattr(self, "bad_tokens"), set) and key2 in self.bad_tokens:
                                            continue

                                        self.watchlist[key2] = {
                                            "chain": chain,
                                            "token": canonical_token_ref(chain, token_addr),
                                            "added_ts": now_ts(),
                                            "pair": "",
                                            "score": float(msum) * 0.001,
                                            "cooldown_until": 0,
                                            "last_feat": None,
                                        }
                                        self.store.save_watchlist(self.watchlist)
                                        self.store.set_state(
                                            "watchlist_last_added",
                                            {"ts": now_ts(), "key": key2, "src": "newsapi", "mentions": int(msum)},
                                        )
                                    added += 1   
                        except Exception:
                               pass

            except Exception as e:
                await self.circuit_fail("narrative", e)

            await asyncio.sleep(float(poll))

    async def task_entry_manage(self):
        while True:
            await asyncio.sleep(float(self.cfg.tick_interval_sec))
            if now_ts() < self.pause_until:
                continue
            try:
                await self.manage_positions()
                await self.try_entries()
                await self.update_metrics()
                async with self._lock:
                    self.store.set_state("cash", self.cash)
                    self.store.set_state("peak_equity", self.peak_equity)
                    self.store.save_positions(self.positions)
            except Exception as e:
                await self.circuit_fail("core_loop", e)

    
    async def try_entries(self):
        strat = (self.cfg.strategy or {}) if isinstance(getattr(self.cfg, "strategy", None), dict) else {}
        inst = (strat.get("institutional") or {}) if isinstance(strat.get("institutional"), dict) else {}

        kill_switch_on = _as_bool(os.getenv("BOT_KILL_SWITCH", "0")) or _as_bool(self.store.get_state("kill_switch", False))
        if kill_switch_on:
            self.store.set_state("entry_intent", None)
            self.store.set_state("entry_last", {
                "ts": now_ts(),
                "key": "",
                "status": "scan",
                "reason": "blocked:kill_switch",
            })
            try:
                self.store.log_audit(ts=now_ts(), event="kill_switch_block", key="", payload={"source": "env_or_state"})
            except Exception:
                pass
            return

        # --- daily rate control ---
        tp_raw = strat.get("target_entries_per_day", 7)
        dc_raw = strat.get("daily_entry_cap", 10)

        try:
            target_per_day = int(float(str(tp_raw).strip().replace(",", "."))) if tp_raw is not None else 7
        except Exception:
            target_per_day = 7

        try:
            daily_cap = int(float(str(dc_raw).strip().replace(",", "."))) if dc_raw is not None else 10
        except Exception:
            daily_cap = 10

        use_pacing = target_per_day > 0
        if not use_pacing:
            target_per_day = 7

        daily_cap = None if (daily_cap is not None and int(daily_cap) <= 0) else int(daily_cap)

        if use_pacing:
            target_per_day = max(1, int(target_per_day))
            if daily_cap is not None:
                daily_cap = max(target_per_day, int(daily_cap))

        cap_str = "∞" if daily_cap is None else str(daily_cap)

        _ = self.can_trade_today()

        if daily_cap is not None and self.day_entries >= daily_cap:
            return

        async with self._lock:
            if len(self.positions) >= self.cfg.max_positions:
                self.store.set_state("entry_intent", None)
                self.store.set_state("entry_last", {
                    "ts": now_ts(),
                    "key": "",
                    "status": "scan",
                    "reason": "blocked:max_positions",
                })
                return
            if not self.can_open_new():
                self.store.set_state("entry_intent", None)
                self.store.set_state("entry_last", {
                    "ts": now_ts(),
                    "key": "",
                    "status": "scan",
                    "reason": "blocked:risk_block",
                })
                return

            inst_enabled = _as_bool(inst.get("enabled", True))
            reject_stale_snapshot_sec = int(inst.get("reject_stale_snapshot_sec", 45) or 45)
            raw_candidates = []
            raw_rejects = defaultdict(int)

            for k, info in self.watchlist.items():
                if k in self.positions:
                    raw_rejects["in_position"] += 1
                    continue
                if k in getattr(self, "banned_set", set()):
                    raw_rejects["banned"] += 1
                    continue
                if int(info.get("cooldown_until") or 0) > now_ts():
                    raw_rejects["cooldown"] += 1
                    continue
                if hasattr(self, "bad_tokens") and k in self.bad_tokens:
                    raw_rejects["bad_token"] += 1
                    continue

                feat = info.get("last_feat")
                if not feat:
                    raw_rejects["no_last_feat"] += 1
                    continue

                if inst_enabled and int(reject_stale_snapshot_sec) > 0:
                    snap_latest = self.store._snap_latest(k)
                    if not snap_latest or (now_ts() - int(snap_latest[0])) > int(reject_stale_snapshot_sec):
                        raw_rejects["stale_snapshot"] += 1
                        continue

                if not passes_filters(self.cfg, feat):
                    raw_rejects["filters"] += 1
                    continue

                raw_candidates.append((k, info, float(info.get("score") or -1.0)))

            if not raw_candidates:
                self.store.set_state("entry_intent", None)
                self.store.set_state("entry_last", {
                    "ts": now_ts(),
                    "key": "",
                    "status": "scan",
                    "reason": "no_raw_candidates",
                    "details": dict(raw_rejects),
                })
                return

        # --- pacing adjustment ---
        adj = 0.0
        if use_pacing:
            lt = time.localtime()
            sec_of_day = lt.tm_hour * 3600 + lt.tm_min * 60 + lt.tm_sec
            progress = sec_of_day / 86400.0
            expected_so_far = target_per_day * progress
            delta = expected_so_far - float(self.day_entries)
            adj = clamp(delta / max(1.0, target_per_day / 2.0), -0.35, 0.35)

        strat_mode = str(strat.get("mode") or "momentum").lower().strip()
        min_order_usd = float(strat.get("min_order_usd", 10.0) or 10.0)

        min_watch_age_sec = int(strat.get("min_watch_age_sec", 45) or 45)
        min_points_window_sec = int(strat.get("min_price_points_window_sec", 60) or 60)
        min_points_before_entry = int(strat.get("min_price_points_before_entry", 10) or 10)
        entry_signal_min = float(strat.get("entry_signal_min", 0.55) or 0.55)

        max_trade_notional_usd = float(inst.get("max_trade_notional_usd", 0.0) or 0.0)
        max_trade_notional_pct = float(inst.get("max_trade_notional_pct", 0.0) or 0.0)
        max_chain_exposure_pct = float(inst.get("max_chain_exposure_pct", 0.0) or 0.0)
        precheck_fail_cooldown_sec = int(inst.get("precheck_fail_cooldown_sec", 600) or 600)

        def chain_exposure_usd(chain_name: str) -> float:
            total = 0.0
            for pk, pos0 in self.positions.items():
                if str(getattr(pos0, "chain", "")) != str(chain_name):
                    continue
                feat0 = (self.watchlist.get(pk) or {}).get("last_feat") or {}
                px0 = safe_float(feat0.get("price_usd"))
                if px0 is None or float(px0) <= 0:
                    px0 = float(getattr(pos0, "avg_px", 0.0) or 0.0)
                total += float(getattr(pos0, "qty", 0.0) or 0.0) * float(px0 or 0.0)
            return float(total)

        def window_low(key: str, now0: int, window_sec: int) -> Optional[float]:
            q = self.cache.data.get(key)
            if not q:
                return None
            target = now0 - int(window_sec)
            low = None
            for t, p in reversed(q):
                if t < target:
                    break
                if p and p > 0 and (low is None or p < low):
                    low = p
            return low

        def recent_points(key: str, now0: int, window_sec: int) -> int:
            q = self.cache.data.get(key)
            if not q:
                return 0
            target = now0 - int(window_sec)
            cnt = 0
            for t, _ in reversed(q):
                if t < target:
                    break
                cnt += 1
            return cnt

        def dip_rebound_signal(feat: Dict[str, Any], dip: float, speed: float, micro_reb: float, speed5: float,
                               min_dip_abs: float, min_reb_speed: float, min_micro_reb: float, min_speed5: float) -> float:
            dip_abs = abs(min(float(dip), 0.0))
            target_dip = max(float(min_dip_abs) * 1.35, 0.01)

            dip_score = 1.0 - abs(dip_abs - target_dip) / max(target_dip, 1e-9)
            dip_score = clamp(dip_score, 0.0, 1.0)

            speed_score = clamp(float(speed) / max(float(min_reb_speed) * 2.5, 1e-9), 0.0, 1.0)
            micro_score = clamp(float(micro_reb) / max(float(min_micro_reb) * 2.0, 1e-9), 0.0, 1.0)
            speed5_score = clamp(float(speed5) / max(float(min_speed5) * 2.0, 1e-9), 0.0, 1.0)

            buys = int(feat.get("buys_m5") or 0)
            sells = int(feat.get("sells_m5") or 0)
            buy_ratio = buys / max(1, buys + sells)
            flow_score = clamp((buy_ratio - 0.50) / 0.20, 0.0, 1.0)

            liq = float(feat.get("liq_usd") or 0.0)
            liq_score = clamp(math.log10(max(liq, 1.0) / max(float(self.cfg.min_liquidity_usd), 1.0) + 1.0), 0.0, 1.0)

            return float(
                0.26 * dip_score
                + 0.24 * speed_score
                + 0.20 * micro_score
                + 0.14 * speed5_score
                + 0.10 * flow_score
                + 0.06 * liq_score
            )

        scored_candidates: List[Dict[str, Any]] = []
        score_rejects = defaultdict(int)
        now0 = now_ts()

        for k, info, raw_score in raw_candidates:
            feat = info.get("last_feat") or {}
            px = feat.get("price_usd")
            if px is None:
                score_rejects["px_none"] += 1
                continue

            age_sec = int(now0 - int(info.get("added_ts") or now0))
            if age_sec < int(min_watch_age_sec):
                score_rejects["warmup_age"] += 1
                continue

            pts = recent_points(k, now0, int(min_points_window_sec))
            if pts < int(min_points_before_entry):
                score_rejects["warmup_pts"] += 1
                continue

            px = float(px)
            entry_signal = 0.0
            dip = None
            speed = None
            micro_reb = None
            speed5 = None

            if strat_mode == "dip_rebound":
                dip_w = int(strat.get("dip_window_sec", 90) or 90)
                sh_w = int(strat.get("short_window_sec", 15) or 15)
                speed5_w = max(5, sh_w // 3)

                dip = self.cache.ret_over(k, now0, dip_w)
                speed = self.cache.speed_pct_per_sec(k, now0, sh_w)
                speed5 = self.cache.speed_pct_per_sec(k, now0, speed5_w)

                db_dip = db_speed = db_micro = db_speed5 = None
                if dip is None or speed is None or speed5 is None:
                    db_dip, db_speed, db_micro, db_speed5 = self.store.snapshot_dip_rebound_features(
                        k,
                        dip_window_sec=dip_w,
                        short_window_sec=sh_w,
                        speed5_window_sec=speed5_w,
                        px_override=px,
                    )
                    if dip is None:
                        dip = db_dip
                    if speed is None:
                        speed = db_speed
                    if speed5 is None:
                        speed5 = db_speed5

                min_dip_base = float(strat.get("min_dip_pct_lookback", -0.0045))
                min_dip = float(min_dip_base) * (1.0 - 0.50 * adj)

                max_dump_speed = strat.get("max_dump_speed_pct_per_sec", -0.00025)
                min_reb_speed = float(strat.get("min_rebound_speed_pct_per_sec", 0.000010) or 0.000010)
                min_micro_reb = float(strat.get("min_micro_rebound_pct", 0.003) or 0.003)
                min_speed5 = float(_as_float(strat.get("min_speed5_pct_per_sec", 0.0)) or 0.0)

                if dip is None:
                    score_rejects["dip_na"] += 1
                    continue
                if dip > float(min_dip):
                    score_rejects["dip"] += 1
                    continue

                if speed is None:
                    score_rejects["reb_speed_na"] += 1
                    continue
                if float(speed) < float(min_reb_speed):
                    score_rejects["reb_speed"] += 1
                    continue
                if max_dump_speed is not None and float(speed) < float(max_dump_speed):
                    score_rejects["dump_fast"] += 1
                    continue

                low = window_low(k, now0, sh_w)
                if low is not None and low > 0:
                    micro_reb = (px / low) - 1.0
                else:
                    micro_reb = db_micro

                if micro_reb is None:
                    score_rejects["micro_reb_na"] += 1
                    continue
                if float(micro_reb) < float(min_micro_reb):
                    score_rejects["micro_reb"] += 1
                    continue

                if speed5 is None:
                    score_rejects["speed5_na"] += 1
                    continue
                if float(speed5) < float(min_speed5):
                    score_rejects["speed5"] += 1
                    continue

                fresh_w_raw = strat.get("freshness_window_sec", 0)
                max_runup_raw = strat.get("max_price_runup_in_freshness_window_pct", None)
                try:
                    fresh_w = int(float(str(fresh_w_raw).strip().replace(",", "."))) if fresh_w_raw is not None else 0
                except Exception:
                    fresh_w = 0
                try:
                    max_runup = float(str(max_runup_raw).strip().replace(",", ".")) if max_runup_raw is not None else None
                except Exception:
                    max_runup = None

                if fresh_w > 0 and max_runup is not None:
                    runup = self.cache.ret_over(k, now0, fresh_w)
                    if runup is None:
                        snap = self.store._snap_price_at_or_before(k, now0 - int(fresh_w))
                        if snap and snap[1] and float(snap[1]) > 0:
                            runup = (float(px) / float(snap[1])) - 1.0
                    if runup is not None and float(runup) > float(max_runup):
                        score_rejects["freshness_runup"] += 1
                        continue

                entry_signal = dip_rebound_signal(
                    feat=feat,
                    dip=float(dip),
                    speed=float(speed),
                    micro_reb=float(micro_reb),
                    speed5=float(speed5),
                    min_dip_abs=abs(float(min_dip_base)),
                    min_reb_speed=float(min_reb_speed),
                    min_micro_reb=float(min_micro_reb),
                    min_speed5=max(float(min_speed5), 1e-9),
                )
            else:
                r60 = self.cache.ret_over(k, now0, 60)
                vol = self.cache.volatility_over(k, now0, 60)
                if r60 is None or r60 < self.cfg.entry_return_60s:
                    score_rejects["r60"] += 1
                    continue
                if vol is None or vol < self.cfg.min_volatility_60s:
                    score_rejects["volatility"] += 1
                    continue
                entry_signal = clamp(
                    0.55 * (float(r60) / max(float(self.cfg.entry_return_60s), 1e-9))
                    + 0.45 * (float(vol) / max(float(self.cfg.min_volatility_60s), 1e-9)),
                    0.0,
                    1.0,
                )

            if float(entry_signal) < float(entry_signal_min):
                score_rejects["signal"] += 1
                continue

            scored_candidates.append({
                "k": k,
                "info": info,
                "feat": feat,
                "raw_score": float(raw_score),
                "px": float(px),
                "dip": dip,
                "speed": speed,
                "micro_reb": micro_reb,
                "speed5": speed5,
                "entry_signal": float(entry_signal),
                "points": int(pts),
                "age_sec": int(age_sec),
            })

        scored_candidates.sort(key=lambda c: (float(c["entry_signal"]), float(c["raw_score"])), reverse=True)
        zerox_max_candidates = int(strat.get("zerox_max_candidates_per_tick", 2) or 2)
        zerox_max_candidates = max(1, zerox_max_candidates)
        scored_candidates = scored_candidates[:zerox_max_candidates]

        opened_this_tick = 0
        max_new_per_tick = int(strat.get("max_new_positions_per_tick", 1) or 1)
        max_new_per_tick = max(1, max_new_per_tick)

        async def _cap_entry_usd(raw_usd: float, *, chain: str) -> float:
            async with self._lock:
                eq = float(self.equity_total_est())
                exp_room = max(0.0, (eq * float(self.cfg.max_total_exposure_pct)) - float(self.total_exposure_usd()))
                usd = min(float(raw_usd), float(self.cash), float(exp_room))

                if inst_enabled and float(max_trade_notional_pct) > 0.0:
                    usd = min(float(usd), float(eq) * float(max_trade_notional_pct))
                if inst_enabled and float(max_trade_notional_usd) > 0.0:
                    usd = min(float(usd), float(max_trade_notional_usd))
                if inst_enabled and float(max_chain_exposure_pct) > 0.0:
                    chain_room = max(0.0, (float(eq) * float(max_chain_exposure_pct)) - float(chain_exposure_usd(chain)))
                    usd = min(float(usd), float(chain_room))

            return float(usd)

        for cand in scored_candidates:
            if (daily_cap is not None and self.day_entries >= daily_cap) or opened_this_tick >= max_new_per_tick:
                break

            k = str(cand["k"])
            info = cand["info"]
            feat = cand["feat"]
            px = float(cand["px"])
            entry_signal = float(cand["entry_signal"])

            if hasattr(self, "bad_tokens") and k in self.bad_tokens:
                continue
            if k in getattr(self, "banned_set", set()):
                continue

            async with self._lock:
                if len(self.positions) >= self.cfg.max_positions:
                    return
                if not self.can_open_new():
                    return

                cur = self.watchlist.get(k) or {}
                if not cur:
                    continue
                if int(cur.get("cooldown_until") or 0) > now_ts():
                    continue
                if k in self.positions:
                    continue

            slip_pre = self.dynamic_slippage_bps(feat, k)
            if self.mode == "paper":
                slip = int(self.paper_fill_slippage_bps(feat, k))
                slip_pre = int(max(int(slip_pre), int(slip)))
            else:
                slip = int(slip_pre)

            base_usd = float(self.position_size_usd())
            seed_usd = await _cap_entry_usd(base_usd, chain=info["chain"])
            if float(seed_usd) < float(min_order_usd):
                try:
                    self.store.log_audit(ts=now_ts(), event="notional_reject", key=k, payload={"usd_amount": float(seed_usd), "min_order_usd": float(min_order_usd), "chain": info.get("chain")})
                except Exception:
                    pass
                continue

            msg = "paper_skip_precheck"
            ok = True
            pre_meta: Dict[str, Any] = {}
            h_details: Dict[str, Any] = {}
            size_meta: Dict[str, Any] = {}
            usd_amount = float(seed_usd)
            paper_use_precheck = str(strat.get("paper_use_zerox_precheck", False)).lower() in ("1", "true", "yes", "y", "on")

            for _precheck_pass in range(2):
                if self.mode == "live" or paper_use_precheck:
                    ok, msg, pre_meta = await self.precheck_dispatch(
                        chain=info["chain"],
                        token=info["token"],
                        slippage_bps=int(slip_pre),
                        quote_usdc=float(usd_amount),
                    )
                else:
                    ok, msg, pre_meta = True, "paper_skip_precheck", {}

                if not ok:
                    self.log.info("SKIP ENTRY %s precheck=%s", k, msg)

                    if isinstance(pre_meta, dict) and pre_meta:
                        try:
                            self._cache_token_meta(k, pre_meta)
                        except Exception:
                            pass

                    async with self._lock:
                        if k in self.watchlist and int(precheck_fail_cooldown_sec) > 0:
                            self.watchlist[k]["cooldown_until"] = now_ts() + int(precheck_fail_cooldown_sec)
                            self.store.save_watchlist(self.watchlist)
                        self.store.set_state("entry_intent", None)
                        self.store.set_state("entry_last", {
                            "ts": now_ts(),
                            "key": k,
                            "status": "skip",
                            "reason": msg,
                            "signal": float(entry_signal),
                            "pre": pre_meta,
                            "details": pre_meta,
                        })
                        try:
                            self.store.log_audit(ts=now_ts(), event="precheck_reject", key=k, payload={"reason": msg, "signal": float(entry_signal), "pre": pre_meta, "quote_usdc": float(usd_amount)})
                        except Exception:
                            pass
                    break

                if isinstance(pre_meta, dict) and pre_meta:
                    self._cache_token_meta(k, pre_meta)

                ok_h, msg_h, h_details = await self.health_gate(
                    k=k,
                    chain=info["chain"],
                    token=info["token"],
                    feat=feat,
                    px=float(px),
                    slip_bps=int(slip_pre),
                    precheck_meta=pre_meta,
                    precheck_quote_usdc=float(usd_amount),
                )
                if not ok_h:
                    self.log.info("SKIP ENTRY %s health=%s", k, msg_h)
                    async with self._lock:
                        if k in self.watchlist and int(precheck_fail_cooldown_sec) > 0:
                            self.watchlist[k]["cooldown_until"] = now_ts() + int(precheck_fail_cooldown_sec)
                            self.store.save_watchlist(self.watchlist)
                        self.store.set_state("entry_intent", None)
                        self.store.set_state("entry_last", {
                            "ts": now_ts(),
                            "key": k,
                            "status": "skip",
                            "reason": f"health:{msg_h}",
                            "signal": float(entry_signal),
                            "details": h_details,
                        })
                        try:
                            self.store.log_audit(ts=now_ts(), event="health_reject", key=k, payload={"reason": msg_h, "signal": float(entry_signal), "details": h_details, "quote_usdc": float(usd_amount)})
                        except Exception:
                            pass
                    ok = False
                    break

                usd_new, size_meta = self.position_size_usd_dynamic(
                    k=k,
                    base_usd=float(base_usd),
                    slip_pre_bps=int(slip_pre),
                    pre_meta=pre_meta,
                    health=h_details,
                    signal_score=float(entry_signal),
                )
                usd_new = await _cap_entry_usd(float(usd_new), chain=info["chain"])

                if float(usd_new) < float(min_order_usd):
                    try:
                        self.store.log_audit(ts=now_ts(), event="notional_reject", key=k, payload={"usd_amount": float(usd_new), "min_order_usd": float(min_order_usd), "chain": info.get("chain")})
                    except Exception:
                        pass
                    ok = False
                    break

                if abs(float(usd_new) - float(usd_amount)) <= 0.01:
                    usd_amount = float(usd_new)
                    break

                usd_amount = float(usd_new)

            if not ok:
                continue

            now1 = now_ts()
            async with self._lock:
                self.store.set_state("entry_intent", {
                    "ts": now1,
                    "key": k,
                    "usd": float(usd_amount),
                    "slip": int(slip),
                    "pre": msg,
                    "signal": float(entry_signal),
                    "points": int(cand["points"]),
                    "age_sec": int(cand["age_sec"]),
                    "size_meta": size_meta,
                })

            try:
                qty, fill_px, usd_spent = await self.exec.buy(
                    chain=info["chain"],
                    token=info["token"],
                    pair=info.get("pair", ""),
                    px_usd=float(px),
                    usd_amount=float(usd_amount),
                    slippage_bps=int(slip),
                )
            except Exception as e:
                await self.circuit_fail("buy", e)
                continue

            async with self._lock:
                self.cash -= float(usd_spent)
                self.store.set_state("cash", float(self.cash))

                self.positions[k] = Position(
                    chain=info["chain"],
                    token=info["token"],
                    pair=info.get("pair", ""),
                    entry_px=float(fill_px),
                    qty=float(qty),
                    entry_ts=now1,
                    peak_px=float(fill_px),
                    avg_px=float(fill_px),
                    pyramids_done=0,
                    tp1_done=False,
                    trail_armed_ts=0,
                    trail_step_n=0,
                    trail_stop_px=0.0,
                    trail_breach_n=0,
                )

                if k in self.watchlist:
                    self.watchlist[k]["cooldown_until"] = now1 + self.cfg.cooldown_sec

                self.store.save_positions(self.positions)
                self.store.save_watchlist(self.watchlist)

                self.store.set_state("entry_intent", None)
                self.store.set_state("entry_last", {
                    "ts": now1,
                    "key": k,
                    "status": "entered",
                    "usd": float(usd_spent),
                    "qty": float(qty),
                    "px": float(fill_px),
                    "precheck": msg,
                    "signal": float(entry_signal),
                    "health_score": float(h_details.get("score_total") or 0.0),
                    "size_meta": size_meta,
                })

                self.day_entries += 1
                self.store.set_state("day_entries", self.day_entries)

            opened_this_tick += 1
            await self.notify.send(
                f"🟢 ENTRY {k} (day_entries={self.day_entries}/{cap_str})\n"
                f"spent={float(usd_spent):.2f}$ qty={float(qty):.6f}\n"
                f"fill_px={float(fill_px):.10f} slip={int(slip)}bps pre={msg}\n"
                f"signal={float(entry_signal):.3f} health={float(h_details.get('score_total') or 0.0):.3f} mult={float(size_meta.get('mult') or 1.0):.3f}"
            )
            try:
                self.store.log_audit(ts=now_ts(), event="entry_opened", key=k, payload={"usd_spent": float(usd_spent), "qty": float(qty), "fill_px": float(fill_px), "signal": float(entry_signal), "pre": msg, "health": h_details, "size_meta": size_meta})
            except Exception:
                pass

    async def exit_full(self, k: str, pos: Position, px: float, reason: str, slip: int):
        """Chiude completamente una posizione e aggiorna stato/counters in modo coerente."""
        qty_sell = float(getattr(pos, "qty", 0.0) or 0.0)
        if qty_sell <= 0.0:
            async with self._lock:
                self.positions.pop(k, None)
                self.store.save_positions(self.positions)
            return

        try:
            usd_got, fill_px = await self.exec.sell(
                position=pos,
                px_usd=float(px),
                qty=float(qty_sell),
                reason=str(reason),
                slippage_bps=int(slip),
            )
        except Exception as e:
            await self.circuit_fail("sell", e)
            return

        now = now_ts()
        usd_got = float(usd_got)
        fill_px = float(fill_px)

        avg_px = float(getattr(pos, "avg_px", 0.0) or 0.0)
        entry_value = float(qty_sell) * float(avg_px)
        pnl_usd = float(usd_got - entry_value)
        pnl_pct = (float(fill_px) / max(float(avg_px), 1e-18)) - 1.0

        strat = (self.cfg.strategy or {}) if isinstance(getattr(self.cfg, "strategy", None), dict) else {}
        base_cd = int(getattr(self.cfg, "cooldown_sec", 0) or 0)
        loss_cd = int(strat.get("cooldown_after_loss_sec", 0) or 0)
        cooldown_sec = max(0, int(base_cd))
        if pnl_pct < 0.0:
            cooldown_sec = max(cooldown_sec, max(0, int(loss_cd)))

        if not hasattr(self, "first_trade_loss_ban_enabled"):
            self.first_trade_loss_ban_enabled = _as_bool(strat.get("ban_token_after_first_trade_loss", False))
        if not hasattr(self, "first_trade_loss_ban_min_pnl_pct"):
            self.first_trade_loss_ban_min_pnl_pct = float(
                safe_float(strat.get("ban_token_after_first_trade_loss_min_pnl_pct", -0.05)) or -0.05
            )
        if not hasattr(self, "bad_tokens") or not isinstance(getattr(self, "bad_tokens"), set):
            self.bad_tokens = set()

        banned_now = False
        ban_reason = None
        ban_until = 0

        async with self._lock:
            self.cash += float(usd_got)
            self.positions.pop(k, None)

            self.day_exits += 1
            self.store.set_state("day_exits", int(self.day_exits))
            self.store.set_state("cash", float(self.cash))

            if not hasattr(self, "token_pnl_usd") or not isinstance(getattr(self, "token_pnl_usd"), dict):
                self.token_pnl_usd = self.store.get_state("token_pnl_usd", {}) or {}
                if not isinstance(self.token_pnl_usd, dict):
                    self.token_pnl_usd = {}

            if not hasattr(self, "banned_tokens") or not isinstance(getattr(self, "banned_tokens"), dict):
                self.banned_tokens = self.store.get_state("banned_tokens", {}) or {}
                if not isinstance(self.banned_tokens, dict):
                    self.banned_tokens = {}

            if not hasattr(self, "banned_set") or not isinstance(getattr(self, "banned_set"), set):
                self.banned_set = set(self.banned_tokens.keys())

            if not hasattr(self, "first_trade_outcome") or not isinstance(getattr(self, "first_trade_outcome"), dict):
                self.first_trade_outcome = self.store.get_state("first_trade_outcome", {}) or {}
                if not isinstance(self.first_trade_outcome, dict):
                    self.first_trade_outcome = {}

            if not hasattr(self, "loss_streak") or not isinstance(getattr(self, "loss_streak"), dict):
                self.loss_streak = self.store.get_state("loss_streak", {}) or {}
                if not isinstance(self.loss_streak, dict):
                    self.loss_streak = {}

            prev_total = float(self.token_pnl_usd.get(k, 0.0) or 0.0)
            new_total = prev_total + float(pnl_usd)
            self.token_pnl_usd[k] = float(new_total)
            self.store.set_state("token_pnl_usd", self.token_pnl_usd)

            # --- first trade outcome (config aware for permanent skip) ---
            if k not in self.first_trade_outcome:
                st = "win" if float(pnl_pct) >= 0.0 else "loss"
                self.first_trade_outcome[k] = {
                    "ts": int(now),
                    "status": str(st),
                    "pnl_usd": float(pnl_usd),
                    "pnl_pct": float(pnl_pct),
                    "reason": str(reason),
                }
                self.store.set_state("first_trade_outcome", self.first_trade_outcome)

                if (
                    self.first_trade_loss_ban_enabled
                    and st == "loss"
                    and float(pnl_pct) <= float(self.first_trade_loss_ban_min_pnl_pct)
                ):
                    self.bad_tokens.add(k)

            # --- loss streak (store as dict, not plain int) ---
            rec = self.loss_streak.get(k) or {"n": 0}
            if not isinstance(rec, dict):
                rec = {"n": int(rec or 0)}

            n = int(rec.get("n", 0) or 0)
            loss_thr = float(strat.get("loss_streak_loss_pct_threshold", -0.01) or -0.01)
            reset_on_win = _as_bool(strat.get("loss_streak_reset_on_win", True))

            if float(pnl_pct) <= float(loss_thr):
                n += 1
            elif float(pnl_pct) >= 0.0 and reset_on_win:
                n = 0

            rec["n"] = int(n)
            rec["ts"] = int(now)
            rec["last_pnl_pct"] = float(pnl_pct)
            self.loss_streak[k] = rec
            self.store.set_state("loss_streak", self.loss_streak)

            # --- optional blacklist: first trade loss ---
            if _as_bool(strat.get("ban_token_after_first_trade_loss", False)):
                out = self.first_trade_outcome.get(k) or {}
                minp = float(strat.get("ban_token_after_first_trade_loss_min_pnl_pct", -0.05) or -0.05)
                if isinstance(out, dict) and out.get("status") == "loss" and float(out.get("pnl_pct", 0.0) or 0.0) <= float(minp):
                    banned_now = True
                    ban_reason = "first_trade_loss"
                    ban_until = 0

            # --- optional blacklist: loss streak ---
            if _as_bool(strat.get("ban_token_after_loss_streak", False)) and not banned_now:
                streak_n = int(strat.get("loss_streak_n", 2) or 2)
                if streak_n > 0 and int(n) >= int(streak_n):
                    ban_sec = int(strat.get("loss_streak_blacklist_sec", 21600) or 21600)
                    perma = _as_bool(strat.get("loss_streak_perma_ban", False))
                    banned_now = True
                    ban_reason = "loss_streak"
                    ban_until = 0 if perma else int(now + ban_sec)

            # --- optional blacklist: cumulative token pnl negative ---
            if _as_bool(strat.get("ban_token_if_cum_pnl_negative", False)) and not banned_now and float(new_total) < 0.0:
                banned_now = True
                ban_reason = "token_pnl_negative"
                ban_until = 0 if _as_bool(strat.get("token_pnl_negative_perma", True)) else int(now + 86400)

            if banned_now:
                self.banned_tokens[k] = {
                    "ts": int(now),
                    "until": int(ban_until),
                    "reason": str(ban_reason),
                    "pnl_pct": float(pnl_pct),
                    "pnl_usd": float(pnl_usd),
                }
                self.banned_set.add(k)
                self.store.set_state("banned_tokens", self.banned_tokens)

            if k in self.watchlist:
                if banned_now:
                    self.watchlist.pop(k, None)
                else:
                    self.watchlist[k]["cooldown_until"] = int(now + cooldown_sec)

            self.store.save_positions(self.positions)
            self.store.save_watchlist(self.watchlist)

        ban_txt = ""
        if banned_now:
            if int(ban_until) == 0:
                ban_txt = f" | BLACKLIST={ban_reason}:∞"
            else:
                ban_txt = f" | BLACKLIST={ban_reason} until={time.strftime('%H:%M:%S', time.localtime(int(ban_until)))}"

        await self.notify.send(
            f"🔴 EXIT {k} reason={reason}{ban_txt}\n"
            f"got={usd_got:.2f}$ fill_px={fill_px:.10f} slip={int(slip)}bps\n"
            f"PNL={pnl_usd:+.2f}$ ({pnl_pct*100:+.2f}%) cash={self.cash:.2f}$"
        )
        try:
            self.store.log_audit(ts=now_ts(), event="exit_closed", key=k, payload={"reason": str(reason), "usd_got": float(usd_got), "fill_px": float(fill_px), "pnl_usd": float(pnl_usd), "pnl_pct": float(pnl_pct), "ban_reason": ban_reason, "ban_until": int(ban_until)})
        except Exception:
            pass

    async def update_metrics(self):
        """Aggiorna metriche Prometheus (se abilitate) e peak equity."""
        eq = float(self.equity_total_est())
        self.peak_equity = max(float(self.peak_equity), eq)
        M_EQUITY.set(eq)
        M_CASH.set(float(self.cash))
        M_EXPOSURE.set(float(self.total_exposure_usd()))
        M_POS.set(int(len(self.positions)))
        M_WATCH.set(int(len(self.watchlist)))

    async def run(self):
        await self.notify.send(
            f"✅ Bot avviato mode={self.mode} watch={len(self.watchlist)} pos={len(self.positions)}"
        )
        tasks = [
            asyncio.create_task(self.task_discovery(), name="discovery"),
            asyncio.create_task(self.task_refresh(), name="refresh"),
            asyncio.create_task(self.task_entry_manage(), name="entry_manage"),
        ]
        # --- START SOCIAL/NARRATIVE SOURCES (Telegram/NewsAPI) ---
        if hasattr(self, "task_narrative_sources") and callable(getattr(self, "task_narrative_sources")):
            # parte solo se abilitato in config (task_narrative_sources fa già il check social.enabled)
            tasks.append(asyncio.create_task(self.task_narrative_sources(), name="narrative"))
        if self.mode == "live":
            tasks.append(asyncio.create_task(self.task_rpc_health(), name="rpc_health"))
            tasks.append(asyncio.create_task(self.task_sync_live_cash(), name="live_cash_sync"))

        try:
            await asyncio.gather(*tasks)
        finally:
            for t in tasks:
                t.cancel()
            await self.close()
# -------------------- main --------------------


# --- SAFETY PATCH ---
# Se per errore (copia/incolla) il metodo run finisce fuori dalla classe, questo blocco lo riattacca
# alla classe MicrocapBot così il bot parte comunque.
if 'MicrocapBot' in globals() and not hasattr(MicrocapBot, 'run'):
    async def _run(self):
        # Preferisci un loop principale se esiste
        if hasattr(self, 'core_loop') and callable(getattr(self, 'core_loop')):
            return await self.core_loop()

        # Fallback: avvia i task legacy se esistono
        tasks = []
        if hasattr(self, 'task_discovery') and callable(getattr(self, 'task_discovery')):
            tasks.append(asyncio.create_task(self.task_discovery(), name='discovery'))
        if hasattr(self, 'task_refresh') and callable(getattr(self, 'task_refresh')):
            tasks.append(asyncio.create_task(self.task_refresh(), name='refresh'))
        if hasattr(self, 'task_entry_manage') and callable(getattr(self, 'task_entry_manage')):
            tasks.append(asyncio.create_task(self.task_entry_manage(), name='entry_manage'))
        # --- START SOCIAL/NARRATIVE SOURCES (Telegram/NewsAPI) ---
        if hasattr(self, 'task_narrative_sources') and callable(getattr(self, 'task_narrative_sources')):
            tasks.append(asyncio.create_task(self.task_narrative_sources(), name='narrative'))
        if not tasks:
            raise RuntimeError('Nessun loop trovato: manca run/core_loop/task_*')

        try:
            await asyncio.gather(*tasks)
        finally:
            for t in tasks:
                t.cancel()

    MicrocapBot.run = _run

# --- SAFETY PATCH: re-attach exit_full if it was accidentally nested (indent bug) ---
if 'MicrocapBot' in globals() and not hasattr(MicrocapBot, 'exit_full'):
    async def _exit_full(self, k: str, pos: 'Position', px: float, reason: str, slip: int):
        qty_sell = float(getattr(pos, "qty", 0.0) or 0.0)
        if qty_sell <= 0.0:
            async with self._lock:
                self.positions.pop(k, None)
                self.store.save_positions(self.positions)
            return

        try:
            usd_got, fill_px = await self.exec.sell(
                position=pos,
                px_usd=float(px),
                qty=float(qty_sell),
                reason=str(reason),
                slippage_bps=int(slip),
            )
        except Exception as e:
            await self.circuit_fail("sell", e)
            return

        now = now_ts()
        usd_got = float(usd_got)
        fill_px = float(fill_px)

        entry_value = float(qty_sell) * float(getattr(pos, "avg_px", 0.0) or 0.0)
        pnl_usd = float(usd_got - entry_value)
        pnl_pct = (float(fill_px) / max(float(getattr(pos, "avg_px", 1e-18) or 1e-18), 1e-18)) - 1.0

        strat = (self.cfg.strategy or {}) if isinstance(getattr(self.cfg, "strategy", None), dict) else {}
        base_cd = int(getattr(self.cfg, "cooldown_sec", 45) or 45)
        loss_cd = int(strat.get("cooldown_after_loss_sec", base_cd) or base_cd)
        cooldown_sec = int(loss_cd if pnl_pct < 0 else base_cd)

        banned_now = False
        ban_until = 0
        ban_reason = ""

        async with self._lock:
            # cash + remove pos
            self.cash += float(usd_got)
            self.store.set_state("cash", float(self.cash))
            self.positions.pop(k, None)

            # day exits
            self.day_exits = int(getattr(self, "day_exits", 0) or 0) + 1
            self.store.set_state("day_exits", int(self.day_exits))

            # ensure dicts exist
            if not hasattr(self, "token_pnl_usd") or not isinstance(getattr(self, "token_pnl_usd"), dict):
                self.token_pnl_usd = self.store.get_state("token_pnl_usd", {}) or {}
                if not isinstance(self.token_pnl_usd, dict):
                    self.token_pnl_usd = {}

            if not hasattr(self, "first_trade_outcome") or not isinstance(getattr(self, "first_trade_outcome"), dict):
                self.first_trade_outcome = self.store.get_state("first_trade_outcome", {}) or {}
                if not isinstance(self.first_trade_outcome, dict):
                    self.first_trade_outcome = {}

            if not hasattr(self, "loss_streak") or not isinstance(getattr(self, "loss_streak"), dict):
                self.loss_streak = self.store.get_state("loss_streak", {}) or {}
                if not isinstance(self.loss_streak, dict):
                    self.loss_streak = {}

            if not hasattr(self, "banned_tokens") or not isinstance(getattr(self, "banned_tokens"), dict):
                self.banned_tokens = self.store.get_state("banned_tokens", {}) or {}
                if not isinstance(self.banned_tokens, dict):
                    self.banned_tokens = {}

            if not hasattr(self, "banned_set") or not isinstance(getattr(self, "banned_set"), set):
                self.banned_set = set(self.banned_tokens.keys())

            # token pnl cumulative
            prev_total = float(self.token_pnl_usd.get(k, 0.0) or 0.0)
            new_total = float(prev_total + pnl_usd)
            self.token_pnl_usd[k] = float(new_total)
            self.store.set_state("token_pnl_usd", self.token_pnl_usd)

            # first trade outcome + optional bad_tokens marker
            if k not in self.first_trade_outcome:
                st = "win" if pnl_pct >= 0 else "loss"
                self.first_trade_outcome[k] = {
                    "status": st,
                    "ts": int(now),
                    "pnl_usd": float(pnl_usd),
                    "pnl_pct": float(pnl_pct),
                }
                self.store.set_state("first_trade_outcome", self.first_trade_outcome)
                if st == "loss":
                    if not hasattr(self, "bad_tokens") or not isinstance(getattr(self, "bad_tokens"), set):
                        self.bad_tokens = set()
                    self.bad_tokens.add(k)

            # loss streak update
            rec = self.loss_streak.get(k) or {"n": 0}
            if not isinstance(rec, dict):
                rec = {"n": 0}
            n = int(rec.get("n", 0) or 0)

            loss_thr = float(strat.get("loss_streak_loss_pct_threshold", -0.10) or -0.10)
            reset_on_win = _as_bool(strat.get("loss_streak_reset_on_win", True))

            if float(pnl_pct) <= float(loss_thr):
                n += 1
            elif float(pnl_pct) >= 0.0 and reset_on_win:
                n = 0

            rec["n"] = int(n)
            rec["ts"] = int(now)
            self.loss_streak[k] = rec
            self.store.set_state("loss_streak", self.loss_streak)

            # optional bans
            if _as_bool(strat.get("ban_token_after_first_trade_loss", False)):
                out = self.first_trade_outcome.get(k) or {}
                minp = float(strat.get("ban_token_after_first_trade_loss_min_pnl_pct", -0.10) or -0.10)
                if isinstance(out, dict) and out.get("status") == "loss" and float(out.get("pnl_pct", 0.0) or 0.0) <= float(minp):
                    banned_now = True
                    ban_reason = "first_trade_loss"
                    ban_until = 0

            if _as_bool(strat.get("ban_token_after_loss_streak", False)) and not banned_now:
                streak_n = int(strat.get("loss_streak_n", 2) or 2)
                if streak_n > 0 and int(n) >= int(streak_n):
                    ban_sec = int(strat.get("loss_streak_blacklist_sec", 21600) or 21600)
                    perma = _as_bool(strat.get("loss_streak_perma_ban", False))
                    banned_now = True
                    ban_reason = "loss_streak"
                    ban_until = 0 if perma else int(now + ban_sec)

            if banned_now:
                self.banned_tokens[k] = {
                    "ts": int(now),
                    "until": int(ban_until),
                    "reason": str(ban_reason),
                    "pnl_pct": float(pnl_pct),
                    "pnl_usd": float(pnl_usd),
                }
                self.banned_set.add(k)
                self.store.set_state("banned_tokens", self.banned_tokens)

            # cooldown/watchlist
            if k in self.watchlist:
                if banned_now:
                    self.watchlist.pop(k, None)
                else:
                    self.watchlist[k]["cooldown_until"] = int(now + cooldown_sec)

            self.store.save_positions(self.positions)
            self.store.save_watchlist(self.watchlist)

        ban_txt = ""
        if banned_now:
            if int(ban_until) == 0:
                ban_txt = f" | BLACKLIST={ban_reason}:∞"
            else:
                ban_txt = f" | BLACKLIST={ban_reason} until={time.strftime('%H:%M:%S', time.localtime(int(ban_until)))}"

        await self.notify.send(
            f"🔴 EXIT {k} reason={reason}{ban_txt}\n"
            f"got={usd_got:.2f}$ fill_px={fill_px:.10f} slip={int(slip)}bps\n"
            f"PNL={pnl_usd:+.2f}$ ({pnl_pct*100:+.2f}%) cash={self.cash:.2f}$"
        )

    MicrocapBot.exit_full = _exit_full

# --- attach helper methods to Store (quick fix) ---
Store.snapshot_liq_range_pct = snapshot_liq_range_pct
Store.snapshot_recent_drawdown_from_peak_pct = snapshot_recent_drawdown_from_peak_pct
Store.log_hype_event = log_hype_event
Store.hype_count = hype_count
Store.log_social_mentions = log_social_mentions
Store.social_mentions_sum = social_mentions_sum
Store.attention_trend = attention_trend
Store.get_watchlist_rows = get_watchlist_rows

async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    load_dotenv()

    cfg_path = "config.yaml"  # SOLO questo file
    cfg = load_config(cfg_path)

    # Prometheus metrics
    disable_prometheus = str(os.getenv("BTTFUSION_DISABLE_PROMETHEUS", "")).strip().lower() in {"1", "true", "yes", "on"}
    if cfg.metrics_enabled and not disable_prometheus:
        try:
            start_http_server(int(cfg.metrics_port))
            logging.getLogger("microcap_bot_v4").info(
                "Prometheus metrics su :%d/metrics | config=%s | build=PATCHED_PAPER_NO0X_2026-01-25",
                int(cfg.metrics_port),
                cfg_path,
            )
        except Exception as e:
            logging.getLogger("microcap_bot_v4").warning(
                "Prometheus metrics NON avviate (porta occupata?) port=%s err=%s | config=%s | build=PATCHED_PAPER_NO0X_2026-01-25",
                str(cfg.metrics_port),
                str(e),
                cfg_path,
            )
    elif cfg.metrics_enabled and disable_prometheus:
                  logging.getLogger("microcap_bot_v4").info(
                            "Prometheus metrics disattivate da BTTFUSION_DISABLE_PROMETHEUS=1 | config=%s",
                            cfg_path,
                  )

    bot = MicrocapBot(cfg)
    await bot.run()


if __name__ == "__main__":
    asyncio.run(main())
