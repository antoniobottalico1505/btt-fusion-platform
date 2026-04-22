import asyncio
import csv
import math
import os
import sqlite3
import time
from typing import Any, Dict, List, Optional, Tuple

import yaml


# ------------------------------------------------------------
# Live CLI dashboard for microcap_bot_v4*.py + config.yaml
# build=DASH_WARFARE_PLUS_2026-03-10
#
# Notes:
# - mirrors the patched bot as closely as possible using bot.db
# - reloads config.yaml every refresh
# - shows watchlist, candidates, positions, skips, precheck cache
# - reflects warmup and entry_signal ranking
# - reflects config-aware first trade loss ban
# - partial limitation: cannot reconstruct buys/sells m5 health gates
#   if the bot does not persist buys_m5 / sells_m5 into snapshots/state
# ------------------------------------------------------------

EXPORT_DIR = os.getenv("BOT_EXPORT_DIR", "exports")
DB_PATH = os.getenv("BOT_DB_PATH", "bot.db")
CONFIG_PATH = os.getenv("BOT_CONFIG_PATH", "config.yaml")

# 0 = show all rows; otherwise limit rows
DASH_ROWS = int(os.getenv("DASH_ROWS", "0") or "0")
TRADES_LIMIT = int(os.getenv("TRADES_LIMIT", "0") or "0")  # 0 = all

# how many trades to print
TRADES_SHOW_N = int(os.getenv("TRADES_SHOW_N", "12") or "12")

# 0 = show all candidate rows; otherwise limit rows
BOT_CANDIDATES_N = int(os.getenv("BOT_CANDIDATES_N", "0") or "0")

# Refresh interval seconds
REFRESH_SEC = float(os.getenv("DASH_REFRESH_SEC", "3") or "3")

# Clear screen each refresh (1/0)
CLEAR = int(os.getenv("DASH_CLEAR", "1") or "1") == 1

WATCHLIST_TOP_N = int(os.getenv("WATCHLIST_TOP_N", "12") or "12")
HIDE_BANNED = str(os.getenv("HIDE_BANNED", "1")).lower() in ("1", "true", "yes", "y", "on")
HIDE_AUTO_BLACKLIST = str(os.getenv("HIDE_AUTO_BLACKLIST", "0")).lower() in ("1", "true", "yes", "y", "on")


def now_ts() -> int:
    return int(time.time())


def as_bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    s = str(v).strip().lower()
    return s in ("1", "true", "yes", "y", "on")


def fmt_pct(x: Optional[float]) -> str:
    if x is None:
        return "-"
    return f"{x * 100:+.2f}%"


def fmt_speed(x: Optional[float]) -> str:
    if x is None:
        return "-"
    return f"{x * 100:+.4f}%/s"


def fmt_num(x: Optional[float], nd: int = 2) -> str:
    if x is None:
        return "-"
    return f"{x:.{nd}f}"


def fmt_int(x: Optional[int]) -> str:
    if x is None:
        return "-"
    return str(int(x))


def short_addr(s: str) -> str:
    s = s or ""
    if len(s) <= 12:
        return s
    return f"{s[:6]}...{s[-4:]}"

def short_url(u: str) -> str:
    s = (u or "").strip()
    if not s or s == "-":
        return "-"
    try:
        parts = s.split("/")
        if len(parts) >= 5 and "dexscreener.com" in parts[2]:
            chain = parts[-2]
            ident = parts[-1]
            return f"{chain}/{short_addr(ident)}"
    except Exception:
        pass
    return short_addr(s)


def fmt_detail_scalar(v: Any) -> str:
    if v is None:
        return "-"
    if isinstance(v, float):
        return f"{v:.6g}"
    return str(v)


def fmt_detail_lines(v: Any, indent: str = "") -> List[str]:
    lines: List[str] = []

    if isinstance(v, dict):
        if not v:
            return [f"{indent}{{}}"]

        scalar_keys = [k for k, val in v.items() if not isinstance(val, (dict, list, tuple))]
        nested_keys = [k for k, val in v.items() if isinstance(val, (dict, list, tuple))]

        def scalar_sort_key(k: Any) -> Tuple[int, float, str]:
            val = v.get(k)
            if isinstance(val, (int, float)) and not isinstance(val, bool):
                return (0, -float(val), str(k))
            return (1, 0.0, str(k))

        for k in sorted(scalar_keys, key=scalar_sort_key):
            lines.append(f"{indent}{k}={fmt_detail_scalar(v.get(k))}")

        for k in sorted(nested_keys, key=lambda x: str(x)):
            lines.append(f"{indent}{k}:")
            lines.extend(fmt_detail_lines(v.get(k), indent + "  "))

        return lines

    if isinstance(v, (list, tuple)):
        if not v:
            return [f"{indent}[]"]

        for i, item in enumerate(v):
            if isinstance(item, (dict, list, tuple)):
                lines.append(f"{indent}[{i}]:")
                lines.extend(fmt_detail_lines(item, indent + "  "))
            else:
                lines.append(f"{indent}[{i}]={fmt_detail_scalar(item)}")

        return lines

    return [f"{indent}{fmt_detail_scalar(v)}"]


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def load_cfg() -> Dict[str, Any]:
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}


def build_url(chain: str, token_or_pair: str) -> str:
    if chain and token_or_pair:
        return f"https://dexscreener.com/{chain}/{token_or_pair}"
    return "-"


def passes_filters_reason(cfg: Dict[str, Any], feat: Dict[str, Any]) -> Tuple[bool, str]:
    try:
        px = float(feat.get("price_usd") or 0.0)
        if px <= 0:
            return False, "px"

        liq = float(feat.get("liq_usd") or 0.0)
        if liq < float(cfg.get("min_liquidity_usd", 0) or 0):
            return False, "liq"

        vol = float(feat.get("vol_m5") or 0.0)
        if vol < float(cfg.get("min_volume_5m_usd", 0) or 0):
            return False, "vol5m"

        tx = int(feat.get("txns_m5") or 0)
        if tx < int(cfg.get("min_txns_5m", 0) or 0):
            return False, "tx5m"

        max_fdv = cfg.get("max_fdv_usd", None)
        fdv = feat.get("fdv")
        if max_fdv is not None and fdv is not None:
            if float(fdv) > float(max_fdv):
                return False, "fdv"

        return True, "ok"
    except Exception:
        return False, "err"


def table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    cur = conn.cursor()
    try:
        cur.execute(f"PRAGMA table_info({table})")
        return [r[1] for r in cur.fetchall()]
    except Exception:
        return []


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    cur = conn.cursor()
    try:
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
        return cur.fetchone() is not None
    except Exception:
        return False


def recent_audit_events(conn: sqlite3.Connection, limit: int = 8) -> List[Dict[str, Any]]:
    if not table_exists(conn, "audit_events"):
        return []
    cur = conn.cursor()
    try:
        cur.execute("SELECT ts, event, key, payload FROM audit_events ORDER BY id DESC LIMIT ?", (int(limit),))
        rows = []
        for ts, event, key, payload in cur.fetchall():
            try:
                payload_obj = yaml.safe_load(payload) if payload else {}
            except Exception:
                payload_obj = {"raw": str(payload)}
            rows.append({
                "ts": int(ts or 0),
                "event": str(event or ""),
                "key": str(key or ""),
                "payload": payload_obj if isinstance(payload_obj, dict) else {"value": payload_obj},
            })
        return rows
    except Exception:
        return []


def last_snapshot_by_key(conn: sqlite3.Connection) -> Dict[str, Dict[str, Any]]:
    cur = conn.cursor()
    cur.execute("""
        SELECT s.key, s.chain, s.token, s.price_usd, s.liq_usd, s.vol_m5, s.txns_m5, s.fdv, s.score, s.ts
        FROM snapshots s
        JOIN (SELECT key, MAX(ts) AS mx FROM snapshots GROUP BY key) t
          ON t.key = s.key AND t.mx = s.ts
    """)
    out: Dict[str, Dict[str, Any]] = {}
    for (k, chain, token, price, liq, vol, tx, fdv, score, ts) in cur.fetchall():
        out[str(k)] = {
            "key": str(k),
            "chain": chain,
            "token": token,
            "price_usd": float(price) if price is not None else None,
            "liq_usd": float(liq) if liq is not None else None,
            "vol_m5": float(vol) if vol is not None else None,
            "txns_m5": int(tx) if tx is not None else 0,
            "fdv": float(fdv) if fdv is not None else None,
            "score": float(score) if score is not None else None,
            "ts": int(ts),
        }
    return out


def price_at_or_before(conn: sqlite3.Connection, key: str, target_ts: int) -> Optional[Tuple[int, float]]:
    cur = conn.cursor()
    cur.execute("""
        SELECT ts, price_usd
        FROM snapshots
        WHERE key=? AND ts<=? AND price_usd>0
        ORDER BY ts DESC
        LIMIT 1
    """, (key, int(target_ts)))
    row = cur.fetchone()
    if not row:
        return None
    return int(row[0]), float(row[1])


def low_in_window(conn: sqlite3.Connection, key: str, start_ts: int) -> Optional[float]:
    cur = conn.cursor()
    cur.execute("""
        SELECT MIN(price_usd)
        FROM snapshots
        WHERE key=? AND ts>=? AND price_usd>0
    """, (key, int(start_ts)))
    row = cur.fetchone()
    if not row:
        return None
    v = row[0]
    return float(v) if v is not None else None


def dip_speed_micro_low_speed5(
    conn: sqlite3.Connection,
    key: str,
    dip_window: int,
    short_window: int,
) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    cur = conn.cursor()
    cur.execute("SELECT ts, price_usd FROM snapshots WHERE key=? ORDER BY ts DESC LIMIT 1", (key,))
    row = cur.fetchone()
    if not row or row[1] is None:
        return None, None, None, None

    t_now, p_now = int(row[0]), float(row[1])

    old = price_at_or_before(conn, key, t_now - int(dip_window))
    sh = price_at_or_before(conn, key, t_now - int(short_window))

    dip = None
    speed = None
    micro_low = None
    speed5 = None

    if old and old[1] > 0:
        dip = (p_now / old[1]) - 1.0

    if sh and sh[1] > 0:
        micro_ret = (p_now / sh[1]) - 1.0
        dt = float(t_now - sh[0])
        if dt > 0:
            speed = micro_ret / dt

    low = low_in_window(conn, key, t_now - int(short_window))
    if low and low > 0:
        micro_low = (p_now / low) - 1.0

    w5 = max(5, int(short_window // 3) if short_window >= 6 else 5)
    sh5 = price_at_or_before(conn, key, t_now - int(w5))
    if sh5 and sh5[1] > 0:
        r5 = (p_now / sh5[1]) - 1.0
        dt5 = float(t_now - sh5[0])
        if dt5 > 0:
            speed5 = r5 / dt5

    return dip, speed, micro_low, speed5


def recent_points_in_window(conn: sqlite3.Connection, key: str, window_sec: int) -> int:
    cur = conn.cursor()
    cur.execute("SELECT MAX(ts) FROM snapshots WHERE key=?", (key,))
    row = cur.fetchone()
    if not row or row[0] is None:
        return 0
    t_ref = int(row[0])
    since = int(t_ref - int(window_sec))
    cur.execute("SELECT COUNT(*) FROM snapshots WHERE key=? AND ts>=?", (key, since))
    row = cur.fetchone()
    return int(row[0] or 0) if row else 0


def liq_range_pct_window(conn: sqlite3.Connection, key: str, window_sec: int) -> Optional[float]:
    cur = conn.cursor()
    cur.execute("SELECT MAX(ts) FROM snapshots WHERE key=?", (key,))
    row = cur.fetchone()
    if not row or row[0] is None:
        return None

    t_ref = int(row[0])
    since = int(t_ref - int(window_sec))

    cur.execute("""
        SELECT MIN(liq_usd), MAX(liq_usd)
        FROM snapshots
        WHERE key=? AND ts>=? AND liq_usd IS NOT NULL
    """, (key, since))
    row = cur.fetchone()
    if not row or row[0] is None or row[1] is None:
        return None

    mn = float(row[0])
    mx = float(row[1])
    if mx <= 0:
        return None
    return max(0.0, (mx - mn) / mx)


def recent_drawdown_from_peak_pct(conn: sqlite3.Connection, key: str, window_sec: int) -> Optional[float]:
    cur = conn.cursor()
    cur.execute("SELECT MAX(ts) FROM snapshots WHERE key=?", (key,))
    row = cur.fetchone()
    if not row or row[0] is None:
        return None

    t_ref = int(row[0])
    since = int(t_ref - int(window_sec))

    cur.execute("""
        SELECT MAX(price_usd)
        FROM snapshots
        WHERE key=? AND ts>=? AND price_usd>0
    """, (key, since))
    row = cur.fetchone()
    if not row or row[0] is None:
        return None

    peak = float(row[0])
    if peak <= 0:
        return None

    cur.execute("""
        SELECT price_usd
        FROM snapshots
        WHERE key=? AND ts<=? AND price_usd>0
        ORDER BY ts DESC
        LIMIT 1
    """, (key, t_ref))
    row = cur.fetchone()
    if not row or row[0] is None:
        return None

    px = float(row[0])
    return max(0.0, (peak - px) / peak)


def dip_rebound_signal(
    *,
    dip: float,
    speed: float,
    micro_reb: float,
    speed5: float,
    liq_usd: float,
    min_dip_abs: float,
    min_reb_speed: float,
    min_micro_reb: float,
    min_speed5: float,
    cfg_min_liq_usd: float,
) -> float:
    dip_abs = abs(min(float(dip), 0.0))
    target_dip = max(float(min_dip_abs) * 1.35, 0.01)

    dip_score = 1.0 - abs(dip_abs - target_dip) / max(target_dip, 1e-9)
    dip_score = clamp(dip_score, 0.0, 1.0)

    speed_score = clamp(float(speed) / max(float(min_reb_speed) * 2.5, 1e-9), 0.0, 1.0)
    micro_score = clamp(float(micro_reb) / max(float(min_micro_reb) * 2.0, 1e-9), 0.0, 1.0)
    speed5_score = clamp(float(speed5) / max(float(min_speed5) * 2.0, 1e-9), 0.0, 1.0)

    liq_score = clamp(
        math.log10(max(float(liq_usd), 1.0) / max(float(cfg_min_liq_usd), 1.0) + 1.0),
        0.0,
        1.0,
    )

    return float(
        0.32 * dip_score +
        0.26 * speed_score +
        0.22 * micro_score +
        0.14 * speed5_score +
        0.06 * liq_score
    )


def parse_state(state: Dict[str, Any], k: str, default: Any) -> Any:
    raw = state.get(k)
    if raw is None:
        return default
    try:
        return yaml.safe_load(raw)
    except Exception:
        try:
            import json
            return json.loads(raw)
        except Exception:
            return default


def coerce_int(v: Any, default: int = 0) -> int:
    if v is None:
        return default

    if isinstance(v, (int, float, bool)):
        try:
            return int(v)
        except Exception:
            return default

    if isinstance(v, dict):
        for kk in ("n", "count", "streak", "value", "until", "ts", "expiry", "expires_at"):
            if kk in v and v[kk] is not None:
                try:
                    return int(v[kk])
                except Exception:
                    pass
        return default

    try:
        return int(v)
    except Exception:
        return default


def day_progress_expected_adj(
    target_per_day_raw: int,
    day_entries: int,
    unlimited_entries: bool,
) -> Tuple[float, Optional[float], float]:
    lt = time.localtime()
    sec_of_day = lt.tm_hour * 3600 + lt.tm_min * 60 + lt.tm_sec
    progress = sec_of_day / 86400.0

    if unlimited_entries or int(target_per_day_raw or 0) <= 0:
        return progress, None, 0.0

    target = int(target_per_day_raw)
    expected = float(target) * float(progress)
    delta = expected - float(day_entries)
    denom = max(1.0, float(target) / 2.0)
    adj = clamp(delta / denom, -0.35, 0.35)
    return progress, expected, adj


def clear_screen() -> None:
    if not CLEAR:
        return
    os.system("cls" if os.name == "nt" else "clear")


def _agg_hype_counts(conn: sqlite3.Connection, window_sec: int) -> Dict[str, int]:
    if not table_exists(conn, "hype_events"):
        return {}
    since = now_ts() - int(window_sec)
    cur = conn.cursor()
    try:
        cur.execute("SELECT key, COUNT(*) FROM hype_events WHERE ts>=? GROUP BY key", (int(since),))
        return {str(k): int(c or 0) for (k, c) in cur.fetchall()}
    except Exception:
        return {}


def _agg_social_mentions(conn: sqlite3.Connection, window_sec: int) -> Dict[str, int]:
    if not table_exists(conn, "social_events"):
        return {}
    since = now_ts() - int(window_sec)
    cur = conn.cursor()
    try:
        cur.execute("SELECT key, COALESCE(SUM(mentions), 0) FROM social_events WHERE ts>=? GROUP BY key", (int(since),))
        return {str(k): int(m or 0) for (k, m) in cur.fetchall()}
    except Exception:
        return {}


def _agg_social_mentions_src(conn: sqlite3.Connection, window_sec: int, source: str) -> Dict[str, int]:
    if not table_exists(conn, "social_events"):
        return {}
    since = now_ts() - int(window_sec)
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT key, COALESCE(SUM(mentions), 0) FROM social_events WHERE ts>=? AND source=? GROUP BY key",
            (int(since), str(source)),
        )
        return {str(k): int(m or 0) for (k, m) in cur.fetchall()}
    except Exception:
        return {}


def _agg_attention_snapshot_avgs(conn: sqlite3.Connection, window_sec: int) -> Dict[str, Dict[str, float]]:
    if not table_exists(conn, "snapshots"):
        return {}
    since = now_ts() - int(window_sec)
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT key,
                   AVG(COALESCE(txns_m5, 0)) AS tx_avg,
                   AVG(COALESCE(vol_m5, 0))  AS vol_avg,
                   MAX(COALESCE(price_usd, 0)) AS px_max
            FROM snapshots
            WHERE ts>=?
            GROUP BY key
        """, (int(since),))
        out: Dict[str, Dict[str, float]] = {}
        for (k, txa, vola, pxm) in cur.fetchall():
            out[str(k)] = {
                "tx_avg": float(txa or 0.0),
                "vol_avg": float(vola or 0.0),
                "px_max": float(pxm or 0.0),
            }
        return out
    except Exception:
        return {}


async def render_loop() -> None:
    os.makedirs(EXPORT_DIR, exist_ok=True)

    while True:
        clear_screen()

        cfg = load_cfg()
        strat = (cfg.get("strategy") or {}) if isinstance(cfg.get("strategy"), dict) else {}
        bot_mode = str(cfg.get("mode") or "paper").strip().lower()
        strat_mode = str(strat.get("mode") or "momentum").strip().lower()

        dip_w = int(strat.get("dip_window_sec", 90) or 90)
        sh_w = int(strat.get("short_window_sec", 15) or 15)
        speed5_w = max(5, int(sh_w // 3))

        min_dip_base = float(strat.get("min_dip_pct_lookback", -0.0045))
        max_dump_speed = strat.get("max_dump_speed_pct_per_sec", -0.00025)
        min_reb_speed = float(strat.get("min_rebound_speed_pct_per_sec", 0.000010) or 0.000010)
        min_micro_reb = float(strat.get("min_micro_rebound_pct", 0.003) or 0.003)

        activate = float(strat.get("min_rebound_pct_before_trail", 0.06))
        step_pct = float(strat.get("trail_step_pct", 0.10) or 0.10)
        giveback = float(strat.get("rebound_retrace_pct", 0.12))
        floor_lock = float(strat.get("trail_floor_profit_pct", 0.00))
        min_after_arm = int(strat.get("min_seconds_after_trail_activation", 0) or 0)
        min_step_to_exit = int(strat.get("trail_min_step_to_exit", 1) or 1)
        exit_confirm_ticks = int(strat.get("trail_exit_confirm_ticks", 1) or 1)

        exit_dump_again = strat.get("exit_if_dump_speed_again_pct_per_sec", None)
        exit_dump_again = float(exit_dump_again) if exit_dump_again is not None else None
        dump_min_hold = int(strat.get("dump_speed_again_min_hold_sec", 60) or 60)
        dump_min_pnl = float(strat.get("dump_speed_again_min_pnl", -0.03) or -0.03)
        dump_disable_when_trailing = as_bool(strat.get("dump_speed_again_disable_when_trailing_armed", True))

        target_per_day_raw = int(strat.get("target_entries_per_day", 7) or 0)
        daily_cap_raw = int(strat.get("daily_entry_cap", 10) or 0)
        unlimited_entries = (target_per_day_raw <= 0) or (daily_cap_raw <= 0)
        max_new_per_tick = int(strat.get("max_new_positions_per_tick", 1) or 1)
        max_new_per_tick = max(1, max_new_per_tick)

        min_order_usd = float(strat.get("min_order_usd", 10) or 10)
        min_speed5 = strat.get("min_speed5_pct_per_sec", 0.0)

        min_watch_age_sec = int(strat.get("min_watch_age_sec", 45) or 45)
        min_points_window_sec = int(strat.get("min_price_points_window_sec", 60) or 60)
        min_points_before_entry = int(strat.get("min_price_points_before_entry", 10) or 10)
        entry_signal_min = float(strat.get("entry_signal_min", 0.55) or 0.55)

        fail_fast_sec = int(strat.get("rebound_fail_fast_sec", 45) or 45)
        fail_fast_pnl = float(strat.get("rebound_fail_fast_pnl_pct", -0.02) or -0.02)
        fail_fast_peak = float(strat.get("rebound_fail_fast_peak_pnl_pct", 0.01) or 0.01)

        breakeven_arm = float(strat.get("breakeven_arm_pct", 0.018) or 0.018)
        breakeven_off = float(strat.get("breakeven_offset_pct", 0.003) or 0.003)

        tp1_take_pct = float(strat.get("tp1_take_pct", 0.045) or 0.045)
        tp1_fraction_raw = strat.get("tp1_fraction", None)
        if tp1_fraction_raw is None:
            tp1_fraction_raw = cfg.get("tp1_fraction", 0.60)
        tp1_fraction = float(tp1_fraction_raw)

        health = (strat.get("health") or {}) if isinstance(strat.get("health"), dict) else {}
        health_enabled = as_bool(health.get("enabled", False))
        health_use_precheck = as_bool(health.get("use_zerox_precheck", False))
        min_health_score = float(health.get("min_health_score", 0.0) or 0.0)
        max_rt_loss = float(health.get("max_roundtrip_loss_pct", 1.0) or 1.0)
        max_buy_tax_bps = int(health.get("max_buy_tax_bps", cfg.get("max_buy_tax_bps", 10000)) or 0)
        max_sell_tax_bps = int(health.get("max_sell_tax_bps", cfg.get("max_sell_tax_bps", 10000)) or 0)

        nar = (health.get("narrative") or {}) if isinstance(health.get("narrative"), dict) else {}
        hype = (health.get("hype") or {}) if isinstance(health.get("hype"), dict) else {}
        social = (health.get("social") or {}) if isinstance(health.get("social"), dict) else {}

        nar_enabled = as_bool(nar.get("enabled", False))
        nar_hard = as_bool(nar.get("hard_reject", False))
        att_win = int(nar.get("attention_window_sec", 1800) or 1800)
        att_min_tx = float(nar.get("min_txns_avg", 0) or 0)
        att_min_vol = float(nar.get("min_vol_avg", 0) or 0)
        att_max_drop = float(nar.get("max_drop_pct", 1.0) or 1.0)

        hype_enabled = as_bool(hype.get("enabled", False))
        hype_hard = as_bool(hype.get("hard_reject", False))
        hype_win = int(hype.get("window_sec", 3600) or 3600)
        hype_need = hype.get("min_events", 1)
        try:
            hype_need_int = int(hype_need) if hype_need is not None else 1
        except Exception:
            hype_need_int = 1
        hype_need_int = max(0, hype_need_int)

        soc_enabled = as_bool(social.get("enabled", False))
        soc_hard = as_bool(social.get("hard_reject", False))
        soc_win = int(social.get("window_sec", 3600) or 3600)
        soc_need = int(social.get("min_mentions", 3) or 3)

        paper_use_precheck = as_bool(strat.get("paper_use_zerox_precheck", False))
        require_precheck = (bot_mode == "live") or paper_use_precheck or (health_enabled and health_use_precheck)

        try:
            conn = sqlite3.connect(DB_PATH, timeout=1.0)
            conn.row_factory = sqlite3.Row
        except Exception as e:
            print(f"[dashboard] DB open error: {e}")
            await asyncio.sleep(REFRESH_SEC)
            continue

        audit_rows = []

        try:
            cur = conn.cursor()

            cur.execute("SELECT k, v FROM state")
            state = {r["k"]: r["v"] for r in cur.fetchall()}

            bans = parse_state(state, "banned_tokens", {}) or {}
            banned_set = set(bans.keys()) if isinstance(bans, dict) else set()

            first_trade_outcome = parse_state(state, "first_trade_outcome", {}) or {}
            first_trade_loss_ban_enabled = as_bool(strat.get("ban_token_after_first_trade_loss", False))
            ban_min_pct = float(strat.get("ban_token_after_first_trade_loss_min_pnl_pct", -0.05) or -0.05)

            bad_tokens_set = set()
            if first_trade_loss_ban_enabled and isinstance(first_trade_outcome, dict):
                for k, v in first_trade_outcome.items():
                    try:
                        if not isinstance(v, dict):
                            continue
                        if str(v.get("status")) != "loss":
                            continue
                        pnl_pct = float(v.get("pnl_pct") or 0.0)
                        if pnl_pct <= float(ban_min_pct):
                            bad_tokens_set.add(str(k))
                    except Exception:
                        continue

            loss_streak = parse_state(state, "loss_streak", {}) or {}
            if not isinstance(loss_streak, dict):
                loss_streak = {}

            auto_blacklist_until = parse_state(state, "auto_blacklist_until", {}) or {}
            if not isinstance(auto_blacklist_until, dict):
                auto_blacklist_until = {}

            token_meta_cache = parse_state(state, "token_meta_cache", {}) or {}
            if not isinstance(token_meta_cache, dict):
                token_meta_cache = {}

            last_position_sizing = parse_state(state, "last_position_sizing", None)

            cash = float(parse_state(state, "cash", 0) or 0.0)
            peak_equity = float(parse_state(state, "peak_equity", cash) or cash)
            day_start_equity = float(parse_state(state, "day_start_equity", cash) or cash)
            day_key = str(parse_state(state, "day_key", time.strftime("%Y-%m-%d")) or time.strftime("%Y-%m-%d"))
            day_entries = int(parse_state(state, "day_entries", 0) or 0)
            day_exits = int(parse_state(state, "day_exits", 0) or 0)

            pos_cols = set(table_columns(conn, "positions"))
            select_cols = ["key", "chain", "token", "pair", "entry_px", "qty", "entry_ts", "peak_px", "avg_px"]
            for extra in ["trail_armed_ts", "trail_step_n", "trail_stop_px", "trail_breach_n"]:
                if extra in pos_cols:
                    select_cols.append(extra)

            cur.execute(f"SELECT {', '.join(select_cols)} FROM positions")
            pos_rows = cur.fetchall()
            pos_keys = {str(r["key"]) for r in pos_rows}

            snaps = last_snapshot_by_key(conn)

            hype_cnt = _agg_hype_counts(conn, hype_win) if hype_enabled else {}
            soc_sum = _agg_social_mentions(conn, soc_win) if soc_enabled else {}
            soc_tg = _agg_social_mentions_src(conn, soc_win, "telegram") if soc_enabled else {}
            att_avgs = _agg_attention_snapshot_avgs(conn, att_win) if nar_enabled else {}

            positions = []
            exposure = 0.0
            for r in pos_rows:
                k = str(r["key"])
                px = snaps.get(k, {}).get("price_usd")
                val = (float(r["qty"]) * float(px)) if (px is not None) else 0.0
                exposure += val

                pnl = None
                if px is not None and float(r["avg_px"]) > 0:
                    pnl = (float(px) / float(r["avg_px"])) - 1.0

                age = now_ts() - int(r["entry_ts"])
                url = build_url(r["chain"], (r["pair"] or r["token"]))

                positions.append({
                    "sym": short_addr(k),
                    "key": k,
                    "price_now": px,
                    "avg_entry": float(r["avg_px"]),
                    "pnl": pnl,
                    "qty": float(r["qty"]),
                    "value": val,
                    "age": age,
                    "peak_px": float(r["peak_px"]),
                    "trail_step_n": int(r["trail_step_n"]) if "trail_step_n" in r.keys() else 0,
                    "trail_breach_n": int(r["trail_breach_n"]) if "trail_breach_n" in r.keys() else 0,
                    "trail_stop_px": float(r["trail_stop_px"]) if "trail_stop_px" in r.keys() else 0.0,
                    "url": url,
                })

            equity = cash + exposure
            dd_peak = 1.0 - (equity / max(peak_equity, 1e-9))
            dd_day = 1.0 - (equity / max(day_start_equity, 1e-9))

            can_open_new = True
            if dd_peak >= float(cfg.get("max_drawdown_from_peak", 0.25) or 0.25):
                can_open_new = False
            if dd_day >= float(cfg.get("max_daily_loss_pct", 0.12) or 0.12):
                can_open_new = False
            if exposure >= equity * float(cfg.get("max_total_exposure_pct", 0.60) or 0.60):
                can_open_new = False

            cur.execute("SELECT key, chain, token, added_ts, pair, score, cooldown_until FROM watchlist")
            wl = cur.fetchall()

            progress, expected, adj = day_progress_expected_adj(target_per_day_raw, day_entries, unlimited_entries)
            min_dip_eff = float(min_dip_base) if unlimited_entries else float(min_dip_base) * (1.0 - 0.50 * adj)

            watch_items = []
            for r in wl:
                k = str(r["key"])

                if HIDE_BANNED and (k in banned_set or k in bad_tokens_set):
                    continue

                if HIDE_AUTO_BLACKLIST:
                    try:
                        if coerce_int(auto_blacklist_until.get(k, 0), 0) > now_ts():
                            continue
                    except Exception:
                        pass

                s = snaps.get(k, {})

                dip = None
                speed = None
                micro_low = None
                speed5 = None
                if strat_mode == "dip_rebound" and s.get("price_usd") is not None:
                    dip, speed, micro_low, speed5 = dip_speed_micro_low_speed5(conn, k, dip_w, sh_w)

                url = build_url(r["chain"], (r["pair"] or r["token"]))
                cd = int(r["cooldown_until"] or 0)

                feat = {
                    "price_usd": s.get("price_usd"),
                    "liq_usd": s.get("liq_usd"),
                    "vol_m5": s.get("vol_m5"),
                    "txns_m5": s.get("txns_m5"),
                    "fdv": s.get("fdv"),
                }
                f_ok, f_why = passes_filters_reason(cfg, feat) if s.get("price_usd") is not None else (False, "no_snap")

                meta = token_meta_cache.get(k) if isinstance(token_meta_cache, dict) else None
                buy_tax = None
                sell_tax = None
                rt_loss = None
                precheck_ok = None
                precheck_reason = None
                precheck_source = None
                price_impact_max_bps = None
                if isinstance(meta, dict):
                    try:
                        buy_tax = int(meta.get("buy_tax_bps")) if meta.get("buy_tax_bps") is not None else None
                    except Exception:
                        buy_tax = None
                    try:
                        sell_tax = int(meta.get("sell_tax_bps")) if meta.get("sell_tax_bps") is not None else None
                    except Exception:
                        sell_tax = None
                    try:
                        rt_loss = float(meta.get("roundtrip_loss_pct")) if meta.get("roundtrip_loss_pct") is not None else None
                    except Exception:
                        rt_loss = None
                    try:
                        if meta.get("precheck_ok") is not None:
                            precheck_ok = bool(meta.get("precheck_ok"))
                    except Exception:
                        precheck_ok = None
                    try:
                        precheck_reason = str(meta.get("precheck_reason") or "").strip() or None
                    except Exception:
                        precheck_reason = None
                    try:
                        precheck_source = str(meta.get("precheck_source") or "").strip() or None
                    except Exception:
                        precheck_source = None
                    try:
                        price_impact_max_bps = int(meta.get("price_impact_max_bps")) if meta.get("price_impact_max_bps") is not None else None
                    except Exception:
                        price_impact_max_bps = None

                att = att_avgs.get(k) if isinstance(att_avgs, dict) else None
                att_tx = float(att.get("tx_avg")) if isinstance(att, dict) else None
                att_vol = float(att.get("vol_avg")) if isinstance(att, dict) else None
                att_pxmax = float(att.get("px_max")) if isinstance(att, dict) else None
                drop_from_peak = None
                if att_pxmax and att_pxmax > 0 and s.get("price_usd"):
                    drop_from_peak = 1.0 - (float(s.get("price_usd")) / float(att_pxmax))

                watch_items.append({
                    "key": k,
                    "sym": short_addr(k),
                    "chain": r["chain"],
                    "token": r["token"],
                    "pair": r["pair"] or "",
                    "added_ts": int(r["added_ts"] or 0),
                    "price": s.get("price_usd"),
                    "liq": s.get("liq_usd"),
                    "vol5m": s.get("vol_m5"),
                    "tx5m": s.get("txns_m5"),
                    "score": r["score"] if r["score"] is not None else s.get("score"),
                    "dip": dip,
                    "speed": speed,
                    "speed5": speed5,
                    "micro_low": micro_low,
                    "cooldown_until": cd,
                    "url": url,
                    "feat": feat,
                    "has_snap": (s.get("price_usd") is not None),
                    "filter_ok": f_ok,
                    "filter_why": f_why,
                    "buy_tax_bps": buy_tax,
                    "sell_tax_bps": sell_tax,
                    "rt_loss_pct": rt_loss,
                    "precheck_ok": precheck_ok,
                    "precheck_reason": precheck_reason,
                    "precheck_source": precheck_source,
                    "price_impact_max_bps": price_impact_max_bps,
                    "hype_cnt": int(hype_cnt.get(k, 0) or 0) if hype_enabled else None,
                    "soc_mentions": int(soc_sum.get(k, 0) or 0) if soc_enabled else None,
                    "tg_mentions": int(soc_tg.get(k, 0) or 0) if soc_enabled else None,
                    "att_tx_avg": att_tx,
                    "att_vol_avg": att_vol,
                    "att_drop_from_peak": drop_from_peak,
                    "loss_streak": coerce_int(loss_streak.get(k, 0), 0),
                    "abl_until": coerce_int(auto_blacklist_until.get(k, 0), 0),
                    "is_banned": (k in banned_set),
                    "is_bad_first_trade": (k in bad_tokens_set),
                })

            for it in watch_items:
                it["watch_age_sec"] = now_ts() - int(it["added_ts"] or now_ts())
                it["points_recent"] = recent_points_in_window(conn, it["key"], min_points_window_sec) if it["has_snap"] else 0
                it["entry_signal"] = None
                it["signal_why"] = "na"

                liq_rng = None
                dd_recent = None
                if it["has_snap"]:
                    liq_rng = liq_range_pct_window(conn, it["key"], int(health.get("liq_stability_window_sec", 450) or 450))
                    dd_recent = recent_drawdown_from_peak_pct(conn, it["key"], int(health.get("absorption_window_sec", 120) or 120))

                it["liq_range_pct"] = liq_rng
                it["recent_dd_pct"] = dd_recent

                if not it["has_snap"]:
                    it["signal_why"] = "no_snap"
                    continue

                if it["watch_age_sec"] < int(min_watch_age_sec):
                    it["signal_why"] = "warmup_age"
                    continue

                if int(it["points_recent"]) < int(min_points_before_entry):
                    it["signal_why"] = "warmup_pts"
                    continue

                if strat_mode != "dip_rebound":
                    it["signal_why"] = "mode_na"
                    continue

                if it["dip"] is None or it["speed"] is None or it["micro_low"] is None:
                    it["signal_why"] = "no_hist"
                    continue

                if it["dip"] > float(min_dip_eff):
                    it["signal_why"] = "dip"
                    continue

                if max_dump_speed is not None and it["speed"] < float(max_dump_speed):
                    it["signal_why"] = "dump_fast"
                    continue

                if min_reb_speed is not None and it["speed"] < float(min_reb_speed):
                    it["signal_why"] = "reb_speed"
                    continue

                if float(it["micro_low"]) < float(min_micro_reb):
                    it["signal_why"] = "micro_low"
                    continue

                if it["speed5"] is None:
                    it["signal_why"] = "speed5_na"
                    continue

                if min_speed5 is not None and it["speed5"] < float(min_speed5):
                    it["signal_why"] = "speed5"
                    continue

                it["entry_signal"] = dip_rebound_signal(
                    dip=float(it["dip"]),
                    speed=float(it["speed"]),
                    micro_reb=float(it["micro_low"]),
                    speed5=float(it["speed5"]),
                    liq_usd=float(it["liq"] or 0.0),
                    min_dip_abs=abs(float(min_dip_base)),
                    min_reb_speed=float(min_reb_speed),
                    min_micro_reb=float(min_micro_reb),
                    min_speed5=max(float(min_speed5 or 0.0), 1e-9),
                    cfg_min_liq_usd=float(cfg.get("min_liquidity_usd", 0) or 0),
                )

                if float(it["entry_signal"] or 0.0) < float(entry_signal_min):
                    it["signal_why"] = "signal"
                else:
                    it["signal_why"] = "ok"

            by_score = sorted(
                watch_items,
                key=lambda x: (
                    float(x.get("entry_signal") or -1.0),
                    ((x["score"] is not None), (x["score"] or -1)),
                ),
                reverse=True,
            )
            by_new = sorted(watch_items, key=lambda x: x["added_ts"], reverse=True)

            candidates: List[Dict[str, Any]] = []
            for it in by_score:
                if it["key"] in pos_keys:
                    continue
                if it["cooldown_until"] and it["cooldown_until"] > now_ts():
                    continue
                if it["is_banned"] or it["is_bad_first_trade"]:
                    continue
                if it["abl_until"] and it["abl_until"] > now_ts():
                    continue
                if not it["has_snap"]:
                    continue
                if not it["filter_ok"]:
                    continue
                if it["watch_age_sec"] < int(min_watch_age_sec):
                    continue
                if int(it["points_recent"]) < int(min_points_before_entry):
                    continue
                if it.get("entry_signal") is None:
                    continue
                if float(it.get("entry_signal") or 0.0) < float(entry_signal_min):
                    continue
                candidates.append(it)

            candidates.sort(
                key=lambda x: (
                    float(x.get("entry_signal") or -1.0),
                    float(x.get("score") or -1.0),
                ),
                reverse=True,
            )

            cand_total = len(candidates)
            if BOT_CANDIDATES_N and BOT_CANDIDATES_N > 0:
                candidates = candidates[:BOT_CANDIDATES_N]
            cand_shown = len(candidates)

            if TRADES_LIMIT and TRADES_LIMIT > 0:
                cur.execute("SELECT ts, side, token, px_usd, usd_value, reason FROM trades ORDER BY id DESC LIMIT ?", (int(TRADES_LIMIT),))
            else:
                cur.execute("SELECT ts, side, token, px_usd, usd_value, reason FROM trades ORDER BY id DESC")
            trades = cur.fetchall()
  
            audit_rows = recent_audit_events(conn, limit=8)

        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower():
                print("[dashboard] DB locked (bot sta scrivendo). Riprovo...")
                try:
                    conn.close()
                except Exception:
                    pass
                await asyncio.sleep(REFRESH_SEC)
                continue
            raise
        finally:
            try:
                conn.close()
            except Exception:
                pass

        def precheck_tag(item: Dict[str, Any]) -> str:
            src = str(item.get("precheck_source") or "").strip().lower()
            ok = item.get("precheck_ok")
            if src == "0x":
                return "0x" if ok is True else ("0x!" if ok is False else "0x?")
            if src == "solana_quote":
                return "SOL" if ok is True else ("SOL!" if ok is False else "SOL?")
            return "-" if ok is None else ("OK" if ok else "FAIL")

        def entry_ok(item: Dict[str, Any]) -> Tuple[bool, str]:
            if (not unlimited_entries) and day_entries >= int(daily_cap_raw):
                return False, "daily_cap"

            if not can_open_new:
                return False, "risk_block"

            if item["is_banned"]:
                return False, "ban_pnl"

            if item["is_bad_first_trade"]:
                return False, "first_trade_ban"

            if item["abl_until"] and item["abl_until"] > now_ts():
                return False, "auto_blacklist"

            if item["cooldown_until"] and item["cooldown_until"] > now_ts():
                return False, "cooldown"

            if len(pos_rows) >= int(cfg.get("max_positions", 1) or 1):
                return False, "max_pos"

            if cash < float(min_order_usd):
                return False, f"cash<{min_order_usd:g}"

            if not item["has_snap"]:
                return False, "no_snap"

            if not item["filter_ok"]:
                return False, f"filters:{item['filter_why']}"

            if item["watch_age_sec"] < int(min_watch_age_sec):
                return False, "warmup_age"

            if int(item["points_recent"]) < int(min_points_before_entry):
                return False, "warmup_pts"

            if strat_mode == "dip_rebound":
                if item["dip"] is None or item["speed"] is None or item.get("micro_low") is None:
                    return False, "no_hist"

                if item["dip"] > float(min_dip_eff):
                    return False, "dip"

                if max_dump_speed is not None and item["speed"] < float(max_dump_speed):
                    return False, "dump_fast"

                if min_reb_speed is not None and item["speed"] < float(min_reb_speed):
                    return False, "reb_speed"

                if float(item["micro_low"]) < float(min_micro_reb):
                    return False, "micro_low"

                if item["speed5"] is None:
                    return False, "speed5_na"

                if min_speed5 is not None and item["speed5"] < float(min_speed5):
                    return False, "speed5"

            if item.get("entry_signal") is None:
                return False, item.get("signal_why") or "signal_na"

            if float(item.get("entry_signal") or 0.0) < float(entry_signal_min):
                return False, "signal"

            if require_precheck and health_enabled:
                if item.get("precheck_ok") is False:
                    return False, item.get("precheck_reason") or "precheck_fail"
                has_precheck_meta = (
                    item.get("precheck_ok") is True
                    or item.get("buy_tax_bps") is not None
                    or item.get("sell_tax_bps") is not None
                    or item.get("rt_loss_pct") is not None
                )
                if not has_precheck_meta:
                    return False, "precheck_pending"

            rt = item.get("rt_loss_pct")
            if rt is not None and float(rt) > float(max_rt_loss):
                return False, "rt_loss"

            bt = item.get("buy_tax_bps")
            st = item.get("sell_tax_bps")
            if bt is not None and int(bt) > int(max_buy_tax_bps):
                return False, "buy_tax"
            if st is not None and int(st) > int(max_sell_tax_bps):
                return False, "sell_tax"

            liq_rng = item.get("liq_range_pct")
            max_liq_rng = float(health.get("max_liq_range_pct", 0.35) or 0.35)
            if liq_rng is not None and float(liq_rng) > float(max_liq_rng):
                return False, "liq_unstable"

            dd_recent = item.get("recent_dd_pct")
            max_recent_dd = float(health.get("max_recent_drawdown_from_peak_pct", 0.10) or 0.10)
            if dd_recent is not None and float(dd_recent) > float(max_recent_dd):
                return False, "collapsing"

            return True, "ok"

        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        watch_total = len(watch_items)
        snaps_total = sum(1 for it in watch_items if it["has_snap"])
        entry_now = sum(1 for it in watch_items if entry_ok(it)[0])

        db_abs = os.path.abspath(DB_PATH)
        try:
            st = os.stat(db_abs)
            db_info = f"{db_abs} | size={st.st_size}B | mtime={time.strftime('%H:%M:%S', time.localtime(st.st_mtime))}"
        except Exception:
            db_info = f"{db_abs} (missing)"

        rules = (
            f"RULES: dip_w={dip_w}s sh_w={sh_w}s speed5_w={speed5_w}s | "
            f"min_dip_base={min_dip_base * 100:+.2f}% eff={min_dip_eff * 100:+.2f}% | "
            f"dump>={('OFF' if max_dump_speed is None else f'{float(max_dump_speed) * 100:+.4f}%/s')} | "
            f"reb>={float(min_reb_speed) * 100:+.4f}%/s | "
            f"micro>={min_micro_reb * 100:+.2f}% | "
            f"speed5>={('OFF' if min_speed5 is None else f'{float(min_speed5) * 100:+.4f}%/s')}"
        )

        cap_str = "inf" if unlimited_entries else str(int(daily_cap_raw))
        target_str = "OFF" if (unlimited_entries or target_per_day_raw <= 0) else str(int(target_per_day_raw))

        print(f"=== DASHBOARD | {ts} ===")
        print(f"DB={db_info} | CFG={CONFIG_PATH} | BOT_MODE={bot_mode} | STRAT_MODE={strat_mode} | build=DASH_WARFARE_PLUS_2026-03-10")
        print(f"VIEW: BOT_CANDIDATES_N={BOT_CANDIDATES_N} (0=all) | DASH_ROWS={DASH_ROWS} (0=all) | TRADES_LIMIT={TRADES_LIMIT} (0=all) | TRADES_SHOW_N={TRADES_SHOW_N}")
        print(f"WATCHLIST: {watch_total} token (snapshot={snaps_total}) | CANDIDATES={cand_total} shown={cand_shown} | ENTRY_now={entry_now}")
        print(f"POS={len(positions)} | CASH={cash:.2f}$ | EXP={exposure:.2f}$ | EQ~={equity:.2f}$ | DD_day={dd_day * 100:.1f}% DD_peak={dd_peak * 100:.1f}% | can_open_new={can_open_new}")
        if expected is None:
            print(f"DAY={day_key} | entries={day_entries} exits={day_exits} | target/day={target_str} cap/day={cap_str} | pace=OFF | adj={adj:+.2f}")
        else:
            print(f"DAY={day_key} | entries={day_entries} exits={day_exits} | target/day={target_str} cap/day={cap_str} | pace={expected:.2f} @ {progress * 100:.0f}% day | adj={adj:+.2f}")
        print(rules)
        print(
            f"WARMUP/SIGNAL: min_watch_age={min_watch_age_sec}s | points_win={min_points_window_sec}s "
            f"min_points={min_points_before_entry} | entry_signal_min={entry_signal_min:.2f}"
        )
        print(
            f"EXITS+: fail_fast={fail_fast_sec}s/{fail_fast_pnl * 100:+.1f}% peak>={fail_fast_peak * 100:.1f}% | "
            f"breakeven={breakeven_arm * 100:.2f}% off={breakeven_off * 100:.2f}% | "
            f"tp1={tp1_take_pct * 100:.2f}% x {tp1_fraction * 100:.0f}%"
        )
        print(
            f"TRAIL: activate={activate * 100:.1f}% step={step_pct * 100:.1f}% giveback={giveback * 100:.1f}% "
            f"floor={floor_lock * 100:.1f}% | min_after_arm={min_after_arm}s min_step_to_exit={min_step_to_exit} "
            f"confirm={exit_confirm_ticks} ticks"
        )
        print(
            f"DUMP_AGAIN: thr={('OFF' if exit_dump_again is None else f'{exit_dump_again * 100:+.4f}%/s')} | "
            f"min_hold={dump_min_hold}s min_pnl={dump_min_pnl * 100:+.1f}% disable_when_trailing={dump_disable_when_trailing}"
        )
        print(
            f"HEALTH: enabled={health_enabled} min_score={min_health_score:.2f} "
            f"max_rt_loss={max_rt_loss * 100:.1f}% max_tax_bps={max_buy_tax_bps}/{max_sell_tax_bps} "
            f"require_precheck={require_precheck}"
        )
        if nar_enabled or hype_enabled or soc_enabled:
            print(
                f"SIGNALS: narrative(enabled={nar_enabled} hard={nar_hard} win={att_win}s "
                f"min_tx_avg={att_min_tx:g} min_vol_avg={att_min_vol:g} max_drop={att_max_drop * 100:.0f}%) | "
                f"hype(enabled={hype_enabled} hard={hype_hard} win={hype_win}s min_events={hype_need_int}) | "
                f"social(enabled={soc_enabled} hard={soc_hard} win={soc_win}s min_mentions={soc_need})"
            )
        inst = (strat.get("institutional") or {}) if isinstance(strat.get("institutional"), dict) else {}
        kill_switch_on = as_bool(os.getenv("BOT_KILL_SWITCH", "0")) or as_bool(state.get("kill_switch", False))
        print(
            f"INST: enabled={as_bool(inst.get('enabled', False))} stale<={int(inst.get('reject_stale_snapshot_sec', 0) or 0)}s "
            f"max_trade_usd={fmt_num(float(inst.get('max_trade_notional_usd', 0.0) or 0.0), 2)} "
            f"max_trade_pct={float(inst.get('max_trade_notional_pct', 0.0) or 0.0)*100:.1f}% "
            f"max_chain_pct={float(inst.get('max_chain_exposure_pct', 0.0) or 0.0)*100:.1f}% "
            f"precheck_cd={int(inst.get('precheck_fail_cooldown_sec', 0) or 0)}s | KILL_SWITCH={kill_switch_on}"
        )
        print()

        if isinstance(last_position_sizing, dict):
            try:
                age = now_ts() - int(last_position_sizing.get("ts") or 0)
            except Exception:
                age = 0
            print("LAST_POSITION_SIZING:")
            print(
                f"  age={age}s | key={short_addr(str(last_position_sizing.get('key') or ''))} | "
                f"base={fmt_num(last_position_sizing.get('base_usd'), 2)}$ mult={fmt_num(last_position_sizing.get('mult'), 3)} "
                f"out={fmt_num(last_position_sizing.get('out_usd'), 2)}$ | slip_bps={last_position_sizing.get('slip_bps')} "
                f"tax_rt_bps={last_position_sizing.get('tax_rt_bps')} rt_loss={fmt_pct(last_position_sizing.get('rt_loss_pct'))}"
            )
            print()

        entry_intent = parse_state(state, "entry_intent", None)
        entry_last = parse_state(state, "entry_last", None)

        if entry_intent:
            try:
                age = now_ts() - int(entry_intent.get("ts") or 0)
            except Exception:
                age = 0

            slip_v = entry_intent.get("slip_bps", entry_intent.get("slip"))
            pre_v = entry_intent.get("precheck", entry_intent.get("pre"))
            sig_v = entry_intent.get("signal")
            pts_v = entry_intent.get("points")
            agew_v = entry_intent.get("age_sec")
            size_meta = entry_intent.get("size_meta") if isinstance(entry_intent.get("size_meta"), dict) else None

            print("ENTRY_INTENT:")
            print(
                f"  age={age}s | key={short_addr(str(entry_intent.get('key') or ''))} | "
                f"usd={entry_intent.get('usd')} | slip={slip_v} | pre={pre_v} | "
                f"signal={sig_v} | pts={pts_v} | watch_age={agew_v}s"
            )

            if size_meta:
                print(
                    f"  size: base={size_meta.get('base_usd')} mult={size_meta.get('mult')} "
                    f"pen={size_meta.get('pen')} boost={size_meta.get('boost')} "
                    f"streak={size_meta.get('streak_mult')} signal_mult={size_meta.get('signal_mult')}"
                )
            print()

        if entry_last:
            try:
                age2 = now_ts() - int(entry_last.get("ts") or 0)
            except Exception:
                age2 = 0

            usd_v = entry_last.get("usd")
            qty_v = entry_last.get("qty")
            px_v = entry_last.get("px")
            pre_v = entry_last.get("precheck", entry_last.get("pre"))
            details_v = entry_last.get("details")

            print("ENTRY_LAST:")
            print(
                f"  age={age2}s | key={short_addr(str(entry_last.get('key') or ''))} | "
                f"status={entry_last.get('status')} | reason={entry_last.get('reason')} | "
                f"signal={entry_last.get('signal')} | health={entry_last.get('health_score')}"
            )

            if any(v is not None for v in (usd_v, qty_v, px_v, pre_v)):
                print(
                    f"  usd={usd_v} | qty={qty_v} | px={px_v} | pre={pre_v}"
                )

            size_meta = entry_last.get("size_meta") if isinstance(entry_last.get("size_meta"), dict) else None
            if size_meta:
                print(
                    f"  size: base={size_meta.get('base_usd')} mult={size_meta.get('mult')} "
                    f"pen={size_meta.get('pen')} boost={size_meta.get('boost')} "
                    f"streak={size_meta.get('streak_mult')} signal_mult={size_meta.get('signal_mult')}"
                )

            if details_v not in (None, "", {}, []):
                print("  details:")
                for line in fmt_detail_lines(details_v, "    "):
                    print(line)

            print()

        print("POSIZIONI:")
        if not positions:
            print("(nessuna)")
            print()
        else:
            print("sym         | pnl    | price        | avg          | peak         | step | br | trail_stop    | value$   | age  | url")
            print("-" * 170)
            for p in positions:
                print(
                    f"{p['sym']:<11} | {fmt_pct(p['pnl']):<8} | {fmt_num(p['price_now'], 10):<12} | {fmt_num(p['avg_entry'], 10):<12} | "
                    f"{fmt_num(p['peak_px'], 10):<12} | {p['trail_step_n']:<4d} | {p['trail_breach_n']:<2d} | "
                    f"{fmt_num(p['trail_stop_px'], 10):<12} | {p['value']:<8.2f} | {p['age']:<4d}s | {p['url']}"
                )
            print()

        cand_label = "ALL" if BOT_CANDIDATES_N == 0 else str(BOT_CANDIDATES_N)
        print(f"BOT CANDIDATES (mirrors bot watchlist->candidates; shown={cand_label} by signal/score):")
        if not candidates:
            print("(nessuno)")
            print()
        else:
            print("sym         | price        | liqK  | vol5mK | tx5m | dip      | spd        | spd5       | microLow | SIG  | pts | rtL | tax | PRE  | hype | soc | tg | LS | ABL | why                  | dex")
            print("-" * 270)
            for it in candidates:
                ok, why = entry_ok(it)
                liq_k = (it["liq"] or 0.0) / 1000.0
                vol_k = (it["vol5m"] or 0.0) / 1000.0
                micro_s = "-" if it.get("micro_low") is None else f"{it['micro_low'] * 100:+.2f}%"

                rt_s = "-" if it.get("rt_loss_pct") is None else f"{float(it['rt_loss_pct']) * 100:.1f}%"
                tax_s = "-"
                if it.get("buy_tax_bps") is not None or it.get("sell_tax_bps") is not None:
                    tax_s = f"{fmt_int(it.get('buy_tax_bps'))}/{fmt_int(it.get('sell_tax_bps'))}"

                hype_s = "-" if it.get("hype_cnt") is None else str(int(it.get("hype_cnt") or 0))
                soc_s = "-" if it.get("soc_mentions") is None else str(int(it.get("soc_mentions") or 0))
                tg_s = "-" if it.get("tg_mentions") is None else str(int(it.get("tg_mentions") or 0))
                ls_s = str(coerce_int(it.get("loss_streak"), 0))

                abl_s = "-"
                if it.get("abl_until") and int(it["abl_until"]) > now_ts():
                    abl_s = f"{int((int(it['abl_until']) - now_ts()) / 60)}m"

                sig_s = "-" if it.get("entry_signal") is None else f"{float(it.get('entry_signal') or 0.0):.3f}"
                pts_s = str(int(it.get("points_recent") or 0))
                pre_s = precheck_tag(it)

                why_out = "ENTRY" if ok else why
                dex_out = short_url(it["url"])
                print(
                    f"{it['sym']:<11} | {fmt_num(it['price'], 10):<12} | {fmt_num(liq_k, 2):>5} | {fmt_num(vol_k, 2):>6} | "
                    f"{int(it['tx5m'] or 0):<4d} | {fmt_pct(it['dip']):<8} | {fmt_speed(it['speed']):<10} | {fmt_speed(it['speed5']):<10} | "
                    f"{micro_s:<8} | {sig_s:>4} | {pts_s:>3} | {rt_s:>4} | {tax_s:<7} | {pre_s:<4} | {hype_s:>4} | {soc_s:>3} | "
                    f"{tg_s:>3} | {ls_s:>2} | {abl_s:>3} | {why_out:<20.20} | {dex_out}"
                )
            print()

        rows = by_score
        if WATCHLIST_TOP_N and WATCHLIST_TOP_N > 0:
            rows = rows[:WATCHLIST_TOP_N]

        print(f"WATCHLIST (top score/signal) rows={len(rows)} (WATCHLIST_TOP_N={WATCHLIST_TOP_N}):")
        print("sym         | age  | snap | pts | price        | liqK  | vol5mK | tx5m | score | SIG  | rtL | tax | PRE  | hype | soc | LS | ABL | why                  | dex")
        print("-" * 255)
        for it in rows:
            ok, why = entry_ok(it)
            age_s = now_ts() - int(it["added_ts"] or now_ts())
            liq_k = (it["liq"] or 0.0) / 1000.0 if it["liq"] is not None else None
            vol_k = (it["vol5m"] or 0.0) / 1000.0 if it["vol5m"] is not None else None
            snap = "Y" if it["has_snap"] else "N"

            rt_s = "-" if it.get("rt_loss_pct") is None else f"{float(it['rt_loss_pct']) * 100:.1f}%"
            tax_s = "-"
            if it.get("buy_tax_bps") is not None or it.get("sell_tax_bps") is not None:
                tax_s = f"{fmt_int(it.get('buy_tax_bps'))}/{fmt_int(it.get('sell_tax_bps'))}"

            hype_s = "-" if it.get("hype_cnt") is None else str(int(it.get("hype_cnt") or 0))
            soc_s = "-" if it.get("soc_mentions") is None else str(int(it.get("soc_mentions") or 0))
            ls_s = str(coerce_int(it.get("loss_streak"), 0))

            abl_s = "-"
            if it.get("abl_until") and int(it["abl_until"]) > now_ts():
                abl_s = f"{int((int(it['abl_until']) - now_ts()) / 60)}m"

            sig_s = "-" if it.get("entry_signal") is None else f"{float(it.get('entry_signal') or 0.0):.3f}"
            pts_s = str(int(it.get("points_recent") or 0))
            pre_s = precheck_tag(it)

            why_out = "ENTRY" if ok else why
            dex_out = short_url(it["url"])
            print(
                f"{it['sym']:<11} | {age_s:>4d}s | {snap:^4} | {pts_s:>3} | {fmt_num(it['price'], 10):<12} | {fmt_num(liq_k, 2):>5} | "
                f"{fmt_num(vol_k, 2):>6} | {int(it['tx5m'] or 0):<4d} | {fmt_num(it['score'], 2):<5} | {sig_s:>4} | "
                f"{rt_s:>4} | {tax_s:<7} | {pre_s:<4} | {hype_s:>4} | {soc_s:>3} | {ls_s:>2} | {abl_s:>3} | {why_out:<20.20} | {dex_out}"
            )
        print()

        print(f"ULTIME TRADE (mostra {TRADES_SHOW_N}, query_limit={TRADES_LIMIT or 'ALL'}):")
        if not trades:
            print("(nessuna)")
            print()
        else:
            print("time     | side | sym         | px           | usd    | reason")
            print("-" * 110)
            for r in trades[:max(0, int(TRADES_SHOW_N))]:
                t = time.strftime("%H:%M:%S", time.localtime(int(r["ts"])))
                print(
                    f"{t:<8} | {r['side']:<4} | {short_addr(r['token']):<11} | {fmt_num(r['px_usd'], 10):<12} | "
                    f"{fmt_num(r['usd_value'], 2):<6} | {r['reason']}"
                )
            print()

        print(f"AUDIT EVENTS (ultimi {len(audit_rows)}):")
        if not audit_rows:
            print("(nessuno)")
            print()
        else:
            print("time     | event              | key         | payload")
            print("-" * 140)
            for a in audit_rows:
                t = time.strftime("%H:%M:%S", time.localtime(int(a.get("ts") or 0)))
                payload = a.get("payload") or {}
                if isinstance(payload, dict):
                    payload_txt = ", ".join([f"{k}={fmt_detail_scalar(v)}" for k, v in list(payload.items())[:4]])
                else:
                    payload_txt = str(payload)
                print(f"{t:<8} | {str(a.get('event') or ''):<18} | {short_addr(str(a.get('key') or '')):<11} | {payload_txt[:80]}")
            print()

        try:
            os.makedirs(EXPORT_DIR, exist_ok=True)

            with open(os.path.join(EXPORT_DIR, "watchlist_all.csv"), "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow([
                    "key", "added_ts", "watch_age_sec", "points_recent", "has_snapshot",
                    "price_usd", "liq_usd", "vol_m5", "txns_m5", "score", "entry_signal",
                    "rt_loss_pct", "buy_tax_bps", "sell_tax_bps", "precheck_ok", "precheck_source", "precheck_reason", "price_impact_max_bps", "hype_cnt", "soc_mentions",
                    "loss_streak", "abl_until", "why", "url"
                ])
                for it in by_new:
                    ok, why = entry_ok(it)
                    w.writerow([
                        it["key"],
                        it["added_ts"],
                        it.get("watch_age_sec"),
                        it.get("points_recent"),
                        int(it["has_snap"]),
                        it["price"],
                        it["liq"],
                        it["vol5m"],
                        it["tx5m"],
                        it["score"],
                        it.get("entry_signal"),
                        it.get("rt_loss_pct"),
                        it.get("buy_tax_bps"),
                        it.get("sell_tax_bps"),
                        it.get("precheck_ok"),
                        it.get("precheck_source"),
                        it.get("precheck_reason"),
                        it.get("price_impact_max_bps"),
                        it.get("hype_cnt"),
                        it.get("soc_mentions"),
                        it.get("loss_streak"),
                        it.get("abl_until"),
                        "ENTRY" if ok else why,
                        it["url"],
                    ])

            with open(os.path.join(EXPORT_DIR, "watchlist_candidates.csv"), "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow([
                    "key", "watch_age_sec", "points_recent", "price_usd", "liq_usd", "vol_m5", "txns_m5",
                    "dip", "speed", "speed5", "micro_low", "score", "entry_signal",
                    "rt_loss_pct", "buy_tax_bps", "sell_tax_bps", "precheck_ok", "precheck_source", "precheck_reason", "price_impact_max_bps", "hype_cnt", "soc_mentions",
                    "loss_streak", "abl_until", "why", "url"
                ])
                for it in candidates:
                    ok, why = entry_ok(it)
                    w.writerow([
                        it["key"],
                        it.get("watch_age_sec"),
                        it.get("points_recent"),
                        it["price"],
                        it["liq"],
                        it["vol5m"],
                        it["tx5m"],
                        it["dip"],
                        it["speed"],
                        it["speed5"],
                        it["micro_low"],
                        it["score"],
                        it.get("entry_signal"),
                        it.get("rt_loss_pct"),
                        it.get("buy_tax_bps"),
                        it.get("sell_tax_bps"),
                        it.get("precheck_ok"),
                        it.get("precheck_source"),
                        it.get("precheck_reason"),
                        it.get("price_impact_max_bps"),
                        it.get("hype_cnt"),
                        it.get("soc_mentions"),
                        it.get("loss_streak"),
                        it.get("abl_until"),
                        "ENTRY" if ok else why,
                        it["url"],
                    ])

            with open(os.path.join(EXPORT_DIR, "positions.csv"), "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow([
                    "key", "price_now", "avg_entry", "pnl", "qty", "value",
                    "age_sec", "peak_px", "trail_step_n", "trail_breach_n", "trail_stop_px", "url"
                ])
                for p in positions:
                    w.writerow([
                        p["key"],
                        p["price_now"],
                        p["avg_entry"],
                        p["pnl"],
                        p["qty"],
                        p["value"],
                        p["age"],
                        p["peak_px"],
                        p["trail_step_n"],
                        p["trail_breach_n"],
                        p["trail_stop_px"],
                        p["url"],
                    ])

        except Exception as e:
            print("[export] error:", e)

        await asyncio.sleep(REFRESH_SEC)


if __name__ == "__main__":
    asyncio.run(render_loop())