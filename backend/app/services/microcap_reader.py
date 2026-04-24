import json
import sqlite3
from typing import Any

from app.services.storage import PRIVATE

DB_PATH = PRIVATE / 'microcap' / 'bot.db'


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def _has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    return any(str(row['name']) == column for row in cur.fetchall())


def _state(conn: sqlite3.Connection) -> dict[str, Any]:
    cur = conn.cursor()
    cur.execute('SELECT k, v FROM state')
    out: dict[str, Any] = {}
    for row in cur.fetchall():
        try:
            out[row['k']] = json.loads(row['v'])
        except Exception:
            out[row['k']] = row['v']
    return out


def _latest_snapshots(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(
        '''
        SELECT s.key, s.chain, s.token, s.price_usd, s.liq_usd, s.vol_m5, s.txns_m5, s.fdv, s.score, s.ts
        FROM snapshots s
        JOIN (SELECT key, MAX(ts) AS mx FROM snapshots GROUP BY key) t
          ON t.key = s.key AND t.mx = s.ts
        ORDER BY COALESCE(s.score, 0) DESC, s.ts DESC
        LIMIT 200
        '''
    )
    return [dict(row) for row in cur.fetchall()]


def _trades(conn: sqlite3.Connection, limit: int = 60) -> list[dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(
        'SELECT ts, mode, chain, token, pair, side, px_usd, qty, usd_value, reason FROM trades ORDER BY id DESC LIMIT ?',
        (limit,),
    )
    return [dict(row) for row in cur.fetchall()]


def _positions(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    cur = conn.cursor()

    if all(
        _has_column(conn, 'positions', col)
        for col in ['trail_armed_ts', 'trail_step_n', 'trail_stop_px', 'trail_breach_n']
    ):
        cur.execute(
            '''
            SELECT key, chain, token, pair, entry_px, qty, entry_ts, peak_px, avg_px, pyramids_done, tp1_done,
                   trail_armed_ts, trail_step_n, trail_stop_px, trail_breach_n
            FROM positions
            ORDER BY entry_ts DESC
            '''
        )
        return [dict(row) for row in cur.fetchall()]

    cur.execute(
        '''
        SELECT key, chain, token, pair, entry_px, qty, entry_ts, peak_px, avg_px, pyramids_done, tp1_done
        FROM positions
        ORDER BY entry_ts DESC
        '''
    )

    rows = []
    for row in cur.fetchall():
        item = dict(row)
        item['trail_armed_ts'] = 0
        item['trail_step_n'] = 0
        item['trail_stop_px'] = 0.0
        item['trail_breach_n'] = 0
        rows.append(item)
    return rows


def _watchlist(conn: sqlite3.Connection, limit: int = 80) -> list[dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(
        '''
        SELECT w.key, w.chain, w.token, w.added_ts, w.pair, w.score, w.cooldown_until,
               s.price_usd, s.liq_usd, s.vol_m5, s.txns_m5, s.fdv, s.ts
        FROM watchlist w
        LEFT JOIN (
            SELECT s1.*
            FROM snapshots s1
            JOIN (SELECT key, MAX(ts) AS mx FROM snapshots GROUP BY key) t
              ON t.key = s1.key AND t.mx = s1.ts
        ) s ON s.key = w.key
        ORDER BY COALESCE(w.score, 0) DESC, w.added_ts DESC
        LIMIT ?
        ''',
        (limit,),
    )
    return [dict(row) for row in cur.fetchall()]


def _overview(state: dict[str, Any], trades: list[dict[str, Any]], positions: list[dict[str, Any]], watchlist: list[dict[str, Any]], snaps: list[dict[str, Any]]) -> dict[str, Any]:
    cash = float(state.get('cash') or 0.0)
    peak = float(state.get('peak_equity') or cash)
    latest_ts = max([x.get('ts') or 0 for x in snaps] + [0])
    return {
        'cash': cash,
        'peak_equity': peak,
        'drawdown_pct': ((cash - peak) / peak) if peak else 0.0,
        'positions_count': len(positions),
        'watchlist_count': len(watchlist),
        'snapshots_count': len(snaps),
        'trades_count': len(trades),
        'day_entries': int(state.get('day_entries') or 0),
        'day_exits': int(state.get('day_exits') or 0),
        'latest_snapshot_ts': latest_ts,
        'entry_last': state.get('entry_last'),
    }


def _equity_curve(state: dict[str, Any], trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
    start = float(state.get('day_start_equity') or 200.0)
    pnl = 0.0
    rows = []
    for trade in reversed(trades):
        if str(trade.get('side')).lower() in {'sell', 'exit'}:
            reason = str(trade.get('reason') or '')
            value = float(trade.get('usd_value') or 0.0)
            pnl += abs(value) * (0.004 if 'sl' not in reason.lower() else -0.02)
        rows.append({'ts': int(trade.get('ts') or 0), 'equity': round(start + pnl, 4)})
    return rows[-60:]


def _empty_dashboard() -> dict[str, Any]:
    return {
        'overview': {
            'cash': 0.0,
            'peak_equity': 0.0,
            'drawdown_pct': 0.0,
            'positions_count': 0,
            'watchlist_count': 0,
            'snapshots_count': 0,
            'trades_count': 0,
            'day_entries': 0,
            'day_exits': 0,
            'latest_snapshot_ts': 0,
            'entry_last': None,
        },
        'trades': [],
        'positions': [],
        'watchlist': [],
        'top_candidates': [],
        'equity_curve': [],
    }


def read_dashboard() -> dict[str, Any]:
    if not DB_PATH.exists():
        return _empty_dashboard()

    conn = _connect()
    try:
        state = _state(conn)
        trades = _trades(conn)
        positions = _positions(conn)
        watchlist = _watchlist(conn)
        snaps = _latest_snapshots(conn)

        candidates = sorted(
            [x for x in watchlist if x.get('price_usd')],
            key=lambda item: ((item.get('score') is not None), item.get('score') or 0, item.get('liq_usd') or 0),
            reverse=True,
        )[:20]

        return {
            'overview': _overview(state, trades, positions, watchlist, snaps),
            'trades': trades,
            'positions': positions,
            'watchlist': watchlist,
            'top_candidates': candidates,
            'equity_curve': _equity_curve(state, trades),
        }
    except Exception:
        return _empty_dashboard()
    finally:
        conn.close()