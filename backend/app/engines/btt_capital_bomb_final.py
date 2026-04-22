
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning, message=r".*pkg_resources.*deprecated.*")

import argparse
import concurrent.futures as cf
import datetime as dt
import hashlib
import json
import math
import random
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import requests

try:
    import investpy
except Exception:
    print("Errore: investpy non importabile. Usa Python 3.11/3.12 oppure pinna setuptools<82 e investpy==1.0.8.")
    raise

APP_NAME = "BTT Capital Bomb Final"
APP_VERSION = "4.0"

USER_AGENT = f"{APP_NAME}/{APP_VERSION}"
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT})

MAJOR_COUNTRIES_DEFAULT = [
    "united states", "canada", "united kingdom", "germany", "france", "italy",
    "spain", "netherlands", "sweden", "switzerland", "japan", "south korea",
    "hong kong", "singapore", "australia", "india", "brazil"
]

WORLD_BANK_COUNTRIES_URL = "https://api.worldbank.org/v2/country?format=json&per_page=400"
WB_INDICATOR_URL = "https://api.worldbank.org/v2/country/{iso2}/indicator/{indicator}?format=json&per_page=80"
WB_INDICATORS = {
    "debt_gdp": "GC.DOD.TOTL.GD.ZS",
    "political_stability": "PV.EST",
    "corruption_control": "CC.EST",
    "rnd_gdp": "GB.XPD.RSDV.GD.ZS",
    "military_gdp": "MS.MIL.XPND.GD.ZS",
}
COUNTRY_ALIASES = {
    "united states": "US", "u.s.": "US", "usa": "US",
    "united kingdom": "GB", "uk": "GB", "great britain": "GB",
    "south korea": "KR", "korea": "KR", "hong kong": "HK",
    "viet nam": "VN", "vietnam": "VN", "czech republic": "CZ", "czechia": "CZ",
    "uae": "AE", "united arab emirates": "AE", "turkiye": "TR", "türkiye": "TR",
}
THEME_KEYWORDS = {
    "ai": ["ai", "artificial intelligence", "machine learning", "ml", "cloud", "software", "data", "chip", "gpu", "semiconductor"],
    "robotics": ["robot", "robotics", "automation", "autonomous", "sensor", "industrial", "vision"],
    "energy_storage": ["battery", "energy storage", "lithium", "ev", "electric vehicle", "solar", "grid", "power electronics", "charging"],
    "blockchain_fintech": ["blockchain", "crypto", "fintech", "payment", "payments", "wallet", "digital bank", "exchange"],
    "multiomics_biotech": ["biotech", "bio", "genomics", "sequencing", "diagnostic", "pharma", "protein", "therapeutic"],
    "space_defense": ["space", "satellite", "aerospace", "defense", "drone", "launch", "missile"],
}
FUTURE_REGEX = re.compile(
    r"(ai|artificial intelligence|machine learning|cloud|software|chip|semi|gpu|robot|automation|autonomous|battery|electric|ev|solar|grid|fintech|payment|blockchain|crypto|biotech|bio|genom|sequenc|diagnostic|space|satellite|aero|defense|drone)",
    re.I,
)

def now_utc_str() -> str:
    return dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def normalize_country(s: str) -> str:
    s = (s or "").strip().lower()
    return re.sub(r"\s+", " ", s)

def normalize_text(s: Any) -> str:
    s = "" if s is None else str(s)
    return re.sub(r"\s+", " ", s.strip().lower())

def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def safe_sleep(base: float = 0.12, jitter: float = 0.05) -> None:
    time.sleep(max(0.0, base + random.uniform(-jitter, jitter)))

def numeric(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        try:
            if math.isnan(value):
                return None
        except Exception:
            pass
        return float(value)
    s = str(value).strip()
    if not s or s.lower() in {"none", "nan", "n/a", "na", "-", "--", "null"}:
        return None
    s = s.replace("\xa0", " ").replace(" ", "")
    negative = False
    if s.startswith("(") and s.endswith(")"):
        negative = True
        s = s[1:-1]
    pct = s.endswith("%")
    if pct:
        s = s[:-1]
    if "," in s and "." in s:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif "," in s:
        if s.count(",") == 1 and re.search(r",\d{1,4}$", s):
            s = s.replace(",", ".")
        else:
            s = s.replace(",", "")
    mult = 1.0
    m = re.match(r"^([-+]?\d*\.?\d+)([kmbt])$", s.lower())
    if m:
        mult = {"k":1e3,"m":1e6,"b":1e9,"t":1e12}[m.group(2)]
        s = m.group(1)
    try:
        out = float(s) * mult
        if negative:
            out = -out
        if pct:
            out /= 100.0
        return out
    except Exception:
        return None

def parse_percent_string(value: Any) -> Optional[float]:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    if s.endswith("%"):
        return numeric(s)
    x = numeric(s)
    if x is None:
        return None
    if abs(x) > 1.5:
        return x / 100.0
    return x

def score_linear(value: Optional[float], lo: float, hi: float, invert: bool = False) -> Optional[float]:
    if value is None:
        return None
    if hi == lo:
        return 50.0
    x = clamp((value - lo) / (hi - lo), 0.0, 1.0)
    if invert:
        x = 1.0 - x
    return x * 100.0

def score_sweet_spot(value: Optional[float], ideal_lo: float, ideal_hi: float, hard_lo: float, hard_hi: float) -> Optional[float]:
    if value is None:
        return None
    if value < hard_lo or value > hard_hi:
        return 0.0
    if ideal_lo <= value <= ideal_hi:
        return 100.0
    if value < ideal_lo:
        return 100.0 * (value - hard_lo) / max(1e-9, ideal_lo - hard_lo)
    return 100.0 * (hard_hi - value) / max(1e-9, hard_hi - ideal_hi)

def geometric_growth(first: Optional[float], last: Optional[float], years: float) -> Optional[float]:
    if first is None or last is None or years <= 0 or first <= 0 or last <= 0:
        return None
    try:
        return (last / first) ** (1.0 / years) - 1.0
    except Exception:
        return None

def weighted_score(items: List[Tuple[float, Optional[float]]], neutral: float = 50.0) -> Tuple[float, float]:
    total_w = sum(w for w, _ in items)
    valid = [(w, v) for w, v in items if v is not None]
    if total_w <= 0 or not valid:
        return neutral, 0.0
    used = sum(w for w, _ in valid)
    return (sum(w * clamp(float(v), 0.0, 100.0) for w, v in valid) / used, used / total_w)

def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    x = df.copy()
    x.columns = [str(c).strip() for c in x.columns]
    return x

def latest_row(df: pd.DataFrame) -> Optional[pd.Series]:
    if df is None or df.empty:
        return None
    x = clean_columns(df)
    try:
        x = x.sort_index()
    except Exception:
        pass
    return x.iloc[-1]

def first_existing(row: pd.Series, candidates: Iterable[str]) -> Optional[float]:
    for c in candidates:
        if c in row.index:
            v = numeric(row.get(c))
            if v is not None:
                return v
    return None

def annual_years_span(df: pd.DataFrame) -> float:
    if df is None or df.empty:
        return 0.0
    years = []
    for i in list(df.index):
        m = re.search(r"(\d{4})", str(i))
        if m:
            years.append(int(m.group(1)))
    if len(years) >= 2:
        return max(years) - min(years)
    return max(1.0, len(df) - 1)

def extract_statement_series(df: pd.DataFrame, fields: List[str]) -> List[Optional[float]]:
    if df is None or df.empty:
        return []
    x = clean_columns(df)
    try:
        x = x.sort_index()
    except Exception:
        pass
    return [first_existing(row, fields) for _, row in x.iterrows()]

def json_to_df(s: Optional[str]) -> pd.DataFrame:
    if not s:
        return pd.DataFrame()
    try:
        return pd.read_json(s, orient="split")
    except Exception:
        return pd.DataFrame()

def df_to_json_or_none(df: pd.DataFrame) -> Optional[str]:
    if not isinstance(df, pd.DataFrame) or df.empty:
        return None
    return df.to_json(orient="split", date_format="iso")

def info_to_dict(obj: Any) -> Dict[str, Any]:
    if isinstance(obj, dict):
        return obj
    if isinstance(obj, str):
        try:
            parsed = json.loads(obj)
            if isinstance(parsed, dict):
                return parsed
            if isinstance(parsed, list) and parsed and isinstance(parsed[0], dict):
                return parsed[0]
        except Exception:
            return {}
    if isinstance(obj, pd.DataFrame) and not obj.empty:
        return obj.iloc[0].to_dict()
    return {}

def theme_hits(text: str) -> Dict[str, int]:
    t = normalize_text(text)
    return {theme: sum(1 for kw in kws if kw in t) for theme, kws in THEME_KEYWORDS.items()}

def theme_score_from_hits(hits: Dict[str, int]) -> float:
    total = sum(hits.values())
    mx = max(hits.values()) if hits else 0
    if mx >= 3 or total >= 4:
        return 100.0
    if mx == 2 or total == 3:
        return 85.0
    if mx == 1 or total == 2:
        return 68.0
    return 45.0

def compute_price_stats(df: pd.DataFrame) -> Dict[str, Optional[float]]:
    out = {"price_change_6m": None, "price_change_1y": None, "price_cagr_3y": None, "vol_1y": None, "max_drawdown_3y": None}
    if df is None or df.empty or "Close" not in df.columns:
        return out
    x = df.copy()
    try:
        x = x.sort_index()
    except Exception:
        pass
    close = pd.to_numeric(x["Close"], errors="coerce").dropna()
    if len(close) < 3:
        return out
    last = numeric(close.iloc[-1])
    if len(close) > 126:
        c6 = numeric(close.iloc[-127]); out["price_change_6m"] = (last / c6 - 1.0) if c6 and last else None
    if len(close) > 252:
        c1 = numeric(close.iloc[-253]); out["price_change_1y"] = (last / c1 - 1.0) if c1 and last else None
        ret = close.pct_change().dropna()
        if not ret.empty:
            out["vol_1y"] = float(ret.tail(min(252, len(ret))).std() * math.sqrt(252))
    if len(close) > 500:
        c0 = numeric(close.iloc[0]); out["price_cagr_3y"] = geometric_growth(c0, last, max(1.0, len(close)/252.0))
    cummax = close.cummax()
    dd = close / cummax - 1.0
    if not dd.empty:
        out["max_drawdown_3y"] = float(dd.min())
    return out

def wb_get_json(url: str) -> Any:
    r = SESSION.get(url, timeout=35)
    r.raise_for_status()
    return r.json()

def build_wb_country_map() -> Dict[str, str]:
    data = wb_get_json(WORLD_BANK_COUNTRIES_URL)
    out = {}
    for row in (data[1] or []):
        iso2 = row.get("id"); name = row.get("name")
        if iso2 and name:
            out[normalize_country(name)] = iso2
    out.update({normalize_country(k): v for k, v in COUNTRY_ALIASES.items()})
    return out

def wb_latest_value(iso2: str, indicator: str) -> Optional[float]:
    data = wb_get_json(WB_INDICATOR_URL.format(iso2=iso2.lower(), indicator=indicator))
    if not isinstance(data, list) or len(data) < 2:
        return None
    for row in (data[1] or []):
        v = row.get("value")
        if v is not None:
            return numeric(v)
    return None

def fetch_country_macro(iso2: str) -> Dict[str, Optional[float]]:
    out = {}
    for k, ind in WB_INDICATORS.items():
        try:
            out[k] = wb_latest_value(iso2, ind)
            safe_sleep(0.10, 0.04)
        except Exception:
            out[k] = None
    out["natural_risk"] = None
    return out

def country_macro_score(m: Dict[str, Optional[float]]) -> Tuple[float, Dict[str, float]]:
    debt = score_linear(m.get("debt_gdp"), 20, 150, invert=True) or 50.0
    politics = 0.6 * (score_linear(m.get("political_stability"), -2.5, 2.5) or 50.0) + 0.4 * (score_linear(m.get("corruption_control"), -2.5, 2.5) or 50.0)
    geop = score_linear(m.get("military_gdp"), 0.5, 8.0, invert=True) or 50.0
    nature = 50.0
    innov = score_linear(m.get("rnd_gdp"), 0.1, 4.5) or 50.0
    total = 0.25 * debt + 0.20 * politics + 0.15 * geop + 0.15 * nature + 0.25 * innov
    return total, {
        "macro_debt_score": debt, "macro_politics_score": politics, "macro_geopolitics_score": geop,
        "macro_nature_score": nature, "macro_innovation_score": innov,
    }

class Cache:
    def __init__(self, path: Path):
        self.path = path
        self.data: Dict[str, Dict[str, Any]] = {}
        if path.exists():
            try:
                self.data = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                self.data = {}
    def get(self, key: str) -> Optional[Dict[str, Any]]:
        return self.data.get(key)
    def set(self, key: str, value: Dict[str, Any]) -> None:
        self.data[key] = value
    def save(self) -> None:
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.data, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(self.path)

def cache_key(kind: str, country: str, symbol: str) -> str:
    return hashlib.sha1(f"{kind}::{normalize_country(country)}::{normalize_text(symbol)}".encode("utf-8")).hexdigest()

def try_country_overview(country: str, n_results: int) -> pd.DataFrame:
    try:
        safe_sleep(0.12, 0.05)
        df = investpy.get_stocks_overview(country=country, n_results=max(10, min(1000, n_results)))
        if isinstance(df, pd.DataFrame) and not df.empty:
            out = df.copy()
            out.columns = [str(c).strip().lower() for c in out.columns]
            return out
    except Exception:
        pass
    return pd.DataFrame()

def load_universe(countries_real: List[str], max_per_country: int, emerging_only: bool) -> List[Dict[str, Any]]:
    all_stocks = investpy.get_stocks()
    all_stocks.columns = [str(c).strip().lower() for c in all_stocks.columns]
    out = []
    for country in countries_real:
        base = all_stocks[all_stocks["country"].astype(str).map(normalize_country) == normalize_country(country)].copy()
        if base.empty:
            continue
        base["symbol_norm"] = base["symbol"].astype(str).map(normalize_text)
        overview = try_country_overview(country, 1000 if max_per_country == 0 else max(100, max_per_country * 4))
        if not overview.empty and "symbol" in overview.columns:
            overview["symbol_norm"] = overview["symbol"].astype(str).map(normalize_text)
            merged = base.merge(overview.drop_duplicates("symbol_norm"), on="symbol_norm", how="left", suffixes=("", "_ov"))
        else:
            merged = base.copy()
        rows = []
        for _, row in merged.iterrows():
            symbol = str(row.get("symbol") or "").strip()
            if not symbol:
                continue
            full_name = row.get("full_name") or row.get("name") or symbol
            theme_text = f"{symbol} {row.get('name','')} {full_name}"
            hits = theme_hits(theme_text)
            thematic = theme_score_from_hits(hits)
            turnover = numeric(row.get("turnover")) or numeric(row.get("turnover_ov")) or 0.0
            change_pct = parse_percent_string(row.get("change_percentage"))
            change_score = score_sweet_spot(change_pct, 0.02, 0.35, -0.30, 0.90) or 40.0
            turn_score = clamp(math.log10(1.0 + max(turnover, 0.0)) / 9.0 * 100.0, 0.0, 100.0)
            rank = 0.45 * thematic + 0.35 * turn_score + 0.20 * change_score
            if emerging_only and FUTURE_REGEX.search(theme_text):
                rank += 15.0
            rows.append({
                "country": country, "symbol": symbol, "name": row.get("name"), "full_name": full_name,
                "isin": row.get("isin"), "currency": row.get("currency"), "exchange": row.get("stock_exchange") or row.get("exchange"),
                "overview_turnover": turnover, "overview_change_pct": change_pct, "overview_rank": rank,
                "overview_last": numeric(row.get("last")), "thematic_seed_score": thematic,
                "theme_ai_hits": hits.get("ai", 0), "theme_robotics_hits": hits.get("robotics", 0),
                "theme_energy_hits": hits.get("energy_storage", 0), "theme_blockchain_hits": hits.get("blockchain_fintech", 0),
                "theme_multiomics_hits": hits.get("multiomics_biotech", 0), "theme_space_hits": hits.get("space_defense", 0),
            })
        rows.sort(key=lambda x: (x["overview_rank"], x["overview_turnover"]), reverse=True)
        seen = set()
        count = 0
        for item in rows:
            key = normalize_text(item["symbol"])
            if key in seen:
                continue
            seen.add(key)
            out.append(item)
            count += 1
            if max_per_country and count >= max_per_country:
                break
    return out

def choose_best_search_result(meta: Dict[str, Any], results: Iterable[Any]) -> Optional[Any]:
    best = None; best_sc = -1e9
    sym = normalize_text(meta.get("symbol"))
    nm = normalize_text(meta.get("full_name") or meta.get("name") or meta.get("symbol"))
    for obj in results:
        sc = 0.0
        if normalize_country(getattr(obj, "country", "")) == normalize_country(meta.get("country", "")):
            sc += 10.0
        obj_sym = normalize_text(getattr(obj, "symbol", ""))
        obj_name = normalize_text(getattr(obj, "name", ""))
        if obj_sym == sym:
            sc += 35.0
        elif sym and obj_sym.startswith(sym):
            sc += 18.0
        sc += 18.0 * (1.0 if obj_name == nm else 0.0)
        sc += 20.0 * (len(set(obj_name.split()) & set(nm.split())) / max(1, len(set(nm.split()))))
        if FUTURE_REGEX.search(obj_name):
            sc += 2.0
        if sc > best_sc:
            best_sc = sc; best = obj
    return best

def search_investpy_object(meta: Dict[str, Any]) -> Tuple[Optional[Any], Optional[str]]:
    queries = []
    for q in [meta.get("symbol"), meta.get("full_name"), meta.get("name"), f"{meta.get('symbol')} {meta.get('full_name') or meta.get('name') or ''}"]:
        qn = normalize_text(q)
        if qn and qn not in [normalize_text(x) for x in queries]:
            queries.append(q)
    last_error = None
    for q in queries:
        try:
            safe_sleep(0.10, 0.04)
            found = investpy.search_quotes(text=q, products=["stocks"], countries=[meta["country"]], n_results=8)
            if found is None:
                continue
            if not isinstance(found, list):
                found = [found]
            obj = choose_best_search_result(meta, found)
            if obj is not None:
                return obj, None
        except Exception as exc:
            last_error = str(exc)
    return None, last_error

def fetch_fundamental_statement(stock: str, country: str, summary_type: str) -> Tuple[pd.DataFrame, Optional[str]]:
    try:
        safe_sleep(0.10, 0.04)
        df = investpy.get_stock_financial_summary(stock=stock, country=country, summary_type=summary_type, period="annual")
        if isinstance(df, pd.DataFrame):
            return df, None
        return pd.DataFrame(), None
    except Exception as exc:
        return pd.DataFrame(), str(exc)

def fetch_one_stock(meta: Dict[str, Any], hist_from: str, hist_to: str, include_technicals: bool = False) -> Dict[str, Any]:
    symbol = meta["symbol"]; country = meta["country"]
    out = {
        "ok": True, "country": country, "symbol": symbol,
        "name": meta.get("name") or symbol, "full_name": meta.get("full_name") or meta.get("name") or symbol,
        "isin": meta.get("isin"), "currency": meta.get("currency"), "exchange": meta.get("exchange"),
        "endpoint_errors": {}, "fetched_at": now_utc_str(),
        "overview_change_pct": meta.get("overview_change_pct"), "overview_turnover": meta.get("overview_turnover"),
        "overview_last": meta.get("overview_last"), "thematic_seed_score": meta.get("thematic_seed_score"),
        "theme_ai_hits": meta.get("theme_ai_hits", 0), "theme_robotics_hits": meta.get("theme_robotics_hits", 0),
        "theme_energy_hits": meta.get("theme_energy_hits", 0), "theme_blockchain_hits": meta.get("theme_blockchain_hits", 0),
        "theme_multiomics_hits": meta.get("theme_multiomics_hits", 0), "theme_space_hits": meta.get("theme_space_hits", 0),
    }
    info = {}
    hist = pd.DataFrame(); recent = pd.DataFrame(); inc = pd.DataFrame(); bs = pd.DataFrame(); cf_ = pd.DataFrame(); technical = pd.DataFrame()

    try:
        safe_sleep(0.10, 0.04)
        info = info_to_dict(investpy.get_stock_information(stock=symbol, country=country, as_json=True))
    except Exception as exc:
        out["endpoint_errors"]["direct_info"] = str(exc)

    try:
        safe_sleep(0.10, 0.04)
        hist = investpy.get_stock_historical_data(stock=symbol, country=country, from_date=hist_from, to_date=hist_to, order="ascending", interval="Daily")
    except Exception as exc:
        out["endpoint_errors"]["direct_hist"] = str(exc)

    try:
        safe_sleep(0.10, 0.04)
        recent = investpy.get_stock_recent_data(stock=symbol, country=country, order="ascending", interval="Daily")
    except Exception as exc:
        out["endpoint_errors"]["direct_recent"] = str(exc)

    inc, err = fetch_fundamental_statement(symbol, country, "income_statement")
    if err: out["endpoint_errors"]["income_statement"] = err
    bs, err = fetch_fundamental_statement(symbol, country, "balance_sheet")
    if err: out["endpoint_errors"]["balance_sheet"] = err
    cf_, err = fetch_fundamental_statement(symbol, country, "cash_flow_statement")
    if err: out["endpoint_errors"]["cash_flow_statement"] = err

    need_search = (not info) or hist.empty or (inc.empty and bs.empty and cf_.empty) or include_technicals
    if need_search:
        obj, search_err = search_investpy_object(meta)
        if search_err:
            out["endpoint_errors"]["search_quotes"] = search_err
        if obj is not None:
            out["search_used"] = True
            out["search_symbol"] = getattr(obj, "symbol", None)
            out["search_name"] = getattr(obj, "name", None)
            out["search_exchange"] = getattr(obj, "exchange", None)
            if not out.get("exchange"):
                out["exchange"] = getattr(obj, "exchange", None)
            try:
                safe_sleep(0.10, 0.04)
                info = {**info_to_dict(obj.retrieve_information()), **info}
            except Exception as exc:
                out["endpoint_errors"]["search_info"] = str(exc)
            if hist.empty:
                try:
                    safe_sleep(0.10, 0.04)
                    hist = obj.retrieve_historical_data(from_date=hist_from, to_date=hist_to)
                except Exception as exc:
                    out["endpoint_errors"]["search_hist"] = str(exc)
            if recent.empty:
                try:
                    safe_sleep(0.10, 0.04)
                    recent = obj.retrieve_recent_data()
                except Exception as exc:
                    out["endpoint_errors"]["search_recent"] = str(exc)
            if include_technicals:
                try:
                    safe_sleep(0.10, 0.04)
                    technical = obj.retrieve_technical_indicators(interval="daily")
                except Exception as exc:
                    out["endpoint_errors"]["technical"] = str(exc)

    has_any = bool(info) or not hist.empty or not recent.empty or not inc.empty or not bs.empty or not cf_.empty or not technical.empty or out.get("overview_change_pct") is not None or out.get("overview_turnover") is not None
    out["ok"] = has_any
    if not has_any:
        out["error"] = "no_data_from_sources"
    out["info"] = info
    out["hist"] = df_to_json_or_none(hist)
    out["recent"] = df_to_json_or_none(recent)
    out["inc"] = df_to_json_or_none(inc)
    out["bs"] = df_to_json_or_none(bs)
    out["cf"] = df_to_json_or_none(cf_)
    out["technical"] = df_to_json_or_none(technical)
    return out

def technical_score_from_df(df: pd.DataFrame) -> Tuple[Optional[float], Dict[str, Any]]:
    if df is None or df.empty:
        return None, {"buy_votes": None, "sell_votes": None, "neutral_votes": None, "rsi": None}
    x = df.copy()
    x.columns = [str(c).strip() for c in x.columns]
    col_action = next((c for c in x.columns if normalize_text(c) in {"signal", "action"}), None)
    col_value = next((c for c in x.columns if normalize_text(c) in {"value", "values"}), None)
    col_name = next((c for c in x.columns if normalize_text(c) in {"technical indicator", "indicator", "name"}), None)
    buy = sell = neutral = 0
    rsi = None
    if col_action:
        for _, row in x.iterrows():
            a = normalize_text(row.get(col_action))
            if "buy" in a:
                buy += 1
            elif "sell" in a:
                sell += 1
            else:
                neutral += 1
            if col_name and normalize_text(row.get(col_name)) == "relative strength index":
                rsi = numeric(row.get(col_value))
    total = buy + sell + neutral
    if total <= 0:
        return None, {"buy_votes": buy, "sell_votes": sell, "neutral_votes": neutral, "rsi": rsi}
    base = 50.0 + (buy - sell) / total * 40.0
    if rsi is not None:
        base = 0.8 * base + 0.2 * (score_sweet_spot(rsi, 45, 62, 20, 80) or 50.0)
    return clamp(base, 0.0, 100.0), {"buy_votes": buy, "sell_votes": sell, "neutral_votes": neutral, "rsi": rsi}

def parse_recent_close(df: pd.DataFrame) -> Optional[float]:
    if df is None or df.empty or "Close" not in df.columns:
        return None
    try:
        return numeric(pd.to_numeric(df["Close"], errors="coerce").dropna().iloc[-1])
    except Exception:
        return None

def compute_stock_metrics(meta: Dict[str, Any], payload: Dict[str, Any], macro_payload: Dict[str, Optional[float]], macro_score_total: float, macro_subscores: Dict[str, float]) -> Dict[str, Any]:
    info = payload.get("info") or {}
    inc = json_to_df(payload.get("inc")); bs = json_to_df(payload.get("bs")); cf_ = json_to_df(payload.get("cf"))
    hist = json_to_df(payload.get("hist")); recent = json_to_df(payload.get("recent")); technical = json_to_df(payload.get("technical"))

    latest_inc = latest_row(inc); latest_bs = latest_row(bs); latest_cf = latest_row(cf_)
    market_cap = numeric(info.get("Market Cap"))
    pe_ratio = numeric(info.get("P/E Ratio")) or numeric(info.get("Price to Earnings Ratio"))
    pb_ratio = numeric(info.get("Price to Book")) or numeric(info.get("P/B Ratio"))
    beta = numeric(info.get("Beta"))
    eps = numeric(info.get("EPS"))
    dy = info.get("Dividend (Yield)") or info.get("Dividend Yield")
    dividend_yield = None
    if dy:
        m = re.search(r"\(([-+0-9.,]+%)\)", str(dy))
        dividend_yield = parse_percent_string(m.group(1)) if m else parse_percent_string(dy)

    revenue = net_income = gross_profit = operating_income = total_assets = total_equity = total_debt = op_cf = capex = fcf = None
    if latest_inc is not None:
        revenue = first_existing(latest_inc, ["Total Revenue","Revenue","Revenue From Operations","Sales/Revenue"])
        gross_profit = first_existing(latest_inc, ["Gross Profit"])
        net_income = first_existing(latest_inc, ["Net Income","Net Income Common","Net Profit","Profit After Tax"])
        operating_income = first_existing(latest_inc, ["Operating Income","Operating Profit","EBIT"])
    if latest_bs is not None:
        total_assets = first_existing(latest_bs, ["Total Assets"])
        total_equity = first_existing(latest_bs, ["Total Equity","Total Shareholders Equity","Total Stockholders' Equity","Total Common Equity"])
        total_debt = first_existing(latest_bs, ["Total Debt","Long Term Debt","Borrowings","Net Debt"])
        if total_debt is None:
            ltd = first_existing(latest_bs, ["Long Term Debt"])
            std = first_existing(latest_bs, ["Short Term Debt","Current Portion of Long-Term Debt"])
            if ltd is not None or std is not None:
                total_debt = (ltd or 0.0) + (std or 0.0)
    if latest_cf is not None:
        op_cf = first_existing(latest_cf, ["Cash From Operating Activities","Operating Cash Flow","Net Cash Provided by Operating Activities"])
        capex = first_existing(latest_cf, ["Capital Expenditures","Capex","Purchase Of Property Plant & Equipment"])
        if capex is not None and capex > 0:
            capex = -capex
        if op_cf is not None:
            fcf = op_cf + (capex or 0.0)

    roe = net_income / total_equity if net_income is not None and total_equity not in (None,0) else None
    roa = net_income / total_assets if net_income is not None and total_assets not in (None,0) else None
    debt_equity = total_debt / total_equity if total_debt is not None and total_equity not in (None,0) else None
    debt_assets = total_debt / total_assets if total_debt is not None and total_assets not in (None,0) else None
    gross_margin = gross_profit / revenue if gross_profit is not None and revenue not in (None,0) else None
    operating_margin = operating_income / revenue if operating_income is not None and revenue not in (None,0) else None
    fcf_margin = fcf / revenue if fcf is not None and revenue not in (None,0) else None
    fcf_yield = fcf / market_cap if fcf is not None and market_cap not in (None,0) else None
    price_to_sales = market_cap / revenue if market_cap not in (None,0) and revenue not in (None,0) else None

    rev_series = extract_statement_series(inc, ["Total Revenue","Revenue","Revenue From Operations","Sales/Revenue"])
    ni_series = extract_statement_series(inc, ["Net Income","Net Income Common","Net Profit","Profit After Tax"])
    rev_first = next((x for x in rev_series if x not in (None,0)), None)
    rev_last = next((x for x in reversed(rev_series) if x is not None), None)
    rev_cagr = geometric_growth(rev_first, rev_last, max(1.0, annual_years_span(inc)))
    ni_positive = [x for x in ni_series if x is not None]
    earnings_consistency = (sum(1 for x in ni_positive if x > 0) / len(ni_positive)) if ni_positive else None

    price_stats = compute_price_stats(hist if not hist.empty else recent)
    price_change_1y = price_stats["price_change_1y"]
    price_change_6m = price_stats["price_change_6m"]
    price_cagr_3y = price_stats["price_cagr_3y"]
    vol_1y = price_stats["vol_1y"]
    max_drawdown_3y = price_stats["max_drawdown_3y"]

    # fallback from overview if history absent
    if price_change_1y is None:
        price_change_1y = payload.get("overview_change_pct")
    tech_score, tech_meta = technical_score_from_df(technical)

    hits = {
        "ai": int(payload.get("theme_ai_hits") or 0),
        "robotics": int(payload.get("theme_robotics_hits") or 0),
        "energy_storage": int(payload.get("theme_energy_hits") or 0),
        "blockchain_fintech": int(payload.get("theme_blockchain_hits") or 0),
        "multiomics_biotech": int(payload.get("theme_multiomics_hits") or 0),
        "space_defense": int(payload.get("theme_space_hits") or 0),
    }
    theme_text = " ".join([str(meta.get("full_name") or meta.get("name") or meta.get("symbol")), str(meta.get("symbol")), str(info.get("Sector") or ""), str(info.get("Industry") or ""), str(meta.get("exchange") or "")])
    extra_hits = theme_hits(theme_text)
    for k, v in extra_hits.items():
        hits[k] = max(hits.get(k,0), v)
    thematic_score = max(payload.get("thematic_seed_score") or 0.0, theme_score_from_hits(hits))

    turnover = numeric(payload.get("overview_turnover"))
    turnover_score = clamp(math.log10(1.0 + max(turnover or 0.0, 0.0)) / 9.0 * 100.0, 0.0, 100.0) if turnover is not None else None
    market_cap_score = score_sweet_spot(market_cap, 5e8, 2e10, 1e8, 2e11) if market_cap is not None else None
    emerging_potential = score_sweet_spot(market_cap, 5e8, 1.5e10, 1e8, 5e10) if market_cap is not None else None

    buffett_items = [
        (0.18, 100.0 * earnings_consistency if earnings_consistency is not None else None),
        (0.16, score_linear(roe, 0.05, 0.25)),
        (0.10, score_linear(roa, 0.03, 0.14)),
        (0.14, score_linear(debt_equity, 0.0, 2.0, invert=True)),
        (0.08, score_linear(debt_assets, 0.0, 0.65, invert=True)),
        (0.10, score_linear(fcf_yield, 0.00, 0.08)),
        (0.07, score_linear(gross_margin, 0.15, 0.70)),
        (0.07, score_linear(operating_margin, 0.05, 0.30)),
        (0.05, score_linear(pe_ratio, 8, 40, invert=True)),
        (0.05, score_linear(pb_ratio, 0.8, 8.0, invert=True)),
    ]
    buffett_score, buff_cov = weighted_score(buffett_items, neutral=50.0)

    overheat_penalty = 0.0
    if price_change_1y is not None and price_change_1y > 1.4:
        overheat_penalty = min(25.0, (price_change_1y - 1.4) * 20.0)

    ark_items = [
        (0.18, score_linear(rev_cagr, 0.03, 0.35)),
        (0.10, score_linear(price_cagr_3y, 0.02, 0.45)),
        (0.10, score_sweet_spot(price_change_1y, 0.08, 0.80, -0.35, 1.60)),
        (0.08, score_sweet_spot(price_change_6m, 0.02, 0.45, -0.30, 0.90)),
        (0.14, market_cap_score),
        (0.16, thematic_score),
        (0.08, score_linear(dividend_yield, 0.00, 0.05, invert=True) if dividend_yield is not None else 60.0),
        (0.06, score_sweet_spot(beta, 0.9, 1.8, 0.4, 3.0)),
        (0.05, tech_score),
        (0.05, turnover_score),
    ]
    ark_score, ark_cov = weighted_score(ark_items, neutral=55.0)
    ark_score = clamp(ark_score - overheat_penalty, 0.0, 100.0)

    dalio_items = [
        (0.25, macro_subscores.get("macro_debt_score")),
        (0.20, macro_subscores.get("macro_politics_score")),
        (0.15, macro_subscores.get("macro_geopolitics_score")),
        (0.15, macro_subscores.get("macro_nature_score")),
        (0.25, macro_subscores.get("macro_innovation_score")),
    ]
    dalio_score, dalio_cov = weighted_score(dalio_items, neutral=macro_score_total)

    moat_items = [
        (0.35, score_linear(gross_margin, 0.20, 0.75)),
        (0.25, score_linear(operating_margin, 0.08, 0.35)),
        (0.20, score_linear(roe, 0.08, 0.28)),
        (0.20, score_linear(max_drawdown_3y, -0.70, -0.15, invert=True)),
    ]
    moat_proxy_score, _ = weighted_score(moat_items, neutral=50.0)

    future_items = [
        (0.30, emerging_potential),
        (0.25, score_linear(rev_cagr, 0.05, 0.40)),
        (0.25, thematic_score),
        (0.20, tech_score),
    ]
    future_optional_score, _ = weighted_score(future_items, neutral=50.0)

    quality_multiplier = 1.0
    if market_cap is not None and market_cap <= 2e10 and rev_cagr is not None:
        if rev_cagr >= 0.15:
            quality_multiplier = 1.12
        elif rev_cagr >= 0.10:
            quality_multiplier = 1.07

    base_total = 0.42 * buffett_score + 0.33 * ark_score + 0.25 * dalio_score
    total_score = base_total * quality_multiplier + 0.05 * (moat_proxy_score - 50.0)
    total_score = clamp(total_score, 0.0, 100.0)
    overall_cov = 0.40 * buff_cov + 0.35 * ark_cov + 0.25 * dalio_cov

    confidence = "LOW"
    if total_score >= 82 and overall_cov >= 0.55:
        confidence = "VERY HIGH"
    elif total_score >= 72 and overall_cov >= 0.45:
        confidence = "HIGH"
    elif total_score >= 60:
        confidence = "MEDIUM"

    last_price = parse_recent_close(recent)
    if last_price is None:
        last_price = numeric(payload.get("overview_last"))
    return {
        "country": meta["country"], "symbol": meta["symbol"], "name": meta.get("full_name") or meta.get("name") or meta["symbol"],
        "isin": meta.get("isin"), "currency": meta.get("currency"), "exchange": meta.get("exchange"),
        "last_price": last_price, "market_cap": market_cap, "pe_ratio": pe_ratio, "pb_ratio": pb_ratio, "eps": eps,
        "revenue": revenue, "net_income": net_income, "roe": roe, "roa": roa, "debt_to_equity": debt_equity, "debt_to_assets": debt_assets,
        "gross_margin": gross_margin, "operating_margin": operating_margin, "fcf": fcf, "fcf_margin": fcf_margin, "fcf_yield": fcf_yield,
        "price_to_sales": price_to_sales, "beta": beta, "dividend_yield": dividend_yield, "rev_cagr_annual": rev_cagr,
        "earnings_consistency": earnings_consistency, "price_change_6m": price_change_6m, "price_change_1y": price_change_1y, "price_cagr_3y": price_cagr_3y,
        "vol_1y": vol_1y, "max_drawdown_3y": max_drawdown_3y,
        "buffett_score": round(buffett_score,2), "ark_score": round(ark_score,2), "dalio_score": round(dalio_score,2), "macro_score": round(macro_score_total,2),
        "moat_proxy_score": round(moat_proxy_score,2), "future_optional_score": round(future_optional_score,2), "coverage_score": round(overall_cov*100.0,2),
        "confidence": confidence, "total_score": round(total_score,2), "quality_multiplier": round(quality_multiplier,3),
        "technical_score": round(tech_score,2) if tech_score is not None else None,
        "technical_buy_votes": tech_meta.get("buy_votes"), "technical_sell_votes": tech_meta.get("sell_votes"),
        "technical_neutral_votes": tech_meta.get("neutral_votes"), "technical_rsi": tech_meta.get("rsi"),
        "overview_turnover": turnover, "overview_change_pct": payload.get("overview_change_pct"), "thematic_seed_score": payload.get("thematic_seed_score"),
        **{k: v for k, v in macro_payload.items()}, **{k: round(v,2) for k,v in macro_subscores.items()},
        "theme_ai_hits": hits.get("ai",0), "theme_robotics_hits": hits.get("robotics",0), "theme_energy_hits": hits.get("energy_storage",0),
        "theme_blockchain_hits": hits.get("blockchain_fintech",0), "theme_multiomics_hits": hits.get("multiomics_biotech",0), "theme_space_hits": hits.get("space_defense",0),
        "screen_ts_utc": now_utc_str(),
    }

def compute_weights(df: pd.DataFrame, portfolio_size: int) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    x = df.head(portfolio_size).copy()
    raw = []
    for _, row in x.iterrows():
        score = float(row.get("total_score") or 0.0)
        cov = max(0.35, float(row.get("coverage_score") or 50.0)/100.0)
        vol = row.get("vol_1y")
        inv_vol = 1.0 / max(0.15, float(vol)) if pd.notna(vol) and vol not in (None, 0) else 1.0
        conf = {"VERY HIGH":1.18,"HIGH":1.08,"MEDIUM":1.0}.get(str(row.get("confidence")), 0.9)
        weight = max(0.1, score - 45.0) * cov * inv_vol * conf
        raw.append(weight)
    total = sum(raw)
    x["portfolio_weight"] = [w / total for w in raw]
    return x

def render_html(df: pd.DataFrame, weights_df: pd.DataFrame, out_path: Path, meta: Dict[str, Any]) -> None:
    def fmt_pct(x):
        return "" if pd.isna(x) else f"{float(x)*100:.1f}%"
    def fmt_num(x):
        return "" if pd.isna(x) else f"{float(x):,.0f}"
    rows = []
    for i, row in df.head(min(100, len(df))).iterrows():
        rows.append(f"""
        <tr>
          <td>{i+1}</td><td>{row.get('symbol','')}</td><td>{row.get('name','')}</td><td>{row.get('country','')}</td>
          <td>{row.get('confidence','')}</td><td>{row.get('total_score','')}</td><td>{row.get('coverage_score','')}</td>
          <td>{row.get('buffett_score','')}</td><td>{row.get('ark_score','')}</td><td>{row.get('dalio_score','')}</td>
          <td>{fmt_num(row.get('market_cap'))}</td><td>{'' if pd.isna(row.get('pe_ratio')) else round(float(row.get('pe_ratio')),2)}</td>
          <td>{fmt_pct(row.get('roe'))}</td><td>{fmt_pct(row.get('debt_to_equity'))}</td><td>{fmt_pct(row.get('rev_cagr_annual'))}</td>
          <td>{fmt_pct(row.get('price_change_1y'))}</td><td>{fmt_pct(row.get('fcf_yield'))}</td>
        </tr>""")
    wrows = []
    for _, row in weights_df.iterrows():
        wrows.append(f"<tr><td>{row.get('symbol','')}</td><td>{row.get('name','')}</td><td>{row.get('country','')}</td><td>{row.get('total_score','')}</td><td>{float(row.get('portfolio_weight'))*100:.2f}%</td></tr>")
    html = f"""<!doctype html><html lang="it"><head><meta charset="utf-8"><title>{APP_NAME}</title>
<style>
body {{ font-family: Arial, sans-serif; margin: 24px; background:#0f1115; color:#e8eaed; }}
h1,h2,h3 {{ margin: 0 0 10px; }} p,li {{ line-height:1.45; }}
.small {{ color:#b8c1cc; font-size:13px; }} .card {{ background:#171a21; border:1px solid #2a2f3a; border-radius:16px; padding:18px; margin:18px 0; }}
.metrics {{ display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:14px; }}
.metric {{ background:#171a21; border:1px solid #2a2f3a; border-radius:16px; padding:16px; }}
table {{ width:100%; border-collapse:collapse; font-size:13px; }}
th,td {{ border-bottom:1px solid #2a2f3a; padding:8px 10px; text-align:left; vertical-align:top; }}
th {{ position:sticky; top:0; background:#171a21; }}
</style></head><body>
<h1>{APP_NAME}</h1>
<p class="small">Creato: {meta.get('created_at','')} | Countries: {meta.get('countries','')} | Candidati broad: {meta.get('broad_candidates','')} | Shortlist deep: {meta.get('shortlist_candidates','')} | Analizzati finali: {meta.get('rows','')}</p>
<div class="metrics">
<div class="metric"><div class="small">Top score medio</div><div>{meta.get('avg_top_score','')}</div></div>
<div class="metric"><div class="small">Coverage media Top</div><div>{meta.get('avg_top_coverage','')}</div></div>
<div class="metric"><div class="small">Paesi coperti</div><div>{meta.get('country_count','')}</div></div>
<div class="metric"><div class="small">Portfolio size</div><div>{meta.get('portfolio_size','')}</div></div>
</div>
<div class="card"><h2>Motore</h2><ul>
<li><b>Buffett/Munger</b>: utili coerenti, ROE/ROA, leva, FCF, margini, valutazione.</li>
<li><b>Wood / ARK</b>: crescita, thematics, emergenza dimensionale, momentum non euforico, technical.</li>
<li><b>Dalio</b>: overlay macro-paese su debito, politica, geopolitica, innovazione.</li>
</ul></div>
<div class="card"><h2>Portafoglio suggerito</h2><table><thead><tr><th>Symbol</th><th>Nome</th><th>Paese</th><th>Score</th><th>Peso</th></tr></thead><tbody>{''.join(wrows)}</tbody></table></div>
<div class="card"><h2>Top ranking</h2><table><thead><tr>
<th>#</th><th>Symbol</th><th>Nome</th><th>Paese</th><th>Confidence</th><th>Total</th><th>Coverage</th><th>Buffett</th><th>Growth</th><th>Dalio</th><th>Market Cap</th><th>P/E</th><th>ROE</th><th>D/E</th><th>Rev CAGR</th><th>1Y</th><th>FCF Yield</th>
</tr></thead><tbody>{''.join(rows)}</tbody></table></div>
</body></html>"""
    out_path.write_text(html, encoding="utf-8")

def main() -> None:
    p = argparse.ArgumentParser(description=f"{APP_NAME} — screener Buffett + Wood + Dalio con pipeline rapida a 2 stadi")
    p.add_argument("--countries", type=str, default=",".join(MAJOR_COUNTRIES_DEFAULT))
    p.add_argument("--all-countries", action="store_true")
    p.add_argument("--max-per-country", type=int, default=30, help="Candidati broad per paese. Più basso = molto più veloce.")
    p.add_argument("--shortlist-multiplier", type=int, default=4, help="Shortlist deep = top * moltiplicatore.")
    p.add_argument("--workers", type=int, default=6)
    p.add_argument("--top", type=int, default=50)
    p.add_argument("--portfolio-size", type=int, default=12)
    p.add_argument("--emerging-only", action="store_true")
    p.add_argument("--resume-cache", type=str, default="btt_capital_bomb_final_cache.json")
    p.add_argument("--output-prefix", type=str, default="btt_capital_bomb_final")
    p.add_argument("--technical-refine", action="store_true")
    args = p.parse_args()

    print("[1/8] Carico universo investpy...")
    all_stocks = investpy.get_stocks()
    all_stocks.columns = [str(c).strip().lower() for c in all_stocks.columns]
    if args.all_countries or normalize_country(args.countries) == "all":
        countries = sorted({normalize_country(c) for c in all_stocks["country"].dropna().astype(str).tolist()})
    else:
        countries = [normalize_country(x) for x in args.countries.split(",") if x.strip()]
    norm_to_real = {}
    for c in all_stocks["country"].dropna().astype(str).tolist():
        norm_to_real.setdefault(normalize_country(c), c)
    countries_real = [norm_to_real.get(c, c.title()) for c in countries]
    print(f"[2/8] Paesi selezionati: {', '.join(countries_real[:20])}" + (" ..." if len(countries_real) > 20 else ""))

    print("[3/8] Costruisco il broad universe veloce...")
    universe = load_universe(countries_real, max_per_country=args.max_per_country, emerging_only=args.emerging_only)
    if not universe:
        print("Nessun titolo selezionato.")
        sys.exit(1)
    print(f"Totale broad candidates: {len(universe)}")

    # Macro
    print("[4/8] Recupero macro-paese (World Bank)...")
    wb_map = build_wb_country_map()
    macro_by_country = {}
    for c in sorted({u["country"] for u in universe}):
        iso2 = wb_map.get(normalize_country(c))
        payload = {k: None for k in list(WB_INDICATORS.keys()) + ["natural_risk"]}
        score_total = 50.0
        subs = {"macro_debt_score":50.0,"macro_politics_score":50.0,"macro_geopolitics_score":50.0,"macro_nature_score":50.0,"macro_innovation_score":50.0}
        if iso2:
            try:
                payload = fetch_country_macro(iso2)
                score_total, subs = country_macro_score(payload)
            except Exception:
                pass
        macro_by_country[c] = {"payload": payload, "score_total": score_total, "subscores": subs}

    # Stage 1 ranking from broad metadata + macro
    print("[5/8] Ranking broad e costruzione shortlist...")
    broad_rows = []
    for u in universe:
        macro = macro_by_country.get(u["country"], {})
        dalio = macro.get("score_total", 50.0)
        rank = 0.55 * float(u.get("overview_rank") or 50.0) + 0.25 * float(u.get("thematic_seed_score") or 45.0) + 0.20 * float(dalio or 50.0)
        broad_rows.append({**u, "broad_total_rank": rank})
    broad_df = pd.DataFrame(broad_rows).sort_values(["broad_total_rank","overview_turnover"], ascending=[False,False]).reset_index(drop=True)
    shortlist_n = min(len(broad_df), max(args.top * max(1, args.shortlist_multiplier), 60))
    shortlist_df = broad_df.head(shortlist_n).copy()
    print(f"Shortlist deep: {len(shortlist_df)}")

    cache = Cache(Path(args.resume_cache))
    today = dt.date.today()
    hist_from = (today - dt.timedelta(days=365 * 4 + 30)).strftime("%d/%m/%Y")
    hist_to = today.strftime("%d/%m/%Y")

    print("[6/8] Fetch deep solo sulla shortlist...")
    pending = [(cache_key("deep", row["country"], row["symbol"]), row.to_dict()) for _, row in shortlist_df.iterrows()]
    results: List[Dict[str, Any]] = []
    done = 0
    def worker(entry):
        key, meta = entry
        hit = cache.get(key)
        if hit and hit.get("ok"):
            return key, hit
        return key, fetch_one_stock(meta, hist_from, hist_to, include_technicals=False)
    with cf.ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
        futs = [ex.submit(worker, item) for item in pending]
        for fut in cf.as_completed(futs):
            key, payload = fut.result()
            cache.set(key, payload)
            results.append(payload)
            done += 1
            if done % 10 == 0 or done == len(pending):
                print(f"  - completati {done}/{len(pending)}")
                cache.save()
    cache.save()

    print("[7/8] Scoring finale...")
    scored = []
    failed = []
    for payload in results:
        meta = {
            "country": payload.get("country"), "symbol": payload.get("symbol"), "name": payload.get("name"),
            "full_name": payload.get("full_name") or payload.get("name"), "isin": payload.get("isin"),
            "currency": payload.get("currency"), "exchange": payload.get("exchange") or payload.get("search_exchange"),
        }
        macro = macro_by_country.get(meta["country"], {"payload":{},"score_total":50.0,"subscores":{}})
        try:
            row = compute_stock_metrics(meta, payload, macro["payload"], macro["score_total"], macro["subscores"])
            scored.append(row)
        except Exception as exc:
            failed.append({
                "country": payload.get("country"), "symbol": payload.get("symbol"), "name": payload.get("full_name") or payload.get("name"),
                "error": f"compute_error: {exc}", "endpoint_errors": json.dumps(payload.get("endpoint_errors") or {}, ensure_ascii=False),
                "fetched_at": payload.get("fetched_at"),
            })
    if not scored:
        failed_path = Path(f"{args.output_prefix}_failed.csv")
        pd.DataFrame(failed).to_csv(failed_path, index=False, encoding="utf-8-sig")
        print("Nessun titolo analizzabile anche sulla shortlist.")
        print(f"Creato: {failed_path}")
        sys.exit(2)

    df = pd.DataFrame(scored).sort_values(
        ["total_score","coverage_score","buffett_score","ark_score","dalio_score","market_cap"],
        ascending=[False,False,False,False,False,True]
    ).reset_index(drop=True)

    # Optional technical refine only top slice
    if args.technical_refine:
        refine_n = min(len(df), max(args.top * 2, 30))
        refine_syms = set(df.head(refine_n)["symbol"].astype(str).tolist())
        print(f"[8/8] Technical refine top {refine_n}...")
        refined_results = []
        done2 = 0
        shortlist_payloads = [r for r in results if str(r.get("symbol")) in refine_syms]
        def refine_worker(payload):
            key = cache_key("tech", payload["country"], payload["symbol"])
            hit = cache.get(key)
            if hit and hit.get("technical"):
                payload["technical"] = hit.get("technical")
                return payload
            pp = dict(payload)
            obj, _ = search_investpy_object({"country": payload.get("country"), "symbol": payload.get("search_symbol") or payload.get("symbol"), "name": payload.get("name"), "full_name": payload.get("full_name")})
            if obj is not None:
                try:
                    safe_sleep(0.10, 0.04)
                    tech = obj.retrieve_technical_indicators(interval="daily")
                    if isinstance(tech, pd.DataFrame) and not tech.empty:
                        pp["technical"] = df_to_json_or_none(tech)
                except Exception:
                    pass
            cache.set(key, {"technical": pp.get("technical")})
            return pp
        with cf.ThreadPoolExecutor(max_workers=max(1, min(args.workers, 4))) as ex:
            futs = [ex.submit(refine_worker, p) for p in shortlist_payloads]
            for fut in cf.as_completed(futs):
                refined_results.append(fut.result())
                done2 += 1
                if done2 % 10 == 0 or done2 == len(shortlist_payloads):
                    print(f"  - technical completati {done2}/{len(shortlist_payloads)}")
                    cache.save()
        cache.save()
        refined_map = {str(p["symbol"]): p for p in refined_results}
        rescored = []
        for _, row0 in df.iterrows():
            sym = str(row0["symbol"])
            payload = refined_map.get(sym) or next((p for p in results if str(p.get("symbol")) == sym), None)
            if not payload:
                rescored.append(row0.to_dict()); continue
            meta = {
                "country": payload.get("country"), "symbol": payload.get("symbol"), "name": payload.get("name"),
                "full_name": payload.get("full_name") or payload.get("name"), "isin": payload.get("isin"),
                "currency": payload.get("currency"), "exchange": payload.get("exchange") or payload.get("search_exchange"),
            }
            macro = macro_by_country.get(meta["country"], {"payload":{},"score_total":50.0,"subscores":{}})
            try:
                rescored.append(compute_stock_metrics(meta, payload, macro["payload"], macro["score_total"], macro["subscores"]))
            except Exception:
                rescored.append(row0.to_dict())
        df = pd.DataFrame(rescored).sort_values(
            ["total_score","coverage_score","buffett_score","ark_score","dalio_score","market_cap"],
            ascending=[False,False,False,False,False,True]
        ).reset_index(drop=True)

    weights_df = compute_weights(df, args.portfolio_size)
    top_df = df.head(args.top).copy()

    csv_top = Path(f"{args.output_prefix}_top.csv")
    csv_weights = Path(f"{args.output_prefix}_weights.csv")
    csv_failed = Path(f"{args.output_prefix}_failed.csv")
    html_path = Path(f"{args.output_prefix}_report.html")
    top_df.to_csv(csv_top, index=False, encoding="utf-8-sig")
    weights_df.to_csv(csv_weights, index=False, encoding="utf-8-sig")
    pd.DataFrame(failed).to_csv(csv_failed, index=False, encoding="utf-8-sig")
    render_html(df=top_df, weights_df=weights_df, out_path=html_path, meta={
        "created_at": now_utc_str(),
        "countries": ", ".join(countries_real[:40]) + (" ..." if len(countries_real) > 40 else ""),
        "broad_candidates": len(universe), "shortlist_candidates": len(shortlist_df), "rows": len(df),
        "avg_top_score": f"{float(top_df['total_score'].mean()):.1f}" if not top_df.empty else "",
        "avg_top_coverage": f"{float(top_df['coverage_score'].mean()):.1f}%" if not top_df.empty else "",
        "country_count": len(sorted(set(df['country'].dropna().astype(str).tolist()))) if not df.empty else 0,
        "portfolio_size": min(args.portfolio_size, len(weights_df)),
    })

    print("Completato.")
    print(f"CSV top:     {csv_top}")
    print(f"CSV weights: {csv_weights}")
    print(f"HTML report: {html_path}")
    print(f"Failed:      {csv_failed}")

if __name__ == "__main__":
    main()
