import json
import threading
import time
import secrets
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session

from app.core.settings import get_settings
from app.db import SessionLocal, get_db
from app.dependencies import get_current_admin, get_current_user
from app.models import AppKV, BttJob, User
from app.schemas import (
    AcceptTermsIn,
    AdminJsonUpdate,
    AdminTextUpdate,
    ExternalMicrocapHeartbeatIn,
    ForgotPasswordIn,
    LoginIn,
    MicrocapControlIn,
    RegisterIn,
    ResetPasswordIn,
    StripeCheckoutIn,
)
from app.security import create_access_token, get_password_hash, verify_password
from app.services.access import ensure_trial, has_access
from app.services.admin_config import (
    get_btt_preset,
    get_microcap_config_text,
    get_microcap_env,
    set_btt_preset,
    set_microcap_config_text,
    set_microcap_env,
)
from app.services.billing import create_checkout_session, handle_checkout_completed, stripe_ready, stripe_status
from app.services.bootstrap import init_app
from app.services.btt_runner import create_btt_job
from app.services.engine_manager import microcap_manager, user_microcap_manager
from app.services.mailer import send_email
from app.services.microcap_reader import read_dashboard

settings = get_settings()
app = FastAPI(title=settings.APP_NAME)


def _tail(value: str | None, size: int) -> str:
    return (value or "")[-size:]


def _json_value(row: AppKV | None, default):
    if not row or not row.value:
        return default
    try:
        return json.loads(row.value)
    except Exception:
        return default


def _upsert_kv(db: Session, key: str, value) -> None:
    row = db.scalar(select(AppKV).where(AppKV.key == key))
    payload = json.dumps(value, ensure_ascii=False)
    if not row:
        db.add(AppKV(key=key, value=payload))
    else:
        row.value = payload
    db.commit()


def _user_live_unlocked(user: User) -> bool:
    return bool(
        getattr(user, "email_verified", False)
        and getattr(user, "subscription_status", "") == "active"
        and settings.MICROCAP_LIVE_ENABLED
    )


def _ensure_live_access(user: User) -> None:
    if not getattr(user, "email_verified", False):
        raise HTTPException(status_code=403, detail="Verifica prima la tua email")

    if getattr(user, "subscription_status", "") != "active":
        raise HTTPException(status_code=402, detail="Abbonamento attivo richiesto per la modalità live")

    if not settings.MICROCAP_LIVE_ENABLED:
        raise HTTPException(status_code=403, detail="Live non abilitato dal server")


def _load_external_microcap(db: Session):
    state_row = db.scalar(select(AppKV).where(AppKV.key == "external_microcap_state"))
    dash_row = db.scalar(select(AppKV).where(AppKV.key == "external_microcap_dashboard"))

    state = _json_value(state_row, {})
    dashboard = _json_value(dash_row, {})

    received = float(state.get("received_at_epoch") or 0)
    if not received:
        return None, None

    if (time.time() - received) > settings.EXTERNAL_MICROCAP_TTL_SEC:
        return None, None

    return state, dashboard


def _safe_num(v, default=0.0):
    try:
        return float(v)
    except Exception:
        return default


def _build_crypto_summary(dashboard: dict) -> dict:
    overview = dict((dashboard or {}).get("overview") or {})
    trades = list((dashboard or {}).get("trades") or [])
    equity_curve = list((dashboard or {}).get("equity_curve") or [])

    first_equity = _safe_num(equity_curve[0].get("equity")) if equity_curve else _safe_num(overview.get("cash"))
    last_equity = _safe_num(equity_curve[-1].get("equity")) if equity_curve else _safe_num(overview.get("cash"))

    pnl_money = last_equity - first_equity
    pnl_pct = (pnl_money / first_equity) if first_equity > 0 else 0.0

    closed_ops = [t for t in trades if str(t.get("side", "")).lower() in {"sell", "exit"}]
    wins = 0
    losses = 0
    flat = 0

    for t in closed_ops:
        reason = str(t.get("reason") or "").lower()
        usd = _safe_num(t.get("usd_value"))
        if "sl" in reason or "loss" in reason:
            losses += 1
        elif usd > 0:
            wins += 1
        elif usd < 0:
            losses += 1
        else:
            flat += 1

    chart = []
    for idx, row in enumerate(equity_curve):
        eq = _safe_num(row.get("equity"))
        base = first_equity if first_equity > 0 else 1.0
        chart.append({
            "x": idx + 1,
            "equity": eq,
            "profit_money": eq - first_equity,
            "profit_pct": ((eq - first_equity) / base) * 100.0,
        })

    return {
        "profit_money": round(pnl_money, 2),
        "profit_pct": round(pnl_pct * 100.0, 2),
        "wins": wins,
        "losses": losses,
        "flat": flat,
        "chart": chart,
    }


def _stock_parse_num(raw):
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        try:
            return float(raw)
        except Exception:
            return None

    s = str(raw).strip()
    if not s:
        return None

    s = s.replace("€", "").replace("$", "").replace(" ", "").replace(",", ".")
    pct = False
    if s.endswith("%"):
        pct = True
        s = s[:-1]

    try:
        val = float(s)
    except Exception:
        return None

    if pct:
        return val
    return val


def _stock_normalize_pct(values: list[float]) -> list[float]:
    if not values:
        return values

    abs_max = max(abs(v) for v in values)
    if abs_max <= 3:
        return [v * 100.0 for v in values]

    return values


def _stock_is_forbidden_metric_key(key: str) -> bool:
    k = str(key).lower()
    return (
        "ticker" in k or
        "symbol" in k or
        "name" in k or
        "isin" in k or
        "country" in k or
        "exchange" in k or
        "sector" in k or
        "industry" in k or
        "currency" in k or
        "market_cap" in k or
        "mcap" in k or
        "price" in k or
        "close" in k or
        "open" in k or
        "high" in k or
        "low" in k or
        "volume" in k or
        "shares" in k or
        "qty" in k or
        "weight" in k or
        "rank" in k or
        k == "id"
    )


def _stock_metric_key_score(key: str) -> int:
    k = str(key).lower()

    if (
        "return" in k or
        "perf" in k or
        "performance" in k or
        "upside" in k or
        "gain" in k or
        "profit" in k or
        "yield" in k or
        "cagr" in k or
        "expected" in k
    ):
        return 100

    if (
        "score" in k or
        "alpha" in k or
        "edge" in k or
        "quality" in k or
        "conviction" in k
    ):
        return 50

    return 0


def _choose_stock_metric_column(rows: list[dict]) -> tuple[str | None, list[float]]:
    if not rows:
        return None, []

    keys = []
    seen = set()

    for row in rows:
        if not isinstance(row, dict):
            continue
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                keys.append(key)

    best_key = None
    best_values = []
    best_score = -10**18

    for key in keys:
        if _stock_is_forbidden_metric_key(str(key)):
            continue

        raw_values = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            val = _stock_parse_num(row.get(key))
            if val is not None:
                raw_values.append(val)

        if len(raw_values) < 3:
            continue

        values = _stock_normalize_pct(raw_values)
        min_v = min(values)
        max_v = max(values)
        spread = max_v - min_v

        if spread == 0:
            continue

        if max(abs(v) for v in values) > 1000:
            continue

        score = (_stock_metric_key_score(str(key)) * 10) + spread + len(raw_values)

        if score > best_score:
            best_score = score
            best_key = str(key)
            best_values = values

    return best_key, best_values


def _build_stock_summary(latest: dict | None) -> dict:
    latest = latest or {}
    summary = dict(latest.get("summary") or {})
    top_rows = list(summary.get("top_rows") or [])
    portfolio_rows = list(summary.get("portfolio_rows") or [])

    perf_source = top_rows if top_rows else portfolio_rows
    metric_key, metric_values = _choose_stock_metric_column(perf_source[:50])

    points = []
    for idx, pct in enumerate(metric_values[:25]):
        points.append({
            "x": idx + 1,
            "label": f"Titolo {idx + 1}",
            "profit_pct": round(float(pct), 2),
            "profit_money": 0.0,
        })

    if not points:
        return {
            "profit_money": None,
            "profit_pct": None,
            "wins": 0,
            "losses": 0,
            "chart": [],
            "public_metrics": {
                "metric_key": metric_key,
                "point_count": 0,
                "avg_pct": None,
                "best_pct": None,
                "worst_pct": None,
                "last_pct": None,
                "positives": 0,
                "negatives": 0,
                "chart": [],
            },
        }

    avg_pct = sum(p["profit_pct"] for p in points) / len(points)
    best_pct = max(p["profit_pct"] for p in points)
    worst_pct = min(p["profit_pct"] for p in points)
    last_pct = points[-1]["profit_pct"]
    positives = sum(1 for p in points if p["profit_pct"] > 0)
    negatives = sum(1 for p in points if p["profit_pct"] < 0)

    public_metrics = {
        "metric_key": metric_key,
        "point_count": len(points),
        "avg_pct": round(avg_pct, 2),
        "best_pct": round(best_pct, 2),
        "worst_pct": round(worst_pct, 2),
        "last_pct": round(last_pct, 2),
        "positives": positives,
        "negatives": negatives,
        "chart": points,
    }

    return {
        "profit_money": None,
        "profit_pct": round(avg_pct, 2),
        "wins": positives,
        "losses": negatives,
        "chart": points,
        "public_metrics": public_metrics,
    }


def _build_combined_summary(crypto_summary: dict, stock_summary: dict) -> dict:
    crypto_chart = list(crypto_summary.get("chart") or [])
    stock_chart = list(stock_summary.get("chart") or [])
    n = max(len(crypto_chart), len(stock_chart))

    chart = []
    for i in range(n):
        c = crypto_chart[i] if i < len(crypto_chart) else {}
        s = stock_chart[i] if i < len(stock_chart) else {}
        chart.append({
            "x": i + 1,
            "crypto_profit_money": _safe_num(c.get("profit_money")),
            "crypto_profit_pct": _safe_num(c.get("profit_pct")),
            "stock_profit_money": _safe_num(s.get("profit_money")),
            "stock_profit_pct": _safe_num(s.get("profit_pct")),
            "combined_profit_money": _safe_num(c.get("profit_money")) + _safe_num(s.get("profit_money")),
            "combined_profit_pct": _safe_num(c.get("profit_pct")) + _safe_num(s.get("profit_pct")),
        })

    return {"chart": chart}


app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_origin_regex=settings.CORS_ALLOW_ORIGIN_REGEX or None,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _public_url(path: str) -> str:
    base = (settings.APP_PUBLIC_URL or "").rstrip("/")
    return f"{base}{path}"


def _user_terms_ok(user: User) -> bool:
    return (user.accepted_terms_version or "") == settings.TERMS_VERSION


def _create_one_time_token() -> str:
    return secrets.token_urlsafe(32)


def _safe_autostart_microcap() -> None:
    try:
        microcap_manager.start(mode=settings.MICROCAP_PUBLIC_MODE)
    except Exception:
        pass


@app.on_event("startup")
def startup() -> None:
    db = SessionLocal()
    try:
        init_app(db)
    finally:
        db.close()

    if settings.MICROCAP_AUTO_START:
        threading.Thread(target=_safe_autostart_microcap, daemon=True).start()


@app.get("/")
def root() -> dict:
    return {
        "ok": True,
        "service": "btt-fusion-api",
        "health": "/health",
        "public_site": "/api/public/site",
    }


@app.get("/health")
def health() -> dict:
    return {"ok": True, "app": settings.APP_NAME, "env": settings.APP_ENV}


@app.get("/api/billing/status")
def billing_status() -> dict:
    return stripe_status()


@app.post("/api/auth/register")
def register(payload: RegisterIn, db: Session = Depends(get_db)):
    email = payload.email.lower().strip()
    existing = db.scalar(select(User).where(User.email == email))

    if existing:
        if existing.email_verified:
            raise HTTPException(
                status_code=400,
                detail="Email già verificata e registrata. Fai direttamente il login."
            )

        verify_token = _create_one_time_token()
        existing.password_hash = get_password_hash(payload.password)
        existing.full_name = payload.full_name or existing.full_name
        existing.is_active = True
        existing.email_verified = False
        existing.email_verify_token = verify_token
        db.commit()
        db.refresh(existing)
        user = existing
    else:
        verify_token = _create_one_time_token()

        user = User(
            email=email,
            password_hash=get_password_hash(payload.password),
            full_name=payload.full_name,
            is_active=True,
            email_verified=False,
            email_verify_token=verify_token,
        )
        db.add(user)
        db.commit()
        db.refresh(user)

    verify_link = _public_url(f"/verify-email?token={user.email_verify_token}")

    try:
        send_email(
            user.email,
            "Verifica il tuo account BTTcapital",
            f"""
            <h2>Benvenuto in BTTcapital</h2>
            <p>Per attivare il tuo account, verifica la tua email:</p>
            <p><a href="{verify_link}">{verify_link}</a></p>
            """,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Invio email verifica fallito: {exc}"
        )

    return {
        "message": "Account creato. Verifica la tua email una sola volta, poi potrai entrare sempre."
    }


@app.post("/api/auth/login")
def login(payload: LoginIn, db: Session = Depends(get_db)):
    email = payload.email.lower().strip()
    user = db.scalar(select(User).where(User.email == email))

    if not user or not verify_password(payload.password, user.password_hash):
        raise HTTPException(status_code=400, detail="Credenziali non valide")

    if not user.email_verified:
        raise HTTPException(
            status_code=403,
            detail="Verifica prima la tua email. La verifica si fa una sola volta."
        )

    token = create_access_token(user.email, extra={"is_admin": user.is_admin})
    return {"access_token": token, "token_type": "bearer"}


@app.get("/api/auth/me")
def auth_me(user: User = Depends(get_current_user)):
    terms_ok = _user_terms_ok(user)

    return {
        "id": user.id,
        "email": user.email,
        "full_name": user.full_name or "",
        "is_admin": bool(user.is_admin),
        "is_active": bool(user.is_active),
        "email_verified": bool(user.email_verified),
        "accepted_terms_version": user.accepted_terms_version or "",
        "accepted_terms_at": user.accepted_terms_at.isoformat() if user.accepted_terms_at else None,
        "terms_version": settings.TERMS_VERSION,
        "terms_ok": bool(terms_ok),
        "subscription_status": user.subscription_status or "inactive",
        "subscription_plan": user.subscription_plan or "none",
    }


@app.get("/api/auth/verify-email")
def verify_email(token: str, db: Session = Depends(get_db)):
    user = db.scalar(select(User).where(User.email_verify_token == token))

    if not user:
        raise HTTPException(status_code=400, detail="Token verifica non valido")

    if user.email_verified:
        return {
            "message": "Email già verificata",
            "email_verified": True
        }

    user.email_verified = True
    user.email_verify_token = ""
    db.commit()

    return {
        "message": "Email verificata con successo. Da ora puoi entrare sempre senza rifare la verifica.",
        "email_verified": True
    }


@app.post("/api/auth/forgot-password")
def forgot_password(payload: ForgotPasswordIn, db: Session = Depends(get_db)):
    user = db.scalar(select(User).where(User.email == payload.email.lower()))
    if not user:
        return {"message": "Se l'email esiste, riceverai un link di reset"}

    token = _create_one_time_token()
    user.reset_password_token = token
    user.reset_password_expires_at = datetime.now(timezone.utc) + timedelta(hours=2)
    db.commit()

    reset_link = _public_url(f"/reset-password?token={token}")
    send_email(
        user.email,
        "Reset password BTTcapital",
        f"""
        <h2>Reset password</h2>
        <p>Per impostare una nuova password:</p>
        <p><a href="{reset_link}">{reset_link}</a></p>
        """,
    )

    return {"message": "Se l'email esiste, riceverai un link di reset"}


@app.post("/api/auth/reset-password")
def reset_password(payload: ResetPasswordIn, db: Session = Depends(get_db)):
    user = db.scalar(select(User).where(User.reset_password_token == payload.token))
    if not user:
        raise HTTPException(status_code=400, detail="Token reset non valido")

    expires = user.reset_password_expires_at
    if not expires or expires <= datetime.now(timezone.utc):
        raise HTTPException(status_code=400, detail="Token reset scaduto")

    user.password_hash = get_password_hash(payload.password)
    user.reset_password_token = ""
    user.reset_password_expires_at = None
    db.commit()

    return {"message": "Password aggiornata con successo"}


@app.post("/api/user/accept-terms")
def accept_terms(payload: AcceptTermsIn, user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    if not payload.accepted:
        raise HTTPException(status_code=400, detail="Accettazione termini obbligatoria")

    user.accepted_terms_version = settings.TERMS_VERSION
    user.accepted_terms_at = datetime.now(timezone.utc)
    db.commit()

    return {"ok": True, "terms_version": settings.TERMS_VERSION}


@app.get("/api/public/site")
def public_site(db: Session = Depends(get_db)):
    row = db.scalar(select(AppKV).where(AppKV.key == "site_copy"))
    copy = json.loads(row.value) if row and row.value else {}
    return {
        "copy": copy,
        "stripe_ready": stripe_ready(),
        "microcap_live_enabled": settings.MICROCAP_LIVE_ENABLED,
        "trial_hours": settings.TRIAL_HOURS,
    }


@app.get("/api/public/microcap")
def public_microcap(db: Session = Depends(get_db)):
    external_state, external_dashboard = _load_external_microcap(db)
    if external_state is not None:
        dashboard = external_dashboard or {}
        return {
            "process": external_state,
            "dashboard": dashboard,
            "summary": _build_crypto_summary(dashboard),
            "public_mode": external_state.get("mode") or settings.MICROCAP_PUBLIC_MODE,
            "live_available": settings.MICROCAP_LIVE_ENABLED,
        }

    status = microcap_manager.status()
    if settings.MICROCAP_AUTO_START and not status.get("running"):
        status = microcap_manager.start(mode=settings.MICROCAP_PUBLIC_MODE)

    dashboard = read_dashboard()
    return {
        "process": status,
        "dashboard": dashboard,
        "summary": _build_crypto_summary(dashboard),
        "public_mode": settings.MICROCAP_PUBLIC_MODE,
        "live_available": settings.MICROCAP_LIVE_ENABLED,
    }


@app.post("/api/external/microcap/heartbeat")
def external_microcap_heartbeat(payload: ExternalMicrocapHeartbeatIn, db: Session = Depends(get_db)):
    if not settings.EXTERNAL_MICROCAP_API_KEY or payload.api_key != settings.EXTERNAL_MICROCAP_API_KEY:
        raise HTTPException(status_code=403, detail="Invalid external microcap api key")

    process = dict(payload.process or {})
    process["running"] = bool(process.get("running", True))
    process["source"] = "external_pc"
    process["received_at_epoch"] = time.time()

    dashboard = dict(payload.dashboard or {})

    _upsert_kv(db, "external_microcap_state", process)
    _upsert_kv(db, "external_microcap_dashboard", dashboard)

    return {"ok": True}


@app.get("/api/public/combined/summary")
def public_combined_summary(db: Session = Depends(get_db)):
    external_state, external_dashboard = _load_external_microcap(db)
    crypto_dashboard = external_dashboard if external_state is not None else read_dashboard()
    crypto_summary = _build_crypto_summary(crypto_dashboard)

    job = db.scalar(select(BttJob).order_by(desc(BttJob.created_at)))
    latest = None
    if job:
        try:
            latest = {
                "id": job.id,
                "status": job.status,
                "created_at": job.created_at,
                "summary": json.loads(job.summary_json or "{}"),
                "stdout_log": _tail(job.stdout_log, 6000),
                "error_log": _tail(job.error_log, 4000),
            }
        except Exception:
            latest = None

    stock_summary = _build_stock_summary(latest)
    combined = _build_combined_summary(crypto_summary, stock_summary)

    return {
        "crypto": crypto_summary,
        "stock": stock_summary,
        "combined": combined,
    }


@app.get("/api/public/btt/latest")
def public_btt_latest(db: Session = Depends(get_db)):
    job = db.scalar(select(BttJob).order_by(desc(BttJob.created_at)))
    if not job:
        return {"has_job": False, "latest": None, "summary_metrics": _build_stock_summary(None)}

    try:
        summary = json.loads(job.summary_json or "{}")
    except Exception:
        summary = {}

    latest = {
        "id": job.id,
        "status": job.status,
        "created_at": job.created_at,
        "summary": summary,
        "stdout_log": _tail(job.stdout_log, 6000),
        "error_log": _tail(job.error_log, 4000),
    }

    return {
        "has_job": True,
        "latest": latest,
        "summary_metrics": _build_stock_summary(latest),
    }


@app.post("/api/user/activate-trial")
def activate_trial(user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    return {
        "ok": True,
        "message": "La trial è stata rimossa. L'accesso è illimitato dopo verifica email.",
        "email_verified": bool(user.email_verified),
    }


@app.get("/api/user/microcap/status")
def user_microcap_status(user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    if not has_access(user):
        raise HTTPException(status_code=403, detail="Verifica prima la tua email per accedere")

    user_status = user_microcap_manager.status(user.id, create=False)

    if user_status is not None and user_status.get("session_exists"):
        dashboard = read_dashboard(user_id=user.id)
        process = dict(user_status)
        process["scope"] = "user"
    else:
        external_state, external_dashboard = _load_external_microcap(db)
        if external_state is not None:
            dashboard = external_dashboard or {}
            process = dict(external_state)
            process["scope"] = "public_fallback_external"
        else:
            public_status = microcap_manager.status()
            if settings.MICROCAP_AUTO_START and not public_status.get("running"):
                public_status = microcap_manager.start(mode=settings.MICROCAP_PUBLIC_MODE)
            process = dict(public_status)
            process["scope"] = "public_fallback_internal"
            dashboard = read_dashboard()

    return {
        "process": process,
        "dashboard": dashboard,
        "summary": _build_crypto_summary(dashboard),
        "public_mode": process.get("mode") or settings.MICROCAP_PUBLIC_MODE,
        "live_available": settings.MICROCAP_LIVE_ENABLED,
        "live_unlocked": _user_live_unlocked(user),
        "subscription_status": user.subscription_status,
        "email_verified": bool(user.email_verified),
        "session_scope": process.get("scope"),
    }


@app.post("/api/user/microcap/start-paper")
def user_microcap_start_paper(user: User = Depends(get_current_user)):
    if not has_access(user):
        raise HTTPException(status_code=403, detail="Verifica prima la tua email per accedere")

    status = user_microcap_manager.restart(user.id, mode="paper")
    return {"ok": bool(status.get("running")), "status": status}


@app.post("/api/user/microcap/start-live")
def user_microcap_start_live(user: User = Depends(get_current_user)):
    _ensure_live_access(user)
    status = user_microcap_manager.restart(user.id, mode="live")
    return {"ok": bool(status.get("running")), "status": status}


@app.post("/api/user/microcap/stop")
def user_microcap_stop(user: User = Depends(get_current_user)):
    if not has_access(user):
        raise HTTPException(status_code=403, detail="Verifica prima la tua email per accedere")

    status = user_microcap_manager.stop(user.id)
    return {"ok": True, "status": status}


@app.post("/api/user/btt/run")
def user_btt_run(user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    if not has_access(user):
        raise HTTPException(status_code=403, detail="Verifica prima la tua email per accedere")

    today = str(__import__("datetime").datetime.utcnow().date())
    n = db.scalar(
        select(func.count(BttJob.id)).where(
            BttJob.user_id == user.id,
            func.date(BttJob.created_at) == today,
        )
    )

    if (n or 0) >= settings.BTT_MAX_RUNS_PER_USER_PER_DAY and not user.is_admin:
        raise HTTPException(status_code=429, detail="Limite giornaliero run raggiunto")

    try:
        job = create_btt_job(db, user.id, SessionLocal, fast_demo=False)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"BTT run bootstrap failed: {type(exc).__name__}: {exc}")
    return {"job_id": job.id, "status": job.status}


@app.post("/api/billing/checkout")
def billing_checkout(payload: StripeCheckoutIn, user: User = Depends(get_current_user)):
    if not user.email_verified:
        raise HTTPException(status_code=400, detail="Verifica prima la tua email")

    if not _user_terms_ok(user):
        raise HTTPException(status_code=400, detail="Devi accettare Termini e Policy prima del checkout")

    try:
        session = create_checkout_session(user, payload.plan)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Errore Stripe checkout: {type(exc).__name__}: {exc}",
        )

    if not getattr(session, "url", None):
        raise HTTPException(status_code=502, detail="Stripe non ha restituito una URL checkout")

    return {"url": session.url}


@app.post("/api/billing/webhook")
async def billing_webhook(request: Request, db: Session = Depends(get_db)):
    raw = await request.body()
    sig = request.headers.get("stripe-signature")

    if not settings.STRIPE_WEBHOOK_SECRET:
        return JSONResponse({"ok": False, "detail": "Webhook non configurato"}, status_code=400)

    import stripe

    try:
        event = stripe.Webhook.construct_event(raw, sig, settings.STRIPE_WEBHOOK_SECRET)
    except Exception as exc:
        return JSONResponse({"ok": False, "detail": str(exc)}, status_code=400)

    if event["type"] == "checkout.session.completed":
        handle_checkout_completed(db, event["data"]["object"])

    return {"ok": True}


@app.get("/api/admin/overview")
def admin_overview(admin: User = Depends(get_current_admin), db: Session = Depends(get_db)):
    users = db.scalar(select(func.count(User.id))) or 0
    paid = db.scalar(select(func.count(User.id)).where(User.subscription_status == "active")) or 0
    jobs = db.scalar(select(func.count(BttJob.id))) or 0

    return {
        "users": users,
        "paid_users": paid,
        "btt_jobs": jobs,
        "microcap_process": microcap_manager.status(),
        "microcap_dashboard": read_dashboard()["overview"],
    }


@app.get("/api/admin/microcap/config")
def admin_get_microcap_config(admin: User = Depends(get_current_admin)):
    return {"value": get_microcap_config_text()}


@app.put("/api/admin/microcap/config")
def admin_put_microcap_config(payload: AdminTextUpdate, admin: User = Depends(get_current_admin)):
    set_microcap_config_text(payload.value)
    return {"ok": True}


@app.get("/api/admin/microcap/env")
def admin_get_microcap_env_endpoint(admin: User = Depends(get_current_admin)):
    return {"value": get_microcap_env(masked=True)}


@app.put("/api/admin/microcap/env")
def admin_put_microcap_env_endpoint(payload: AdminJsonUpdate, admin: User = Depends(get_current_admin)):
    set_microcap_env(payload.value)
    return {"ok": True}


@app.post("/api/admin/microcap/start")
def admin_start_microcap(payload: MicrocapControlIn | None = None, admin: User = Depends(get_current_admin)):
    return microcap_manager.start(mode=payload.mode if payload else None)


@app.post("/api/admin/microcap/stop")
def admin_stop_microcap(admin: User = Depends(get_current_admin)):
    return microcap_manager.stop()


@app.post("/api/admin/microcap/restart")
def admin_restart_microcap(payload: MicrocapControlIn | None = None, admin: User = Depends(get_current_admin)):
    return microcap_manager.restart(mode=payload.mode if payload else None)


@app.get("/api/admin/btt/preset")
def admin_get_btt_preset(admin: User = Depends(get_current_admin)):
    return {"value": get_btt_preset()}


@app.put("/api/admin/btt/preset")
def admin_put_btt_preset(payload: AdminJsonUpdate, admin: User = Depends(get_current_admin)):
    set_btt_preset(payload.value)
    return {"ok": True}


@app.post("/api/admin/btt/run")
def admin_run_btt(admin: User = Depends(get_current_admin), db: Session = Depends(get_db)):
    try:
        job = create_btt_job(db, admin.id, SessionLocal, fast_demo=False)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"BTT admin run failed: {type(exc).__name__}: {exc}")
    return {"job_id": job.id, "status": job.status}


@app.get("/api/admin/btt/jobs")
def admin_list_btt_jobs(admin: User = Depends(get_current_admin), db: Session = Depends(get_db)):
    jobs = db.scalars(select(BttJob).order_by(desc(BttJob.created_at)).limit(30)).all()
    out = []

    for job in jobs:
        try:
            summary = json.loads(job.summary_json or "{}")
        except Exception:
            summary = {}

        out.append(
            {
                "id": job.id,
                "status": job.status,
                "created_at": job.created_at,
                "summary": summary,
                "stdout_log": _tail(job.stdout_log, 5000),
                "error_log": _tail(job.error_log, 3000),
            }
        )

    return out


@app.get("/api/public/btt/report/{job_id}", response_class=HTMLResponse)
def public_btt_report(job_id: int, db: Session = Depends(get_db)):
    job = db.get(BttJob, job_id)
    if not job or not job.report_path:
        raise HTTPException(status_code=404, detail="Report non trovato")

    path = Path(job.report_path)
    if not path.exists():
        raise HTTPException(status_code=404, detail="File report mancante")

    return HTMLResponse(path.read_text(encoding="utf-8"))