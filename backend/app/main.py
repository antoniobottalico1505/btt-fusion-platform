import json
import threading
import time
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
    AdminJsonUpdate,
    AdminTextUpdate,
    ExternalMicrocapHeartbeatIn,
    LoginIn,
    MicrocapControlIn,
    RegisterIn,
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
from app.services.billing import create_checkout_session, handle_checkout_completed, stripe_ready
from app.services.bootstrap import init_app
from app.services.btt_runner import create_btt_job
from app.services.engine_manager import microcap_manager
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


app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_origin_regex=settings.CORS_ALLOW_ORIGIN_REGEX or None,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


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


@app.post("/api/auth/register")
def register(payload: RegisterIn, db: Session = Depends(get_db)):
    existing = db.scalar(select(User).where(User.email == payload.email.lower()))
    if existing:
        raise HTTPException(status_code=400, detail="Email già registrata")

    user = User(
        email=payload.email.lower(),
        password_hash=get_password_hash(payload.password),
        full_name=payload.full_name,
        is_active=True,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    ensure_trial(user, db)

    token = create_access_token(user.email, extra={"is_admin": user.is_admin})
    return {"access_token": token, "token_type": "bearer"}


@app.post("/api/auth/login")
def login(payload: LoginIn, db: Session = Depends(get_db)):
    user = db.scalar(select(User).where(User.email == payload.email.lower()))
    if not user or not verify_password(payload.password, user.password_hash):
        raise HTTPException(status_code=400, detail="Credenziali non valide")

    token = create_access_token(user.email, extra={"is_admin": user.is_admin})
    return {"access_token": token, "token_type": "bearer"}


@app.get("/api/auth/me")
def me(user: User = Depends(get_current_user)):
    return {
        "id": user.id,
        "email": user.email,
        "full_name": user.full_name,
        "is_admin": user.is_admin,
        "subscription_status": user.subscription_status,
        "subscription_plan": user.subscription_plan,
        "trial_started_at": user.trial_started_at,
        "trial_expires_at": user.trial_expires_at,
        "has_access": has_access(user),
    }


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
        return {
            "process": external_state,
            "dashboard": external_dashboard or {},
            "public_mode": external_state.get("mode") or settings.MICROCAP_PUBLIC_MODE,
            "live_available": settings.MICROCAP_LIVE_ENABLED,
        }

    status = microcap_manager.status()
    if settings.MICROCAP_AUTO_START and not status.get("running"):
        status = microcap_manager.start(mode=settings.MICROCAP_PUBLIC_MODE)

    return {
        "process": status,
        "dashboard": read_dashboard(),
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


@app.get("/api/public/btt/latest")
def public_btt_latest(db: Session = Depends(get_db)):
    job = db.scalar(select(BttJob).order_by(desc(BttJob.created_at)))
    if not job:
        return {"has_job": False, "latest": None}

    try:
        summary = json.loads(job.summary_json or "{}")
    except Exception:
        summary = {}

    return {
        "has_job": True,
        "latest": {
            "id": job.id,
            "status": job.status,
            "created_at": job.created_at,
            "summary": summary,
            "stdout_log": _tail(job.stdout_log, 6000),
            "error_log": _tail(job.error_log, 4000),        
        },
    }


@app.post("/api/user/activate-trial")
def activate_trial(user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    user = ensure_trial(user, db)
    return {"trial_started_at": user.trial_started_at, "trial_expires_at": user.trial_expires_at}


@app.post("/api/user/btt/run")
def user_btt_run(user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    user = ensure_trial(user, db)
    if not has_access(user):
        raise HTTPException(status_code=402, detail="Trial scaduto o abbonamento richiesto")

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
        job = create_btt_job(db, user.id, SessionLocal, fast_demo=True)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"BTT run bootstrap failed: {type(exc).__name__}: {exc}")
    return {"job_id": job.id, "status": job.status}

@app.post("/api/billing/checkout")
def billing_checkout(payload: StripeCheckoutIn, user: User = Depends(get_current_user)):
    session = create_checkout_session(user, payload.plan)
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