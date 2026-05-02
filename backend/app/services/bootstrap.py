import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy import inspect, select, text
from sqlalchemy.orm import Session

from app.core.settings import get_settings
from app.db import Base, engine
from app.models import AppKV, User
from app.security import get_password_hash
from app.services.storage import ensure_storage

settings = get_settings()


def _column_exists(inspector, table: str, column: str) -> bool:
    try:
        cols = inspector.get_columns(table)
    except Exception:
        return False
    return any(str(col.get("name")) == column for col in cols)


def _dialect_text_type() -> str:
    return "TEXT"


def _dialect_bool_type() -> str:
    if engine.dialect.name == "postgresql":
        return "BOOLEAN"
    return "BOOLEAN"


def _dialect_datetime_type() -> str:
    if engine.dialect.name == "postgresql":
        return "TIMESTAMP WITH TIME ZONE"
    return "TEXT"


def _add_column_if_missing(table: str, column: str, ddl_tail: str) -> None:
    inspector = inspect(engine)
    if _column_exists(inspector, table, column):
        return

    stmt = text(f"ALTER TABLE {table} ADD COLUMN {column} {ddl_tail}")
    with engine.begin() as conn:
        conn.execute(stmt)


def _ensure_schema_compat() -> None:
    Base.metadata.create_all(bind=engine)

    # users
    _add_column_if_missing("users", "full_name", f"{_dialect_text_type()} DEFAULT ''")
    _add_column_if_missing("users", "is_admin", f"{_dialect_bool_type()} DEFAULT FALSE")
    _add_column_if_missing("users", "is_active", f"{_dialect_bool_type()} DEFAULT TRUE")
    _add_column_if_missing("users", "trial_started_at", _dialect_datetime_type())
    _add_column_if_missing("users", "trial_expires_at", _dialect_datetime_type())
    _add_column_if_missing("users", "subscription_status", f"{_dialect_text_type()} DEFAULT 'inactive'")
    _add_column_if_missing("users", "subscription_plan", f"{_dialect_text_type()} DEFAULT 'none'")
    _add_column_if_missing("users", "stripe_customer_id", f"{_dialect_text_type()} DEFAULT ''")
    _add_column_if_missing("users", "stripe_subscription_id", f"{_dialect_text_type()} DEFAULT ''")
    _add_column_if_missing("users", "created_at", _dialect_datetime_type())
    _add_column_if_missing("users", "updated_at", _dialect_datetime_type())
    _add_column_if_missing("users", "wallet_address", f"{_dialect_text_type()} DEFAULT ''")
    _add_column_if_missing("users", "wallet_chain_id", "INTEGER DEFAULT 8453")
    _add_column_if_missing("users", "wallet_connected_at", _dialect_datetime_type())
    _add_column_if_missing("users", "wallet_link_message", f"{_dialect_text_type()} DEFAULT ''")
    _add_column_if_missing("users", "wallet_link_signature", f"{_dialect_text_type()} DEFAULT ''")

    # btt_jobs
    _add_column_if_missing("btt_jobs", "user_id", "INTEGER")
    _add_column_if_missing("btt_jobs", "created_at", _dialect_datetime_type())
    _add_column_if_missing("btt_jobs", "updated_at", _dialect_datetime_type())
    _add_column_if_missing("btt_jobs", "status", f"{_dialect_text_type()} DEFAULT 'queued'")
    _add_column_if_missing("btt_jobs", "stdout_log", f"{_dialect_text_type()} DEFAULT ''")
    _add_column_if_missing("btt_jobs", "error_log", f"{_dialect_text_type()} DEFAULT ''")
    _add_column_if_missing("btt_jobs", "run_dir", f"{_dialect_text_type()} DEFAULT ''")
    _add_column_if_missing("btt_jobs", "report_path", f"{_dialect_text_type()} DEFAULT ''")
    _add_column_if_missing("btt_jobs", "top_csv_path", f"{_dialect_text_type()} DEFAULT ''")
    _add_column_if_missing("btt_jobs", "weights_csv_path", f"{_dialect_text_type()} DEFAULT ''")
    _add_column_if_missing("btt_jobs", "failed_csv_path", f"{_dialect_text_type()} DEFAULT ''")
    _add_column_if_missing("btt_jobs", "summary_json", f"{_dialect_text_type()} DEFAULT '{{}}'")

    # audit_logs
    _add_column_if_missing("audit_logs", "event_type", _dialect_text_type())
    _add_column_if_missing("audit_logs", "actor_email", f"{_dialect_text_type()} DEFAULT ''")
    _add_column_if_missing("audit_logs", "payload", f"{_dialect_text_type()} DEFAULT '{{}}'")
    _add_column_if_missing("audit_logs", "created_at", _dialect_datetime_type())

    # app_kv
    _add_column_if_missing("app_kv", "key", _dialect_text_type())
    _add_column_if_missing("app_kv", "value", f"{_dialect_text_type()} DEFAULT ''")
    _add_column_if_missing("app_kv", "updated_at", _dialect_datetime_type())

    with engine.begin() as conn:
        conn.execute(text("UPDATE btt_jobs SET stdout_log = '' WHERE stdout_log IS NULL"))
        conn.execute(text("UPDATE btt_jobs SET error_log = '' WHERE error_log IS NULL"))
        conn.execute(text("UPDATE btt_jobs SET summary_json = '{}' WHERE summary_json IS NULL"))
        conn.execute(text("UPDATE btt_jobs SET status = 'queued' WHERE status IS NULL"))
        conn.execute(text("UPDATE app_kv SET value = '' WHERE value IS NULL"))


def init_app(db: Session) -> None:
    ensure_storage()
    _ensure_schema_compat()
    ensure_admin(db)
    ensure_site_copy(db)


def ensure_admin(db: Session) -> None:
    user = db.scalar(select(User).where(User.email == settings.ADMIN_EMAIL))
    if not user:
        user = User(
            email=settings.ADMIN_EMAIL,
            password_hash=get_password_hash(settings.ADMIN_PASSWORD),
            full_name='Owner',
            is_admin=True,
            is_active=True,
            subscription_status='active',
            subscription_plan='owner',
            trial_started_at=datetime.now(timezone.utc),
            trial_expires_at=datetime.now(timezone.utc) + timedelta(days=3650),
        )
        db.add(user)
        db.commit()
        return

    user.password_hash = get_password_hash(settings.ADMIN_PASSWORD)
    user.is_admin = True
    user.is_active = True
    user.subscription_status = 'active'
    user.subscription_plan = 'owner'
    db.commit()


def ensure_site_copy(db: Session) -> None:
    default = {
        'hero_title': 'BTT Fusion',
        'hero_subtitle': 'Due engine separati. Un’unica esperienza premium.',
        'microcap_tagline': 'Paper demo osservabile, live sbloccabile solo da backend.',
        'btt_tagline': 'Ranking azionario server-side con report visuali e portafogli suggeriti.',
    }
    row = db.scalar(select(AppKV).where(AppKV.key == 'site_copy'))
    if not row:
        db.add(AppKV(key='site_copy', value=json.dumps(default, ensure_ascii=False)))
        db.commit()