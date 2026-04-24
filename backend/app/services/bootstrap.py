import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.settings import get_settings
from app.db import Base, engine
from app.models import AppKV, User
from app.security import get_password_hash
from app.services.storage import ensure_storage

settings = get_settings()


def _sqlite_has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    rows = cur.fetchall()
    return any(str(row[1]) == column for row in rows)


def _sqlite_add_column_if_missing(conn: sqlite3.Connection, table: str, column: str, ddl_tail: str) -> None:
    if _sqlite_has_column(conn, table, column):
        return
    cur = conn.cursor()
    cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl_tail}")


def _migrate_sqlite_app_db() -> None:
    if engine.url.get_backend_name() != "sqlite":
        return

    db_name = engine.url.database
    if not db_name:
        return

    db_path = Path(db_name)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()

        cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            email TEXT UNIQUE,
            password_hash TEXT,
            full_name TEXT DEFAULT '',
            is_admin BOOLEAN DEFAULT 0,
            is_active BOOLEAN DEFAULT 1,
            trial_started_at TEXT,
            trial_expires_at TEXT,
            subscription_status TEXT DEFAULT 'inactive',
            subscription_plan TEXT DEFAULT 'none',
            stripe_customer_id TEXT DEFAULT '',
            stripe_subscription_id TEXT DEFAULT '',
            created_at TEXT,
            updated_at TEXT
        )
        """)

        _sqlite_add_column_if_missing(conn, "users", "full_name", "TEXT DEFAULT ''")
        _sqlite_add_column_if_missing(conn, "users", "is_admin", "BOOLEAN DEFAULT 0")
        _sqlite_add_column_if_missing(conn, "users", "is_active", "BOOLEAN DEFAULT 1")
        _sqlite_add_column_if_missing(conn, "users", "trial_started_at", "TEXT")
        _sqlite_add_column_if_missing(conn, "users", "trial_expires_at", "TEXT")
        _sqlite_add_column_if_missing(conn, "users", "subscription_status", "TEXT DEFAULT 'inactive'")
        _sqlite_add_column_if_missing(conn, "users", "subscription_plan", "TEXT DEFAULT 'none'")
        _sqlite_add_column_if_missing(conn, "users", "stripe_customer_id", "TEXT DEFAULT ''")
        _sqlite_add_column_if_missing(conn, "users", "stripe_subscription_id", "TEXT DEFAULT ''")
        _sqlite_add_column_if_missing(conn, "users", "created_at", "TEXT")
        _sqlite_add_column_if_missing(conn, "users", "updated_at", "TEXT")
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ix_users_email ON users(email)")

        cur.execute("""
        CREATE TABLE IF NOT EXISTS btt_jobs (
            id INTEGER PRIMARY KEY,
            user_id INTEGER,
            created_at TEXT,
            updated_at TEXT,
            status TEXT DEFAULT 'queued',
            stdout_log TEXT DEFAULT '',
            error_log TEXT DEFAULT '',
            run_dir TEXT DEFAULT '',
            report_path TEXT DEFAULT '',
            top_csv_path TEXT DEFAULT '',
            weights_csv_path TEXT DEFAULT '',
            failed_csv_path TEXT DEFAULT '',
            summary_json TEXT DEFAULT '{}'
        )
        """)

        _sqlite_add_column_if_missing(conn, "btt_jobs", "user_id", "INTEGER")
        _sqlite_add_column_if_missing(conn, "btt_jobs", "created_at", "TEXT")
        _sqlite_add_column_if_missing(conn, "btt_jobs", "updated_at", "TEXT")
        _sqlite_add_column_if_missing(conn, "btt_jobs", "status", "TEXT DEFAULT 'queued'")
        _sqlite_add_column_if_missing(conn, "btt_jobs", "stdout_log", "TEXT DEFAULT ''")
        _sqlite_add_column_if_missing(conn, "btt_jobs", "error_log", "TEXT DEFAULT ''")
        _sqlite_add_column_if_missing(conn, "btt_jobs", "run_dir", "TEXT DEFAULT ''")
        _sqlite_add_column_if_missing(conn, "btt_jobs", "report_path", "TEXT DEFAULT ''")
        _sqlite_add_column_if_missing(conn, "btt_jobs", "top_csv_path", "TEXT DEFAULT ''")
        _sqlite_add_column_if_missing(conn, "btt_jobs", "weights_csv_path", "TEXT DEFAULT ''")
        _sqlite_add_column_if_missing(conn, "btt_jobs", "failed_csv_path", "TEXT DEFAULT ''")
        _sqlite_add_column_if_missing(conn, "btt_jobs", "summary_json", "TEXT DEFAULT '{}'")

        cur.execute("""
        CREATE TABLE IF NOT EXISTS audit_logs (
            id INTEGER PRIMARY KEY,
            event_type TEXT,
            actor_email TEXT DEFAULT '',
            payload TEXT DEFAULT '{}',
            created_at TEXT
        )
        """)

        _sqlite_add_column_if_missing(conn, "audit_logs", "event_type", "TEXT")
        _sqlite_add_column_if_missing(conn, "audit_logs", "actor_email", "TEXT DEFAULT ''")
        _sqlite_add_column_if_missing(conn, "audit_logs", "payload", "TEXT DEFAULT '{}'")
        _sqlite_add_column_if_missing(conn, "audit_logs", "created_at", "TEXT")

        cur.execute("""
        CREATE TABLE IF NOT EXISTS app_kv (
            id INTEGER PRIMARY KEY,
            key TEXT,
            value TEXT DEFAULT '',
            updated_at TEXT
        )
        """)

        _sqlite_add_column_if_missing(conn, "app_kv", "key", "TEXT")
        _sqlite_add_column_if_missing(conn, "app_kv", "value", "TEXT DEFAULT ''")
        _sqlite_add_column_if_missing(conn, "app_kv", "updated_at", "TEXT")
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_app_kv_key ON app_kv(key)")

        conn.commit()
    finally:
        conn.close()


def init_app(db: Session) -> None:
    ensure_storage()
    Base.metadata.create_all(bind=engine)
    _migrate_sqlite_app_db()
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