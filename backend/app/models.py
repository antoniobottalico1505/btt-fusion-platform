from datetime import datetime, timezone

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db import Base


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class User(Base):
    __tablename__ = 'users'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    email: Mapped[str] = mapped_column(String(320), unique=True, index=True)
    password_hash: Mapped[str] = mapped_column(String(255))
    full_name: Mapped[str] = mapped_column(String(120), default='')
    is_admin: Mapped[bool] = mapped_column(Boolean, default=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    trial_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    trial_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    subscription_status: Mapped[str] = mapped_column(String(40), default='inactive')
    subscription_plan: Mapped[str] = mapped_column(String(40), default='none')
    stripe_customer_id: Mapped[str] = mapped_column(String(120), default='')
    stripe_subscription_id: Mapped[str] = mapped_column(String(120), default='')
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)

    btt_jobs: Mapped[list['BttJob']] = relationship(back_populates='user')


class BttJob(Base):
    __tablename__ = 'btt_jobs'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int | None] = mapped_column(ForeignKey('users.id'), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)
    status: Mapped[str] = mapped_column(String(40), default='queued')
    stdout_log: Mapped[str] = mapped_column(Text, default='')
    error_log: Mapped[str] = mapped_column(Text, default='')
    run_dir: Mapped[str] = mapped_column(String(500), default='')
    report_path: Mapped[str] = mapped_column(String(500), default='')
    top_csv_path: Mapped[str] = mapped_column(String(500), default='')
    weights_csv_path: Mapped[str] = mapped_column(String(500), default='')
    failed_csv_path: Mapped[str] = mapped_column(String(500), default='')
    summary_json: Mapped[str] = mapped_column(Text, default='{}')

    user: Mapped['User | None'] = relationship(back_populates='btt_jobs')


class AuditLog(Base):
    __tablename__ = 'audit_logs'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    event_type: Mapped[str] = mapped_column(String(80), index=True)
    actor_email: Mapped[str] = mapped_column(String(320), default='')
    payload: Mapped[str] = mapped_column(Text, default='{}')
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class AppKV(Base):
    __tablename__ = 'app_kv'
    __table_args__ = (UniqueConstraint('key', name='uq_app_kv_key'),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    key: Mapped[str] = mapped_column(String(120), index=True)
    value: Mapped[str] = mapped_column(Text, default='')
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)
