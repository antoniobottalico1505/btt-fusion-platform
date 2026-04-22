import json
from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db import Base, engine
from app.models import AppKV, User
from app.security import get_password_hash
from app.services.storage import ensure_storage
from app.core.settings import get_settings

settings = get_settings()


def init_app(db: Session) -> None:
    Base.metadata.create_all(bind=engine)
    ensure_storage()
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
