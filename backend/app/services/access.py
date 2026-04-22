from datetime import datetime, timedelta, timezone

from sqlalchemy.orm import Session

from app.core.settings import get_settings
from app.models import User

settings = get_settings()


def ensure_trial(user: User, db: Session) -> User:
    if user.subscription_status == 'active':
        return user
    now = datetime.now(timezone.utc)
    if user.trial_started_at and user.trial_expires_at and user.trial_expires_at > now:
        return user
    user.trial_started_at = now
    user.trial_expires_at = now + timedelta(hours=settings.TRIAL_HOURS)
    db.commit()
    db.refresh(user)
    return user


def has_access(user: User) -> bool:
    now = datetime.now(timezone.utc)
    if user.subscription_status == 'active':
        return True
    if user.trial_expires_at and user.trial_expires_at > now:
        return True
    return False
