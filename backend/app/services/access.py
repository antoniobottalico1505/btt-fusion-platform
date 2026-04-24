from datetime import datetime, timedelta, timezone

from sqlalchemy.orm import Session

from app.core.settings import get_settings
from app.models import User

settings = get_settings()


def _as_utc(dt_value):
    if dt_value is None:
        return None

    if isinstance(dt_value, str):
        try:
            dt_value = datetime.fromisoformat(dt_value.replace("Z", "+00:00"))
        except Exception:
            return None

    if not isinstance(dt_value, datetime):
        return None

    if dt_value.tzinfo is None:
        return dt_value.replace(tzinfo=timezone.utc)

    return dt_value.astimezone(timezone.utc)


def ensure_trial(user: User, db: Session) -> User:
    if user.subscription_status == 'active':
        return user

    now = datetime.now(timezone.utc)
    trial_started = _as_utc(user.trial_started_at)
    trial_expires = _as_utc(user.trial_expires_at)

    if trial_started and trial_expires and trial_expires > now:
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

    trial_expires = _as_utc(user.trial_expires_at)
    if trial_expires and trial_expires > now:
        return True

    return False