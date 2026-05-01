from sqlalchemy.orm import Session

from app.models import User


def ensure_trial(user: User, db: Session) -> User:
    # Trial disattivata: accesso illimitato dopo verifica email.
    try:
        db.refresh(user)
    except Exception:
        pass
    return user


def has_access(user: User) -> bool:
    if getattr(user, "is_admin", False):
        return True

    if getattr(user, "subscription_status", "") == "active":
        return True

    return bool(getattr(user, "email_verified", False))