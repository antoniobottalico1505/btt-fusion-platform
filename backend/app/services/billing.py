import stripe
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.settings import get_settings
from app.models import User

settings = get_settings()


def _clean(value: str | None) -> str:
    return str(value or "").strip()


def _configure_stripe() -> None:
    secret = _clean(settings.STRIPE_SECRET_KEY)
    if secret:
        stripe.api_key = secret


_configure_stripe()


def stripe_ready() -> bool:
    return bool(
        _clean(settings.STRIPE_SECRET_KEY)
        and _clean(settings.STRIPE_PRICE_MONTHLY)
        and _clean(settings.STRIPE_PRICE_YEARLY)
    )


def stripe_status() -> dict:
    return {
        "stripe_ready": stripe_ready(),
        "has_secret_key": bool(_clean(settings.STRIPE_SECRET_KEY)),
        "has_monthly_price": bool(_clean(settings.STRIPE_PRICE_MONTHLY)),
        "has_yearly_price": bool(_clean(settings.STRIPE_PRICE_YEARLY)),
        "monthly_price_prefix_ok": _clean(settings.STRIPE_PRICE_MONTHLY).startswith("price_"),
        "yearly_price_prefix_ok": _clean(settings.STRIPE_PRICE_YEARLY).startswith("price_"),
        "success_url": _clean(settings.STRIPE_SUCCESS_URL),
        "cancel_url": _clean(settings.STRIPE_CANCEL_URL),
    }


def price_for_plan(plan: str) -> str:
    p = (plan or "").strip().lower()

    mapping = {
        "monthly": _clean(settings.STRIPE_PRICE_MONTHLY),
        "yearly": _clean(settings.STRIPE_PRICE_YEARLY),
        "bundle_monthly": _clean(settings.STRIPE_PRICE_MONTHLY),
        "bundle_yearly": _clean(settings.STRIPE_PRICE_YEARLY),
    }

    value = mapping.get(p)
    if not value:
        raise ValueError(f"Piano non valido: {plan}")

    if not value.startswith("price_"):
        raise ValueError(
            f"Stripe price id non valido per piano {plan}: deve iniziare con price_, non con prod_ o altro"
        )

    return value


def create_checkout_session(user: User, plan: str):
    _configure_stripe()

    if not stripe_ready():
        raise RuntimeError(
            "Stripe non configurato: controlla STRIPE_SECRET_KEY, STRIPE_PRICE_MONTHLY e STRIPE_PRICE_YEARLY su Render"
        )

    frontend_url = _clean(settings.FRONTEND_URL) or "https://bttcapital.cc"

    success_url = _clean(settings.STRIPE_SUCCESS_URL) or f"{frontend_url}/dashboard?checkout=success"
    cancel_url = _clean(settings.STRIPE_CANCEL_URL) or f"{frontend_url}/pricing?checkout=cancel"

    return stripe.checkout.Session.create(
        mode="subscription",
        success_url=success_url,
        cancel_url=cancel_url,
        line_items=[{"price": price_for_plan(plan), "quantity": 1}],
        customer_email=user.email,
        metadata={"user_email": user.email, "plan": plan},
    )


def handle_checkout_completed(db: Session, session_obj: dict) -> None:
    email = ((session_obj.get("metadata") or {}).get("user_email")) or session_obj.get("customer_email")
    if not email:
        return

    user = db.scalar(select(User).where(User.email == email))
    if not user:
        return

    user.subscription_status = "active"
    user.subscription_plan = ((session_obj.get("metadata") or {}).get("plan")) or "paid"
    user.stripe_customer_id = str(session_obj.get("customer") or "")
    user.stripe_subscription_id = str(session_obj.get("subscription") or "")
    db.commit()