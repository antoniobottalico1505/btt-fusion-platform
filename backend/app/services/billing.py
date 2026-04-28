import stripe
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.settings import get_settings
from app.models import User

settings = get_settings()
if settings.STRIPE_SECRET_KEY:
    stripe.api_key = settings.STRIPE_SECRET_KEY


def stripe_ready() -> bool:
    return bool(settings.STRIPE_SECRET_KEY and settings.STRIPE_PRICE_MONTHLY and settings.STRIPE_PRICE_YEARLY)


def price_for_plan(plan: str) -> str:
    p = (plan or '').strip().lower()

    mapping = {
        'crypto_monthly': settings.STRIPE_PRICE_CRYPTO_MONTHLY,
        'crypto_yearly': settings.STRIPE_PRICE_CRYPTO_YEARLY,
        'stock_monthly': settings.STRIPE_PRICE_STOCK_MONTHLY,
        'stock_yearly': settings.STRIPE_PRICE_STOCK_YEARLY,
        'bundle_monthly': settings.STRIPE_PRICE_BUNDLE_MONTHLY,
        'bundle_yearly': settings.STRIPE_PRICE_BUNDLE_YEARLY,
    }

    value = mapping.get(p)
    if not value:
        raise ValueError('Invalid plan')

    return value


def create_checkout_session(user: User, plan: str):
    if not stripe_ready():
        raise RuntimeError('Stripe non configurato')
    return stripe.checkout.Session.create(
        mode='subscription',
        success_url=settings.STRIPE_SUCCESS_URL,
        cancel_url=settings.STRIPE_CANCEL_URL,
        line_items=[{'price': price_for_plan(plan), 'quantity': 1}],
        customer_email=user.email,
        metadata={'user_email': user.email, 'plan': plan},
    )


def handle_checkout_completed(db: Session, session_obj: dict) -> None:
    email = ((session_obj.get('metadata') or {}).get('user_email')) or session_obj.get('customer_email')
    if not email:
        return
    user = db.scalar(select(User).where(User.email == email))
    if not user:
        return
    user.subscription_status = 'active'
    user.subscription_plan = ((session_obj.get('metadata') or {}).get('plan')) or 'paid'
    user.stripe_customer_id = str(session_obj.get('customer') or '')
    user.stripe_subscription_id = str(session_obj.get('subscription') or '')
    db.commit()
