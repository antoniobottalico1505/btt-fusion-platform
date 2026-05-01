import smtplib
import ssl
from email.mime.text import MIMEText

from app.core.settings import get_settings

settings = get_settings()


def send_email(to_email: str, subject: str, html: str) -> None:
    smtp_host = (settings.SMTP_HOST or "").strip()
    smtp_user = (settings.SMTP_USER or "").strip()
    smtp_password = settings.SMTP_PASSWORD or ""
    smtp_from = ((settings.SMTP_FROM_EMAIL or "").strip() or smtp_user)

    if not smtp_host:
        raise RuntimeError("SMTP_HOST mancante")

    if not smtp_from:
        raise RuntimeError("SMTP_FROM_EMAIL o SMTP_USER mancante")

    if smtp_user and not smtp_password:
        raise RuntimeError("SMTP_PASSWORD mancante")

    msg = MIMEText(html, "html", "utf-8")
    msg["Subject"] = subject
    msg["From"] = smtp_from
    msg["To"] = to_email

    context = ssl.create_default_context()

    with smtplib.SMTP(smtp_host, settings.SMTP_PORT, timeout=30) as server:
        server.ehlo()
        server.starttls(context=context)
        server.ehlo()

        if smtp_user:
            server.login(smtp_user, smtp_password)

        server.sendmail(smtp_from, [to_email], msg.as_string())