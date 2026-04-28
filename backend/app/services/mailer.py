import smtplib
from email.mime.text import MIMEText

from app.core.settings import get_settings

settings = get_settings()

def send_email(to_email: str, subject: str, html: str) -> None:
    if not settings.SMTP_HOST or not settings.SMTP_FROM_EMAIL:
        return

    msg = MIMEText(html, 'html', 'utf-8')
    msg['Subject'] = subject
    msg['From'] = settings.SMTP_FROM_EMAIL
    msg['To'] = to_email

    with smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT) as server:
        server.starttls()
        if settings.SMTP_USER:
            server.login(settings.SMTP_USER, settings.SMTP_PASSWORD)
        server.sendmail(settings.SMTP_FROM_EMAIL, [to_email], msg.as_string())