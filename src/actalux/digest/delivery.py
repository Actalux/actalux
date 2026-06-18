"""Email a draft to the operator over SMTP (provider-agnostic).

The operator chose email as the review surface. This uses stdlib ``smtplib`` so
any SMTP provider works (Gmail app-password, Resend SMTP, Fastmail, ...) with no
new dependency. Delivery is *optional*: when SMTP is unconfigured the caller
still has the draft file, so the pipeline never fails for lack of a mail secret.
"""

from __future__ import annotations

import logging
import smtplib
import ssl
from email.message import EmailMessage

logger = logging.getLogger(__name__)

# Implicit-TLS SMTP port (SMTPS). Anything else uses STARTTLS on a plain socket.
_SMTPS_PORT = 465


def send_draft_email(
    subject: str,
    body_markdown: str,
    *,
    host: str,
    port: int,
    user: str,
    password: str,
    email_from: str,
    email_to: str,
) -> bool:
    """Send the draft as a plain-text (markdown) email. Return True iff it was sent.

    Returns ``False`` without raising when SMTP is unconfigured (no host/from/to),
    so the weekly job degrades to "draft written, not emailed" rather than failing.
    ``email_to`` may be a comma-separated list. Authentication is skipped when no
    ``user`` is set (open relays); port 465 uses implicit TLS, any other port uses
    STARTTLS.
    """
    recipients = [addr.strip() for addr in email_to.split(",") if addr.strip()]
    if not (host and email_from and recipients):
        logger.info("SMTP not configured (host/from/to); skipping draft email")
        return False

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = email_from
    message["To"] = ", ".join(recipients)
    message.set_content(body_markdown)

    context = ssl.create_default_context()
    if port == _SMTPS_PORT:
        with smtplib.SMTP_SSL(host, port, context=context) as server:
            if user:
                server.login(user, password)
            server.send_message(message)
    else:
        with smtplib.SMTP(host, port) as server:
            server.starttls(context=context)
            if user:
                server.login(user, password)
            server.send_message(message)

    logger.info("Emailed draft %r to %s", subject, ", ".join(recipients))
    return True
