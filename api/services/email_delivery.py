"""
Email Delivery Service for Search Intelligence Reports.

Sends completed PDF reports to users via email using SMTP or
a transactional email provider (SendGrid, AWS SES, or generic SMTP).

Environment variables:
    EMAIL_PROVIDER      : "sendgrid" | "ses" | "smtp" (default: "smtp")
    EMAIL_FROM           : Sender email address
    EMAIL_FROM_NAME      : Sender display name (default: "Search Intelligence")

    # SendGrid
    SENDGRID_API_KEY     : SendGrid API key

    # AWS SES
    AWS_ACCESS_KEY_ID    : AWS access key
    AWS_SECRET_ACCESS_KEY: AWS secret key
    AWS_REGION           : AWS region (default: "us-east-1")

    # SMTP (generic)
    SMTP_HOST            : SMTP server hostname
    SMTP_PORT            : SMTP server port (default: 587)
    SMTP_USERNAME        : SMTP login username
    SMTP_PASSWORD        : SMTP login password
    SMTP_USE_TLS         : "true" | "false" (default: "true")

Usage:
    from api.services.email_delivery import send_report_email
    result = await send_report_email(
        to_email="user@example.com",
        report_data=report_dict,
        pdf_bytes=b"...",
    )
"""

from __future__ import annotations

import logging
import os
import smtplib
from datetime import datetime
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def _get_config() -> Dict[str, Any]:
    """Read email configuration from environment."""
    return {
        "provider": os.getenv("EMAIL_PROVIDER", "smtp").lower(),
        "from_email": os.getenv("EMAIL_FROM", "reports@clankermarketing.com"),
        "from_name": os.getenv("EMAIL_FROM_NAME", "Search Intelligence"),
        # SendGrid
        "sendgrid_api_key": os.getenv("SENDGRID_API_KEY", ""),
        # AWS SES
        "aws_access_key": os.getenv("AWS_ACCESS_KEY_ID", ""),
        "aws_secret_key": os.getenv("AWS_SECRET_ACCESS_KEY", ""),
        "aws_region": os.getenv("AWS_REGION", "us-east-1"),
        # SMTP
        "smtp_host": os.getenv("SMTP_HOST", ""),
        "smtp_port": int(os.getenv("SMTP_PORT", "587")),
        "smtp_username": os.getenv("SMTP_USERNAME", ""),
        "smtp_password": os.getenv("SMTP_PASSWORD", ""),
        "smtp_use_tls": os.getenv("SMTP_USE_TLS", "true").lower() == "true",
    }


# ---------------------------------------------------------------------------
# Email template
# ---------------------------------------------------------------------------

def _esc(text: str) -> str:
    """Escape HTML entities in user data."""
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _build_comparison_section(comparison_summary: Dict[str, Any]) -> str:
    """Build an HTML snippet summarising period-over-period comparison.

    The comparison_summary dict is expected to contain keys such as:
      - clicks_change_pct: float
      - impressions_change_pct: float
      - position_change: float
      - improved_pages: int
      - declined_pages: int
      - summary_text: str (optional human-readable sentence)

    Returns an HTML string to embed in the email body.  If the summary
    is empty or missing useful keys, returns an empty string.
    """
    if not comparison_summary:
        return ""

    clicks_pct = comparison_summary.get("clicks_change_pct")
    impressions_pct = comparison_summary.get("impressions_change_pct")
    improved = comparison_summary.get("improved_pages")
    declined = comparison_summary.get("declined_pages")
    summary_text = comparison_summary.get("summary_text", "")

    # Only render if we have at least one meaningful metric
    has_data = any(v is not None for v in [clicks_pct, impressions_pct, improved, declined])
    if not has_data and not summary_text:
        return ""

    rows_html = ""

    if clicks_pct is not None:
        arrow = "&#9650;" if clicks_pct >= 0 else "&#9660;"
        colour = "#16a34a" if clicks_pct >= 0 else "#dc2626"
        rows_html += (
            f'<tr><td style="padding:6px 12px;color:#475569;font-size:14px;">Clicks</td>'
            f'<td style="padding:6px 12px;color:{colour};font-size:14px;font-weight:600;">'
            f'{arrow} {abs(clicks_pct):.1f}%</td></tr>'
        )

    if impressions_pct is not None:
        arrow = "&#9650;" if impressions_pct >= 0 else "&#9660;"
        colour = "#16a34a" if impressions_pct >= 0 else "#dc2626"
        rows_html += (
            f'<tr><td style="padding:6px 12px;color:#475569;font-size:14px;">Impressions</td>'
            f'<td style="padding:6px 12px;color:{colour};font-size:14px;font-weight:600;">'
            f'{arrow} {abs(impressions_pct):.1f}%</td></tr>'
        )

    if improved is not None and declined is not None:
        rows_html += (
            f'<tr><td style="padding:6px 12px;color:#475569;font-size:14px;">Pages improved</td>'
            f'<td style="padding:6px 12px;color:#16a34a;font-size:14px;font-weight:600;">{improved}</td></tr>'
            f'<tr><td style="padding:6px 12px;color:#475569;font-size:14px;">Pages declined</td>'
            f'<td style="padding:6px 12px;color:#dc2626;font-size:14px;font-weight:600;">{declined}</td></tr>'
        )

    section = ""
    if rows_html:
        section += (
            '<table role="presentation" cellpadding="0" cellspacing="0" '
            'style="margin:0 0 16px;border:1px solid #e2e8f0;border-radius:6px;overflow:hidden;width:100%;">'
            '<tr><td colspan="2" style="background-color:#f8fafc;padding:10px 12px;'
            'font-size:13px;font-weight:600;color:#1e293b;border-bottom:1px solid #e2e8f0;">'
            'Changes since last report</td></tr>'
            f'{rows_html}</table>'
        )

    if summary_text:
        section += (
            f'<p style="margin:0 0 16px;color:#475569;font-size:13px;line-height:1.5;">'
            f'{_esc(str(summary_text))}</p>'
        )

    return section


def _build_html_body(report_data: Dict[str, Any]) -> str:
    """Build a branded HTML email body with optional comparison section."""
    domain = report_data.get("domain", "your website")
    created = report_data.get("created_at", "")
    if isinstance(created, str) and len(created) >= 10:
        date_str = created[:10]
    else:
        date_str = datetime.utcnow().strftime("%Y-%m-%d")

    report_id = report_data.get("id", "")

    # Build optional comparison section
    comparison_html = _build_comparison_section(
        report_data.get("comparison_summary", {})
    )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"></head>
<body style="margin:0;padding:0;background-color:#f1f5f9;font-family:Arial,Helvetica,sans-serif;">
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background-color:#f1f5f9;">
    <tr><td align="center" style="padding:40px 20px;">
      <table role="presentation" width="600" cellpadding="0" cellspacing="0"
             style="background-color:#ffffff;border-radius:8px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.08);">
        <!-- Header -->
        <tr>
          <td style="background-color:#1e293b;padding:32px 40px;text-align:center;">
            <h1 style="margin:0;color:#ffffff;font-size:22px;font-weight:700;">
              Search Intelligence Report
            </h1>
            <p style="margin:8px 0 0;color:#94a3b8;font-size:14px;">
              Prepared for <strong style="color:#ffffff;">{_esc(domain)}</strong>
            </p>
          </td>
        </tr>
        <!-- Body -->
        <tr>
          <td style="padding:32px 40px;">
            <p style="margin:0 0 16px;color:#1e293b;font-size:15px;line-height:1.6;">
              Hi there,
            </p>
            <p style="margin:0 0 16px;color:#1e293b;font-size:15px;line-height:1.6;">
              Your Search Intelligence Report for <strong>{_esc(domain)}</strong> is
              ready. The full analysis is attached as a PDF &mdash; it covers all 12
              modules including health trajectory, page triage, SERP landscape,
              content intelligence, and revenue attribution.
            </p>
            <p style="margin:0 0 24px;color:#1e293b;font-size:15px;line-height:1.6;">
              Report date: <strong>{_esc(date_str)}</strong>
            </p>
            {comparison_html}
            <!-- CTA -->
            <table role="presentation" cellpadding="0" cellspacing="0" style="margin:0 auto 24px;">
              <tr>
                <td style="background-color:#1a56db;border-radius:6px;">
                  <a href="https://clankermarketing.com/contact"
                     style="display:inline-block;padding:14px 32px;color:#ffffff;
                            font-size:15px;font-weight:600;text-decoration:none;">
                    Discuss Your Results With an Expert
                  </a>
                </td>
              </tr>
            </table>
            <p style="margin:0 0 8px;color:#64748b;font-size:13px;line-height:1.5;">
              Want help turning these insights into a growth plan? Our search
              consultants specialise in translating data into action.
            </p>
          </td>
        </tr>
        <!-- Footer -->
        <tr>
          <td style="background-color:#f8fafc;padding:20px 40px;border-top:1px solid #e2e8f0;">
            <p style="margin:0;color:#94a3b8;font-size:12px;text-align:center;line-height:1.5;">
              &copy; {datetime.utcnow().year} Clanker Marketing &bull;
              <a href="https://clankermarketing.com" style="color:#1a56db;text-decoration:none;">
                clankermarketing.com
              </a>
            </p>
            <p style="margin:6px 0 0;color:#cbd5e1;font-size:11px;text-align:center;">
              Report ID: {_esc(report_id)}
            </p>
          </td>
        </tr>
      </table>
    </td></tr>
  </table>
</body>
</html>"""
    return html


def _build_plain_body(report_data: Dict[str, Any]) -> str:
    """Build a plain-text email body with optional comparison summary."""
    domain = report_data.get("domain", "your website")
    created = report_data.get("created_at", "")
    if isinstance(created, str) and len(created) >= 10:
        date_str = created[:10]
    else:
        date_str = datetime.utcnow().strftime("%Y-%m-%d")

    comparison = report_data.get("comparison_summary", {})
    comparison_text = ""
    if comparison:
        lines = ["Changes since last report:"]
        clicks_pct = comparison.get("clicks_change_pct")
        if clicks_pct is not None:
            direction = "up" if clicks_pct >= 0 else "down"
            lines.append(f"  Clicks: {direction} {abs(clicks_pct):.1f}%")
        impressions_pct = comparison.get("impressions_change_pct")
        if impressions_pct is not None:
            direction = "up" if impressions_pct >= 0 else "down"
            lines.append(f"  Impressions: {direction} {abs(impressions_pct):.1f}%")
        improved = comparison.get("improved_pages")
        declined = comparison.get("declined_pages")
        if improved is not None and declined is not None:
            lines.append(f"  Pages improved: {improved}")
            lines.append(f"  Pages declined: {declined}")
        summary_text = comparison.get("summary_text")
        if summary_text:
            lines.append(f"  {summary_text}")
        comparison_text = "\n".join(lines) + "\n\n"

    return f"""Search Intelligence Report
===========================

Your report for {domain} is ready.

Report date: {date_str}

The full analysis is attached as a PDF. It covers all 12 modules
including health trajectory, page triage, SERP landscape, content
intelligence, and revenue attribution.

{comparison_text}---
Want help turning these insights into action?
Book a consultation: https://clankermarketing.com/contact

Clanker Marketing — https://clankermarketing.com
"""


# ---------------------------------------------------------------------------
# Provider: SMTP
# ---------------------------------------------------------------------------

async def _send_via_smtp(
    config: Dict[str, Any],
    to_email: str,
    subject: str,
    html_body: str,
    plain_body: str,
    pdf_bytes: Optional[bytes] = None,
    pdf_filename: Optional[str] = None,
) -> Dict[str, Any]:
    """Send email via generic SMTP."""
    if not config["smtp_host"]:
        return {"success": False, "error": "SMTP not configured: SMTP_HOST is empty"}

    msg = MIMEMultipart("mixed")
    msg["From"] = f"{config['from_name']} <{config['from_email']}>"
    msg["To"] = to_email
    msg["Subject"] = subject

    # Text alternatives
    alt = MIMEMultipart("alternative")
    alt.attach(MIMEText(plain_body, "plain", "utf-8"))
    alt.attach(MIMEText(html_body, "html", "utf-8"))
    msg.attach(alt)

    # PDF attachment
    if pdf_bytes and pdf_filename:
        att = MIMEApplication(pdf_bytes, _subtype="pdf")
        att.add_header("Content-Disposition", "attachment", filename=pdf_filename)
        msg.attach(att)

    try:
        if config["smtp_use_tls"]:
            server = smtplib.SMTP(config["smtp_host"], config["smtp_port"], timeout=30)
            server.ehlo()
            server.starttls()
            server.ehlo()
        else:
            server = smtplib.SMTP(config["smtp_host"], config["smtp_port"], timeout=30)
            server.ehlo()

        if config["smtp_username"]:
            server.login(config["smtp_username"], config["smtp_password"])

        server.sendmail(config["from_email"], [to_email], msg.as_string())
        server.quit()

        logger.info("Email sent via SMTP to %s", to_email)
        return {"success": True, "provider": "smtp"}

    except Exception as e:
        logger.error("SMTP send failed: %s", e)
        return {"success": False, "error": str(e), "provider": "smtp"}


# ---------------------------------------------------------------------------
# Provider: SendGrid
# ---------------------------------------------------------------------------

async def _send_via_sendgrid(
    config: Dict[str, Any],
    to_email: str,
    subject: str,
    html_body: str,
    plain_body: str,
    pdf_bytes: Optional[bytes] = None,
    pdf_filename: Optional[str] = None,
) -> Dict[str, Any]:
    """Send email via SendGrid Web API v3."""
    import base64 as b64

    api_key = config.get("sendgrid_api_key", "")
    if not api_key:
        return {"success": False, "error": "SendGrid not configured: SENDGRID_API_KEY is empty"}

    try:
        import httpx
    except ImportError:
        # Fall back to urllib if httpx isn't available
        return await _send_sendgrid_urllib(config, to_email, subject, html_body, plain_body, pdf_bytes, pdf_filename)

    payload: Dict[str, Any] = {
        "personalizations": [{"to": [{"email": to_email}]}],
        "from": {"email": config["from_email"], "name": config["from_name"]},
        "subject": subject,
        "content": [
            {"type": "text/plain", "value": plain_body},
            {"type": "text/html", "value": html_body},
        ],
    }

    if pdf_bytes and pdf_filename:
        payload["attachments"] = [{
            "content": b64.b64encode(pdf_bytes).decode("ascii"),
            "type": "application/pdf",
            "filename": pdf_filename,
            "disposition": "attachment",
        }]

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                "https://api.sendgrid.com/v3/mail/send",
                json=payload,
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
            )

        if resp.status_code in (200, 201, 202):
            logger.info("Email sent via SendGrid to %s", to_email)
            return {"success": True, "provider": "sendgrid", "status_code": resp.status_code}
        else:
            body = resp.text
            logger.error("SendGrid error %d: %s", resp.status_code, body)
            return {"success": False, "error": body, "provider": "sendgrid", "status_code": resp.status_code}

    except Exception as e:
        logger.error("SendGrid send failed: %s", e)
        return {"success": False, "error": str(e), "provider": "sendgrid"}


async def _send_sendgrid_urllib(
    config: Dict[str, Any],
    to_email: str,
    subject: str,
    html_body: str,
    plain_body: str,
    pdf_bytes: Optional[bytes] = None,
    pdf_filename: Optional[str] = None,
) -> Dict[str, Any]:
    """Fallback SendGrid sender using urllib (no httpx dependency)."""
    import base64 as b64
    import json as _json
    import urllib.request

    api_key = config.get("sendgrid_api_key", "")
    payload: Dict[str, Any] = {
        "personalizations": [{"to": [{"email": to_email}]}],
        "from": {"email": config["from_email"], "name": config["from_name"]},
        "subject": subject,
        "content": [
            {"type": "text/plain", "value": plain_body},
            {"type": "text/html", "value": html_body},
        ],
    }
    if pdf_bytes and pdf_filename:
        payload["attachments"] = [{
            "content": b64.b64encode(pdf_bytes).decode("ascii"),
            "type": "application/pdf",
            "filename": pdf_filename,
            "disposition": "attachment",
        }]

    data = _json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        "https://api.sendgrid.com/v3/mail/send",
        data=data,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            status = resp.status
            if status in (200, 201, 202):
                logger.info("Email sent via SendGrid (urllib) to %s", to_email)
                return {"success": True, "provider": "sendgrid", "status_code": status}
            body = resp.read().decode("utf-8", errors="replace")
            return {"success": False, "error": body, "provider": "sendgrid", "status_code": status}
    except Exception as e:
        logger.error("SendGrid urllib send failed: %s", e)
        return {"success": False, "error": str(e), "provider": "sendgrid"}


# ---------------------------------------------------------------------------
# Provider: AWS SES
# ---------------------------------------------------------------------------

async def _send_via_ses(
    config: Dict[str, Any],
    to_email: str,
    subject: str,
    html_body: str,
    plain_body: str,
    pdf_bytes: Optional[bytes] = None,
    pdf_filename: Optional[str] = None,
) -> Dict[str, Any]:
    """Send email via AWS SES using boto3."""
    try:
        import boto3
    except ImportError:
        return {"success": False, "error": "boto3 not installed — cannot use AWS SES provider"}

    if not config.get("aws_access_key") or not config.get("aws_secret_key"):
        return {"success": False, "error": "AWS SES not configured: missing credentials"}

    # Build raw MIME message (SES SendRawEmail supports attachments)
    msg = MIMEMultipart("mixed")
    msg["From"] = f"{config['from_name']} <{config['from_email']}>"
    msg["To"] = to_email
    msg["Subject"] = subject

    alt = MIMEMultipart("alternative")
    alt.attach(MIMEText(plain_body, "plain", "utf-8"))
    alt.attach(MIMEText(html_body, "html", "utf-8"))
    msg.attach(alt)

    if pdf_bytes and pdf_filename:
        att = MIMEApplication(pdf_bytes, _subtype="pdf")
        att.add_header("Content-Disposition", "attachment", filename=pdf_filename)
        msg.attach(att)

    try:
        ses = boto3.client(
            "ses",
            region_name=config["aws_region"],
            aws_access_key_id=config["aws_access_key"],
            aws_secret_access_key=config["aws_secret_key"],
        )
        response = ses.send_raw_email(
            Source=msg["From"],
            Destinations=[to_email],
            RawMessage={"Data": msg.as_string()},
        )
        msg_id = response.get("MessageId", "")
        logger.info("Email sent via SES to %s (MessageId: %s)", to_email, msg_id)
        return {"success": True, "provider": "ses", "message_id": msg_id}

    except Exception as e:
        logger.error("SES send failed: %s", e)
        return {"success": False, "error": str(e), "provider": "ses"}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def send_report_email(
    to_email: str,
    report_data: Dict[str, Any],
    pdf_bytes: Optional[bytes] = None,
    pdf_filename: Optional[str] = None,
    subject: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Send a Search Intelligence Report via email.

    Parameters
    ----------
    to_email : str
        Recipient email address.
    report_data : dict
        Report metadata (must contain at least ``domain`` and ``id``).
        May also contain ``comparison_summary`` for period-over-period
        comparison data to embed in the email body.
    pdf_bytes : bytes, optional
        Pre-generated PDF content.  If *None* the email is sent without an
        attachment and the body invites the user to download from the app.
    pdf_filename : str, optional
        Filename for the PDF attachment.  Auto-generated if omitted.
    subject : str, optional
        Custom email subject line.  Auto-generated if omitted.

    Returns
    -------
    dict
        ``{"success": True/False, ...}`` with provider-specific metadata.
    """
    config = _get_config()

    domain = report_data.get("domain", "report")

    if not subject:
        subject = f"Your Search Intelligence Report — {domain}"

    if not pdf_filename and pdf_bytes:
        safe_domain = domain.replace(".", "_")
        pdf_filename = f"search_intelligence_{safe_domain}_{datetime.utcnow().strftime('%Y%m%d')}.pdf"

    html_body = _build_html_body(report_data)
    plain_body = _build_plain_body(report_data)

    provider = config["provider"]

    if provider == "sendgrid":
        result = await _send_via_sendgrid(config, to_email, subject, html_body, plain_body, pdf_bytes, pdf_filename)
    elif provider == "ses":
        result = await _send_via_ses(config, to_email, subject, html_body, plain_body, pdf_bytes, pdf_filename)
    else:
        result = await _send_via_smtp(config, to_email, subject, html_body, plain_body, pdf_bytes, pdf_filename)

    # Enrich result
    result["to_email"] = to_email
    result["report_id"] = report_data.get("id", "")
    result["domain"] = domain
    return result


async def check_email_config() -> Dict[str, Any]:
    """
    Verify that email delivery is configured.

    Returns a dict with ``configured: True/False`` and the active provider.
    Useful for the health/status endpoint.
    """
    config = _get_config()
    provider = config["provider"]

    if provider == "sendgrid":
        configured = bool(config.get("sendgrid_api_key"))
    elif provider == "ses":
        configured = bool(config.get("aws_access_key") and config.get("aws_secret_key"))
    else:
        configured = bool(config.get("smtp_host"))

    return {
        "configured": configured,
        "provider": provider,
        "from_email": config["from_email"],
    }
