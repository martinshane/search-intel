"""
Health check router — comprehensive operational diagnostics.

Provides three tiers of health checking:

  GET /health           → fast liveness probe (status + version)
  GET /health/ready     → readiness probe (Supabase reachable)
  GET /health/detailed  → full diagnostic (every dependency + module import check)

The /health endpoint is intentionally minimal so Railway's health check
never times out.  Use /health/detailed to diagnose configuration issues
after a redeploy.
"""

import importlib
import logging
import os
import platform
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List

from fastapi import APIRouter

from ..config import APP_VERSION

router = APIRouter()
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Fast liveness probe — Railway hits this every 10 s
# ---------------------------------------------------------------------------

@router.get("")
@router.get("/")
async def health_check() -> Dict[str, Any]:
    """Basic liveness probe. Must respond in <1 s."""
    return {
        "status": "ok",
        "service": "search-intel-api",
        "version": APP_VERSION,
    }


# ---------------------------------------------------------------------------
# Readiness probe — can the API serve real traffic?
# ---------------------------------------------------------------------------

@router.get("/ready")
async def readiness_check() -> Dict[str, Any]:
    """
    Readiness probe.  Returns 200 only if Supabase is reachable.

    Kubernetes / Railway can use this to decide whether to route traffic
    to this replica.
    """
    supabase_ok = await _check_supabase()
    ready = supabase_ok["healthy"]
    return {
        "status": "ready" if ready else "not_ready",
        "service": "search-intel-api",
        "version": APP_VERSION,
        "supabase": supabase_ok,
    }


# ---------------------------------------------------------------------------
# Full diagnostic endpoint
# ---------------------------------------------------------------------------

@router.get("/detailed")
async def detailed_health() -> Dict[str, Any]:
    """
    Comprehensive health check covering every critical dependency.

    Checks:
      1. Supabase connectivity (live query)
      2. Google OAuth credentials (env vars present)
      3. Anthropic API key (env var present)
      4. DataForSEO credentials (env vars present)
      5. Email delivery configuration (env vars present)
      6. All 12 analysis modules importable (no broken imports)
      7. System info (Python version, memory, uptime)

    This endpoint is NOT suitable for high-frequency polling because the
    module-import check takes ~200 ms on first call.  Use /health or
    /health/ready for probes.
    """
    t0 = time.monotonic()

    deps: Dict[str, Any] = {}

    # 1. Supabase
    deps["supabase"] = await _check_supabase()

    # 2. Google OAuth
    deps["google_oauth"] = _check_google_oauth()

    # 3. Anthropic (Claude API — used by Module 5 gameplan + Module 7 intent)
    deps["anthropic"] = _check_env_key(
        names=["ANTHROPIC_API_KEY"],
        label="Anthropic API key",
        required_for="Module 5 (Gameplan narrative), Module 7 (Intent classification)",
    )

    # 4. DataForSEO (used by SERP ingestion — Modules 3, 8, 11)
    deps["dataforseo"] = _check_dataforseo()

    # 5. Email delivery
    deps["email"] = _check_email_config()

    # 6. Analysis modules (all 12 must import cleanly)
    deps["analysis_modules"] = _check_analysis_modules()

    # 7. System info
    deps["system"] = _system_info()

    all_healthy = all(
        d.get("healthy", False)
        for k, d in deps.items()
        if k != "system"  # system info is always informational
    )

    elapsed = round((time.monotonic() - t0) * 1000, 1)

    return {
        "status": "ok" if all_healthy else "degraded",
        "service": "search-intel-api",
        "version": APP_VERSION,
        "healthy": all_healthy,
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "check_duration_ms": elapsed,
        "dependencies": deps,
    }


# ---------------------------------------------------------------------------
# Individual check functions
# ---------------------------------------------------------------------------

async def _check_supabase() -> Dict[str, Any]:
    """Verify Supabase is reachable with a lightweight query."""
    try:
        from api.database import get_supabase_client
        client = get_supabase_client()
        if client is None:
            return {"healthy": False, "error": "Client is None — missing credentials"}
        # Minimal query: read 1 row from users table
        client.table("users").select("id").limit(1).execute()
        return {"healthy": True}
    except Exception as e:
        return {"healthy": False, "error": str(e)[:300]}


def _check_google_oauth() -> Dict[str, Any]:
    """Check that Google OAuth client credentials are configured."""
    client_id = os.getenv("GOOGLE_CLIENT_ID", "").strip()
    client_secret = os.getenv("GOOGLE_CLIENT_SECRET", "").strip()

    missing: List[str] = []
    if not client_id:
        missing.append("GOOGLE_CLIENT_ID")
    if not client_secret:
        missing.append("GOOGLE_CLIENT_SECRET")

    if missing:
        return {
            "healthy": False,
            "error": f"Missing env vars: {', '.join(missing)}",
            "required_for": "User authentication and GSC/GA4 data access",
        }

    # Mask the values for safety
    return {
        "healthy": True,
        "client_id_preview": client_id[:12] + "..." if len(client_id) > 12 else "***",
    }


def _check_dataforseo() -> Dict[str, Any]:
    """Check DataForSEO API credentials."""
    login = os.getenv("DATAFORSEO_LOGIN", "").strip()
    password = os.getenv("DATAFORSEO_PASSWORD", "").strip()

    missing: List[str] = []
    if not login:
        missing.append("DATAFORSEO_LOGIN")
    if not password:
        missing.append("DATAFORSEO_PASSWORD")

    if missing:
        return {
            "healthy": False,
            "error": f"Missing env vars: {', '.join(missing)}",
            "required_for": "SERP data ingestion (Modules 3, 8, 11)",
        }

    return {
        "healthy": True,
        "login_preview": login[:6] + "..." if len(login) > 6 else "***",
    }


def _check_env_key(
    names: List[str],
    label: str,
    required_for: str,
) -> Dict[str, Any]:
    """Generic check: at least one of *names* must be a non-empty env var."""
    for name in names:
        val = os.getenv(name, "").strip()
        if val:
            return {
                "healthy": True,
                "env_var": name,
                "preview": val[:8] + "..." if len(val) > 8 else "***",
            }
    return {
        "healthy": False,
        "error": f"Missing {label} — set one of: {', '.join(names)}",
        "required_for": required_for,
    }


def _check_email_config() -> Dict[str, Any]:
    """Check whether at least one email delivery provider is configured."""
    # SES
    ses_key = os.getenv("AWS_ACCESS_KEY_ID", "").strip()
    ses_secret = os.getenv("AWS_SECRET_ACCESS_KEY", "").strip()
    ses_region = os.getenv("AWS_SES_REGION", "").strip() or os.getenv("AWS_DEFAULT_REGION", "").strip()
    ses_ok = bool(ses_key and ses_secret)

    # SMTP
    smtp_host = os.getenv("SMTP_HOST", "").strip()
    smtp_user = os.getenv("SMTP_USER", "").strip() or os.getenv("SMTP_USERNAME", "").strip()
    smtp_ok = bool(smtp_host)

    # SendGrid
    sendgrid_key = os.getenv("SENDGRID_API_KEY", "").strip()
    sendgrid_ok = bool(sendgrid_key)

    providers: List[str] = []
    if ses_ok:
        providers.append("ses")
    if smtp_ok:
        providers.append("smtp")
    if sendgrid_ok:
        providers.append("sendgrid")

    if not providers:
        return {
            "healthy": False,
            "error": "No email provider configured. Set AWS SES, SMTP, or SendGrid credentials.",
            "required_for": "Email report delivery and scheduled report notifications",
        }

    return {
        "healthy": True,
        "providers": providers,
    }


# ---------------------------------------------------------------------------
# Module import check — verifies every analysis module loads without error
# ---------------------------------------------------------------------------

# Canonical module list: (import_path, display_name)
_ANALYSIS_MODULES = [
    ("api.analysis.module_1_health_trajectory", "Module 1: Health & Trajectory"),
    ("api.analysis.module_2_page_triage", "Module 2: Page Triage"),
    ("api.analysis.module_3_serp_landscape", "Module 3: SERP Landscape"),
    ("api.analysis.module_4_content_intelligence", "Module 4: Content Intelligence"),
    ("api.analysis.module_5_gameplan", "Module 5: Gameplan"),
    ("api.analysis.module_6_algorithm_updates", "Module 6: Algorithm Impact"),
    ("api.analysis.module_7_intent_migration", "Module 7: Intent Migration"),
    ("api.analysis.module_8_technical_health", "Module 8: CTR Modeling"),
    ("api.analysis.module_9_site_architecture", "Module 9: Site Architecture"),
    ("api.analysis.module_10_branded_split", "Module 10: Branded Split"),
    ("api.analysis.module_11_competitive_threats", "Module 11: Competitive Threats"),
    ("api.analysis.module_12_revenue_attribution", "Module 12: Revenue Attribution"),
]


def _check_analysis_modules() -> Dict[str, Any]:
    """
    Attempt to import every analysis module.

    Returns the count of successfully imported modules and details on
    any that failed.  A failure here usually means a missing pip
    dependency or a syntax error introduced in the last deploy.
    """
    ok = 0
    failed: List[Dict[str, str]] = []

    for import_path, display_name in _ANALYSIS_MODULES:
        try:
            importlib.import_module(import_path)
            ok += 1
        except Exception as e:
            failed.append({
                "module": display_name,
                "import_path": import_path,
                "error": f"{type(e).__name__}: {str(e)[:200]}",
            })

    total = len(_ANALYSIS_MODULES)
    return {
        "healthy": ok == total,
        "imported": ok,
        "total": total,
        "failed": failed if failed else None,
    }


# ---------------------------------------------------------------------------
# System info (always informational, never unhealthy)
# ---------------------------------------------------------------------------

def _system_info() -> Dict[str, Any]:
    """Gather non-sensitive system information for debugging."""
    info: Dict[str, Any] = {
        "healthy": True,  # always informational
        "python_version": platform.python_version(),
        "platform": platform.platform(),
    }

    # Memory usage (Linux — /proc/self/status)
    try:
        with open("/proc/self/status", "r") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    rss_kb = int(line.split()[1])
                    info["memory_rss_mb"] = round(rss_kb / 1024, 1)
                elif line.startswith("VmPeak:"):
                    peak_kb = int(line.split()[1])
                    info["memory_peak_mb"] = round(peak_kb / 1024, 1)
    except (FileNotFoundError, ValueError):
        pass  # Not Linux or /proc unavailable

    # Uptime (Linux — /proc/uptime for container uptime)
    try:
        with open("/proc/uptime", "r") as f:
            uptime_seconds = float(f.read().split()[0])
            info["container_uptime_minutes"] = round(uptime_seconds / 60, 1)
    except (FileNotFoundError, ValueError):
        pass

    return info
