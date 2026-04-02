"""
Scheduled Report Trigger — calls the API's /api/schedules/trigger endpoint.

This script is the cron component that makes scheduled reports actually
execute.  Users create schedules via the UI (POST /api/schedules/create)
with a frequency (weekly/biweekly/monthly) and next_run_at timestamp.
The API endpoint POST /api/schedules/trigger finds all due schedules and
runs them.  THIS script is what calls that endpoint on a regular cadence.

Deployment options:
  1. Railway cron service — run every 15 minutes
  2. GitHub Actions scheduled workflow — run every 15 minutes
  3. External cron service (e.g. cron-job.org) — hit the URL on schedule
  4. Railway deploy hook — trigger via webhook

Environment variables:
  API_BASE_URL    — Railway API service URL (required)
  CRON_SECRET     — shared secret matching the API's CRON_SECRET (required)
  SUPABASE_URL    — for logging results to build_log (optional)
  SUPABASE_KEY    — service role key for Supabase (optional)

Usage:
  python schedule_trigger.py              # Run once (for cron)
  python schedule_trigger.py --loop 900   # Run every 900s (15 min) in a loop
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [schedule-trigger] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

API_BASE_URL = os.getenv(
    "API_BASE_URL",
    os.getenv(
        "RAILWAY_API_URL",
        "https://search-intel-api-production.up.railway.app",
    ),
)
CRON_SECRET = os.getenv("CRON_SECRET", "")
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", os.getenv("SUPABASE_KEY", ""))
TRIGGER_ENDPOINT = f"{API_BASE_URL.rstrip('/')}/api/schedules/trigger"
HEALTH_ENDPOINT = f"{API_BASE_URL.rstrip('/')}/health"
REQUEST_TIMEOUT = 600  # 10 minutes — schedules can take a while


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _check_api_health() -> bool:
    """Verify the API is up before triggering schedules."""
    try:
        req = Request(HEALTH_ENDPOINT, method="GET")
        resp = urlopen(req, timeout=15)
        data = json.loads(resp.read().decode("utf-8"))
        if data.get("status") == "ok":
            logger.info("API health check passed: %s", HEALTH_ENDPOINT)
            return True
        logger.warning("API health check returned unexpected status: %s", data)
        return False
    except Exception as exc:
        logger.error("API health check failed (%s): %s", HEALTH_ENDPOINT, exc)
        return False


def _trigger_schedules() -> Dict[str, Any]:
    """
    Call POST /api/schedules/trigger with the cron secret.

    Returns the API response as a dict, or an error dict on failure.
    """
    headers = {
        "Content-Type": "application/json",
        "X-Cron-Secret": CRON_SECRET,
    }

    req = Request(
        TRIGGER_ENDPOINT,
        data=b"{}",
        headers=headers,
        method="POST",
    )

    try:
        resp = urlopen(req, timeout=REQUEST_TIMEOUT)
        body = json.loads(resp.read().decode("utf-8"))
        logger.info(
            "Trigger response: processed=%d, succeeded=%d, failed=%d",
            body.get("processed", 0),
            body.get("succeeded", 0),
            body.get("failed", 0),
        )
        return body

    except HTTPError as exc:
        error_body = ""
        try:
            error_body = exc.read().decode("utf-8")
        except Exception:
            pass
        logger.error(
            "Trigger request failed with HTTP %d: %s",
            exc.code, error_body or str(exc),
        )
        return {
            "error": True,
            "http_status": exc.code,
            "message": error_body or str(exc),
        }

    except URLError as exc:
        logger.error("Trigger request failed (network): %s", exc.reason)
        return {"error": True, "message": str(exc.reason)}

    except Exception as exc:
        logger.error("Trigger request failed: %s", exc)
        return {"error": True, "message": str(exc)}


def _log_to_supabase(result: Dict[str, Any]) -> None:
    """
    Optionally log the trigger result to the search_intel_build_log table.

    This creates visibility in the same build log that Shane checks each
    morning, so scheduled report executions are trackable alongside the
    nightly build agent runs.
    """
    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.debug("Supabase not configured — skipping log write")
        return

    processed = result.get("processed", 0)
    succeeded = result.get("succeeded", 0)
    failed = result.get("failed", 0)
    is_error = result.get("error", False)

    if processed == 0 and not is_error:
        # No schedules were due — don't clutter the log
        return

    if is_error:
        task = "Schedule trigger: API error"
        status = "fail"
        notes = f"Error: {result.get('message', 'unknown')}"
    else:
        task = f"Schedule trigger: {processed} due, {succeeded} succeeded, {failed} failed"
        status = "pass" if failed == 0 else "fail"
        details = result.get("details", [])
        detail_lines = []
        for d in details[:5]:  # Cap at 5 to keep notes readable
            domain = d.get("domain", "?")
            s = d.get("status", "?")
            detail_lines.append(f"  - {domain}: {s}")
        notes = "\n".join(detail_lines) if detail_lines else "All schedules processed"

    row = {
        "task": task,
        "status": status,
        "score_before": 100,
        "score_after": 100,
        "notes": notes,
    }

    try:
        import urllib.request
        url = f"{SUPABASE_URL}/rest/v1/search_intel_build_log"
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal",
        }
        req = Request(url, data=json.dumps(row).encode(), headers=headers, method="POST")
        urlopen(req, timeout=15)
        logger.info("Logged trigger result to Supabase build_log")
    except Exception as exc:
        logger.warning("Failed to log to Supabase: %s", exc)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_once() -> Dict[str, Any]:
    """Run a single trigger cycle."""
    logger.info("=" * 60)
    logger.info("Schedule trigger starting at %s", datetime.now(timezone.utc).isoformat())
    logger.info("API endpoint: %s", TRIGGER_ENDPOINT)

    if not CRON_SECRET:
        logger.error("CRON_SECRET not set — cannot authenticate with the API")
        return {"error": True, "message": "CRON_SECRET not configured"}

    # Health check first — skip trigger if API is down
    if not _check_api_health():
        logger.warning("Skipping trigger — API is not healthy")
        return {"error": True, "message": "API health check failed"}

    # Trigger due schedules
    result = _trigger_schedules()

    # Log to Supabase for visibility
    _log_to_supabase(result)

    logger.info("Schedule trigger complete")
    return result


def main():
    """Entry point — supports single run or looping mode."""
    # Parse --loop argument
    loop_interval = 0
    if "--loop" in sys.argv:
        idx = sys.argv.index("--loop")
        if idx + 1 < len(sys.argv):
            try:
                loop_interval = int(sys.argv[idx + 1])
            except ValueError:
                logger.error("--loop requires an integer (seconds)")
                sys.exit(1)
        else:
            loop_interval = 900  # Default: 15 minutes

    if loop_interval > 0:
        logger.info("Running in loop mode: every %d seconds", loop_interval)
        while True:
            try:
                run_once()
            except Exception as exc:
                logger.exception("Unexpected error in trigger loop: %s", exc)
            logger.info("Sleeping %d seconds until next check...", loop_interval)
            time.sleep(loop_interval)
    else:
        result = run_once()
        if result.get("error"):
            sys.exit(1)


if __name__ == "__main__":
    main()
