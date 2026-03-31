"""
Search Intelligence Report — Autonomous Build Loop
Inspired by Karpathy's autoresearch pattern.

Each night:
1. Evaluate the current state of the codebase objectively
2. If there are problems — fix the worst one, verify the fix
3. If no problems — identify and build the next most valuable thing
4. Keep if better, revert if worse
5. Log and notify

The agent decides what to do. The metric decides if it worked.
DO NOT MODIFY THIS FILE — it is the autonomous loop itself.
"""

import os
import ast
import json
import time
import datetime
import traceback
import subprocess
import httpx
from supabase import create_client
from anthropic import Anthropic
from github import Github
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# ── Clients ────────────────────────────────────────────────────────────────────

SUPABASE_URL  = os.environ["SUPABASE_URL"]
SUPABASE_KEY  = os.environ["SUPABASE_SERVICE_KEY"]
ANTHROPIC_KEY = os.environ["ANTHROPIC_API_KEY"]
SLACK_TOKEN   = os.environ["SLACK_BOT_TOKEN"]
SLACK_CHANNEL = os.environ["SLACK_CHANNEL_ID"]
GITHUB_TOKEN  = os.environ["GITHUB_TOKEN"]
RAILWAY_TOKEN = os.environ["RAILWAY_TOKEN"]
GITHUB_REPO   = "martinshane/search-intel"
API_URL       = os.environ.get("API_URL", "")

supabase  = create_client(SUPABASE_URL, SUPABASE_KEY)
anthropic = Anthropic(api_key=ANTHROPIC_KEY)
slack     = WebClient(token=SLACK_TOKEN)
gh        = Github(GITHUB_TOKEN)
repo      = gh.get_repo(GITHUB_REPO)

# ── Codebase Evaluation ────────────────────────────────────────────────────────

def scan_syntax_errors() -> list[dict]:
    """Find all Python files with syntax errors by fetching from GitHub."""
    errors = []
    try:
        contents = repo.get_contents("api")
        stack = list(contents)
        while stack:
            item = stack.pop()
            if item.type == "dir":
                stack.extend(repo.get_contents(item.path))
            elif item.path.endswith(".py") and item.path != "cron/loop.py":
                try:
                    code = item.decoded_content.decode("utf-8")
                    ast.parse(code)
                except SyntaxError as e:
                    errors.append({
                        "path": item.path,
                        "error": str(e),
                        "line": e.lineno,
                        "code_snippet": code[:500]
                    })
    except Exception as e:
        print(f"Scan error: {e}")
    return errors

def check_live_endpoints() -> list[dict]:
    """Hit live endpoints and report what's broken."""
    if not API_URL:
        return []
    
    failures = []
    checks = [
        ("/health", "GET", {"status": "ok"}),
        ("/api/v1/reports", "GET", None),   # Should return 401, not 500
        ("/auth/google", "GET", None),       # Should redirect, not 500
    ]
    
    for path, method, expected in checks:
        try:
            url = f"{API_URL.rstrip('/')}{path}"
            r = httpx.get(url, timeout=10, follow_redirects=False)
            if r.status_code in (500, 502, 503, 504):
                failures.append({
                    "path": path,
                    "status": r.status_code,
                    "body": r.text[:200]
                })
            elif expected and r.status_code == 200:
                data = r.json()
                for k, v in expected.items():
                    if data.get(k) != v:
                        failures.append({
                            "path": path,
                            "status": r.status_code,
                            "body": f"Expected {k}={v}, got {data}"
                        })
        except Exception as e:
            failures.append({"path": path, "status": "unreachable", "body": str(e)})
    
    return failures

def get_missing_files() -> list[str]:
    """Find files that are imported but don't exist."""
    missing = []
    known_missing = []
    
    try:
        # Check auth module specifically - known gap
        auth_init = repo.get_contents("api/auth/__init__.py").decoded_content.decode()
        for line in auth_init.splitlines():
            if line.startswith("from ."):
                module = line.split("from .")[1].split(" ")[0]
                try:
                    repo.get_contents(f"api/auth/{module}.py")
                except Exception:
                    missing.append(f"api/auth/{module}.py")
    except Exception:
        pass
    
    return missing

def evaluate_codebase() -> dict:
    """
    Full objective evaluation of codebase health.
    Returns a score and list of problems, ordered by severity.
    """
    print("Evaluating codebase...")
    
    syntax_errors = scan_syntax_errors()
    endpoint_failures = check_live_endpoints()
    missing_files = get_missing_files()
    
    problems = []
    
    # Syntax errors are critical — code won't run
    for err in syntax_errors:
        problems.append({
            "severity": "critical",
            "type": "syntax_error",
            "file": err["path"],
            "description": f"SyntaxError at line {err['line']}: {err['error']}",
            "detail": err
        })
    
    # Missing files are critical — imports will fail
    for f in missing_files:
        problems.append({
            "severity": "critical",
            "type": "missing_file",
            "file": f,
            "description": f"File imported but does not exist: {f}",
            "detail": {"path": f}
        })
    
    # Endpoint failures are high severity
    for fail in endpoint_failures:
        problems.append({
            "severity": "high",
            "type": "endpoint_failure",
            "file": fail["path"],
            "description": f"Endpoint {fail['path']} returning {fail['status']}",
            "detail": fail
        })
    
    score = 100 - (len([p for p in problems if p["severity"] == "critical"]) * 20) \
                - (len([p for p in problems if p["severity"] == "high"]) * 10)
    score = max(0, score)
    
    print(f"Score: {score}/100 | Critical: {len([p for p in problems if p['severity']=='critical'])} | High: {len([p for p in problems if p['severity']=='high'])}")
    
    return {
        "score": score,
        "problems": problems,
        "syntax_errors": len(syntax_errors),
        "endpoint_failures": len(endpoint_failures),
        "missing_files": len(missing_files)
    }

# ── GitHub Helpers ─────────────────────────────────────────────────────────────

def get_file_content(path: str) -> str | None:
    """Get current content of a file from GitHub."""
    try:
        return repo.get_contents(path).decoded_content.decode("utf-8")
    except Exception:
        return None

def push_file(path: str, content: str, message: str):
    """Create or update a file. Never touches cron/loop.py."""
    if path == "cron/loop.py":
        print(f"  PROTECTED — skipped: {path}")
        return
    try:
        existing = repo.get_contents(path)
        repo.update_file(path, message, content, existing.sha)
    except Exception:
        repo.create_file(path, message, content)
    print(f"  Pushed: {path}")

def revert_file(path: str, original_content: str, message: str):
    """Revert a file to its original content."""
    push_file(path, original_content, message)
    print(f"  Reverted: {path}")

def get_latest_commit_url() -> str:
    try:
        return list(repo.get_commits())[0].html_url
    except Exception:
        return ""

def read_spec() -> str:
    try:
        return repo.get_contents("supabase/spec.md").decoded_content.decode("utf-8")
    except Exception:
        return ""

# ── Railway ────────────────────────────────────────────────────────────────────

RAILWAY_API = "https://backboard.railway.app/graphql/v2"

def wait_for_deployment(timeout=180) -> bool:
    if not API_URL:
        return True  # Can't check, assume ok
    print("Waiting for deployment...")
    start = time.time()
    while time.time() - start < timeout:
        try:
            r = httpx.get(f"{API_URL}/health", timeout=10)
            if r.status_code == 200:
                print("  Deployment live.")
                return True
        except Exception:
            pass
        time.sleep(10)
    return False

# ── Supabase + Slack ───────────────────────────────────────────────────────────

def log_run(action: str, status: str, score_before: int, score_after: int,
            notes: str, commit_url: str = "", duration: int = 0):
    row = {
        "run_date": datetime.date.today().isoformat(),
        "task": action,
        "status": status,
        "notes": f"Score: {score_before} \u2192 {score_after}\n{notes}",
        "commit_url": commit_url,
        "duration_seconds": duration
    }

    # Write to search-intel Supabase project (app data)
    try:
        supabase.table("build_log").insert({**row, "day": 0}).execute()
    except Exception as e:
        print(f"search-intel log error: {e}")

    # Also write to main Supabase project (Cowork can read this)
    main_url = os.environ.get("MAIN_SUPABASE_URL")
    main_key = os.environ.get("MAIN_SUPABASE_KEY")
    if main_url and main_key:
        try:
            from supabase import create_client as _mk
            main_sb = _mk(main_url, main_key)
            main_sb.table("search_intel_build_log").insert({
                **row,
                "score_before": score_before,
                "score_after": score_after,
            }).execute()
            print("  Logged to main Supabase ✓")
        except Exception as e:
            print(f"Main Supabase log error: {e}")

def post_to_slack(action: str, status: str, score_before: int, score_after: int,
                  notes: str, commit_url: str = ""):
    icon = "✅" if status == "pass" else ("↩️" if status == "reverted" else "❌")
    delta = score_after - score_before
    delta_str = f"+{delta}" if delta > 0 else str(delta)
    color_bar = "█" * (score_after // 10) + "░" * (10 - score_after // 10)

    blocks = [
        {"type": "header", "text": {"type": "plain_text",
            "text": f"🌙 Search Intel — {datetime.date.today().strftime('%B %d, %Y')}"}},
        {"type": "section", "fields": [
            {"type": "mrkdwn", "text": f"*Action:*\n{action[:80]}"},
            {"type": "mrkdwn", "text": f"*Result:*\n{icon} {status.title()}"}
        ]},
        {"type": "section", "fields": [
            {"type": "mrkdwn", "text": f"*Score:*\n{score_before} → {score_after} ({delta_str})"},
            {"type": "mrkdwn", "text": f"*Health:*\n`{color_bar}` {score_after}/100"}
        ]}
    ]
    if notes:
        blocks.append({"type": "section",
            "text": {"type": "mrkdwn", "text": f"*Notes:*\n{notes[:400]}"}})
    if commit_url:
        blocks.append({"type": "section",
            "text": {"type": "mrkdwn", "text": f"*Commit:* <{commit_url}|View changes>"}})
    try:
        slack.chat_postMessage(channel=SLACK_CHANNEL, blocks=blocks)
    except SlackApiError as e:
        print(f"Slack error: {e}")

# ── Agent ──────────────────────────────────────────────────────────────────────

PLAN_SYSTEM = """You are an autonomous software engineer building the Search Intelligence Report —
a free web tool that generates a 12-module SEO analysis report for any site connected via GSC + GA4 OAuth.
It is the front door to a search consulting business at clankermarketing.com.

The tool has 4 phases:
1. MVP (Phase 1): OAuth + data ingestion + modules 1, 2, 5 + basic report UI — largely complete, fixing syntax errors
2. SERP Intelligence (Phase 2): DataForSEO + modules 3, 8, 11 — competitor mapping, CTR modeling, SERP features
3. Deep Analysis (Phase 3): modules 4, 6, 7, 9 — content intelligence, algorithm impacts, site architecture graph
4. Revenue & Polish (Phase 4): modules 10, 12 + PDF export + email delivery + consulting CTAs

Each night you receive a codebase health score and problem list. Make ONE improvement.

Priority order (strict):
1. Fix syntax errors — code that won't parse blocks everything
2. Fix missing files that break imports
3. Fix endpoint failures (500s on live API)
4. Complete incomplete module implementations per the spec
5. Wire modules into the report pipeline
6. Build frontend visualizations for completed modules
7. Add Phase 2 features (DataForSEO, modules 3/8/11)
8. Add Phase 3 features (modules 4/6/7/9, site crawler)
9. Add Phase 4 features (modules 10/12, PDF, email, CTAs)

Respond with ONLY a JSON plan — NO file contents:
{
    "action": "one-line description of what you are doing",
    "reasoning": "why this is the highest priority improvement right now",
    "commit_message": "concise git commit message",
    "files_to_write": [
        {
            "path": "relative/path/from/repo/root",
            "description": "exactly what this file should contain and what it fixes/adds"
        }
    ]
}

Rules:
- Output ONLY valid JSON. NO file contents in this response.
- NEVER include cron/loop.py in files_to_write
- Max 2 files per run
- All files must be under api/, web/, cron/, supabase/, or tests/
- Never create files in backend/, src/, or any other directory
"""

FILE_SYSTEM = """You are writing a single Python or TypeScript file for the Search Intelligence Report project.

Write ONLY the raw file content. No JSON, no markdown fences, no explanation.
Start with the first character of the file. End with the last character.
Write complete, production-quality code. Never truncate.
"""

def get_broken_file_contents(problems: list) -> str:
    """Fetch actual content of broken files from GitHub so agent can fix them."""
    broken = [p for p in problems if p["type"] == "syntax_error"][:2]
    if not broken:
        return ""
    result = []
    for p in broken:
        path = p["file"]
        file_content = get_file_content(path)
        if file_content:
            result.append(f"\n--- BROKEN FILE: {path} ---\n{file_content}\n--- END {path} ---")
    return "\n".join(result)

def run_agent(evaluation: dict, spec: str) -> dict:
    """
    Two-pass agent:
    Pass 1 — JSON plan (no file contents, always valid JSON)
    Pass 2 — Raw file content per file (no JSON parsing issues)
    """
    problems_text = "\n".join([
        f"  [{p['severity'].upper()}] {p['type']}: {p['description']}"
        for p in evaluation["problems"][:10]
    ])
    broken_files_text = get_broken_file_contents(evaluation["problems"])

    # Pass 1: Plan only — no file contents
    plan_message = f"""Codebase health: {evaluation['score']}/100

Problems ({len(evaluation['problems'])} total):
{problems_text if problems_text else "  None"}

{broken_files_text}

Output JSON plan only — no file contents."""

    plan_response = anthropic.messages.create(
        model="claude-sonnet-4-5",
        max_tokens=2000,
        system=PLAN_SYSTEM,
        messages=[{"role": "user", "content": plan_message}]
    )

    raw = plan_response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"): raw = raw[4:]
    if raw.endswith("```"): raw = raw[:-3]

    plan = json.loads(raw.strip())
    action = plan.get("action", "Unknown")
    print(f"Plan: {action}")

    files_to_write = [f for f in plan.get("files_to_write", [])
                      if f["path"] != "cron/loop.py"
                      and any(f["path"].startswith(d) for d in ("api/", "web/", "cron/", "supabase/", "tests/"))]
    print(f"Files: {[f['path'] for f in files_to_write]}")

    # Pass 2: Write each file as raw content — no JSON parsing
    files = []
    for fi in files_to_write:
        path = fi["path"]
        desc = fi.get("description", "")
        current = get_file_content(path)
        current_section = f"\nCurrent broken content:\n{current[:3000]}" if current else ""

        file_response = anthropic.messages.create(
            model="claude-sonnet-4-5",
            max_tokens=16000,
            system=FILE_SYSTEM,
            messages=[{"role": "user", "content":
                f"Write the complete fixed content for: {path}\n"
                f"What it should do: {desc}\n"
                f"Spec reference:\n{spec[:20000]}"
                f"{current_section}\n\n"
                f"Write ONLY the raw file content starting now:"}]
        )

        file_content = file_response.content[0].text
        # Strip accidental fences
        if file_content.strip().startswith("```"):
            lines = file_content.strip().split("\n")
            file_content = "\n".join(lines[1:])
            if file_content.strip().endswith("```"):
                file_content = file_content.strip()[:-3]

        files.append({"path": path, "content": file_content})

    return {
        "action": action,
        "reasoning": plan.get("reasoning", ""),
        "commit_message": plan.get("commit_message", action[:50]),
        "files": files
    }


ALLOWED_DIRS = ("api/", "web/", "cron/", "supabase/", "tests/")

def validate_files(files: list[dict]) -> tuple[bool, list[str]]:
    """Syntax-check Python files and reject files in wrong directories."""
    errors = []
    for f in files:
        path = f["path"]
        # Reject files outside allowed directories
        if not any(path.startswith(d) for d in ALLOWED_DIRS):
            errors.append(f"{path}: REJECTED — not in allowed directory (api/, web/, cron/, supabase/, tests/)")
            continue
        if path.endswith(".py") and path != "cron/loop.py":
            try:
                ast.parse(f["content"])
            except SyntaxError as e:
                errors.append(f"{path}: SyntaxError at line {e.lineno}: {e.msg}")
    return len(errors) == 0, errors

# ── Main Loop ──────────────────────────────────────────────────────────────────

def main():
    start = time.time()
    today = datetime.date.today().isoformat()
    print(f"\n{'='*60}\nSearch Intel Autonomous Loop — {today}\n{'='*60}\n")

    try:
        spec = read_spec()

        # Step 1: Evaluate current state
        evaluation_before = evaluate_codebase()
        score_before = evaluation_before["score"]

        # Step 2: Ask agent what to improve
        print("\nAsking agent what to fix/build...")
        agent_result = run_agent(evaluation_before, spec)

        action = agent_result.get("action", "Unknown action")
        reasoning = agent_result.get("reasoning", "")
        files = [f for f in agent_result.get("files", [])
                 if f["path"] != "cron/loop.py"]
        commit_msg = agent_result.get("commit_message", action[:50])

        print(f"Action: {action}")
        print(f"Files: {[f['path'] for f in files]}")

        if not files:
            msg = "Agent returned no files to write"
            print(msg)
            log_run(action, "skip", score_before, score_before, msg)
            post_to_slack(action, "skip", score_before, score_before, msg)
            return

        # Step 3: Validate before committing
        valid, syntax_errs = validate_files(files)
        if not valid:
            msg = f"Agent wrote invalid Python — not committing:\n" + "\n".join(syntax_errs)
            print(msg)
            log_run(action, "fail", score_before, score_before, msg)
            post_to_slack(action, "fail", score_before, score_before, msg)
            return

        # Step 4: Save originals for potential rollback
        originals = {}
        for f in files:
            original = get_file_content(f["path"])
            if original:
                originals[f["path"]] = original

        # Step 5: Commit the changes
        print("Committing changes...")
        for f in files:
            push_file(f["path"], f["content"], commit_msg)
        commit_url = get_latest_commit_url()

        # Step 6: Wait for deployment
        wait_for_deployment(timeout=120)

        # Step 7: Re-evaluate — did we actually improve?
        print("\nRe-evaluating after change...")
        time.sleep(5)  # Brief pause for deployment to settle
        evaluation_after = evaluate_codebase()
        score_after = evaluation_after["score"]

        if score_after >= score_before:
            # Improvement or neutral — keep it
            status = "pass"
            notes = (f"{reasoning}\n\n"
                    f"Fixed: {evaluation_before['syntax_errors'] - evaluation_after['syntax_errors']} syntax errors, "
                    f"{evaluation_before['missing_files'] - evaluation_after['missing_files']} missing files")
            print(f"Score improved: {score_before} → {score_after} ✅ Keeping changes")
        else:
            # Got worse — revert
            status = "reverted"
            notes = (f"Score dropped {score_before} → {score_after}. Reverting.\n"
                    f"Reasoning was: {reasoning}")
            print(f"Score dropped: {score_before} → {score_after} ↩️ Reverting")
            for path, original_content in originals.items():
                revert_file(path, original_content, f"revert: {commit_msg}")
            score_after = score_before  # Back to where we were

        # Step 8: Log and notify
        duration = int(time.time() - start)
        log_run(action, status, score_before, score_after, notes, commit_url, duration)
        post_to_slack(action, status, score_before, score_after, notes, commit_url)
        print(f"\nDone in {duration}s — Final score: {score_after}/100")

    except Exception as e:
        err = traceback.format_exc()
        print(f"FATAL:\n{err}")
        try:
            log_to_supabase = lambda: supabase.table("build_log").insert({
                "day": 0, "run_date": datetime.date.today().isoformat(),
                "task": "cron loop", "status": "fail",
                "notes": err[:1000]
            }).execute()
            log_to_supabase()
            post_to_slack("Cron loop", "fail", 0, 0,
                         f"Fatal error:\n```{str(e)[:300]}```")
        except Exception:
            pass

if __name__ == "__main__":
    main()
