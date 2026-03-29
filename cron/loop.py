"""
Search Intelligence Report — Nightly Autoresearch Loop
Reads program.md, executes next task, logs result, posts to Slack.
Runs every night at 11pm via Railway cron.
"""

import os
import time
import datetime
import traceback
import json
import httpx
from supabase import create_client
from anthropic import Anthropic
from github import Github
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# ── Clients ────────────────────────────────────────────────────────────────────

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
ANTHROPIC_KEY = os.environ["ANTHROPIC_API_KEY"]
SLACK_TOKEN   = os.environ["SLACK_BOT_TOKEN"]
SLACK_CHANNEL = os.environ["SLACK_CHANNEL_ID"]
GITHUB_TOKEN  = os.environ["GITHUB_TOKEN"]
GITHUB_REPO   = "martinshane/search-intel"

supabase  = create_client(SUPABASE_URL, SUPABASE_KEY)
anthropic = Anthropic(api_key=ANTHROPIC_KEY)
slack     = WebClient(token=SLACK_TOKEN)
gh        = Github(GITHUB_TOKEN)
repo      = gh.get_repo(GITHUB_REPO)

# ── Helpers ────────────────────────────────────────────────────────────────────

def read_program_md() -> str:
    """Read program.md from the GitHub repo (always latest version)."""
    contents = repo.get_contents("program.md")
    return contents.decoded_content.decode("utf-8")

def read_spec() -> str:
    """Read the technical spec from the repo."""
    contents = repo.get_contents("supabase/spec.md")
    return contents.decoded_content.decode("utf-8")

def update_program_md(new_content: str, commit_message: str):
    """Push updated program.md back to the repo."""
    contents = repo.get_contents("program.md")
    repo.update_file(
        path="program.md",
        message=commit_message,
        content=new_content,
        sha=contents.sha
    )

def push_file(path: str, content: str, commit_message: str):
    """Create or update a file in the repo."""
    try:
        existing = repo.get_contents(path)
        repo.update_file(path, commit_message, content, existing.sha)
    except Exception:
        repo.create_file(path, commit_message, content)

def get_next_task(program: str) -> tuple:
    """Find the next unchecked task in the task queue. Returns (day, task_description)."""
    for line in program.splitlines():
        if line.strip().startswith("- [ ] **DAY"):
            try:
                day_part = line.split("**DAY")[1].split("**")[0].strip()
                day = int(day_part)
                desc = line.split("—", 1)[1].strip() if "—" in line else line.strip()
                return day, desc
            except Exception:
                continue
    return 0, "No tasks remaining"

def mark_task_complete(program: str, day: int) -> str:
    """Mark a task as complete in program.md."""
    lines = program.splitlines()
    result = []
    for line in lines:
        if f"- [ ] **DAY {day:02d}**" in line or f"- [ ] **DAY {day}**" in line:
            line = line.replace("- [ ]", "- [x]", 1)
        result.append(line)
    return "\n".join(result)

def update_current_state(program: str, day: int, task: str, status: str) -> str:
    """Update the Current State section in program.md."""
    lines = program.splitlines()
    result = []
    in_state = False

    for line in lines:
        if line.startswith("## Current State"):
            in_state = True
        elif line.startswith("## ") and in_state:
            in_state = False

        if in_state:
            if line.startswith("**Current Day:**"):
                line = f"**Current Day:** {day}"
            elif line.startswith("**Last Task:**"):
                line = f"**Last Task:** {task[:80]}"
            elif line.startswith("**Last Run:**"):
                line = f"**Last Run:** {datetime.date.today().isoformat()} — {status}"
            elif line.startswith("**Next Task:**") and "Pass" in status:
                next_day, next_task = get_next_task(program)
                line = f"**Next Task:** DAY {next_day:02d} — {next_task[:60]}"

        result.append(line)
    return "\n".join(result)

def log_to_supabase(day: int, task: str, status: str, notes: str,
                    commit_url: str = None, duration: int = None):
    """Write a row to the build_log table."""
    supabase.table("build_log").insert({
        "day": day,
        "run_date": datetime.date.today().isoformat(),
        "task": task,
        "status": status,
        "notes": notes,
        "commit_url": commit_url,
        "duration_seconds": duration
    }).execute()

def post_to_slack(day: int, task: str, status: str, notes: str,
                  commit_url: str = None, completed: int = 0, total: int = 28,
                  web_url: str = None):
    """Post nightly summary to #search-intel-builds."""
    icon = "✅" if status == "pass" else "❌"
    filled = min(completed, total)
    progress_bar = "█" * filled + "░" * (total - filled)

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"🌙 Search Intel Build — {datetime.date.today().strftime('%B %d, %Y')}"
            }
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Task:*\nDAY {day:02d} — {task[:60]}"},
                {"type": "mrkdwn", "text": f"*Status:*\n{icon} {status.title()}"}
            ]
        }
    ]

    if notes:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Notes:*\n{notes[:400]}"}
        })

    blocks.append({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": f"*Phase 1 Progress:* {completed}/{total}\n`{progress_bar}`"
        }
    })

    if commit_url:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Commit:* <{commit_url}|View changes>"}
        })

    if web_url:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Dashboard:* <{web_url}|Open build dashboard>"}
        })

    try:
        slack.chat_postMessage(channel=SLACK_CHANNEL, blocks=blocks)
    except SlackApiError as e:
        print(f"Slack error: {e}")

def count_completed_tasks(program: str) -> int:
    """Count how many tasks are marked complete."""
    return program.count("- [x]")

def commit_files(result: dict, commit_message: str) -> str:
    """Push all files from the agent result to GitHub. Returns latest commit URL."""
    for file_obj in result.get("files", []):
        path = file_obj["path"]
        content = file_obj["content"]
        push_file(path, content, commit_message)
        print(f"  Pushed: {path}")

    try:
        commits = list(repo.get_commits())
        if commits:
            return commits[0].html_url
    except Exception:
        pass
    return None

# ── System Prompt ───────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are an autonomous software build agent working on the Search Intelligence Report project.

Each night you receive program.md (current build state), the technical spec, and the current task.

Your job is to execute exactly one task. Output ONLY a valid JSON object — no preamble, no markdown fences.

CRITICAL: File contents must be base64-encoded to avoid JSON parsing errors. Use standard base64 encoding.

{
    "status": "pass" | "fail",
    "task_summary": "one line description of what was done",
    "notes": "detailed notes on what was built, decisions made, anything Shane should know",
    "files": [
        {
            "path": "relative/path/from/repo/root",
            "content_b64": "<base64-encoded file content>"
        }
    ],
    "commit_message": "concise git commit message",
    "failure_reason": "if status=fail, exactly why",
    "shrunk_task": "if status=fail, smaller scope to attempt tomorrow"
}

Rules:
- Output ONLY valid JSON. Nothing else.
- ALL file content MUST be base64-encoded in content_b64. Never use a plain "content" field.
- File content must be complete and production-quality before encoding.
- Follow the spec exactly for function signatures, schemas, file locations.
- Include proper error handling in all code.
- If you cannot complete the full task, fail gracefully and suggest a smaller scope.
- Tests must pass on synthetic data before marking pass.
"""

# ── Main Agent ──────────────────────────────────────────────────────────────────

def run_agent(program: str, spec: str, day: int, task: str) -> dict:
    """Call Claude API to execute the current task."""
    user_message = f"""Execute this task:

DAY {day:02d}: {task}

Current program.md:
<program>
{program}
</program>

Technical spec (first 50k chars):
<spec>
{spec[:50000]}
</spec>

IMPORTANT: Encode ALL file contents as base64 in the content_b64 field.
Output only valid JSON."""

    response = anthropic.messages.create(
        model="claude-sonnet-4-5",
        max_tokens=8000,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_message}]
    )

    raw = response.content[0].text.strip()

    # Strip markdown fences if present
    if raw.startswith("```"):
        parts = raw.split("```")
        raw = parts[1] if len(parts) > 1 else raw
        if raw.startswith("json"):
            raw = raw[4:]
    if raw.endswith("```"):
        raw = raw[:-3]

    raw = raw.strip()

    try:
        result = json.loads(raw)
    except json.JSONDecodeError as e:
        # Try to extract just the JSON object if there's surrounding text
        import re
        match = re.search(r'\{.*\}', raw, re.DOTALL)
        if match:
            result = json.loads(match.group())
        else:
            raise e

    # Decode base64 file contents
    import base64
    for file_obj in result.get("files", []):
        if "content_b64" in file_obj:
            try:
                file_obj["content"] = base64.b64decode(
                    file_obj["content_b64"]
                ).decode("utf-8")
            except Exception:
                # If decode fails, use raw value as fallback
                file_obj["content"] = file_obj["content_b64"]
        elif "content" not in file_obj:
            file_obj["content"] = ""

    return result

# ── Entry Point ─────────────────────────────────────────────────────────────────

def main():
    start_time = time.time()
    today = datetime.date.today().isoformat()
    print(f"\n{'='*60}")
    print(f"Search Intel Autoresearch Loop — {today}")
    print(f"{'='*60}\n")

    try:
        # 1. Read program.md and spec
        print("Reading program.md...")
        program = read_program_md()
        spec = read_spec()

        # 2. Find next task
        day, task = get_next_task(program)
        if day == 0:
            print("All tasks complete!")
            post_to_slack(0, "All Phase 1 tasks complete!", "pass",
                         "28/28 tasks done. Time to start Phase 2.",
                         completed=28, total=28)
            return

        print(f"Task: DAY {day:02d} — {task[:80]}")

        # 3. Run the agent
        print("Calling Claude API...")
        result = run_agent(program, spec, day, task)

        status       = result.get("status", "fail")
        notes        = result.get("notes", "")
        task_summary = result.get("task_summary", task[:60])
        commit_msg   = result.get("commit_message", f"DAY {day:02d}: {task[:50]}")

        print(f"Status: {status}")
        print(f"Notes: {notes[:200]}")

        # 4. Commit files + update program.md
        commit_url = None
        if status == "pass" and result.get("files"):
            print("Committing files...")
            commit_url = commit_files(result, commit_msg)
            updated = mark_task_complete(program, day)
            completed_count = count_completed_tasks(updated)
            updated = update_current_state(updated, day, task_summary, "✅ Pass")
            update_program_md(updated, f"DAY {day:02d} complete: {task_summary[:50]}")
            print(f"Tasks complete: {completed_count}/28")
        elif status == "pass":
            # Pass but no files (e.g. config-only task)
            updated = mark_task_complete(program, day)
            completed_count = count_completed_tasks(updated)
            updated = update_current_state(updated, day, task_summary, "✅ Pass")
            update_program_md(updated, f"DAY {day:02d} complete: {task_summary[:50]}")
            print(f"Tasks complete: {completed_count}/28")
        else:
            completed_count = count_completed_tasks(program)
            failure_reason  = result.get("failure_reason", "Unknown")
            shrunk          = result.get("shrunk_task", "Retry with smaller scope")
            notes = f"FAILED: {failure_reason}\nTomorrow: {shrunk}"
            updated = update_current_state(program, day, task_summary, "❌ Fail")
            update_program_md(updated, f"DAY {day:02d} failed: {failure_reason[:50]}")

        # 5. Log to Supabase
        duration = int(time.time() - start_time)
        log_to_supabase(
            day=day,
            task=task_summary,
            status="pass" if status == "pass" else "fail",
            notes=notes,
            commit_url=commit_url,
            duration=duration
        )

        # 6. Post to Slack
        post_to_slack(
            day=day,
            task=task_summary,
            status="pass" if status == "pass" else "fail",
            notes=notes[:500],
            commit_url=commit_url,
            completed=completed_count,
            total=28
        )

        print(f"\nDone in {duration}s")

    except Exception as e:
        error_msg = traceback.format_exc()
        print(f"FATAL ERROR:\n{error_msg}")
        try:
            log_to_supabase(
                day=0,
                task="cron loop itself",
                status="fail",
                notes=f"Fatal error:\n{error_msg[:1000]}"
            )
            post_to_slack(
                day=0,
                task="Cron loop",
                status="fail",
                notes=f"Fatal error — check Railway logs:\n```{str(e)[:300]}```"
            )
        except Exception:
            pass

if __name__ == "__main__":
    main()
