"""
Search Intelligence Report — Nightly Autoresearch Loop
DO NOT MODIFY — this is the autoresearch engine itself.
The agent builds files in /api and /web, not this file.
"""

import os
import re
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

RAILWAY_API = "https://backboard.railway.app/graphql/v2"

def get_latest_deployment_status(service_name="search-intel-api"):
    query = """query { me { projects { edges { node { services { edges { node {
        name deployments(first:1) { edges { node { status url createdAt } } }
    } } } } } } } }"""
    try:
        r = httpx.post(RAILWAY_API,
            headers={"Authorization": f"Bearer {RAILWAY_TOKEN}", "Content-Type": "application/json"},
            json={"query": query}, timeout=30)
        projects = r.json().get("data", {}).get("me", {}).get("projects", {}).get("edges", [])
        for p in projects:
            for s in p.get("node", {}).get("services", {}).get("edges", []):
                node = s.get("node", {})
                if node.get("name") == service_name:
                    deps = node.get("deployments", {}).get("edges", [])
                    if deps:
                        dep = deps[0].get("node", {})
                        return {"status": dep.get("status", "unknown"), "url": dep.get("url", "")}
    except Exception as e:
        print(f"Railway API error: {e}")
    return {"status": "unknown", "url": ""}

def wait_for_deployment(service_name="search-intel-api", timeout_seconds=180):
    print(f"Waiting for {service_name} deployment...")
    start = time.time()
    while time.time() - start < timeout_seconds:
        result = get_latest_deployment_status(service_name)
        status = result.get("status", "unknown")
        print(f"  Deployment: {status}")
        if status == "SUCCESS":
            return True
        elif status in ("FAILED", "CRASHED", "REMOVED"):
            return False
        time.sleep(10)
    print(f"  Timed out after {timeout_seconds}s")
    return False

def verify_endpoint(url, path="/health", expected_key="status", expected_value="ok"):
    if not url:
        return False, "No API_URL set"
    try:
        full_url = f"{url.rstrip('/')}{path}"
        print(f"  Verifying: GET {full_url}")
        r = httpx.get(full_url, timeout=15)
        if r.status_code == 200:
            data = r.json()
            if data.get(expected_key) == expected_value:
                return True, f"{full_url} → {data}"
            return False, f"Unexpected response: {data}"
        return False, f"HTTP {r.status_code}"
    except Exception as e:
        return False, f"Error: {e}"

def run_deployment_checks(day):
    if not API_URL:
        return True, "API_URL not set yet — skipping live checks"
    passed, msg = verify_endpoint(API_URL, "/health", "status", "ok")
    if not passed:
        return False, f"Health check failed: {msg}"
    day_checks = {
        6: ["/api/gsc/test"], 7: ["/api/ga4/test"],
        8: ["/api/modules/health"], 13: ["/api/reports"],
    }
    for path in day_checks.get(day, []):
        try:
            r = httpx.get(f"{API_URL.rstrip('/')}{path}", timeout=10)
            if r.status_code in (404, 500, 502, 503):
                return False, f"{path} returned {r.status_code}"
            print(f"  {path} → {r.status_code} ✓")
        except Exception as e:
            return False, f"{path} unreachable: {e}"
    return True, msg

def read_program_md():
    return repo.get_contents("program.md").decoded_content.decode("utf-8")

def read_spec():
    return repo.get_contents("supabase/spec.md").decoded_content.decode("utf-8")

def update_program_md(content, message):
    f = repo.get_contents("program.md")
    repo.update_file("program.md", message, content, f.sha)

def push_file(path, content, message):
    if path == "cron/loop.py":
        print(f"  PROTECTED — skipped: {path}")
        return
    try:
        existing = repo.get_contents(path)
        repo.update_file(path, message, content, existing.sha)
    except Exception:
        repo.create_file(path, message, content)

def get_next_task(program):
    for line in program.splitlines():
        if line.strip().startswith("- [ ] **DAY"):
            try:
                day = int(line.split("**DAY")[1].split("**")[0].strip())
                desc = line.split("—", 1)[1].strip() if "—" in line else line.strip()
                return day, desc
            except Exception:
                continue
    return 0, "No tasks remaining"

def mark_task_complete(program, day):
    lines = program.splitlines()
    return "\n".join(
        line.replace("- [ ]", "- [x]", 1)
        if (f"- [ ] **DAY {day:02d}**" in line or f"- [ ] **DAY {day}**" in line)
        else line
        for line in lines
    )

def update_current_state(program, day, task, status):
    lines, result, in_state = program.splitlines(), [], False
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
                nd, nt = get_next_task(program)
                line = f"**Next Task:** DAY {nd:02d} — {nt[:60]}"
        result.append(line)
    return "\n".join(result)

def count_completed(program):
    return program.count("- [x]")

def commit_files(result, message):
    for f in result.get("files", []):
        if f["path"] != "cron/loop.py":
            push_file(f["path"], f["content"], message)
            print(f"  Pushed: {f['path']}")
    try:
        return list(repo.get_commits())[0].html_url
    except Exception:
        return None

def log_to_supabase(day, task, status, notes, commit_url=None, duration=None):
    supabase.table("build_log").insert({
        "day": day, "run_date": datetime.date.today().isoformat(),
        "task": task, "status": status, "notes": notes,
        "commit_url": commit_url, "duration_seconds": duration
    }).execute()

def post_to_slack(day, task, status, notes, commit_url=None, completed=0, total=28):
    icon = "✅" if status == "pass" else "❌"
    bar = "█" * min(completed, total) + "░" * (total - min(completed, total))
    blocks = [
        {"type": "header", "text": {"type": "plain_text",
            "text": f"🌙 Search Intel — {datetime.date.today().strftime('%B %d, %Y')}"}},
        {"type": "section", "fields": [
            {"type": "mrkdwn", "text": f"*Task:*\nDAY {day:02d} — {task[:60]}"},
            {"type": "mrkdwn", "text": f"*Status:*\n{icon} {status.title()}"}
        ]}
    ]
    if notes:
        blocks.append({"type": "section",
            "text": {"type": "mrkdwn", "text": f"*Notes:*\n{notes[:400]}"}})
    blocks.append({"type": "section",
        "text": {"type": "mrkdwn",
            "text": f"*Phase 1:* {completed}/{total}\n`{bar}`"}})
    if commit_url:
        blocks.append({"type": "section",
            "text": {"type": "mrkdwn", "text": f"*Commit:* <{commit_url}|View>"}})
    try:
        slack.chat_postMessage(channel=SLACK_CHANNEL, blocks=blocks)
    except SlackApiError as e:
        print(f"Slack error: {e}")

PLAN_PROMPT = """You are an autonomous build agent for the Search Intelligence Report.

Output a JSON plan with NO file contents:
{
    "status": "pass"|"fail",
    "task_summary": "one line",
    "notes": "details for Shane",
    "commit_message": "git message",
    "failure_reason": "if fail",
    "shrunk_task": "if fail, smaller scope",
    "files_to_write": [{"path": "path/from/root", "description": "what it does"}]
}

Rules: Valid JSON only. Never include cron/loop.py. Build only what the task requires."""

FILE_PROMPT = """Write ONLY the raw file content for the Search Intelligence Report project.
No JSON wrapper, no markdown fences, no explanation.
Start with the first character. End with the last.
Production-quality code, proper error handling, follow the spec exactly."""

def run_agent(program, spec, day, task):
    plan_r = anthropic.messages.create(
        model="claude-sonnet-4-5", max_tokens=2000, system=PLAN_PROMPT,
        messages=[{"role": "user", "content":
            f"Task: DAY {day:02d}: {task}\n\nprogram.md:\n<program>\n{program}\n</program>\n\n"
            f"Spec (40k):\n<spec>\n{spec[:40000]}\n</spec>\n\nJSON plan only:"}]
    )
    raw = plan_r.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"): raw = raw[4:]
    if raw.endswith("```"): raw = raw[:-3]
    plan = json.loads(raw.strip())

    files_to_write = [f for f in plan.get("files_to_write", [])
                      if f["path"] != "cron/loop.py"]
    print(f"Plan: {plan.get('status')} | Files: {[f['path'] for f in files_to_write]}")

    if plan.get("status") == "fail":
        return {"status": "fail", "task_summary": plan.get("task_summary", task[:60]),
                "notes": plan.get("notes", ""), "failure_reason": plan.get("failure_reason", ""),
                "shrunk_task": plan.get("shrunk_task", ""), "files": []}

    files = []
    for fi in files_to_write:
        print(f"  Writing: {fi['path']}")
        fr = anthropic.messages.create(
            model="claude-sonnet-4-5", max_tokens=6000, system=FILE_PROMPT,
            messages=[{"role": "user", "content":
                f"File: {fi['path']}\nDescription: {fi.get('description','')}\n"
                f"Task: DAY {day:02d} — {task}\nSpec:\n<spec>\n{spec[:40000]}\n</spec>\n"
                f"Write ONLY the raw file content:"}]
        )
        content = fr.content[0].text
        if content.strip().startswith("```"):
            lines = content.strip().split("\n")
            content = "\n".join(lines[1:])
            if content.strip().endswith("```"): content = content.strip()[:-3]
        files.append({"path": fi["path"], "content": content})

    return {"status": "pass", "task_summary": plan.get("task_summary", task[:60]),
            "notes": plan.get("notes", ""),
            "commit_message": plan.get("commit_message", f"DAY {day:02d}: {task[:50]}"),
            "files": files}

def main():
    start = time.time()
    print(f"\n{'='*60}\nSearch Intel Loop — {datetime.date.today()}\n{'='*60}\n")
    try:
        program = read_program_md()
        spec = read_spec()
        day, task = get_next_task(program)

        if day == 0:
            post_to_slack(0, "Phase 1 complete!", "pass", "28/28 done.", completed=28)
            return

        print(f"Task: DAY {day:02d} — {task[:80]}")
        result = run_agent(program, spec, day, task)
        status = result.get("status", "fail")
        notes = result.get("notes", "")
        task_summary = result.get("task_summary", task[:60])
        commit_msg = result.get("commit_message", f"DAY {day:02d}: {task[:50]}")

        commit_url = None
        if status == "pass" and result.get("files"):
            commit_url = commit_files(result, commit_msg)
            deployed = wait_for_deployment("search-intel-api", 180)
            if deployed or not API_URL:
                ok, msg = run_deployment_checks(day)
                if not ok:
                    status, notes = "fail", f"Committed but endpoint failed: {msg}"
                else:
                    notes = f"{notes}\nVerified: {msg}"
            else:
                status, notes = "fail", "Committed but Railway deployment failed"

        if status == "pass":
            updated = mark_task_complete(program, day)
            completed_count = count_completed(updated)
            updated = update_current_state(updated, day, task_summary, "✅ Pass")
            update_program_md(updated, f"DAY {day:02d} complete: {task_summary[:50]}")
        else:
            completed_count = count_completed(program)
            failure = result.get("failure_reason", notes or "Unknown")
            notes = notes or f"FAILED: {failure}"
            updated = update_current_state(program, day, task_summary, "❌ Fail")
            update_program_md(updated, f"DAY {day:02d} failed: {failure[:50]}")

        duration = int(time.time() - start)
        log_to_supabase(day, task_summary, "pass" if status=="pass" else "fail",
                        notes, commit_url, duration)
        post_to_slack(day, task_summary, "pass" if status=="pass" else "fail",
                      notes[:500], commit_url, completed_count)
        print(f"\nDone in {duration}s")

    except Exception as e:
        err = traceback.format_exc()
        print(f"FATAL:\n{err}")
        try:
            log_to_supabase(0, "cron loop", "fail", err[:1000])
            post_to_slack(0, "Cron loop", "fail", f"Fatal error:\n```{str(e)[:300]}```")
        except Exception:
            pass

if __name__ == "__main__":
    main()
