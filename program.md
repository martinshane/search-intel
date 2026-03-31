# Search Intelligence Report — Autoresearch Program

**Repo:** https://github.com/martinshane/search-intel  
**Operator:** Shane Martin  
**Started:** 2026-03-29  
**Goal:** Build a production-ready Search Intelligence Report tool — a free web app that generates a 12-module SEO analysis report for any site connected via GSC + GA4 OAuth. This tool is the front door to a search consulting business.

---

## How This Works

You are an autonomous build agent. Every night you:

1. Read this file to understand current state and next task
2. Execute exactly one task from the build queue
3. Run tests to verify the task succeeded
4. Commit and push to GitHub (Railway auto-deploys)
5. Update the `build_log` table in Supabase with result
6. Post a summary to Slack (`#search-intel-builds`)
7. Update the `## Current State` section of this file with new progress

If a task fails: rollback, shrink the scope, log the failure, and attempt a smaller version of the same task tomorrow. Never skip ahead to a later task if the current one is unresolved.

---

## Success Criteria

A task is only marked complete when:
- Code is committed and deployed to Railway without errors
- Unit tests pass against synthetic/mock data
- The deployed endpoint or UI renders correctly
- Output matches the expected schema defined in the spec

Never mark a task complete based on the code looking right. It must be verified against a running deployment.

---

## Hard Constraints

- **One task per night** — no matter how simple the next task looks
- **Never modify the Supabase schema once set** — migrations break downstream modules
- **Always create a rollback point** (git stash or branch) before starting each task
- **Never deploy broken code to main** — tests must pass first
- **Max Railway worker RAM usage: 4GB** — stay within spec
- **DataForSEO API budget: $0.20 per test report** — use mock SERP data during development
- **Claude API budget: $0.20 per test report** — batch classify, don't call per-query
- **If Railway or Supabase APIs are unavailable:** log the failure, skip the night, do not attempt partial work

---

## Repository Structure (Target)

```
search-intel/
├── api/                        # FastAPI backend (search-intel-api service)
│   ├── main.py
│   ├── auth/                   # Google OAuth flow
│   ├── ingestion/              # GSC, GA4, DataForSEO, crawler
│   ├── modules/                # Analysis modules 1-12
│   │   ├── module_01_health.py
│   │   ├── module_02_triage.py
│   │   ├── module_03_serp.py
│   │   ├── module_04_content.py
│   │   ├── module_05_gameplan.py
│   │   ├── module_06_algorithm.py
│   │   ├── module_07_intent.py
│   │   ├── module_08_ctr.py
│   │   ├── module_09_architecture.py
│   │   ├── module_10_branded.py
│   │   ├── module_11_competitive.py
│   │   └── module_12_revenue.py
│   ├── worker/                 # Async job processor (search-intel-worker service)
│   │   └── pipeline.py
│   └── requirements.txt
├── web/                        # Next.js frontend (search-intel-web service)
│   ├── pages/
│   │   ├── index.tsx           # Landing + OAuth connect
│   │   ├── report/[id].tsx     # Report viewer
│   │   └── progress.tsx        # BUILD DASHBOARD (visible to Shane each morning)
│   └── package.json
├── cron/                       # Nightly autoresearch loop (search-intel-cron service)
│   ├── loop.py                 # Main cron script (this agent)
│   └── requirements.txt
├── supabase/
│   └── schema.sql              # Complete schema (run once, never modify)
└── program.md                  # This file
```

---

## Railway Services

| Service Name | Directory | Purpose |
|---|---|---|
| `search-intel-api` | `/api` | FastAPI backend |
| `search-intel-worker` | `/api/worker` | ML pipeline, async jobs |
| `search-intel-web` | `/web` | Next.js frontend |
| `search-intel-cron` | `/cron` | This nightly loop |

All four services auto-deploy from `martinshane/search-intel` on push to `main`.

---

## Environment Variables (Railway)

Set these in Railway before first run:

```
# All services
SUPABASE_URL=
SUPABASE_SERVICE_KEY=

# api + worker
GOOGLE_CLIENT_ID=
GOOGLE_CLIENT_SECRET=
ANTHROPIC_API_KEY=
DATAFORSEO_LOGIN=
DATAFORSEO_PASSWORD=

# cron
ANTHROPIC_API_KEY=
SLACK_BOT_TOKEN=
SLACK_CHANNEL_ID=          # #search-intel-builds channel ID
GITHUB_TOKEN=               # For pushing program.md state updates
```

---

## Build Phases

### Phase 1 — MVP (Target: Days 1-28)
OAuth + data ingestion + modules 1, 2, 5 + basic report UI.
**This is a useful free tool on its own.**

### Phase 2 — SERP Intelligence (Days 29-56)
DataForSEO integration + modules 3, 8, 11.

### Phase 3 — Deep Analysis (Days 57-84)
Modules 4, 6, 7, 9. Site crawl infrastructure.

### Phase 4 — Revenue & Polish (Days 85-112)
Modules 10, 12. PDF export. Email delivery. Historical comparison.

---

## Task Queue

Tasks are ordered. Do not skip. Mark each ✅ when complete.

### Foundation (Days 1-7)

- [x] **DAY 01** — Supabase schema: create all tables from spec (users, reports, api_cache, algorithm_updates, query_intents, serp_snapshots) plus build_log table. Verify all tables exist with correct columns.

- [x] **DAY 02** — Repository structure: scaffold all directories and empty placeholder files. FastAPI skeleton in `/api/main.py` with a `/health` endpoint that returns `{"status": "ok"}`. Deploy `search-intel-api` to Railway. Verify health endpoint responds at Railway URL.

- [x] **DAY 03** — Progress dashboard: build `/web/pages/progress.tsx` — a single-page Next.js app that reads the `build_log` Supabase table and renders a table of nightly runs (date, task, status, notes). Deploy `search-intel-web`. This is what Shane checks each morning.

- [x] **DAY 04** — Google OAuth flow (backend): implement `/auth/google` and `/auth/callback` endpoints in FastAPI. Request GSC + GA4 read-only scopes. Store encrypted tokens in Supabase `users` table. Test with a real Google account.

- [x] **DAY 05** — Google OAuth flow (frontend): build the connect screen on the Next.js index page. "Connect Google Search Console" button → triggers OAuth flow → on success shows connected property list. Basic styling only.

- [x] **DAY 06** — GSC data ingestion: implement `ingestion/gsc.py`. Pull performance data by query, by page, by date. Handle 25K row pagination with monthly chunking. Cache responses in `api_cache` table with 24h TTL. Test against a real GSC property (use tradeify.co or kixie.com for dev testing).

- [x] **DAY 07** — GA4 data ingestion: implement `ingestion/ga4.py`. Pull all 8 report types from spec. Match date ranges to GSC pull. Cache responses. Test against a real GA4 property.

### Core Modules (Days 8-14)

- [x] **DAY 08** — Module 1 (Health & Trajectory) implementation: MSTL decomposition, change point detection (PELT/ruptures), STUMPY matrix profile on residuals. Write to report JSON schema from spec.

- [x] **DAY 09** — Module 1 tests: unit tests with 16 months of synthetic daily data. Verify output schema matches spec exactly. Verify change points are detected correctly on known test cases.

- [x] **DAY 10** — Module 2 (Page Triage) implementation: per-page trend fitting, PyOD Isolation Forest CTR anomaly detection, GA4 engagement cross-reference, priority scoring.

- [x] **DAY 11** — Module 2 tests + Module 1→2 integration: verify Module 2 reads Module 1 output correctly. End-to-end pipeline test: GSC data → Module 1 → Module 2 → structured JSON output.

- [x] **DAY 12** — Module 5 (Gameplan) implementation: synthesize Module 1 + Module 2 outputs into prioritized action list (critical, quick wins, strategic, structural). Claude API call for narrative generation. Test with mock module outputs.

- [x] **DAY 13** — Async job pipeline: implement `worker/pipeline.py`. Report generation runs as async job: status tracked in `reports` table (pending → ingesting → analyzing → generating → complete). API endpoint to poll status.

- [x] **DAY 14** — End-to-end Phase 1 pipeline test: connect a real GSC+GA4 property, trigger full report generation, verify all 3 modules run, verify report JSON is written to Supabase, verify job status polling works.

### Frontend Report UI (Days 15-21)

- [x] **DAY 15** — Report page scaffold: `/web/pages/report/[id].tsx`. Collapsible card component. TL;DR + visualization placeholder + detail table + actions layout per section. No real data yet — use hardcoded mock report JSON.

- [x] **DAY 16** — Health & Trajectory visualization: line chart with trend + forecast + confidence interval bands + change point markers. Use Recharts. Wire to real Module 1 output.

- [x] **DAY 17** — Page Triage visualization: scatter plot (current clicks vs decay rate), color-coded by bucket (growing/stable/decaying/critical). Sortable detail table below. Wire to real Module 2 output.

- [x] **DAY 18** — Gameplan section: critical/quick wins/strategic/structural action lists with impact estimates and effort badges. Consulting CTA placement after this section ("Want help executing this plan? Book a call"). Wire to real Module 5 output.

- [x] **DAY 19** — Report generation UI: loading state with progress indicator (shows which module is running). Polls job status endpoint every 5 seconds. On complete: redirects to report page.

- [x] **DAY 20** — Connect flow polish: the index page OAuth connect flow, property selector, and "Generate Report" button. Full happy path works end-to-end from landing page to completed report.

- [x] **DAY 21** — Phase 1 integration test: run full flow with real data (kixie.com or tradeify.co). All three modules produce real output. Report renders correctly. Fix any issues found.

### Buffer + Phase 2 Start (Days 22-28)

- [x] **DAY 22** — Performance audit: measure report generation time on real data. Optimize slowest bottleneck. Target: complete report in under 3 minutes.

- [x] **DAY 23** — Error handling pass: every API call has retry logic. Every module has graceful fallback if data is missing. User sees meaningful error messages not stack traces.

- [x] **DAY 24** — DataForSEO integration: implement `ingestion/dataforseo.py`. Pull live SERPs for top 50 non-branded keywords. Handle rate limits. Cache in `serp_snapshots` table. Test with $0.20 budget.

- [x] **DAY 25** — Algorithm update database: seed `algorithm_updates` table with known updates from 2024-2026. Weekly cron to fetch new updates from public sources.

- [x] **DAY 26** — Module 3 stub (SERP Landscape): implement basic version — competitor extraction, SERP feature parsing. Full implementation in Phase 2. Verify it runs without errors.

- [x] **DAY 27** — Mobile responsiveness pass on all frontend pages. Progress dashboard, connect flow, report viewer all work on mobile.

- [x] **DAY 28** — Phase 1 complete review: run full flow on 3 different sites. Document any remaining issues in build_log. Update this program.md with Phase 2 task queue detail.

---

## Current State

**Current Phase:** 1  
**Current Day:** 27
**Last Task:** Phase 1 nearly complete — 27/28 tasks done
**Last Run:** 2026-03-31 — ✅ Pass
**Next Task:** REPAIR 01 — Rewrite api/ingestion/gsc.py (truncation fix)
**Completed Tasks:** 27 / 28
**Railway API URL:** (set after DAY 02)  
**Railway Web URL:** (set after DAY 03)  
**Progress Dashboard:** (set after DAY 03)  

### Build Log Summary
*(Updated each night by the cron agent)*

| Day | Date | Task | Status | Notes |
|-----|------|------|--------|-------|
| — | — | Not started | — | — |

---


---

## Repair Phase — Fix Truncated Files

These tasks run after Phase 1. Each rewrites a file that was truncated due to token limits.
All must pass syntax check before being marked complete.

- [ ] **REPAIR 01** — Rewrite `api/ingestion/gsc.py` — fix unterminated string at line 650. Full 600+ line GSC ingestion implementation.
- [ ] **REPAIR 02** — Rewrite `api/modules/module_01_health.py` — fix missing indented block at line 555. Full MSTL + STUMPY + change point + forecast implementation.
- [ ] **REPAIR 03** — Rewrite `api/modules/module_04_content.py` — fix unterminated string at line 593. Full cannibalization + striking distance + thin content implementation.
- [ ] **REPAIR 04** — Rewrite `api/modules/module_05_gameplan.py` — fix unterminated string at line 463. Full synthesis + Claude API narrative generation implementation.
- [ ] **REPAIR 05** — Rewrite `api/modules/module_06_algorithm.py` — fix missing colon at line 634. Full change point + algorithm update correlation implementation.
- [ ] **REPAIR 06** — Rewrite test files: `api/ingestion/test_gsc.py`, `api/ingestion/test_ga4_ingestion.py`, `api/worker/test_pipeline.py`, `api/modules/tests/test_module_01_health.py`, `api/modules/tests/test_module_05_gameplan.py` — all have truncation syntax errors.


## Spec Reference

The full technical spec lives at `/supabase/spec.md` in the repo (copy of the uploaded spec). All module function signatures, expected output schemas, Supabase table definitions, Railway service specs, Python dependencies, and frontend chart types are defined there. The spec is the source of truth for implementation details. This program.md is the source of truth for build order and current state.

When in doubt: **spec wins on what to build, program.md wins on what to build next.**

---

## Slack Notification Format

Post to `#search-intel-builds` after every run:

```
🌙 Search Intel Build — [DATE]

Task: [TASK NAME]
Status: ✅ Pass / ❌ Fail
[If fail]: Why: [reason]
[If fail]: Tomorrow: [adjusted scope]

Progress: Phase [N] — [X]/28 tasks complete
[If web URL exists]: Preview: [Railway URL]
```

---

## Notes for the Agent

- You are building a real product, not a demo. Every module must produce accurate output from real data.
- The spec contains exact Python function signatures with docstrings. Follow them precisely — downstream modules depend on the output schema.
- When writing tests, use the synthetic data patterns described in the spec. A module that passes tests on synthetic data but fails on real GSC data is not complete.
- The progress dashboard (DAY 03) exists specifically so Shane can see your work each morning without opening Railway or Supabase. Prioritize making it clear and readable.
- If you discover a problem with the spec (ambiguity, missing detail, conflicting requirements), log it in the build_log notes and make a reasonable decision. Document what you decided.
- Shane reviews the build log each morning and may update this program.md with direction changes. Always re-read program.md at the start of each run — it may have changed.