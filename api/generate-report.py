"""
DEPRECATED — this file is intentionally kept as a tombstone.

This was the original monolithic report generation script with bare absolute
imports (from modules.xxx, from data_ingestion.xxx) that do not resolve in
the current package structure.  It also initialised Supabase at module level
using SUPABASE_ANON_KEY which crashes when the env var is unset.

Report generation has been refactored into:
  - api/routes/modules.py   — per-module execution endpoints
  - api/routers/reports.py  — report CRUD, PDF export, email delivery
  - api/helpers/             — GSC, GA4, SERP, crawl data fetching
  - api/analysis/            — all 12 analysis module implementations

Do NOT add new code here.  Delete this file once the refactor is fully verified.
"""
