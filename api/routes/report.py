"""
DEPRECATED — this file is intentionally kept as a tombstone.

The report generation and module execution routes have been refactored into:
  - api/routes/modules.py   — individual module execution endpoints (GET /api/v1/modules/{n}/{report_id})
  - api/routers/reports.py  — report CRUD, PDF export, email delivery, consulting CTAs

This file previously contained a monolithic generate_report_sync() function that
tried to orchestrate all 12 analysis modules in a single request.  That design was
replaced by the per-module architecture in routes/modules.py which allows:
  1. Independent module execution and retry
  2. Progress tracking per module
  3. Partial report delivery when some modules fail

Do NOT add new code here.  Delete this file once the refactor is fully verified.
"""
