"""
TOMBSTONE — This file is dead code, kept only as a historical reference.

The canonical report generation pipeline lives in:
  - api/worker/report_runner.py  (ingestion orchestration + Supabase I/O)
  - api/worker/pipeline.py       (12-module analysis pipeline with progress callbacks)

This legacy file imported from api.services.modules.* — a package that
was never created (the canonical modules live in api/analysis/*).
All 13 imports were broken, causing an ImportError on any import attempt.

The active code path for report generation is:
  POST /reports/create  →  api.routers.reports  →  api.worker.report_runner.run_report_pipeline()
                            →  api.worker.pipeline.AnalysisPipeline.execute()
                                →  api.analysis.module_1_health_trajectory ... module_12_revenue_attribution

Do not import from this file. See api/worker/ for the working implementation.
"""
