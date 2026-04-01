"""
DEPRECATED — this file is intentionally kept as a tombstone.

This integration test tested the legacy api/modules/module_01_health.py and
api/modules/module_02_triage.py implementations which have been replaced by
the canonical api/analysis/ modules:
  - api/analysis/module_1_health_trajectory.py
  - api/analysis/module_2_page_triage.py

The tests imported from api.modules.module_01_health and api.modules.module_02_triage
which are now tombstones — running this file would crash at import time.

To test the canonical modules, write new tests that import from api.analysis.*.
"""
