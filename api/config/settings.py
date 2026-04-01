"""
DEPRECATED — this file is intentionally kept as a tombstone.

This was an older, plain-class Settings implementation that duplicated and
conflicted with api/config.py (the canonical Pydantic-based configuration).

Having both api/config.py (module) and api/config/ (directory) creates a
Python import ambiguity: depending on sys.path order and Python version,
``from api.config import settings`` could resolve to either the module or
the namespace package — leading to subtle, hard-to-debug import failures.

The original file also called settings.validate() at module level, which
crashed with ValueError when required env vars (SECRET_KEY, GOOGLE_CLIENT_ID,
etc.) were not set.

Canonical configuration lives in:
  - api/config.py  — Pydantic BaseSettings with env-var loading, CORS helpers,
                      and the APP_VERSION constant used by health.py and main.py

Do NOT add new code here.  Delete this file (and the api/config/ directory)
once the codebase has been fully audited for any stale references.
"""
