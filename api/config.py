"""
⚠️  TOMBSTONE — this file is intentionally empty.

The Settings class, APP_VERSION, and ``settings`` singleton that used to
live here have been moved to api/config/__init__.py (the package init).

WHY: Python's import system resolves ``from .config import ...`` to
api/config/ (directory) over api/config.py (file) when both exist.
The directory had no __init__.py, making it a namespace package that
lacked the settings object, causing ImportError on deployment.

Moving the code into api/config/__init__.py makes the directory a
proper regular package.  This flat-file module is kept as a tombstone
to prevent accidental re-creation of the conflict.

CANONICAL LOCATION: api/config/__init__.py
"""

# Re-export from the package so any stale imports still work.
# (In practice this file should never be imported — Python resolves
# the api/config/ package first — but belt-and-suspenders.)
try:
    from api.config import settings, APP_VERSION, get_settings, Settings  # noqa: F401
except ImportError:
    pass
