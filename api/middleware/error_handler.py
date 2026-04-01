"""
TOMBSTONE — api/middleware/error_handler.py

This file contained a BaseHTTPMiddleware subclass (ErrorHandlerMiddleware) that
duplicated all exception handling already defined inline in api/main.py.

It was never imported or registered by main.py (which uses @app.exception_handler
decorators and a request-logging @app.middleware("http") directly).  The 16KB of
dead code was a maintenance hazard — any future developer might try to add it as
middleware, creating duplicate error handling with conflicting behavior.

Exception handling is centralized in:
    api/main.py  →  exception handlers + request logging middleware

Replaced with this tombstone on 2026-03-31 by the build agent.
"""
