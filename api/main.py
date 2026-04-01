import logging
from contextlib import asynccontextmanager
from typing import Any, Dict

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from httpx import HTTPError, TimeoutException

from .config import settings, APP_VERSION

from .routers import health  # health always loads; others are lazy
from .middleware.rate_limiter import RateLimitMiddleware

# Configure logging
logging.basicConfig(
    level=logging.INFO if not settings.debug else logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager for startup and shutdown events."""
    # Startup
    logger.info("Starting Search Intelligence Report API v%s", APP_VERSION)
    logger.info("Environment: %s", settings.environment)
    logger.info("Debug mode: %s", settings.debug)
    logger.info(
        "CORS origins: %s (+ pattern matching for *.railway.app, *.vercel.app)",
        settings.get_cors_origins(),
    )

    # Check critical dependencies
    try:
        from .routers.health import detailed_health
        health_status = await detailed_health()
        if not health_status.get("healthy", False):
            logger.warning("Some dependencies are unhealthy:")
            for dep, status_info in health_status.get("dependencies", {}).items():
                if not status_info.get("healthy", False):
                    err_msg = status_info.get("error", "Unknown error")
                    logger.warning("  - %s: %s", dep, err_msg)
        else:
            logger.info("All dependencies are healthy")
    except Exception as e:
        logger.error("Failed to check dependencies on startup: %s", str(e))

    yield

    # Shutdown
    logger.info("Shutting down Search Intelligence Report API")


app = FastAPI(
    title="Search Intelligence Report API",
    description="Backend API for generating comprehensive search intelligence reports from GSC + GA4 data",
    version=APP_VERSION,
    lifespan=lifespan,
)

# ---------------------------------------------------------------------------
# CORS middleware — supports both explicit origins and regex patterns.
#
# Explicit origins come from settings.get_cors_origins() which merges
# env vars, FRONTEND_URL, ALLOWED_ORIGINS, and hard-coded production
# domains.  Regex patterns (e.g. *.railway.app) are handled by
# allow_origin_regex for Railway preview deploys and similar.
# ---------------------------------------------------------------------------

_cors_origins = settings.get_cors_origins()

# Build a combined regex from all patterns in config
_cors_patterns = settings.cors_origin_patterns
_cors_regex = None
if _cors_patterns:
    # Combine patterns with | and compile once
    combined = "|".join(f"({p})" for p in _cors_patterns)
    _cors_regex = combined

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_origin_regex=_cors_regex,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Rate limiting middleware — protects against abuse, especially for
# report generation (DataForSEO budget) and auth brute-force.
# Must be added AFTER CORSMiddleware so CORS preflight is not rate-limited.
# ---------------------------------------------------------------------------
app.add_middleware(RateLimitMiddleware)
logger.info("Rate limiting middleware enabled")


# Custom exception handlers
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    """Handle request validation errors with user-friendly messages."""
    errors = []
    for error in exc.errors():
        field = " -> ".join(str(x) for x in error["loc"][1:])
        message = error["msg"]
        errors.append({"field": field or "request", "message": message})

    logger.warning("Validation error on %s: %s", request.url.path, errors)

    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "error": "Invalid request data",
            "message": "Please check your input and try again",
            "details": errors,
        },
    )


@app.exception_handler(HTTPError)
async def http_exception_handler(request: Request, exc: HTTPError) -> JSONResponse:
    """Handle external HTTP errors with retry guidance."""
    logger.error("External HTTP error on %s: %s", request.url.path, str(exc))

    is_transient = False
    status_code = status.HTTP_502_BAD_GATEWAY

    if hasattr(exc, "response") and exc.response is not None:
        response_status = exc.response.status_code
        is_transient = response_status in [408, 429, 500, 502, 503, 504]
        status_code = status.HTTP_503_SERVICE_UNAVAILABLE if is_transient else status.HTTP_502_BAD_GATEWAY

    message = "An external service is temporarily unavailable" if is_transient else "Failed to communicate with external service"

    return JSONResponse(
        status_code=status_code,
        content={
            "error": "External service error",
            "message": message,
            "retry": is_transient,
            "details": str(exc) if settings.debug else None,
        },
    )


@app.exception_handler(TimeoutException)
async def timeout_exception_handler(request: Request, exc: TimeoutException) -> JSONResponse:
    """Handle timeout errors with retry guidance."""
    logger.error("Request timeout on %s: %s", request.url.path, str(exc))

    return JSONResponse(
        status_code=status.HTTP_504_GATEWAY_TIMEOUT,
        content={
            "error": "Request timeout",
            "message": "The operation took too long to complete. Please try again.",
            "retry": True,
        },
    )


@app.exception_handler(ValueError)
async def value_error_handler(request: Request, exc: ValueError) -> JSONResponse:
    """Handle value errors with user-friendly messages."""
    logger.warning("Value error on %s: %s", request.url.path, str(exc))

    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={
            "error": "Invalid data",
            "message": str(exc),
        },
    )


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Handle all other exceptions with safe error messages."""
    logger.exception("Unhandled exception on %s", request.url.path)

    message = "An unexpected error occurred. Our team has been notified."
    details = None

    if settings.debug:
        message = "Internal error: %s" % type(exc).__name__
        details = str(exc)

    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": "Internal server error",
            "message": message,
            "details": details,
        },
    )


# Request logging middleware
@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Log all incoming requests and responses."""
    logger.info("-> %s %s", request.method, request.url.path)

    try:
        response = await call_next(request)
        logger.info("<- %s %s - %s", request.method, request.url.path, response.status_code)
        return response
    except Exception as e:
        logger.exception("Request failed: %s %s", request.method, request.url.path)
        raise


# ---------------------------------------------------------------------------
# Include routers — each wrapped in try/except so one broken import
# does not prevent the remaining routers from loading.
#
# MOUNT STRATEGY (matches frontend URL expectations):
#   Auth:      /api/auth/*      (index.tsx, schedules.tsx)
#              /auth/*           (OAuth callback redirect URI compat)
#   Reports:   /api/v1/reports/* (report/[id].tsx, compare.tsx)
#              /api/reports/*    (index.tsx, progress.tsx)
#              /reports/*        (legacy / direct API callers)
#   Schedules: /schedules/*     (schedules.tsx)
#   Modules:   /api/v1/modules/* (individual module endpoints)
# ---------------------------------------------------------------------------
app.include_router(health.router, prefix="/health", tags=["Health"])

try:
    from .routers.auth import router as auth_router
    # Primary mount — matches frontend fetch("/api/auth/status") etc.
    app.include_router(auth_router, prefix="/api/auth", tags=["Authentication"])
    # Secondary mount — keeps OAuth redirect URIs at /auth/callback working
    app.include_router(auth_router, prefix="/auth", tags=["Authentication (compat)"])
    logger.info("Auth routes loaded at /api/auth and /auth")
except Exception as e:
    logger.warning("Could not load auth routes (will retry on first request): %s", e)

try:
    from .routers.reports import router as reports_router
    # Primary mount — matches report/[id].tsx fetch("/api/v1/reports/...")
    app.include_router(reports_router, prefix="/api/v1/reports", tags=["Reports"])
    # Secondary mount — matches index.tsx fetch("/api/reports/generate")
    app.include_router(reports_router, prefix="/api/reports", tags=["Reports (v0)"])
    # Tertiary mount — direct /reports/* for backward compat
    app.include_router(reports_router, prefix="/reports", tags=["Reports (compat)"])
    logger.info("Report routes loaded at /api/v1/reports, /api/reports, /reports")
except Exception as e:
    logger.warning("Could not load report routes: %s", e)

try:
    from .routers.schedules import router as schedules_router
    app.include_router(schedules_router, prefix="/schedules", tags=["Schedules"])
    logger.info("Schedule routes loaded at /schedules")
except Exception as e:
    logger.warning("Could not load schedule routes: %s", e)

try:
    from .routes.modules import router as modules_router
    app.include_router(modules_router, prefix="/api/v1/modules", tags=["Modules"])
    logger.info("Module routes loaded at /api/v1/modules")
except Exception as e:
    logger.warning("Could not load module routes: %s", e)


@app.get("/", include_in_schema=False)
async def root() -> Dict[str, Any]:
    """Root endpoint with API information."""
    return {
        "message": "Search Intelligence Report API",
        "version": APP_VERSION,
        "status": "operational",
        "docs": "/docs",
        "health": "/health",
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.debug,
        log_level="debug" if settings.debug else "info",
    )
