import logging
from contextlib import asynccontextmanager
from typing import Any, Dict

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from httpx import HTTPError, TimeoutException

from .config import settings, APP_VERSION
from .routers import health
from .middleware.rate_limiter import RateLimitMiddleware
from .config.env_validator import validate_environment

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

    # Validate environment configuration
    logger.info("Validating environment configuration...")
    try:
        validation_result = validate_environment()
        
        if not validation_result["valid"]:
            logger.error("Environment validation FAILED:")
            for error in validation_result["errors"]:
                logger.error("  - %s", error)
            
            if validation_result["critical_errors"]:
                logger.critical("Critical configuration errors found. API cannot start safely.")
                raise RuntimeError(
                    f"Critical environment validation failed: {', '.join(validation_result['critical_errors'])}"
                )
            else:
                logger.warning("Non-critical validation errors found. API will start but some features may not work.")
        else:
            logger.info("Environment validation passed")
            if validation_result.get("warnings"):
                for warning in validation_result["warnings"]:
                    logger.warning("  - %s", warning)
    except Exception as e:
        logger.critical("Failed to validate environment: %s", str(e))
        raise

    # Check integration health
    try:
        from .routers.health import detailed_health
        health_status = await detailed_health()
        
        if not health_status.get("healthy", False):
            logger.warning("Some integrations are unhealthy:")
            for dep, status_info in health_status.get("dependencies", {}).items():
                if not status_info.get("healthy", False):
                    err_msg = status_info.get("error", "Unknown error")
                    logger.warning("  - %s: %s", dep, err_msg)
        else:
            logger.info("All integrations are healthy")
            
        # Log individual integration statuses
        deps = health_status.get("dependencies", {})
        if deps:
            logger.info("Integration status:")
            for dep_name, dep_info in deps.items():
                status_str = "✓ healthy" if dep_info.get("healthy") else "✗ unhealthy"
                logger.info("  - %s: %s", dep_name, status_str)
                
    except Exception as e:
        logger.error("Failed to check integration health on startup: %s", str(e))

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
# ---------------------------------------------------------------------------

_cors_origins = settings.get_cors_origins()
_cors_patterns = settings.cors_origin_patterns
_cors_regex = None

if _cors_patterns:
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
# Rate limiting middleware
# ---------------------------------------------------------------------------

app.add_middleware(RateLimitMiddleware)

# ---------------------------------------------------------------------------
# Exception handlers
# ---------------------------------------------------------------------------


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Handle validation errors with detailed messages."""
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "error": "Validation Error",
            "detail": exc.errors(),
            "body": exc.body,
        },
    )


@app.exception_handler(HTTPError)
async def http_exception_handler(request: Request, exc: HTTPError):
    """Handle HTTP errors from external API calls."""
    logger.error("HTTP error: %s", str(exc))
    return JSONResponse(
        status_code=status.HTTP_502_BAD_GATEWAY,
        content={
            "error": "External API Error",
            "detail": str(exc),
        },
    )


@app.exception_handler(TimeoutException)
async def timeout_exception_handler(request: Request, exc: TimeoutException):
    """Handle timeout errors from external API calls."""
    logger.error("Timeout error: %s", str(exc))
    return JSONResponse(
        status_code=status.HTTP_504_GATEWAY_TIMEOUT,
        content={
            "error": "Request Timeout",
            "detail": "The external service took too long to respond",
        },
    )


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Handle all other exceptions."""
    logger.exception("Unhandled exception: %s", str(exc))
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": "Internal Server Error",
            "detail": str(exc) if settings.debug else "An unexpected error occurred",
        },
    )


# ---------------------------------------------------------------------------
# Routers
# ---------------------------------------------------------------------------

app.include_router(health.router, prefix="/health", tags=["Health"])

# Lazy load other routers to avoid import errors if dependencies are missing
try:
    from .routers import auth
    app.include_router(auth.router, prefix="/api/auth", tags=["Authentication"])
except ImportError as e:
    logger.warning("Could not load auth router: %s", str(e))

try:
    from .routers import data_ingestion
    app.include_router(data_ingestion.router, prefix="/api/data", tags=["Data Ingestion"])
except ImportError as e:
    logger.warning("Could not load data_ingestion router: %s", str(e))

try:
    from .routers import reports
    app.include_router(reports.router, prefix="/api/reports", tags=["Reports"])
except ImportError as e:
    logger.warning("Could not load reports router: %s", str(e))

try:
    from .routers import analysis
    app.include_router(analysis.router, prefix="/api/analysis", tags=["Analysis"])
except ImportError as e:
    logger.warning("Could not load analysis router: %s", str(e))


# ---------------------------------------------------------------------------
# Root endpoint
# ---------------------------------------------------------------------------


@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "name": "Search Intelligence Report API",
        "version": APP_VERSION,
        "environment": settings.environment,
        "status": "operational",
        "docs": "/docs",
        "health": "/health",
    }


# ---------------------------------------------------------------------------
# Health endpoint (detailed version available at /health)
# ---------------------------------------------------------------------------


@app.get("/ping")
async def ping():
    """Simple ping endpoint for basic health checks."""
    return {"status": "ok", "version": APP_VERSION}
