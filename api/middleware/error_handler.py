"""
FastAPI middleware to catch all exceptions and return user-friendly JSON responses.

Implements DAY 23 error handling requirements:
- All exceptions caught and transformed to structured JSON
- User-friendly error messages (no stack traces exposed)
- Proper HTTP status codes
- Detailed logging for debugging
- Retry hints where applicable
"""

import logging
import traceback
from typing import Callable
from fastapi import Request, Response, status
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
import time

logger = logging.getLogger(__name__)


class ErrorResponse:
    """Structured error response format."""
    
    def __init__(
        self,
        error_code: str,
        message: str,
        details: dict = None,
        retryable: bool = False,
        retry_after: int = None
    ):
        self.error_code = error_code
        self.message = message
        self.details = details or {}
        self.retryable = retryable
        self.retry_after = retry_after
    
    def to_dict(self) -> dict:
        response = {
            "error": {
                "code": self.error_code,
                "message": self.message,
                "details": self.details,
                "retryable": self.retryable
            }
        }
        if self.retry_after:
            response["error"]["retry_after_seconds"] = self.retry_after
        return response


class ErrorHandlerMiddleware(BaseHTTPMiddleware):
    """
    Middleware that catches all exceptions and returns structured error responses.
    
    Features:
    - User-friendly error messages
    - Proper HTTP status codes
    - Retry guidance for transient failures
    - Detailed logging with stack traces
    - Request context preservation
    """
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        request_id = request.headers.get("X-Request-ID", f"req_{int(time.time() * 1000)}")
        start_time = time.time()
        
        try:
            response = await call_next(request)
            
            # Log successful requests
            duration_ms = (time.time() - start_time) * 1000
            logger.info(
                f"Request completed - ID: {request_id}, "
                f"Method: {request.method}, Path: {request.url.path}, "
                f"Status: {response.status_code}, Duration: {duration_ms:.2f}ms"
            )
            
            return response
            
        except Exception as exc:
            duration_ms = (time.time() - start_time) * 1000
            
            # Log the full exception with stack trace
            logger.error(
                f"Request failed - ID: {request_id}, "
                f"Method: {request.method}, Path: {request.url.path}, "
                f"Duration: {duration_ms:.2f}ms\n"
                f"Exception: {type(exc).__name__}: {str(exc)}\n"
                f"Stack trace:\n{traceback.format_exc()}"
            )
            
            # Convert exception to user-friendly error response
            error_response, status_code = self._handle_exception(exc, request_id)
            
            # Add request ID to response headers for correlation
            headers = {"X-Request-ID": request_id}
            
            return JSONResponse(
                status_code=status_code,
                content=error_response.to_dict(),
                headers=headers
            )
    
    def _handle_exception(self, exc: Exception, request_id: str) -> tuple[ErrorResponse, int]:
        """
        Convert an exception to a structured error response with appropriate status code.
        
        Returns:
            Tuple of (ErrorResponse, HTTP status code)
        """
        exc_type = type(exc).__name__
        
        # Import specific exception types to avoid circular imports
        try:
            from httpx import HTTPStatusError, TimeoutException, ConnectError
            from google.auth.exceptions import RefreshError, GoogleAuthError
        except ImportError:
            HTTPStatusError = None
            TimeoutException = None
            ConnectError = None
            RefreshError = None
            GoogleAuthError = None
        
        # Google API authentication errors
        if RefreshError and isinstance(exc, RefreshError):
            return (
                ErrorResponse(
                    error_code="AUTH_TOKEN_EXPIRED",
                    message="Your Google authentication token has expired. Please reconnect your account.",
                    details={
                        "request_id": request_id,
                        "action_required": "Re-authenticate with Google Search Console and Google Analytics"
                    },
                    retryable=False
                ),
                status.HTTP_401_UNAUTHORIZED
            )
        
        if GoogleAuthError and isinstance(exc, GoogleAuthError):
            return (
                ErrorResponse(
                    error_code="AUTH_ERROR",
                    message="Unable to authenticate with Google services. Please check your account connection.",
                    details={
                        "request_id": request_id,
                        "error_detail": str(exc)
                    },
                    retryable=False
                ),
                status.HTTP_401_UNAUTHORIZED
            )
        
        # HTTP client errors (external API failures)
        if HTTPStatusError and isinstance(exc, HTTPStatusError):
            status_code = exc.response.status_code
            
            if status_code == 429:
                # Rate limit exceeded
                retry_after = int(exc.response.headers.get("Retry-After", 60))
                return (
                    ErrorResponse(
                        error_code="RATE_LIMIT_EXCEEDED",
                        message="Too many requests to external API. Please wait before retrying.",
                        details={
                            "request_id": request_id,
                            "api": self._extract_api_name(exc)
                        },
                        retryable=True,
                        retry_after=retry_after
                    ),
                    status.HTTP_429_TOO_MANY_REQUESTS
                )
            elif status_code >= 500:
                # External API server error
                return (
                    ErrorResponse(
                        error_code="EXTERNAL_API_ERROR",
                        message="The external data provider is experiencing issues. Please try again in a few minutes.",
                        details={
                            "request_id": request_id,
                            "api": self._extract_api_name(exc),
                            "status_code": status_code
                        },
                        retryable=True,
                        retry_after=30
                    ),
                    status.HTTP_503_SERVICE_UNAVAILABLE
                )
            elif status_code == 403:
                return (
                    ErrorResponse(
                        error_code="API_ACCESS_DENIED",
                        message="Access denied to external API. Your credentials may be invalid or lack required permissions.",
                        details={
                            "request_id": request_id,
                            "api": self._extract_api_name(exc)
                        },
                        retryable=False
                    ),
                    status.HTTP_403_FORBIDDEN
                )
            else:
                return (
                    ErrorResponse(
                        error_code="EXTERNAL_API_ERROR",
                        message=f"Error communicating with external API (status {status_code}).",
                        details={
                            "request_id": request_id,
                            "api": self._extract_api_name(exc)
                        },
                        retryable=False
                    ),
                    status.HTTP_502_BAD_GATEWAY
                )
        
        # Network timeout errors
        if TimeoutException and isinstance(exc, TimeoutException):
            return (
                ErrorResponse(
                    error_code="TIMEOUT",
                    message="The request took too long to complete. This usually happens with large datasets.",
                    details={
                        "request_id": request_id,
                        "suggestion": "Try reducing the date range or contact support if this persists"
                    },
                    retryable=True,
                    retry_after=10
                ),
                status.HTTP_504_GATEWAY_TIMEOUT
            )
        
        # Network connection errors
        if ConnectError and isinstance(exc, ConnectError):
            return (
                ErrorResponse(
                    error_code="CONNECTION_ERROR",
                    message="Unable to connect to external service. Please check your network connection.",
                    details={
                        "request_id": request_id
                    },
                    retryable=True,
                    retry_after=30
                ),
                status.HTTP_503_SERVICE_UNAVAILABLE
            )
        
        # Validation errors (likely from Pydantic)
        if "ValidationError" in exc_type:
            return (
                ErrorResponse(
                    error_code="VALIDATION_ERROR",
                    message="The request data is invalid or missing required fields.",
                    details={
                        "request_id": request_id,
                        "validation_errors": str(exc)
                    },
                    retryable=False
                ),
                status.HTTP_422_UNPROCESSABLE_ENTITY
            )
        
        # Database errors
        if "OperationalError" in exc_type or "DatabaseError" in exc_type:
            return (
                ErrorResponse(
                    error_code="DATABASE_ERROR",
                    message="Database is temporarily unavailable. Please try again in a moment.",
                    details={
                        "request_id": request_id
                    },
                    retryable=True,
                    retry_after=5
                ),
                status.HTTP_503_SERVICE_UNAVAILABLE
            )
        
        # Key errors (missing data)
        if isinstance(exc, KeyError):
            return (
                ErrorResponse(
                    error_code="DATA_MISSING",
                    message="Required data is missing from the source. This may indicate incomplete API response.",
                    details={
                        "request_id": request_id,
                        "missing_key": str(exc),
                        "suggestion": "Some data sources may not have complete information for your site"
                    },
                    retryable=False
                ),
                status.HTTP_424_FAILED_DEPENDENCY
            )
        
        # Value errors (data format issues)
        if isinstance(exc, ValueError):
            return (
                ErrorResponse(
                    error_code="INVALID_DATA",
                    message="The data received is in an unexpected format.",
                    details={
                        "request_id": request_id,
                        "error_detail": str(exc)
                    },
                    retryable=False
                ),
                status.HTTP_422_UNPROCESSABLE_ENTITY
            )
        
        # Memory errors (data too large)
        if isinstance(exc, MemoryError):
            return (
                ErrorResponse(
                    error_code="RESOURCE_EXHAUSTED",
                    message="The dataset is too large to process. Try reducing the date range.",
                    details={
                        "request_id": request_id,
                        "suggestion": "Limit analysis to 6-12 months or contact support for large site processing"
                    },
                    retryable=False
                ),
                status.HTTP_507_INSUFFICIENT_STORAGE
            )
        
        # Permission errors
        if isinstance(exc, PermissionError):
            return (
                ErrorResponse(
                    error_code="PERMISSION_DENIED",
                    message="Insufficient permissions to access this resource.",
                    details={
                        "request_id": request_id
                    },
                    retryable=False
                ),
                status.HTTP_403_FORBIDDEN
            )
        
        # File not found (cached data missing)
        if isinstance(exc, FileNotFoundError):
            return (
                ErrorResponse(
                    error_code="RESOURCE_NOT_FOUND",
                    message="The requested resource could not be found.",
                    details={
                        "request_id": request_id,
                        "suggestion": "This may be due to expired cache. Try regenerating the report."
                    },
                    retryable=True,
                    retry_after=0
                ),
                status.HTTP_404_NOT_FOUND
            )
        
        # Generic fallback for unknown exceptions
        return (
            ErrorResponse(
                error_code="INTERNAL_ERROR",
                message="An unexpected error occurred while processing your request. Our team has been notified.",
                details={
                    "request_id": request_id,
                    "error_type": exc_type,
                    "suggestion": "If this persists, please contact support with the request ID"
                },
                retryable=True,
                retry_after=10
            ),
            status.HTTP_500_INTERNAL_SERVER_ERROR
        )
    
    def _extract_api_name(self, exc: Exception) -> str:
        """Extract API name from exception context if possible."""
        try:
            if hasattr(exc, 'response') and hasattr(exc.response, 'url'):
                url = str(exc.response.url)
                if 'googleapis.com' in url:
                    if 'searchconsole' in url:
                        return 'Google Search Console'
                    elif 'analytics' in url:
                        return 'Google Analytics'
                    else:
                        return 'Google API'
                elif 'dataforseo' in url:
                    return 'DataForSEO'
                else:
                    return 'External API'
        except:
            pass
        return 'Unknown API'


def create_error_response(
    error_code: str,
    message: str,
    status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR,
    details: dict = None,
    retryable: bool = False,
    retry_after: int = None
) -> JSONResponse:
    """
    Helper function to create error responses from application code.
    
    Use this when you need to explicitly return an error response
    rather than raising an exception.
    
    Args:
        error_code: Machine-readable error code (e.g., "PROPERTY_NOT_FOUND")
        message: User-friendly error message
        status_code: HTTP status code
        details: Additional context (optional)
        retryable: Whether the client should retry
        retry_after: Suggested retry delay in seconds
    
    Returns:
        JSONResponse with structured error
    """
    error_response = ErrorResponse(
        error_code=error_code,
        message=message,
        details=details,
        retryable=retryable,
        retry_after=retry_after
    )
    
    return JSONResponse(
        status_code=status_code,
        content=error_response.to_dict()
    )
