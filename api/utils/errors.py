"""
Custom exception classes and error message formatting utilities.

Provides structured error handling across the API with:
- Specific exception types for different failure modes
- User-friendly error messages (no stack traces exposed)
- Error context preservation for logging
- Graceful degradation patterns
"""

from typing import Optional, Dict, Any
from enum import Enum


class ErrorSeverity(Enum):
    """Error severity levels for classification."""
    CRITICAL = "critical"  # Analysis cannot continue
    HIGH = "high"          # Feature degraded but can continue
    MEDIUM = "medium"      # Minor feature loss, graceful fallback
    LOW = "low"            # Informational, no impact


class SearchIntelError(Exception):
    """Base exception for all Search Intelligence Report errors."""
    
    def __init__(
        self,
        message: str,
        user_message: Optional[str] = None,
        severity: ErrorSeverity = ErrorSeverity.HIGH,
        context: Optional[Dict[str, Any]] = None,
        original_error: Optional[Exception] = None
    ):
        """
        Initialize error with context.
        
        Args:
            message: Technical error message for logging
            user_message: User-friendly message to display (optional)
            severity: Error severity level
            context: Additional context dict for debugging
            original_error: Original exception if this wraps another error
        """
        super().__init__(message)
        self.message = message
        self.user_message = user_message or self._default_user_message()
        self.severity = severity
        self.context = context or {}
        self.original_error = original_error
    
    def _default_user_message(self) -> str:
        """Generate default user-friendly message."""
        return "An unexpected error occurred. Our team has been notified."
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert error to API response format."""
        return {
            "error": True,
            "error_type": self.__class__.__name__,
            "message": self.user_message,
            "severity": self.severity.value,
            "context": self.context
        }


# ============================================================================
# Data Ingestion Errors
# ============================================================================

class DataIngestionError(SearchIntelError):
    """Base class for data ingestion failures."""
    
    def _default_user_message(self) -> str:
        return "Unable to fetch data from external source. Please try again."


class GSCAuthError(DataIngestionError):
    """Google Search Console authentication failed."""
    
    def __init__(self, message: str, context: Optional[Dict] = None):
        super().__init__(
            message=message,
            user_message="Your Google Search Console connection has expired. Please reconnect your account.",
            severity=ErrorSeverity.CRITICAL,
            context=context
        )


class GSCAPIError(DataIngestionError):
    """Google Search Console API request failed."""
    
    def __init__(self, message: str, endpoint: str, status_code: Optional[int] = None, context: Optional[Dict] = None):
        ctx = context or {}
        ctx.update({"endpoint": endpoint, "status_code": status_code})
        
        user_msg = "Unable to fetch Search Console data. "
        if status_code == 429:
            user_msg += "API rate limit reached. Please try again in a few minutes."
        elif status_code and status_code >= 500:
            user_msg += "Google's servers are experiencing issues. Please try again later."
        else:
            user_msg += "Please check your property access and try again."
        
        super().__init__(
            message=message,
            user_message=user_msg,
            severity=ErrorSeverity.HIGH if status_code == 429 else ErrorSeverity.CRITICAL,
            context=ctx
        )


class GA4AuthError(DataIngestionError):
    """Google Analytics 4 authentication failed."""
    
    def __init__(self, message: str, context: Optional[Dict] = None):
        super().__init__(
            message=message,
            user_message="Your Google Analytics connection has expired. Please reconnect your account.",
            severity=ErrorSeverity.HIGH,  # GA4 is supplementary, not critical
            context=context
        )


class GA4APIError(DataIngestionError):
    """Google Analytics 4 API request failed."""
    
    def __init__(self, message: str, property_id: str, status_code: Optional[int] = None, context: Optional[Dict] = None):
        ctx = context or {}
        ctx.update({"property_id": property_id, "status_code": status_code})
        
        user_msg = "Unable to fetch Google Analytics data. "
        if status_code == 429:
            user_msg += "API rate limit reached. Please try again in a few minutes."
        elif status_code == 403:
            user_msg += "Access denied. Please ensure you have read access to this GA4 property."
        else:
            user_msg += "Some engagement metrics may be unavailable."
        
        super().__init__(
            message=message,
            user_message=user_msg,
            severity=ErrorSeverity.MEDIUM,  # Can proceed without GA4 data
            context=ctx
        )


class DataForSEOError(DataIngestionError):
    """DataForSEO API request failed."""
    
    def __init__(self, message: str, keyword: Optional[str] = None, status_code: Optional[int] = None, context: Optional[Dict] = None):
        ctx = context or {}
        ctx.update({"keyword": keyword, "status_code": status_code})
        
        user_msg = "Unable to fetch live SERP data. "
        if status_code == 429:
            user_msg += "API rate limit reached. SERP analysis will use cached data."
        elif status_code == 402:
            user_msg += "API credits exhausted. SERP features analysis will be limited."
        else:
            user_msg += "SERP feature analysis may be incomplete."
        
        super().__init__(
            message=message,
            user_message=user_msg,
            severity=ErrorSeverity.MEDIUM,  # Can proceed without SERP data
            context=ctx
        )


class CrawlError(DataIngestionError):
    """Site crawl failed."""
    
    def __init__(self, message: str, url: Optional[str] = None, context: Optional[Dict] = None):
        ctx = context or {}
        ctx.update({"url": url})
        
        super().__init__(
            message=message,
            user_message="Unable to crawl your site for internal link data. Architecture analysis will be limited.",
            severity=ErrorSeverity.MEDIUM,
            context=ctx
        )


class InsufficientDataError(DataIngestionError):
    """Not enough data available for analysis."""
    
    def __init__(self, message: str, data_source: str, required_days: int, available_days: int, context: Optional[Dict] = None):
        ctx = context or {}
        ctx.update({
            "data_source": data_source,
            "required_days": required_days,
            "available_days": available_days
        })
        
        super().__init__(
            message=message,
            user_message=f"Not enough historical data available ({available_days} days). At least {required_days} days needed for reliable analysis.",
            severity=ErrorSeverity.CRITICAL,
            context=ctx
        )


# ============================================================================
# Analysis Module Errors
# ============================================================================

class AnalysisError(SearchIntelError):
    """Base class for analysis module failures."""
    
    def _default_user_message(self) -> str:
        return "Analysis could not be completed for this section. Other sections will continue."


class TimeSeriesAnalysisError(AnalysisError):
    """Time series decomposition or forecasting failed."""
    
    def __init__(self, message: str, module: str, context: Optional[Dict] = None):
        ctx = context or {}
        ctx.update({"module": module})
        
        super().__init__(
            message=message,
            user_message=f"Unable to complete trend analysis. Historical patterns could not be detected.",
            severity=ErrorSeverity.MEDIUM,
            context=ctx
        )


class AnomalyDetectionError(AnalysisError):
    """Anomaly detection failed."""
    
    def __init__(self, message: str, method: str, context: Optional[Dict] = None):
        ctx = context or {}
        ctx.update({"method": method})
        
        super().__init__(
            message=message,
            user_message="Anomaly detection could not be completed. CTR analysis will use standard benchmarks.",
            severity=ErrorSeverity.LOW,
            context=ctx
        )


class ChangePointDetectionError(AnalysisError):
    """Change point detection failed."""
    
    def __init__(self, message: str, context: Optional[Dict] = None):
        super().__init__(
            message=message,
            user_message="Unable to identify significant traffic changes. Algorithm impact analysis will be limited.",
            severity=ErrorSeverity.MEDIUM,
            context=context
        )


class GraphAnalysisError(AnalysisError):
    """Network graph analysis failed."""
    
    def __init__(self, message: str, operation: str, context: Optional[Dict] = None):
        ctx = context or {}
        ctx.update({"operation": operation})
        
        super().__init__(
            message=message,
            user_message="Unable to analyze site architecture. Internal linking recommendations will be unavailable.",
            severity=ErrorSeverity.MEDIUM,
            context=ctx
        )


class ModelTrainingError(AnalysisError):
    """Machine learning model training failed."""
    
    def __init__(self, message: str, model_type: str, context: Optional[Dict] = None):
        ctx = context or {}
        ctx.update({"model_type": model_type})
        
        super().__init__(
            message=message,
            user_message="Predictive modeling could not be completed. Analysis will use benchmark data instead.",
            severity=ErrorSeverity.MEDIUM,
            context=ctx
        )


class LLMError(AnalysisError):
    """LLM API call failed."""
    
    def __init__(self, message: str, operation: str, context: Optional[Dict] = None):
        ctx = context or {}
        ctx.update({"operation": operation})
        
        super().__init__(
            message=message,
            user_message="Unable to generate narrative summary. Raw data and recommendations are still available.",
            severity=ErrorSeverity.LOW,
            context=ctx
        )


# ============================================================================
# Database Errors
# ============================================================================

class DatabaseError(SearchIntelError):
    """Database operation failed."""
    
    def __init__(self, message: str, operation: str, table: Optional[str] = None, context: Optional[Dict] = None):
        ctx = context or {}
        ctx.update({"operation": operation, "table": table})
        
        user_msg = "Database error occurred. "
        if operation == "read":
            user_msg += "Unable to retrieve cached data. Analysis will fetch fresh data."
        elif operation == "write":
            user_msg += "Results could not be saved but analysis completed successfully."
        else:
            user_msg += "Please try again."
        
        super().__init__(
            message=message,
            user_message=user_msg,
            severity=ErrorSeverity.MEDIUM if operation in ["read", "write"] else ErrorSeverity.HIGH,
            context=ctx
        )


class CacheError(DatabaseError):
    """Cache operation failed."""
    
    def __init__(self, message: str, cache_key: str, context: Optional[Dict] = None):
        ctx = context or {}
        ctx.update({"cache_key": cache_key})
        
        super().__init__(
            message=message,
            operation="cache",
            user_message="Cache unavailable. Fresh data will be fetched.",
            context=ctx
        )
        self.severity = ErrorSeverity.LOW  # Cache failures are not critical


# ============================================================================
# Report Generation Errors
# ============================================================================

class ReportGenerationError(SearchIntelError):
    """Report generation failed."""
    
    def __init__(self, message: str, stage: str, context: Optional[Dict] = None):
        ctx = context or {}
        ctx.update({"stage": stage})
        
        super().__init__(
            message=message,
            user_message=f"Report generation failed at stage: {stage}. Please try again or contact support.",
            severity=ErrorSeverity.CRITICAL,
            context=ctx
        )


class ValidationError(SearchIntelError):
    """Input validation failed."""
    
    def __init__(self, message: str, field: str, value: Any, context: Optional[Dict] = None):
        ctx = context or {}
        ctx.update({"field": field, "value": str(value)})
        
        super().__init__(
            message=message,
            user_message=f"Invalid input for {field}: {message}",
            severity=ErrorSeverity.HIGH,
            context=ctx
        )


# ============================================================================
# Error Formatting Utilities
# ============================================================================

def format_error_for_user(error: Exception) -> Dict[str, Any]:
    """
    Format any exception into user-friendly response.
    
    Args:
        error: Any exception
        
    Returns:
        Dict suitable for API JSON response
    """
    if isinstance(error, SearchIntelError):
        return error.to_dict()
    
    # Unknown error - don't expose details
    return {
        "error": True,
        "error_type": "UnexpectedError",
        "message": "An unexpected error occurred. Our team has been notified.",
        "severity": ErrorSeverity.HIGH.value,
        "context": {}
    }


def format_error_for_logging(error: Exception) -> Dict[str, Any]:
    """
    Format exception with full details for logging.
    
    Args:
        error: Any exception
        
    Returns:
        Dict with complete error context for logs
    """
    base_info = {
        "error_type": type(error).__name__,
        "message": str(error),
    }
    
    if isinstance(error, SearchIntelError):
        base_info.update({
            "severity": error.severity.value,
            "context": error.context,
            "user_message": error.user_message,
        })
        
        if error.original_error:
            base_info["original_error"] = {
                "type": type(error.original_error).__name__,
                "message": str(error.original_error)
            }
    
    return base_info


def should_retry(error: Exception) -> bool:
    """
    Determine if an operation should be retried based on error type.
    
    Args:
        error: Exception that occurred
        
    Returns:
        True if operation should be retried
    """
    # Rate limit errors should be retried with backoff
    if isinstance(error, (GSCAPIError, GA4APIError, DataForSEOError)):
        status_code = error.context.get("status_code")
        if status_code == 429:
            return True
    
    # Temporary server errors should be retried
    if isinstance(error, (GSCAPIError, GA4APIError)):
        status_code = error.context.get("status_code")
        if status_code and 500 <= status_code < 600:
            return True
    
    # Network/timeout errors should be retried
    if isinstance(error, (ConnectionError, TimeoutError)):
        return True
    
    # Auth errors should not be retried (user action needed)
    if isinstance(error, (GSCAuthError, GA4AuthError)):
        return False
    
    # Validation errors should not be retried
    if isinstance(error, ValidationError):
        return False
    
    # Database errors can be retried
    if isinstance(error, DatabaseError):
        return True
    
    # Default: don't retry unknown errors
    return False


def get_fallback_message(error: Exception, module: str) -> str:
    """
    Get graceful fallback message for a failed module.
    
    Args:
        error: Exception that occurred
        module: Name of module that failed
        
    Returns:
        User-friendly message explaining what data is missing
    """
    fallback_messages = {
        "health_trajectory": "Trend analysis unavailable. Current period metrics are shown.",
        "page_triage": "Page-level insights unavailable. Overall site metrics are shown.",
        "serp_landscape": "Live SERP data unavailable. Analysis uses historical position data only.",
        "content_intelligence": "Content recommendations unavailable. Manual review suggested.",
        "gameplan": "Prioritized action plan unavailable. Individual section insights are available.",
        "algorithm_impacts": "Algorithm correlation unavailable. Manual timeline review suggested.",
        "intent_migration": "Intent analysis unavailable. Query data shown without classification.",
        "ctr_modeling": "CTR predictions unavailable. Industry benchmarks shown instead.",
        "site_architecture": "Link graph analysis unavailable. Manual architecture review suggested.",
        "branded_split": "Brand classification unavailable. All traffic shown as aggregate.",
        "competitive_radar": "Competitor analysis unavailable. Manual SERP review suggested.",
        "revenue_attribution": "Revenue estimates unavailable. Traffic metrics shown only."
    }
    
    return fallback_messages.get(module, f"{module} analysis unavailable. Other sections still available.")


# ============================================================================
# Error Context Builders
# ============================================================================

def build_api_error_context(
    endpoint: str,
    status_code: Optional[int] = None,
    response_body: Optional[str] = None,
    request_params: Optional[Dict] = None
) -> Dict[str, Any]:
    """Build standardized context for API errors."""
    return {
        "endpoint": endpoint,
        "status_code": status_code,
        "response_preview": response_body[:200] if response_body else None,
        "request_params": request_params
    }


def build_analysis_error_context(
    module: str,
    input_shape: Optional[tuple] = None,
    input_summary: Optional[Dict] = None,
    step: Optional[str] = None
) -> Dict[str, Any]:
    """Build standardized context for analysis errors."""
    return {
        "module": module,
        "input_shape": input_shape,
        "input_summary": input_summary,
        "step": step
    }
