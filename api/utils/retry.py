"""
Retry decorator with exponential backoff for API calls.

Provides robust retry logic for external API calls (GSC, GA4, DataForSEO, Claude)
with exponential backoff, jitter, and configurable retry conditions.
"""

import functools
import random
import time
from typing import Any, Callable, Optional, Tuple, Type
import logging

import httpx

logger = logging.getLogger(__name__)


class RetryableError(Exception):
    """Base exception for errors that should trigger a retry."""
    pass


class RateLimitError(RetryableError):
    """Raised when API rate limit is hit."""
    pass


class TransientError(RetryableError):
    """Raised for temporary network or server errors."""
    pass


def is_retryable_error(error: Exception) -> bool:
    """
    Determine if an error should trigger a retry.
    
    Args:
        error: The exception that was raised
        
    Returns:
        True if the error is retryable, False otherwise
    """
    # Explicit retryable errors
    if isinstance(error, RetryableError):
        return True
    
    # HTTP errors
    if isinstance(error, httpx.HTTPStatusError):
        status_code = error.response.status_code
        # Retry on rate limits (429), server errors (500-599), and service unavailable (503)
        if status_code == 429 or status_code >= 500:
            return True
        # Don't retry on client errors (400-499 except 429)
        return False
    
    # Network errors
    if isinstance(error, (
        httpx.ConnectError,
        httpx.TimeoutException,
        httpx.RemoteProtocolError,
        httpx.ReadTimeout,
        httpx.WriteTimeout,
    )):
        return True
    
    # Google API errors (if google-api-python-client is installed)
    try:
        from googleapiclient.errors import HttpError
        if isinstance(error, HttpError):
            status_code = error.resp.status
            # Same logic as httpx
            if status_code == 429 or status_code >= 500:
                return True
            return False
    except ImportError:
        pass
    
    # Default: don't retry
    return False


def calculate_backoff(
    attempt: int,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    jitter: bool = True
) -> float:
    """
    Calculate exponential backoff delay with optional jitter.
    
    Args:
        attempt: Current retry attempt (0-indexed)
        base_delay: Initial delay in seconds
        max_delay: Maximum delay in seconds
        exponential_base: Base for exponential calculation
        jitter: Whether to add random jitter
        
    Returns:
        Delay in seconds before next retry
    """
    delay = min(base_delay * (exponential_base ** attempt), max_delay)
    
    if jitter:
        # Add random jitter: ±25% of delay
        jitter_range = delay * 0.25
        delay = delay + random.uniform(-jitter_range, jitter_range)
    
    return max(0, delay)


def retry(
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
    retryable_errors: Optional[Tuple[Type[Exception], ...]] = None,
    on_retry: Optional[Callable[[Exception, int, float], None]] = None
):
    """
    Decorator for retrying functions with exponential backoff.
    
    Args:
        max_attempts: Maximum number of attempts (including initial call)
        base_delay: Initial delay between retries in seconds
        max_delay: Maximum delay between retries in seconds
        exponential_base: Base for exponential backoff calculation
        jitter: Whether to add random jitter to delays
        retryable_errors: Tuple of exception types to retry on (in addition to auto-detection)
        on_retry: Optional callback called on each retry: fn(error, attempt, delay)
        
    Example:
        @retry(max_attempts=5, base_delay=2.0)
        async def fetch_gsc_data():
            response = await client.get("...")
            response.raise_for_status()
            return response.json()
            
        @retry(max_attempts=3, retryable_errors=(CustomAPIError,))
        def call_external_api():
            return api.call()
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs) -> Any:
            last_error: Optional[Exception] = None
            
            for attempt in range(max_attempts):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    last_error = e
                    
                    # Check if we should retry
                    should_retry = (
                        is_retryable_error(e) or
                        (retryable_errors and isinstance(e, retryable_errors))
                    )
                    
                    # Don't retry on last attempt or if error is not retryable
                    if attempt == max_attempts - 1 or not should_retry:
                        logger.error(
                            f"{func.__name__} failed after {attempt + 1} attempts: {e}",
                            exc_info=True
                        )
                        raise
                    
                    # Calculate backoff delay
                    delay = calculate_backoff(
                        attempt=attempt,
                        base_delay=base_delay,
                        max_delay=max_delay,
                        exponential_base=exponential_base,
                        jitter=jitter
                    )
                    
                    logger.warning(
                        f"{func.__name__} attempt {attempt + 1}/{max_attempts} failed: {e}. "
                        f"Retrying in {delay:.2f}s..."
                    )
                    
                    # Call retry callback if provided
                    if on_retry:
                        try:
                            on_retry(e, attempt + 1, delay)
                        except Exception as callback_error:
                            logger.error(f"Retry callback failed: {callback_error}")
                    
                    # Wait before retry
                    await asyncio.sleep(delay)
            
            # Should never reach here due to raise in loop, but for type safety
            raise last_error or Exception("Retry logic error")
        
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs) -> Any:
            last_error: Optional[Exception] = None
            
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_error = e
                    
                    # Check if we should retry
                    should_retry = (
                        is_retryable_error(e) or
                        (retryable_errors and isinstance(e, retryable_errors))
                    )
                    
                    # Don't retry on last attempt or if error is not retryable
                    if attempt == max_attempts - 1 or not should_retry:
                        logger.error(
                            f"{func.__name__} failed after {attempt + 1} attempts: {e}",
                            exc_info=True
                        )
                        raise
                    
                    # Calculate backoff delay
                    delay = calculate_backoff(
                        attempt=attempt,
                        base_delay=base_delay,
                        max_delay=max_delay,
                        exponential_base=exponential_base,
                        jitter=jitter
                    )
                    
                    logger.warning(
                        f"{func.__name__} attempt {attempt + 1}/{max_attempts} failed: {e}. "
                        f"Retrying in {delay:.2f}s..."
                    )
                    
                    # Call retry callback if provided
                    if on_retry:
                        try:
                            on_retry(e, attempt + 1, delay)
                        except Exception as callback_error:
                            logger.error(f"Retry callback failed: {callback_error}")
                    
                    # Wait before retry
                    time.sleep(delay)
            
            # Should never reach here due to raise in loop, but for type safety
            raise last_error or Exception("Retry logic error")
        
        # Return appropriate wrapper based on whether function is async
        import asyncio
        import inspect
        if inspect.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator


def retry_with_circuit_breaker(
    max_attempts: int = 3,
    failure_threshold: int = 5,
    recovery_timeout: float = 60.0,
    **retry_kwargs
):
    """
    Enhanced retry decorator with circuit breaker pattern.
    
    After failure_threshold consecutive failures, the circuit opens and
    all calls fail fast for recovery_timeout seconds before attempting again.
    
    This prevents cascading failures and gives failing services time to recover.
    
    Args:
        max_attempts: Maximum retry attempts per call
        failure_threshold: Number of consecutive failures before opening circuit
        recovery_timeout: Seconds to wait before attempting recovery
        **retry_kwargs: Additional arguments passed to @retry decorator
        
    Example:
        @retry_with_circuit_breaker(max_attempts=3, failure_threshold=5)
        async def fetch_dataforseo():
            # If DataForSEO is down, after 5 failures the circuit opens
            # and subsequent calls fail immediately for 60s
            pass
    """
    class CircuitBreaker:
        def __init__(self):
            self.failure_count = 0
            self.last_failure_time: Optional[float] = None
            self.is_open = False
        
        def record_success(self):
            self.failure_count = 0
            self.is_open = False
        
        def record_failure(self):
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            if self.failure_count >= failure_threshold:
                self.is_open = True
                logger.error(
                    f"Circuit breaker opened after {failure_threshold} failures. "
                    f"Will attempt recovery in {recovery_timeout}s"
                )
        
        def should_attempt(self) -> bool:
            if not self.is_open:
                return True
            
            # Check if recovery timeout has passed
            if self.last_failure_time is None:
                return True
            
            elapsed = time.time() - self.last_failure_time
            if elapsed >= recovery_timeout:
                logger.info("Circuit breaker attempting recovery...")
                self.is_open = False
                self.failure_count = 0
                return True
            
            return False
    
    # One circuit breaker per decorated function
    circuit_breakers = {}
    
    def decorator(func: Callable) -> Callable:
        # Create circuit breaker for this function
        breaker = CircuitBreaker()
        circuit_breakers[func.__name__] = breaker
        
        def on_retry_callback(error: Exception, attempt: int, delay: float):
            # Record failure in circuit breaker
            if attempt == max_attempts:
                breaker.record_failure()
        
        # Apply standard retry decorator with our callback
        retry_decorator = retry(
            max_attempts=max_attempts,
            on_retry=on_retry_callback,
            **retry_kwargs
        )
        
        retried_func = retry_decorator(func)
        
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            if not breaker.should_attempt():
                raise RetryableError(
                    f"Circuit breaker is open for {func.__name__}. "
                    f"Service unavailable, please try again later."
                )
            
            try:
                result = await retried_func(*args, **kwargs)
                breaker.record_success()
                return result
            except Exception as e:
                # Retried function already logged and exhausted retries
                raise
        
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            if not breaker.should_attempt():
                raise RetryableError(
                    f"Circuit breaker is open for {func.__name__}. "
                    f"Service unavailable, please try again later."
                )
            
            try:
                result = retried_func(*args, **kwargs)
                breaker.record_success()
                return result
            except Exception as e:
                # Retried function already logged and exhausted retries
                raise
        
        import asyncio
        import inspect
        if inspect.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator
