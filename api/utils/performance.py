"""
Performance monitoring utilities for the Search Intelligence Report system.

Provides timing decorators, metrics logging, and performance analysis tools
to help identify bottlenecks in the report generation pipeline.
"""

import functools
import time
import logging
from typing import Callable, Any, Dict, Optional
from contextlib import contextmanager
import json
from datetime import datetime

logger = logging.getLogger(__name__)


class PerformanceMetrics:
    """
    Singleton class to collect and store performance metrics across the application.
    """
    _instance = None
    _metrics: Dict[str, list] = {}
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._metrics = {}
        return cls._instance
    
    def record(self, operation: str, duration: float, metadata: Optional[Dict[str, Any]] = None):
        """
        Record a timing measurement for an operation.
        
        Args:
            operation: Name/identifier for the operation
            duration: Duration in seconds
            metadata: Additional context (e.g., record counts, API calls)
        """
        if operation not in self._metrics:
            self._metrics[operation] = []
        
        self._metrics[operation].append({
            'duration': duration,
            'timestamp': datetime.utcnow().isoformat(),
            'metadata': metadata or {}
        })
    
    def get_stats(self, operation: Optional[str] = None) -> Dict[str, Any]:
        """
        Get performance statistics for an operation or all operations.
        
        Args:
            operation: Specific operation name, or None for all operations
            
        Returns:
            Dictionary with min/max/avg/total times and call counts
        """
        if operation:
            if operation not in self._metrics:
                return {}
            
            durations = [m['duration'] for m in self._metrics[operation]]
            return {
                'operation': operation,
                'call_count': len(durations),
                'total_time': sum(durations),
                'avg_time': sum(durations) / len(durations) if durations else 0,
                'min_time': min(durations) if durations else 0,
                'max_time': max(durations) if durations else 0
            }
        
        # Return stats for all operations
        all_stats = {}
        for op in self._metrics.keys():
            all_stats[op] = self.get_stats(op)
        
        return all_stats
    
    def get_bottlenecks(self, top_n: int = 5) -> list:
        """
        Identify the slowest operations by total time.
        
        Args:
            top_n: Number of top bottlenecks to return
            
        Returns:
            List of operations sorted by total time (descending)
        """
        stats = self.get_stats()
        sorted_ops = sorted(
            stats.items(),
            key=lambda x: x[1].get('total_time', 0),
            reverse=True
        )
        return sorted_ops[:top_n]
    
    def reset(self):
        """Clear all recorded metrics."""
        self._metrics = {}
    
    def export_json(self) -> str:
        """
        Export all metrics as JSON string.
        
        Returns:
            JSON string containing all metrics and statistics
        """
        return json.dumps({
            'summary': self.get_stats(),
            'bottlenecks': self.get_bottlenecks(10),
            'raw_metrics': self._metrics
        }, indent=2)
    
    def log_summary(self):
        """Log a summary of all performance metrics."""
        stats = self.get_stats()
        total_time = sum(s.get('total_time', 0) for s in stats.values())
        
        logger.info("=" * 60)
        logger.info("PERFORMANCE SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total measured time: {total_time:.2f}s")
        logger.info(f"Operations tracked: {len(stats)}")
        logger.info("")
        logger.info("Top 10 bottlenecks by total time:")
        
        for idx, (op, op_stats) in enumerate(self.get_bottlenecks(10), 1):
            pct = (op_stats['total_time'] / total_time * 100) if total_time > 0 else 0
            logger.info(
                f"{idx:2d}. {op:40s} "
                f"{op_stats['total_time']:8.2f}s ({pct:5.1f}%) "
                f"[{op_stats['call_count']} calls, avg: {op_stats['avg_time']:.3f}s]"
            )
        logger.info("=" * 60)


# Global instance
metrics = PerformanceMetrics()


def timed(operation_name: Optional[str] = None):
    """
    Decorator to measure and log execution time of a function.
    
    Args:
        operation_name: Custom name for the operation (defaults to function name)
        
    Usage:
        @timed()
        def slow_function():
            time.sleep(1)
            
        @timed("custom_operation_name")
        def another_function():
            pass
    """
    def decorator(func: Callable) -> Callable:
        op_name = operation_name or f"{func.__module__}.{func.__name__}"
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                return result
            finally:
                duration = time.time() - start_time
                
                # Extract metadata from result if it's a dict with metadata
                metadata = {}
                if isinstance(result, dict) and '_metadata' in result:
                    metadata = result['_metadata']
                
                metrics.record(op_name, duration, metadata)
                logger.info(f"⏱️  {op_name} completed in {duration:.3f}s")
        
        return wrapper
    return decorator


def async_timed(operation_name: Optional[str] = None):
    """
    Decorator to measure and log execution time of an async function.
    
    Args:
        operation_name: Custom name for the operation (defaults to function name)
        
    Usage:
        @async_timed()
        async def slow_async_function():
            await asyncio.sleep(1)
    """
    def decorator(func: Callable) -> Callable:
        op_name = operation_name or f"{func.__module__}.{func.__name__}"
        
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = await func(*args, **kwargs)
                return result
            finally:
                duration = time.time() - start_time
                
                # Extract metadata from result if it's a dict with metadata
                metadata = {}
                if isinstance(result, dict) and '_metadata' in result:
                    metadata = result['_metadata']
                
                metrics.record(op_name, duration, metadata)
                logger.info(f"⏱️  {op_name} completed in {duration:.3f}s")
        
        return wrapper
    return decorator


@contextmanager
def measure_time(operation_name: str, metadata: Optional[Dict[str, Any]] = None):
    """
    Context manager for measuring execution time of a code block.
    
    Args:
        operation_name: Name/identifier for the operation
        metadata: Additional context to record
        
    Usage:
        with measure_time("data_processing", {"rows": 1000}):
            # ... processing code ...
            pass
    """
    start_time = time.time()
    try:
        yield
    finally:
        duration = time.time() - start_time
        metrics.record(operation_name, duration, metadata)
        logger.info(f"⏱️  {operation_name} completed in {duration:.3f}s")


class ProgressTracker:
    """
    Track progress through multi-step operations with timing.
    
    Usage:
        tracker = ProgressTracker("report_generation", total_steps=12)
        
        tracker.start_step("module_1", "Health & Trajectory Analysis")
        # ... do work ...
        tracker.complete_step()
        
        tracker.start_step("module_2", "Page-Level Triage")
        # ... do work ...
        tracker.complete_step()
        
        summary = tracker.get_summary()
    """
    
    def __init__(self, operation_name: str, total_steps: int):
        self.operation_name = operation_name
        self.total_steps = total_steps
        self.current_step = 0
        self.current_step_name = None
        self.current_step_start = None
        self.step_timings = []
        self.overall_start = time.time()
    
    def start_step(self, step_name: str, description: str = ""):
        """
        Begin timing a step.
        
        Args:
            step_name: Identifier for this step
            description: Human-readable description
        """
        if self.current_step_name:
            # Auto-complete previous step if not explicitly completed
            self.complete_step()
        
        self.current_step += 1
        self.current_step_name = step_name
        self.current_step_start = time.time()
        
        progress_pct = (self.current_step / self.total_steps) * 100
        logger.info(
            f"📊 [{progress_pct:5.1f}%] Step {self.current_step}/{self.total_steps}: "
            f"{step_name} — {description}"
        )
    
    def complete_step(self, metadata: Optional[Dict[str, Any]] = None):
        """
        Mark current step as complete and record timing.
        
        Args:
            metadata: Additional context about this step
        """
        if not self.current_step_name or not self.current_step_start:
            logger.warning("complete_step called but no step is active")
            return
        
        duration = time.time() - self.current_step_start
        
        self.step_timings.append({
            'step': self.current_step,
            'name': self.current_step_name,
            'duration': duration,
            'metadata': metadata or {}
        })
        
        # Record in global metrics
        metrics.record(
            f"{self.operation_name}.{self.current_step_name}",
            duration,
            metadata
        )
        
        logger.info(f"✅ {self.current_step_name} completed in {duration:.2f}s")
        
        self.current_step_name = None
        self.current_step_start = None
    
    def get_summary(self) -> Dict[str, Any]:
        """
        Get a summary of all step timings.
        
        Returns:
            Dictionary with timing breakdown and total duration
        """
        total_duration = time.time() - self.overall_start
        
        return {
            'operation': self.operation_name,
            'total_duration': total_duration,
            'steps_completed': len(self.step_timings),
            'steps_total': self.total_steps,
            'step_timings': self.step_timings,
            'slowest_step': max(self.step_timings, key=lambda x: x['duration']) if self.step_timings else None,
            'average_step_time': sum(s['duration'] for s in self.step_timings) / len(self.step_timings) if self.step_timings else 0
        }
    
    def log_summary(self):
        """Log a formatted summary of the operation."""
        summary = self.get_summary()
        
        logger.info("=" * 60)
        logger.info(f"OPERATION COMPLETE: {self.operation_name}")
        logger.info("=" * 60)
        logger.info(f"Total duration: {summary['total_duration']:.2f}s")
        logger.info(f"Steps completed: {summary['steps_completed']}/{summary['steps_total']}")
        logger.info(f"Average step time: {summary['average_step_time']:.2f}s")
        
        if summary['slowest_step']:
            logger.info(
                f"Slowest step: {summary['slowest_step']['name']} "
                f"({summary['slowest_step']['duration']:.2f}s)"
            )
        
        logger.info("\nStep breakdown:")
        for step in self.step_timings:
            pct = (step['duration'] / summary['total_duration'] * 100) if summary['total_duration'] > 0 else 0
            logger.info(
                f"  {step['step']:2d}. {step['name']:35s} "
                f"{step['duration']:7.2f}s ({pct:5.1f}%)"
            )
        logger.info("=" * 60)


def format_duration(seconds: float) -> str:
    """
    Format a duration in seconds to a human-readable string.
    
    Args:
        seconds: Duration in seconds
        
    Returns:
        Formatted string (e.g., "2m 34s", "1h 15m", "3.2s")
    """
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{minutes}m {secs}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"


def log_performance_warning(operation: str, duration: float, threshold: float):
    """
    Log a warning if an operation exceeds its expected duration threshold.
    
    Args:
        operation: Name of the operation
        duration: Actual duration in seconds
        threshold: Expected threshold in seconds
    """
    if duration > threshold:
        overage_pct = ((duration - threshold) / threshold) * 100
        logger.warning(
            f"⚠️  Performance warning: {operation} took {format_duration(duration)}, "
            f"expected < {format_duration(threshold)} ({overage_pct:.0f}% over threshold)"
        )


class PerformanceThresholds:
    """
    Define and check performance thresholds for operations.
    """
    
    # Default thresholds in seconds
    THRESHOLDS = {
        'gsc_api_call': 5.0,
        'ga4_api_call': 5.0,
        'dataforseo_api_call': 3.0,
        'module_1_analysis': 15.0,
        'module_2_analysis': 20.0,
        'module_3_analysis': 10.0,
        'module_4_analysis': 15.0,
        'module_5_generation': 30.0,
        'module_6_analysis': 10.0,
        'module_7_analysis': 20.0,
        'module_8_analysis': 15.0,
        'module_9_analysis': 25.0,
        'module_10_analysis': 10.0,
        'module_11_analysis': 10.0,
        'module_12_analysis': 10.0,
        'llm_api_call': 10.0,
        'total_report_generation': 180.0  # 3 minutes
    }
    
    @classmethod
    def check(cls, operation: str, duration: float) -> bool:
        """
        Check if an operation duration is within acceptable threshold.
        
        Args:
            operation: Operation name
            duration: Duration in seconds
            
        Returns:
            True if within threshold, False otherwise
        """
        threshold = cls.THRESHOLDS.get(operation)
        if threshold is None:
            return True  # No threshold defined
        
        within_threshold = duration <= threshold
        
        if not within_threshold:
            log_performance_warning(operation, duration, threshold)
        
        return within_threshold
    
    @classmethod
    def set_threshold(cls, operation: str, threshold: float):
        """
        Set or update a performance threshold.
        
        Args:
            operation: Operation name
            threshold: Threshold in seconds
        """
        cls.THRESHOLDS[operation] = threshold
