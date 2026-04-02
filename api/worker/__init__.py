"""Worker package for async job processing.

This package handles the async report generation pipeline.
"""

from .pipeline import AnalysisPipeline, PipelineResult
from .report_runner import run_report_pipeline

# Backwards-compatible aliases
ReportPipeline = AnalysisPipeline
ReportStatus = PipelineResult

__all__ = [
    "AnalysisPipeline",
    "PipelineResult",
    "ReportPipeline",
    "ReportStatus",
    "run_report_pipeline",
]
