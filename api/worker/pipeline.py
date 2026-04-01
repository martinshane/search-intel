"""
Analysis pipeline coordinator with comprehensive error handling.

Orchestrates the execution of all analysis modules, continuing on failures,
tracking errors, and generating partial reports when necessary.
"""

import logging
import traceback
from datetime import datetime
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field, asdict

import pandas as pd

from api.analysis.module_1_health_trajectory import analyze_health_trajectory
from api.analysis.module_2_page_triage import analyze_page_triage
from api.analysis.module_3_serp_landscape import analyze_serp_landscape
from api.analysis.module_4_content_intelligence import analyze_content_intelligence
from api.analysis.module_5_gameplan import generate_gameplan
from api.analysis.module_6_algorithm_updates import analyze_algorithm_impacts
from api.analysis.module_7_intent_migration import analyze_intent_migration
from api.analysis.module_8_technical_health import analyze_technical_health
from api.analysis.module_9_site_architecture import analyze_site_architecture
from api.analysis.module_10_branded_split import analyze_branded_split
from api.analysis.module_11_competitive_threats import analyze_competitive_threats
from api.analysis.module_12_revenue_attribution import estimate_revenue_attribution

logger = logging.getLogger(__name__)


@dataclass
class ModuleError:
    """Represents an error that occurred during module execution."""
    module_name: str
    error_type: str
    error_message: str
    traceback: str
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    user_message: str = ""


@dataclass
class ModuleResult:
    """Result from a single module execution."""
    module_name: str
    status: str  # "success", "failed", "skipped"
    data: Optional[Dict[str, Any]] = None
    error: Optional[ModuleError] = None
    execution_time_seconds: float = 0.0


@dataclass
class PipelineResult:
    """Complete pipeline execution result."""
    status: str  # "complete", "partial", "failed"
    modules: List[ModuleResult]
    errors: List[ModuleError]
    total_execution_time: float
    completed_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())


# ---------------------------------------------------------------------------
# Data conversion helpers
# ---------------------------------------------------------------------------

def _crawl_dict_to_page_dataframe(crawl_data: Any) -> Optional[pd.DataFrame]:
    """
    Convert crawler output dict into a pandas DataFrame suitable for
    Module 4 (Content Intelligence).

    The crawler returns a dict shaped like:
        {
            "pages": [
                {"url": "...", "title": "...", "h1": "...",
                 "meta_description": "...", "word_count": 123,
                 "canonical": "...", "schema_types": [...],
                 "internal_links": [...]},
                ...
            ],
            "link_graph": { url: [linked_urls] },
            "sitemap_urls": [...],
            "stats": { ... }
        }

    Module 4 expects a DataFrame with columns:
        [url, word_count, last_modified, title, h1]

    We map available fields and fill missing ones with sensible defaults.
    """
    if crawl_data is None:
        return None

    # Already a DataFrame — pass through
    if isinstance(crawl_data, pd.DataFrame):
        return crawl_data

    # Must be a dict with a "pages" key
    if not isinstance(crawl_data, dict):
        logger.warning(
            "crawl_data is neither dict nor DataFrame (type=%s); returning None",
            type(crawl_data).__name__,
        )
        return None

    pages = crawl_data.get("pages")
    if not pages:
        logger.warning("crawl_data dict has no 'pages' key or pages list is empty")
        return None

    try:
        df = pd.DataFrame(pages)

        # Ensure required columns exist with defaults
        required_cols = {
            "url": "",
            "word_count": 0,
            "title": "",
            "h1": "",
            "last_modified": None,
            "meta_description": "",
        }
        for col, default in required_cols.items():
            if col not in df.columns:
                df[col] = default

        logger.info(
            "Converted crawl_data dict to DataFrame: %d rows, columns=%s",
            len(df),
            list(df.columns),
        )
        return df

    except Exception as exc:
        logger.error("Failed to convert crawl_data to DataFrame: %s", exc)
        return None


def _ensure_dataframe(data: Any, name: str) -> Optional[pd.DataFrame]:
    """
    Coerce various input formats into a DataFrame.

    Handles:
      - None → None
      - pd.DataFrame → passthrough
      - list of dicts → pd.DataFrame
      - dict with a single list value → pd.DataFrame (common API shape)
    """
    if data is None:
        return None
    if isinstance(data, pd.DataFrame):
        return data
    if isinstance(data, list):
        if len(data) == 0:
            return pd.DataFrame()
        try:
            return pd.DataFrame(data)
        except Exception as exc:
            logger.warning("Could not convert list to DataFrame for %s: %s", name, exc)
            return None
    if isinstance(data, dict):
        # If the dict has a "rows" or "data" key that is a list, use that
        for key in ("rows", "data", "results"):
            if key in data and isinstance(data[key], list):
                try:
                    return pd.DataFrame(data[key])
                except Exception:
                    pass
    return data  # return as-is; module will handle or fail gracefully


class AnalysisPipeline:
    """
    Orchestrates the sequential execution of all analysis modules.
    
    Implements graceful degradation:
    - Continues execution even if individual modules fail
    - Tracks all errors for user reporting
    - Generates partial reports with available data
    - Provides meaningful error messages for each failure type
    """
    
    def __init__(self):
        self.modules = [
            ("health_trajectory", analyze_health_trajectory),
            ("page_triage", analyze_page_triage),
            ("serp_landscape", analyze_serp_landscape),
            ("content_intelligence", analyze_content_intelligence),
            ("gameplan", generate_gameplan),
            ("algorithm_impact", analyze_algorithm_impacts),
            ("intent_migration", analyze_intent_migration),
            ("technical_health", analyze_technical_health),
            ("site_architecture", analyze_site_architecture),
            ("branded_split", analyze_branded_split),
            ("competitive_threats", analyze_competitive_threats),
            ("revenue_attribution", estimate_revenue_attribution),
        ]
        
        self.module_dependencies = {
            "gameplan": ["health_trajectory", "page_triage", "serp_landscape", "content_intelligence"],
            "technical_health": ["serp_landscape"],
            "competitive_threats": ["serp_landscape"],
        }
        
        self.user_friendly_errors = {
            "insufficient_data": "Not enough data available to complete this analysis. This section requires at least 30 days of search data.",
            "api_failure": "Unable to retrieve required data from external service. This section will be retried automatically.",
            "processing_error": "An error occurred while processing the data for this section. Other sections are unaffected.",
            "missing_dependency": "This analysis depends on data from another section that failed to complete.",
            "configuration_error": "Configuration issue prevented this analysis from running. Please contact support.",
        }
    
    def _categorize_error(self, error: Exception, module_name: str) -> str:
        """Categorize an error to provide appropriate user message."""
        error_str = str(error).lower()
        
        if "insufficient" in error_str or "not enough" in error_str or "empty" in error_str:
            return "insufficient_data"
        elif "api" in error_str or "request" in error_str or "connection" in error_str:
            return "api_failure"
        elif "dependency" in error_str or "required" in error_str:
            return "missing_dependency"
        elif "config" in error_str or "setting" in error_str:
            return "configuration_error"
        else:
            return "processing_error"
    
    def _create_module_error(
        self,
        module_name: str,
        error: Exception,
        tb: str
    ) -> ModuleError:
        """Create a structured error object with user-friendly message."""
        error_category = self._categorize_error(error, module_name)
        user_message = self.user_friendly_errors.get(
            error_category,
            "An error occurred in this analysis section. The report will continue with remaining sections."
        )
        
        return ModuleError(
            module_name=module_name,
            error_type=type(error).__name__,
            error_message=str(error),
            traceback=tb,
            user_message=user_message
        )
    
    def _check_dependencies(
        self,
        module_name: str,
        completed_modules: Dict[str, ModuleResult]
    ) -> tuple:
        """
        Check if all dependencies for a module are satisfied.
        
        Returns:
            (can_run, skip_reason)
        """
        deps = self.module_dependencies.get(module_name, [])
        
        for dep in deps:
            if dep not in completed_modules:
                return False, f"Required module '{dep}' has not run yet"
            
            if completed_modules[dep].status != "success":
                return False, f"Required module '{dep}' failed or was skipped"
        
        return True, None
    
    def _execute_module(
        self,
        module_name: str,
        module_func,
        data_context: Dict[str, Any],
        completed_modules: Dict[str, ModuleResult]
    ) -> ModuleResult:
        """
        Execute a single module with error handling.
        
        Args:
            module_name: Name of the module
            module_func: The analysis function to call
            data_context: All input data (GSC, GA4, SERP, etc.)
            completed_modules: Results from previously run modules
            
        Returns:
            ModuleResult with status and data or error
        """
        start_time = datetime.utcnow()
        
        try:
            # Check dependencies
            can_run, skip_reason = self._check_dependencies(module_name, completed_modules)
            if not can_run:
                logger.warning(f"Skipping {module_name}: {skip_reason}")
                return ModuleResult(
                    module_name=module_name,
                    status="skipped",
                    error=ModuleError(
                        module_name=module_name,
                        error_type="DependencyNotMet",
                        error_message=skip_reason,
                        traceback="",
                        user_message="This analysis was skipped because a required previous analysis did not complete successfully."
                    )
                )
            
            # Prepare module-specific inputs
            module_data = self._prepare_module_inputs(
                module_name,
                data_context,
                completed_modules
            )
            
            # Execute module
            logger.info(f"Executing module: {module_name}")
            result = module_func(**module_data)
            
            execution_time = (datetime.utcnow() - start_time).total_seconds()
            
            logger.info(f"Module {module_name} completed successfully in {execution_time:.2f}s")
            
            return ModuleResult(
                module_name=module_name,
                status="success",
                data=result,
                execution_time_seconds=execution_time
            )
            
        except Exception as e:
            execution_time = (datetime.utcnow() - start_time).total_seconds()
            tb = traceback.format_exc()
            
            logger.error(f"Module {module_name} failed: {e}")
            logger.debug(f"Traceback: {tb}")
            
            error = self._create_module_error(module_name, e, tb)
            
            return ModuleResult(
                module_name=module_name,
                status="failed",
                error=error,
                execution_time_seconds=execution_time
            )
    
    def _prepare_module_inputs(
        self,
        module_name: str,
        data_context: Dict[str, Any],
        completed_modules: Dict[str, ModuleResult]
    ) -> Dict[str, Any]:
        """
        Prepare input parameters for a specific module.
        
        Maps from raw data context and completed module outputs to the
        specific parameters each module expects.  Handles data format
        conversions (e.g. crawler dict → DataFrame for Module 4).
        """
        inputs = {}
        
        if module_name == "health_trajectory":
            inputs = {
                "daily_data": _ensure_dataframe(
                    data_context.get("gsc_daily_data"), "gsc_daily_data"
                ),
            }
        
        elif module_name == "page_triage":
            inputs = {
                "page_daily_data": _ensure_dataframe(
                    data_context.get("gsc_page_daily_data"), "gsc_page_daily_data"
                ),
                "ga4_landing_data": _ensure_dataframe(
                    data_context.get("ga4_landing_pages"), "ga4_landing_pages"
                ),
                "gsc_page_summary": _ensure_dataframe(
                    data_context.get("gsc_page_summary"), "gsc_page_summary"
                ),
            }
        
        elif module_name == "serp_landscape":
            inputs = {
                "serp_data": data_context.get("serp_data"),
                "gsc_keyword_data": _ensure_dataframe(
                    data_context.get("gsc_keyword_data"), "gsc_keyword_data"
                ),
            }
        
        elif module_name == "content_intelligence":
            # Module 4 expects page_data as a pd.DataFrame with columns:
            # [url, word_count, last_modified, title, h1]
            # The crawler returns a dict with "pages" list — convert it.
            inputs = {
                "gsc_query_page": _ensure_dataframe(
                    data_context.get("gsc_query_page_data"), "gsc_query_page_data"
                ),
                "page_data": _crawl_dict_to_page_dataframe(
                    data_context.get("crawl_data")
                ),
                "ga4_engagement": _ensure_dataframe(
                    data_context.get("ga4_engagement_data"), "ga4_engagement_data"
                ),
            }
        
        elif module_name == "gameplan":
            inputs = {
                "health": (completed_modules.get("health_trajectory") or ModuleResult(module_name="", status="")).data,
                "triage": (completed_modules.get("page_triage") or ModuleResult(module_name="", status="")).data,
                "serp": (completed_modules.get("serp_landscape") or ModuleResult(module_name="", status="")).data,
                "content": (completed_modules.get("content_intelligence") or ModuleResult(module_name="", status="")).data,
            }
        
        elif module_name == "algorithm_impact":
            inputs = {
                "daily_data": _ensure_dataframe(
                    data_context.get("gsc_daily_data"), "gsc_daily_data"
                ),
                "change_points_from_module1": (
                    (completed_modules.get("health_trajectory") or ModuleResult(module_name="", status="")).data or {}
                ).get("change_points"),
            }
        
        elif module_name == "intent_migration":
            inputs = {
                "gsc_query_date_data": _ensure_dataframe(
                    data_context.get("gsc_query_date_data"), "gsc_query_date_data"
                ),
            }
        
        elif module_name == "technical_health":
            # Module 8 accepts crawl_technical as a dict — no conversion needed.
            inputs = {
                "gsc_coverage": data_context.get("gsc_keyword_data"),
                "crawl_technical": data_context.get("crawl_data"),
            }
        
        elif module_name == "site_architecture":
            # Module 9 handles the crawler dict format directly (reads "link_graph" key).
            inputs = {
                "link_graph": data_context.get("internal_link_graph") or data_context.get("crawl_data"),
            }
        
        elif module_name == "branded_split":
            inputs = {
                "gsc_query_data": _ensure_dataframe(
                    data_context.get("gsc_query_data"), "gsc_query_data"
                ),
                "brand_terms": data_context.get("brand_terms", []),
            }
        
        elif module_name == "competitive_threats":
            inputs = {
                "serp_data": data_context.get("serp_data"),
                "gsc_data": _ensure_dataframe(
                    data_context.get("gsc_keyword_data"), "gsc_keyword_data"
                ),
            }
        
        elif module_name == "revenue_attribution":
            inputs = {
                "gsc_data": _ensure_dataframe(
                    data_context.get("gsc_page_summary"), "gsc_page_summary"
                ),
                "ga4_conversions": _ensure_dataframe(
                    data_context.get("ga4_conversions"), "ga4_conversions"
                ),
                "ga4_engagement": _ensure_dataframe(
                    data_context.get("ga4_landing_pages"), "ga4_landing_pages"
                ),
            }
        
        return inputs
    
    def _generate_partial_report_message(
        self,
        successful: int,
        failed: int,
        skipped: int
    ) -> str:
        """Generate a user-friendly message for partial report completion."""
        total = successful + failed + skipped
        
        if failed == 0 and skipped == 0:
            return "All analysis sections completed successfully."
        
        msg = f"Report completed with {successful} of {total} sections. "
        
        if failed > 0:
            msg += f"{failed} section(s) encountered errors and were excluded. "
        
        if skipped > 0:
            msg += f"{skipped} section(s) were skipped due to missing dependencies. "
        
        msg += "The report includes all available insights from successfully completed sections."
        
        return msg
    
    def execute(self, data_context: Dict[str, Any]) -> PipelineResult:
        """
        Execute the complete analysis pipeline.
        
        Args:
            data_context: Dictionary containing all input data:
                - gsc_daily_data: Daily time series from GSC
                - gsc_page_daily_data: Per-page daily data
                - gsc_page_summary: Page-level aggregates
                - gsc_keyword_data: Keyword performance data
                - gsc_query_page_data: Query-to-page mapping
                - gsc_query_date_data: Query time series
                - ga4_landing_pages: Landing page performance
                - ga4_conversions: Conversion data
                - ga4_engagement_data: Engagement metrics
                - serp_data: SERP data from DataForSEO
                - crawl_data: Crawler result dict (pages, link_graph, stats)
                - internal_link_graph: Alias for crawl_data (Module 9)
                - brand_terms: List of brand keywords
        
        Returns:
            PipelineResult with all module results and any errors
        """
        pipeline_start = datetime.utcnow()
        
        logger.info("Starting analysis pipeline execution")
        logger.info(f"Available data keys: {list(data_context.keys())}")
        
        completed_modules: Dict[str, ModuleResult] = {}
        all_errors: List[ModuleError] = []
        module_results: List[ModuleResult] = []
        
        for module_name, module_func in self.modules:
            result = self._execute_module(
                module_name,
                module_func,
                data_context,
                completed_modules
            )
            
            module_results.append(result)
            completed_modules[module_name] = result
            
            if result.error:
                all_errors.append(result.error)
        
        successful = sum(1 for r in module_results if r.status == "success")
        failed = sum(1 for r in module_results if r.status == "failed")
        skipped = sum(1 for r in module_results if r.status == "skipped")
        
        total_time = (datetime.utcnow() - pipeline_start).total_seconds()
        
        if successful == len(self.modules):
            status = "complete"
        elif successful > 0:
            status = "partial"
        else:
            status = "failed"
        
        logger.info(f"Pipeline execution completed: {status}")
        logger.info(f"Successful: {successful}, Failed: {failed}, Skipped: {skipped}")
        logger.info(f"Total execution time: {total_time:.2f}s")
        
        pipeline_result = PipelineResult(
            status=status,
            modules=module_results,
            errors=all_errors,
            total_execution_time=total_time
        )
        
        return pipeline_result
    
    def get_report_data(self, pipeline_result: PipelineResult) -> Dict[str, Any]:
        """
        Extract successful module data into report structure.
        """
        report = {
            "metadata": {
                "status": pipeline_result.status,
                "generated_at": pipeline_result.completed_at,
                "execution_time_seconds": pipeline_result.total_execution_time,
                "completion_message": self._generate_partial_report_message(
                    sum(1 for m in pipeline_result.modules if m.status == "success"),
                    sum(1 for m in pipeline_result.modules if m.status == "failed"),
                    sum(1 for m in pipeline_result.modules if m.status == "skipped")
                ),
            },
            "sections": {},
            "errors": [asdict(e) for e in pipeline_result.errors]
        }
        
        for module_result in pipeline_result.modules:
            section_data = {
                "status": module_result.status,
                "execution_time_seconds": module_result.execution_time_seconds,
            }
            
            if module_result.status == "success" and module_result.data:
                section_data["data"] = module_result.data
            elif module_result.error:
                section_data["error"] = {
                    "type": module_result.error.error_type,
                    "message": module_result.error.user_message,
                }
            
            report["sections"][module_result.module_name] = section_data
        
        return report


def run_analysis_pipeline(data_context: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convenience function to execute pipeline and return report data.
    
    Args:
        data_context: All input data required for analysis
        
    Returns:
        Complete report data structure ready for rendering
    """
    pipeline = AnalysisPipeline()
    result = pipeline.execute(data_context)
    return pipeline.get_report_data(result)
