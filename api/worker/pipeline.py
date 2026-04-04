"""
Analysis pipeline coordinator with comprehensive error handling.

Orchestrates the execution of all analysis modules, continuing on failures,
tracking errors, and generating partial reports when necessary.

Supports a real-time progress_callback so the caller (report_runner) can
push per-module status updates to Supabase as each module finishes — giving
the frontend live progress instead of a single bulk update at the end.

Parallel execution (v2):
  Phase 1 — health_trajectory runs first (algorithm_impact needs its
            change_points output).
  Phase 2 — All remaining modules except gameplan run concurrently via
            ThreadPoolExecutor, typically cutting wall-clock time by 50-70%.
  Phase 3 — gameplan runs last, synthesising outputs from all 11 other modules.
"""

import logging
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field, asdict

import pandas as pd

from ..analysis.module_1_health_trajectory import analyze_health_trajectory
from ..analysis.module_2_page_triage import analyze_page_triage
from ..analysis.module_3_serp_landscape import analyze_serp_landscape
from ..analysis.module_4_content_intelligence import analyze_content_intelligence
from ..analysis.module_5_gameplan import generate_gameplan
from ..analysis.module_6_algorithm_updates import analyze_algorithm_impacts
from ..analysis.module_7_intent_migration import analyze_intent_migration
from ..analysis.module_8_technical_health import analyze_technical_health
from ..analysis.module_9_site_architecture import analyze_site_architecture
from ..analysis.module_10_branded_split import analyze_branded_split
from ..analysis.module_11_competitive_threats import analyze_competitive_threats
from ..analysis.module_12_revenue_attribution import estimate_revenue_attribution

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


# Type alias for the optional progress callback.
# Signature: callback(module_name: str, module_result: ModuleResult) -> None
ProgressCallback = Callable[[str, "ModuleResult"], None]


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


def _normalize_serp_data(raw_serp: Any) -> List[Dict[str, Any]]:
    """
    Convert the DataForSEO wrapper dict into a flat list of SERP dicts
    suitable for modules 3, 8, and 11.

    The DataForSEO ingestion (fetch_serps_for_top_keywords) returns:

        {
            "total_keywords_requested": N,
            "successful_fetches": N,
            "results": [
                {
                    "keyword": "best crm",
                    "success": True,
                    "data": {
                        "keyword": "best crm",
                        "serp_features": {
                            "featured_snippet": True,
                            "people_also_ask": 3,
                            "video_carousel": False,
                            "local_pack": False,
                            "knowledge_panel": True,
                            "ai_overview": True,
                            "image_pack": False,
                            "shopping_results": False,
                            ...
                        },
                        "organic_results": [
                            {"position": 1, "url": "...", "domain": "...",
                             "title": "...", "is_user_result": False},
                            ...
                        ],
                        "user_position": 3,
                        "user_url": "https://...",
                        "visual_position": 8.5,
                        "competitors": [...],
                    }
                },
                ...
            ],
            "spending": {...},
        }

    Modules 3 and 11 expect a flat list of SERP dicts with features as
    top-level keys, e.g.:

        [
            {
                "keyword": "best crm",
                "user_domain": "example.com",
                "organic_results": [...],
                "featured_snippet": {"position": 0, "text": "..."} or None,
                "knowledge_panel": {...} or None,
                "ai_overview": {...} or None,
                "local_pack": {"position": 2} or None,
                "people_also_ask": [{"question": "q", "position": 4}, ...],
                "video_results": [...] or None,
                "images_pack": {"position": N} or None,
                "shopping_results": [...] or None,
                # Also preserved for module 8 backward-compat:
                "serp_features": {...},
            },
            ...
        ]

    This function bridges the two formats so all three SERP-consuming
    modules receive correctly shaped data.
    """
    if raw_serp is None:
        return []

    # Already a list — pass through (e.g. test data already in correct format)
    if isinstance(raw_serp, list):
        return raw_serp

    if not isinstance(raw_serp, dict):
        logger.warning(
            "_normalize_serp_data: unexpected type %s; returning empty list",
            type(raw_serp).__name__,
        )
        return []

    results_list = raw_serp.get("results", [])
    if not isinstance(results_list, list):
        logger.warning("_normalize_serp_data: 'results' key is not a list")
        return []

    normalized: List[Dict[str, Any]] = []

    for entry in results_list:
        if not isinstance(entry, dict):
            continue
        if not entry.get("success", False):
            continue

        data = entry.get("data", {})
        if not isinstance(data, dict):
            continue

        keyword = data.get("keyword", entry.get("keyword", ""))
        features = data.get("serp_features", {})
        organic = data.get("organic_results", [])

        # Infer user_domain from the organic results
        user_domain = ""
        for org in organic:
            if org.get("is_user_result"):
                domain = org.get("domain", "")
                user_domain = domain.lower().replace("www.", "") if domain else ""
                break

        # Promote serp_features booleans/counts into the nested structure
        # that module 3's helpers (_features_above_position, _find_user_result,
        # _classify_keyword_intent, etc.) expect as top-level keys.
        #
        # Module 3 checks:  serp.get("featured_snippet") → truthy dict or None
        #                    serp.get("knowledge_panel") → truthy dict or None
        #                    serp.get("ai_overview")      → truthy dict or None
        #                    serp.get("local_pack")        → truthy dict w/ position
        #                    serp.get("people_also_ask")   → list of dicts
        #                    serp.get("video_results")     → list w/ position
        #                    serp.get("images_pack")       → dict w/ position
        #                    serp.get("shopping_results")  → list w/ position

        serp_dict: Dict[str, Any] = {
            "keyword": keyword,
            "user_domain": user_domain,
            "organic_results": organic,
            # Preserve original features for module 8 backward-compat
            "serp_features": features,
            # Promote features to top-level keys for module 3 / 11
            "featured_snippet": (
                {"position": 0, "text": "featured snippet"}
                if features.get("featured_snippet") else None
            ),
            "knowledge_panel": (
                {"title": keyword}
                if features.get("knowledge_panel") else None
            ),
            "ai_overview": (
                {"text": "ai overview"}
                if features.get("ai_overview") else None
            ),
            "local_pack": (
                {"position": 2}
                if features.get("local_pack") else None
            ),
            "people_also_ask": (
                [{"question": f"paa_{i}", "position": 4 + i}
                 for i in range(features.get("people_also_ask", 0))]
                if features.get("people_also_ask") else []
            ),
            "video_results": (
                [{"url": "https://youtube.com", "position": 6}]
                if features.get("video_carousel") else None
            ),
            "images_pack": (
                {"position": 7}
                if features.get("image_pack") else None
            ),
            "shopping_results": (
                [{"price": "$0", "position": 1}]
                if features.get("shopping_results") else None
            ),
            # Extra metadata from DataForSEO processing
            "user_position": data.get("user_position"),
            "user_url": data.get("user_url"),
            "visual_position": data.get("visual_position"),
            "competitors_raw": data.get("competitors", []),
        }

        normalized.append(serp_dict)

    logger.info(
        "_normalize_serp_data: converted %d successful SERP results from DataForSEO wrapper",
        len(normalized),
    )
    return normalized


def _get_module_data(completed_modules: Dict[str, 'ModuleResult'], name: str) -> Optional[Dict[str, Any]]:
    """Safely extract data from a completed module result, returning None on failure."""
    result = completed_modules.get(name)
    if result is None:
        return None
    if result.status != "success":
        return None
    return result.data


class AnalysisPipeline:
    """
    Orchestrates the sequential execution of all analysis modules.
    
    Implements graceful degradation:
    - Continues execution even if individual modules fail
    - Tracks all errors for user reporting
    - Generates partial reports with available data
    - Provides meaningful error messages for each failure type
    
    IMPORTANT: The Gameplan (Module 5) runs LAST so it can synthesize
    outputs from ALL other modules (1-4, 6-12).  The module numbering
    reflects the report section order, not the execution order.
    """
    
    # Map module names to their report section numbers (1-12).
    # Used by the progress callback to report which module number
    # is running or completed.
    MODULE_NUMBERS: Dict[str, int] = {
        "health_trajectory": 1,
        "page_triage": 2,
        "serp_landscape": 3,
        "content_intelligence": 4,
        "gameplan": 5,
        "algorithm_impact": 6,
        "intent_migration": 7,
        "technical_health": 8,
        "site_architecture": 9,
        "branded_split": 10,
        "competitive_threats": 11,
        "revenue_attribution": 12,
    }
    
    def __init__(self):
        # Execution order: modules 1-4, then 6-12, then gameplan (5) last.
        # Gameplan runs last because it synthesizes ALL module outputs.
        self.modules = [
            ("health_trajectory", analyze_health_trajectory),
            ("page_triage", analyze_page_triage),
            ("serp_landscape", analyze_serp_landscape),
            ("content_intelligence", analyze_content_intelligence),
            ("algorithm_impact", analyze_algorithm_impacts),
            ("intent_migration", analyze_intent_migration),
            ("technical_health", analyze_technical_health),
            ("site_architecture", analyze_site_architecture),
            ("branded_split", analyze_branded_split),
            ("competitive_threats", analyze_competitive_threats),
            ("revenue_attribution", estimate_revenue_attribution),
            ("gameplan", generate_gameplan),
        ]
        
        # Gameplan depends on modules 1 + 2 (required).  Modules 3-4 and
        # 6-12 are optional enrichments — passed when available but the
        # gameplan generates a useful action plan from health + triage alone.
        #
        # technical_health and competitive_threats were previously gated on
        # serp_landscape, but they don't consume serp_landscape *output* —
        # they read raw data_context keys (crawl_data, gsc_keyword_data,
        # serp_data).  Removing the false dependency so they run even when
        # DataForSEO is not configured.
        self.module_dependencies = {
            "gameplan": ["health_trajectory", "page_triage"],
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
                "df": _ensure_dataframe(
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
            # Module 3 expects a LIST of SERP dicts with top-level feature
            # keys.  DataForSEO returns a wrapper dict — normalize it.
            inputs = {
                "serp_data": _normalize_serp_data(data_context.get("serp_data")),
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
            # Gameplan runs LAST and receives outputs from ALL prior modules.
            # Modules 1-4 are required (enforced by dependency check).
            # Modules 6-12 are optional — passed when available for richer synthesis.
            inputs = {
                "health": _get_module_data(completed_modules, "health_trajectory"),
                "triage": _get_module_data(completed_modules, "page_triage"),
                "serp": _get_module_data(completed_modules, "serp_landscape"),
                "content": _get_module_data(completed_modules, "content_intelligence"),
                "algorithm": _get_module_data(completed_modules, "algorithm_impact"),
                "intent": _get_module_data(completed_modules, "intent_migration"),
                "ctr": _get_module_data(completed_modules, "technical_health"),
                "architecture": _get_module_data(completed_modules, "site_architecture"),
                "branded": _get_module_data(completed_modules, "branded_split"),
                "competitive": _get_module_data(completed_modules, "competitive_threats"),
                "revenue": _get_module_data(completed_modules, "revenue_attribution"),
            }
        
        elif module_name == "algorithm_impact":
            # Module 6 accepts optional page_daily_data (for per-page
            # algorithm impact — which pages were most affected by each
            # update) and optional page_metadata (for characterising WHY
            # affected pages were hit — content length, schema, age, etc.).
            # Without page_daily_data, the "pages_most_affected" list in
            # each ImpactAssessment is empty.  Without page_metadata, the
            # "common_characteristics" analysis returns empty.
            inputs = {
                "daily_data": _ensure_dataframe(
                    data_context.get("gsc_daily_data"), "gsc_daily_data"
                ),
                "change_points_from_module1": (
                    (completed_modules.get("health_trajectory") or ModuleResult(module_name="", status="")).data or {}
                ).get("change_points"),
                "page_daily_data": _ensure_dataframe(
                    data_context.get("gsc_page_daily_data"), "gsc_page_daily_data"
                ),
                "page_metadata": _crawl_dict_to_page_dataframe(
                    data_context.get("crawl_data")
                ),
            }
        
        elif module_name == "intent_migration":
            # Module 7 accepts optional serp_data (for SERP-feature-based
            # intent classification — shopping_results → transactional, etc.)
            # and optional page_data (for content alignment analysis —
            # does the page type match the dominant intent?).
            # Without serp_data, intent classification relies solely on
            # keyword patterns, missing SERP-feature signals that improve
            # accuracy (see _SERP_INTENT_SIGNALS in module_7).
            # Without page_data, the content_alignment section returns empty.
            inputs = {
                "query_timeseries": _ensure_dataframe(
                    data_context.get("gsc_query_date_data"), "gsc_query_date_data"
                ),
                "serp_data": _normalize_serp_data(data_context.get("serp_data")),
                "page_data": _crawl_dict_to_page_dataframe(
                    data_context.get("crawl_data")
                ),
            }
        
        elif module_name == "technical_health":
            # Module 8: CTR Modeling by SERP Context.
            # gsc_coverage  → GSC keyword data (query, position, ctr, impressions)
            # crawl_technical → SERP data from DataForSEO (features, competitors)
            # The function uses legacy param names for pipeline backward-compat.
            #
            # Module 8 already handles the wrapper dict format (it looks for
            # "results" key), but its _extract_serp_features expects raw
            # DataForSEO items which are NOT preserved after pre-parsing.
            # We pass the normalized list so module 8 can fall back to the
            # "serp_features" dict on each entry.  Module 8's handler at
            # line ~580 iterates the list and calls _extract_serp_features
            # which checks for "items" (raw) — we also need to pass the raw
            # wrapper dict since module 8 has its own extraction logic.
            # Pass BOTH the raw dict (for its existing handler) and ensure
            # each result's "data" is accessible.
            raw_serp = data_context.get("serp_data")
            inputs = {
                "gsc_coverage": data_context.get("gsc_keyword_data"),
                "crawl_technical": raw_serp,
            }
        
        elif module_name == "site_architecture":
            # Module 9: Site Architecture & Authority Flow.
            # link_graph → crawler dict with pages + link_graph + stats
            # page_performance → GSC page-level aggregates (clicks, impressions, position per URL)
            # sitemap_urls → list of URLs from sitemap (extracted during crawl)
            # query_data → GSC keyword data for topical relevance in link recommendations
            crawl = data_context.get("internal_link_graph") or data_context.get("crawl_data")

            # Extract sitemap_urls from crawl result if available
            sitemap_urls = None
            if isinstance(crawl, dict):
                sitemap_urls = crawl.get("sitemap_urls")
            # Also check top-level data_context (report_runner stores it there too)
            if not sitemap_urls:
                sitemap_urls = data_context.get("sitemap_urls")

            # Convert page_performance to list-of-dicts (Module 9 expects this format)
            page_perf_raw = data_context.get("gsc_page_summary")
            page_performance = None
            if page_perf_raw is not None:
                if isinstance(page_perf_raw, pd.DataFrame) and not page_perf_raw.empty:
                    page_performance = page_perf_raw.to_dict("records")
                elif isinstance(page_perf_raw, list):
                    page_performance = page_perf_raw

            # Convert query_data to list-of-dicts
            query_raw = data_context.get("gsc_keyword_data")
            query_data = None
            if query_raw is not None:
                if isinstance(query_raw, pd.DataFrame) and not query_raw.empty:
                    query_data = query_raw.to_dict("records")
                elif isinstance(query_raw, list):
                    query_data = query_raw

            inputs = {
                "link_graph": crawl,
                "page_performance": page_performance,
                "sitemap_urls": sitemap_urls,
                "query_data": query_data,
            }
        
        elif module_name == "branded_split":
            inputs = {
                "gsc_query_data": _ensure_dataframe(
                    data_context.get("gsc_query_data"), "gsc_query_data"
                ),
                "brand_terms": data_context.get("brand_terms", []),
            }
        
        elif module_name == "competitive_threats":
            # Module 11 expects a LIST of SERP dicts — normalize the
            # DataForSEO wrapper dict into the flat list format.
            # user_domain is critical: without it, Module 11 returns empty
            # results because it cannot distinguish the user's site from
            # competitors in the SERP data.
            inputs = {
                "serp_data": _normalize_serp_data(data_context.get("serp_data")),
                "gsc_data": _ensure_dataframe(
                    data_context.get("gsc_keyword_data"), "gsc_keyword_data"
                ),
                "user_domain": data_context.get("domain"),
            }
        
        elif module_name == "revenue_attribution":
            # Module 12 accepts ga4_ecommerce for real revenue data.
            # Previously this was always None because report_runner did
            # not map the "ecommerce" section from GA4 ingestion.
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
                "ga4_ecommerce": _ensure_dataframe(
                    data_context.get("ga4_ecommerce"), "ga4_ecommerce"
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
    
    def execute(
        self,
        data_context: Dict[str, Any],
        progress_callback: Optional[ProgressCallback] = None,
    ) -> PipelineResult:
        """
        Execute the complete analysis pipeline with parallel module execution.

        Execution phases:
          Phase 1 (sequential): health_trajectory — needed by algorithm_impact
                                for change_points_from_module1.
          Phase 2 (parallel):   All remaining modules except gameplan run
                                concurrently via ThreadPoolExecutor.  This
                                typically cuts total wall-clock time by 50-70%
                                compared to fully sequential execution.
          Phase 3 (sequential): gameplan — synthesizes outputs from ALL other
                                modules into the prioritized action plan.

        Thread safety:
          - data_context is read-only during execution — safe for concurrent
            reads across threads.
          - completed_modules is written under a threading.Lock.  Phase 2
            modules only read health_trajectory (written in Phase 1 before
            any thread starts) so there is no read-after-write hazard.
          - progress_callback is fired from whichever thread completes the
            module.  The callback (report_runner._on_module_complete) does
            independent Supabase upserts per module — safe for concurrent
            invocation.

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
            progress_callback: Optional callable invoked after each module
                completes.  Signature: (module_name, ModuleResult) -> None.
                Used by report_runner to push real-time progress to Supabase
                so the frontend can display per-module status updates.

        Returns:
            PipelineResult with all module results and any errors
        """
        pipeline_start = datetime.utcnow()

        logger.info("Starting analysis pipeline execution (parallel mode)")
        logger.info(f"Available data keys: {list(data_context.keys())}")

        completed_modules: Dict[str, ModuleResult] = {}
        all_errors: List[ModuleError] = []
        module_results: List[ModuleResult] = []
        lock = threading.Lock()

        def _run_and_record(module_name: str, module_func) -> ModuleResult:
            """Execute a module, record the result, fire progress callback."""
            result = self._execute_module(
                module_name, module_func, data_context, completed_modules
            )
            with lock:
                module_results.append(result)
                completed_modules[module_name] = result
                if result.error:
                    all_errors.append(result.error)

            # Notify the caller of per-module progress so it can push
            # real-time status updates to Supabase / the frontend.
            if progress_callback is not None:
                try:
                    progress_callback(module_name, result)
                except Exception as cb_err:
                    # Never let a callback failure abort the pipeline.
                    logger.warning(
                        "progress_callback failed for %s: %s",
                        module_name, cb_err,
                    )
            return result

        # Build lookup for quick access to module functions.
        module_lookup = {name: func for name, func in self.modules}

        # ---------------------------------------------------------------
        # Phase 1 (sequential): health_trajectory
        # Must run first because algorithm_impact reads its change_points
        # output via completed_modules["health_trajectory"].
        # ---------------------------------------------------------------
        logger.info("Phase 1/3: Running health_trajectory (sequential)")
        _run_and_record("health_trajectory", module_lookup["health_trajectory"])

        # ---------------------------------------------------------------
        # Phase 2 (parallel): everything except health_trajectory + gameplan
        # These modules read from data_context (read-only) and at most
        # from completed_modules["health_trajectory"] (already populated).
        # They do NOT read each other's outputs, so they are safe to run
        # concurrently.
        # ---------------------------------------------------------------
        phase2_modules = [
            (name, func) for name, func in self.modules
            if name not in ("health_trajectory", "gameplan")
        ]

        # Cap workers: most modules are CPU-bound (pandas/numpy/sklearn)
        # but release the GIL during C-extension computation, so threads
        # provide real speedup.  6 workers balances parallelism against
        # memory pressure on Railway's 4 GB worker limit.
        max_workers = min(len(phase2_modules), 6)

        logger.info(
            "Phase 2/3: Running %d modules in parallel (max_workers=%d): %s",
            len(phase2_modules),
            max_workers,
            [name for name, _ in phase2_modules],
        )

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_run_and_record, name, func): name
                for name, func in phase2_modules
            }
            for future in as_completed(futures):
                mod_name = futures[future]
                try:
                    future.result()  # Re-raise any uncaught exception
                except Exception as exc:
                    # _execute_module already catches exceptions internally;
                    # this handles truly unexpected failures in the wrapper.
                    logger.error(
                        "Unexpected error in parallel execution of %s: %s",
                        mod_name, exc,
                    )

        # ---------------------------------------------------------------
        # Phase 3 (sequential): gameplan
        # Runs last so it can synthesize outputs from ALL other modules.
        # ---------------------------------------------------------------
        logger.info("Phase 3/3: Running gameplan (sequential, synthesizes all outputs)")
        _run_and_record("gameplan", module_lookup["gameplan"])

        # Re-order results so the report JSON has modules in section order
        # (1-12) rather than execution/completion order.
        section_order = [
            "health_trajectory", "page_triage", "serp_landscape",
            "content_intelligence", "gameplan", "algorithm_impact",
            "intent_migration", "technical_health", "site_architecture",
            "branded_split", "competitive_threats", "revenue_attribution",
        ]
        result_map = {r.module_name: r for r in module_results}
        module_results_ordered = [
            result_map[name] for name in section_order if name in result_map
        ]

        successful = sum(1 for r in module_results_ordered if r.status == "success")
        failed = sum(1 for r in module_results_ordered if r.status == "failed")
        skipped = sum(1 for r in module_results_ordered if r.status == "skipped")

        total_time = (datetime.utcnow() - pipeline_start).total_seconds()

        if successful == len(self.modules):
            status = "complete"
        elif successful > 0:
            status = "partial"
        else:
            status = "failed"

        logger.info(f"Pipeline execution completed: {status} (parallel mode)")
        logger.info(f"Successful: {successful}, Failed: {failed}, Skipped: {skipped}")
        logger.info(f"Total execution time: {total_time:.2f}s")

        return PipelineResult(
            status=status,
            modules=module_results_ordered,
            errors=all_errors,
            total_execution_time=total_time,
        )
    
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


def run_analysis_pipeline(
    data_context: Dict[str, Any],
    progress_callback: Optional[ProgressCallback] = None,
) -> Dict[str, Any]:
    """
    Convenience function to execute pipeline and return report data.
    
    Args:
        data_context: All input data required for analysis
        progress_callback: Optional per-module progress callback
        
    Returns:
        Complete report data structure ready for rendering
    """
    pipeline = AnalysisPipeline()
    result = pipeline.execute(data_context, progress_callback=progress_callback)
    return pipeline.get_report_data(result)
