"""
Pipeline orchestrator for Search Intelligence Report generation.
Coordinates data ingestion and analysis module execution with comprehensive
error handling, retry logic, and progress tracking.
"""

import asyncio
import logging
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field

from api.data.gsc_client import GSCClient
from api.data.ga4_client import GA4Client
from api.data.dataforseo_client import DataForSEOClient
from api.analysis.health_trajectory import analyze_health_trajectory
from api.analysis.page_triage import analyze_page_triage
from api.analysis.gameplan import generate_gameplan
from api.core.supabase_client import get_supabase_client

logger = logging.getLogger(__name__)


@dataclass
class ModuleResult:
    """Result from a single analysis module."""
    module_name: str
    success: bool
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    warning: Optional[str] = None
    execution_time_seconds: float = 0.0


@dataclass
class PipelineContext:
    """Shared context passed between pipeline stages."""
    report_id: str
    user_id: str
    gsc_property: str
    ga4_property: Optional[str]
    
    # Raw data storage
    gsc_data: Dict[str, Any] = field(default_factory=dict)
    ga4_data: Dict[str, Any] = field(default_factory=dict)
    serp_data: Dict[str, Any] = field(default_factory=dict)
    
    # Analysis results
    results: Dict[str, ModuleResult] = field(default_factory=dict)
    
    # Tracking
    start_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


class ReportPipeline:
    """
    Orchestrates the complete report generation pipeline with error handling,
    retry logic, and graceful degradation.
    """
    
    def __init__(
        self,
        gsc_client: GSCClient,
        ga4_client: Optional[GA4Client] = None,
        dataforseo_client: Optional[DataForSEOClient] = None,
        max_retries: int = 3,
        retry_delay_seconds: int = 5
    ):
        self.gsc_client = gsc_client
        self.ga4_client = ga4_client
        self.dataforseo_client = dataforseo_client
        self.max_retries = max_retries
        self.retry_delay_seconds = retry_delay_seconds
        self.supabase = get_supabase_client()
    
    async def generate_report(
        self,
        report_id: str,
        user_id: str,
        gsc_property: str,
        ga4_property: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Generate a complete Search Intelligence Report.
        
        Args:
            report_id: UUID of the report record
            user_id: UUID of the user
            gsc_property: GSC property URL
            ga4_property: Optional GA4 property ID
            
        Returns:
            Complete report data structure
        """
        ctx = PipelineContext(
            report_id=report_id,
            user_id=user_id,
            gsc_property=gsc_property,
            ga4_property=ga4_property
        )
        
        try:
            # Update status to ingesting
            await self._update_report_status(report_id, "ingesting", {
                "stage": "data_ingestion",
                "progress": 0
            })
            
            # Phase 1: Data Ingestion
            logger.info(f"Report {report_id}: Starting data ingestion")
            await self._ingest_data(ctx)
            
            # Update status to analyzing
            await self._update_report_status(report_id, "analyzing", {
                "stage": "analysis",
                "progress": 33
            })
            
            # Phase 2: Analysis Modules
            logger.info(f"Report {report_id}: Starting analysis modules")
            await self._run_analysis_modules(ctx)
            
            # Update status to generating
            await self._update_report_status(report_id, "generating", {
                "stage": "report_generation",
                "progress": 66
            })
            
            # Phase 3: Report Assembly
            logger.info(f"Report {report_id}: Assembling final report")
            report_data = await self._assemble_report(ctx)
            
            # Mark complete
            await self._update_report_status(report_id, "complete", {
                "stage": "complete",
                "progress": 100
            }, report_data)
            
            logger.info(f"Report {report_id}: Generation complete")
            return report_data
            
        except Exception as e:
            logger.error(f"Report {report_id}: Fatal error - {str(e)}")
            logger.error(traceback.format_exc())
            await self._update_report_status(report_id, "failed", {
                "stage": "error",
                "error": str(e),
                "traceback": traceback.format_exc()
            })
            raise
    
    async def _ingest_data(self, ctx: PipelineContext) -> None:
        """
        Ingest data from GSC, GA4, and optionally DataForSEO.
        Uses retry logic and graceful degradation.
        """
        # GSC data is mandatory
        gsc_success = await self._retry_with_backoff(
            self._fetch_gsc_data,
            ctx,
            "GSC data ingestion"
        )
        
        if not gsc_success:
            raise RuntimeError("Failed to fetch GSC data after all retries")
        
        # GA4 data is optional but highly valuable
        if self.ga4_client and ctx.ga4_property:
            ga4_success = await self._retry_with_backoff(
                self._fetch_ga4_data,
                ctx,
                "GA4 data ingestion"
            )
            
            if not ga4_success:
                ctx.warnings.append(
                    "GA4 data unavailable - report will continue with GSC data only. "
                    "Some sections (engagement metrics, conversions) will be limited."
                )
                logger.warning(f"Report {ctx.report_id}: GA4 data unavailable, continuing without it")
        else:
            ctx.warnings.append(
                "GA4 not connected - engagement and conversion data unavailable"
            )
        
        # SERP data is optional (Phase 2 feature)
        if self.dataforseo_client:
            serp_success = await self._retry_with_backoff(
                self._fetch_serp_data,
                ctx,
                "SERP data ingestion"
            )
            
            if not serp_success:
                ctx.warnings.append(
                    "SERP data unavailable - SERP landscape and CTR modeling sections will be limited"
                )
                logger.warning(f"Report {ctx.report_id}: SERP data unavailable")
    
    async def _fetch_gsc_data(self, ctx: PipelineContext) -> bool:
        """Fetch all required GSC data."""
        try:
            logger.info(f"Report {ctx.report_id}: Fetching GSC data")
            
            # Fetch daily time series (16 months)
            daily_data = await self.gsc_client.get_daily_performance(
                ctx.gsc_property,
                months_back=16
            )
            
            if not daily_data or daily_data.empty:
                logger.error(f"Report {ctx.report_id}: No GSC daily data returned")
                return False
            
            ctx.gsc_data['daily'] = daily_data
            logger.info(f"Report {ctx.report_id}: Fetched {len(daily_data)} days of GSC data")
            
            # Fetch per-page data
            page_data = await self.gsc_client.get_page_performance(
                ctx.gsc_property,
                months_back=16
            )
            
            if not page_data or page_data.empty:
                logger.warning(f"Report {ctx.report_id}: No GSC page data returned")
                ctx.gsc_data['pages'] = None
            else:
                ctx.gsc_data['pages'] = page_data
                logger.info(f"Report {ctx.report_id}: Fetched {len(page_data)} pages")
            
            # Fetch per-query data
            query_data = await self.gsc_client.get_query_performance(
                ctx.gsc_property,
                months_back=16
            )
            
            if not query_data or query_data.empty:
                logger.warning(f"Report {ctx.report_id}: No GSC query data returned")
                ctx.gsc_data['queries'] = None
            else:
                ctx.gsc_data['queries'] = query_data
                logger.info(f"Report {ctx.report_id}: Fetched {len(query_data)} queries")
            
            # Fetch page-date time series for per-page trends
            try:
                page_date_data = await self.gsc_client.get_page_date_performance(
                    ctx.gsc_property,
                    months_back=6  # 6 months is sufficient for trend analysis
                )
                
                if page_date_data and not page_date_data.empty:
                    ctx.gsc_data['page_date'] = page_date_data
                    logger.info(f"Report {ctx.report_id}: Fetched page-date time series")
                else:
                    ctx.gsc_data['page_date'] = None
                    ctx.warnings.append("Page-level time series unavailable - per-page trend analysis will be limited")
            except Exception as e:
                logger.warning(f"Report {ctx.report_id}: Could not fetch page-date data: {e}")
                ctx.gsc_data['page_date'] = None
                ctx.warnings.append("Page-level time series unavailable - per-page trend analysis will be limited")
            
            # Fetch query-page mapping
            try:
                query_page_data = await self.gsc_client.get_query_page_performance(
                    ctx.gsc_property,
                    months_back=3  # Last 3 months for current state
                )
                
                if query_page_data and not query_page_data.empty:
                    ctx.gsc_data['query_page'] = query_page_data
                    logger.info(f"Report {ctx.report_id}: Fetched query-page mapping")
                else:
                    ctx.gsc_data['query_page'] = None
                    ctx.warnings.append("Query-page mapping unavailable - cannibalization detection will be limited")
            except Exception as e:
                logger.warning(f"Report {ctx.report_id}: Could not fetch query-page data: {e}")
                ctx.gsc_data['query_page'] = None
                ctx.warnings.append("Query-page mapping unavailable - cannibalization detection will be limited")
            
            return True
            
        except Exception as e:
            logger.error(f"Report {ctx.report_id}: GSC data fetch failed: {e}")
            logger.error(traceback.format_exc())
            return False
    
    async def _fetch_ga4_data(self, ctx: PipelineContext) -> bool:
        """Fetch all required GA4 data."""
        try:
            logger.info(f"Report {ctx.report_id}: Fetching GA4 data")
            
            if not self.ga4_client or not ctx.ga4_property:
                return False
            
            # Fetch landing page engagement
            landing_pages = await self.ga4_client.get_landing_page_engagement(
                ctx.ga4_property,
                months_back=6
            )
            
            if not landing_pages or landing_pages.empty:
                logger.warning(f"Report {ctx.report_id}: No GA4 landing page data")
                ctx.ga4_data['landing_pages'] = None
            else:
                ctx.ga4_data['landing_pages'] = landing_pages
                logger.info(f"Report {ctx.report_id}: Fetched GA4 data for {len(landing_pages)} landing pages")
            
            # Fetch conversion data
            try:
                conversions = await self.ga4_client.get_conversions(
                    ctx.ga4_property,
                    months_back=6
                )
                
                if conversions and not conversions.empty:
                    ctx.ga4_data['conversions'] = conversions
                    logger.info(f"Report {ctx.report_id}: Fetched GA4 conversion data")
                else:
                    ctx.ga4_data['conversions'] = None
                    logger.info(f"Report {ctx.report_id}: No GA4 conversion data available")
            except Exception as e:
                logger.warning(f"Report {ctx.report_id}: Could not fetch conversion data: {e}")
                ctx.ga4_data['conversions'] = None
            
            # Fetch traffic sources
            try:
                sources = await self.ga4_client.get_traffic_sources(
                    ctx.ga4_property,
                    months_back=6
                )
                
                if sources and not sources.empty:
                    ctx.ga4_data['sources'] = sources
                    logger.info(f"Report {ctx.report_id}: Fetched GA4 traffic sources")
                else:
                    ctx.ga4_data['sources'] = None
            except Exception as e:
                logger.warning(f"Report {ctx.report_id}: Could not fetch traffic sources: {e}")
                ctx.ga4_data['sources'] = None
            
            return True
            
        except Exception as e:
            logger.error(f"Report {ctx.report_id}: GA4 data fetch failed: {e}")
            logger.error(traceback.format_exc())
            return False
    
    async def _fetch_serp_data(self, ctx: PipelineContext) -> bool:
        """Fetch SERP data for top keywords (Phase 2 feature)."""
        try:
            if not self.dataforseo_client:
                return False
            
            logger.info(f"Report {ctx.report_id}: Fetching SERP data")
            
            # Get top keywords from GSC query data
            if 'queries' not in ctx.gsc_data or ctx.gsc_data['queries'] is None:
                logger.warning(f"Report {ctx.report_id}: No query data for SERP lookup")
                return False
            
            queries_df = ctx.gsc_data['queries']
            
            # Filter and sort to get top non-branded keywords
            # TODO: Implement brand filtering logic
            top_keywords = queries_df.nlargest(50, 'impressions')['query'].tolist()
            
            if not top_keywords:
                logger.warning(f"Report {ctx.report_id}: No keywords for SERP lookup")
                return False
            
            # Fetch SERP data for top keywords
            serp_results = await self.dataforseo_client.get_serp_data(
                keywords=top_keywords[:30],  # Limit to 30 for cost control
                location="United States"  # TODO: Make configurable
            )
            
            if not serp_results:
                logger.warning(f"Report {ctx.report_id}: No SERP data returned")
                return False
            
            ctx.serp_data['keywords'] = serp_results
            logger.info(f"Report {ctx.report_id}: Fetched SERP data for {len(serp_results)} keywords")
            
            return True
            
        except Exception as e:
            logger.error(f"Report {ctx.report_id}: SERP data fetch failed: {e}")
            logger.error(traceback.format_exc())
            return False
    
    async def _run_analysis_modules(self, ctx: PipelineContext) -> None:
        """
        Run all analysis modules in sequence.
        Each module is independent and failures are isolated.
        """
        modules = [
            ("health_trajectory", self._run_health_trajectory),
            ("page_triage", self._run_page_triage),
            ("gameplan", self._run_gameplan),
        ]
        
        for i, (module_name, module_func) in enumerate(modules):
            progress = 33 + int((i / len(modules)) * 33)
            
            await self._update_report_status(ctx.report_id, "analyzing", {
                "stage": "analysis",
                "progress": progress,
                "current_module": module_name
            })
            
            result = await self._run_module_with_error_handling(
                module_name,
                module_func,
                ctx
            )
            
            ctx.results[module_name] = result
            
            if result.error:
                ctx.errors.append(f"{module_name}: {result.error}")
            
            if result.warning:
                ctx.warnings.append(f"{module_name}: {result.warning}")
    
    async def _run_module_with_error_handling(
        self,
        module_name: str,
        module_func: Any,
        ctx: PipelineContext
    ) -> ModuleResult:
        """
        Run a single analysis module with comprehensive error handling.
        """
        start_time = datetime.now(timezone.utc)
        
        try:
            logger.info(f"Report {ctx.report_id}: Running module {module_name}")
            
            result_data = await module_func(ctx)
            
            execution_time = (datetime.now(timezone.utc) - start_time).total_seconds()
            
            logger.info(f"Report {ctx.report_id}: Module {module_name} completed in {execution_time:.2f}s")
            
            return ModuleResult(
                module_name=module_name,
                success=True,
                data=result_data,
                execution_time_seconds=execution_time
            )
            
        except Exception as e:
            execution_time = (datetime.now(timezone.utc) - start_time).total_seconds()
            
            logger.error(f"Report {ctx.report_id}: Module {module_name} failed: {e}")
            logger.error(traceback.format_exc())
            
            return ModuleResult(
                module_name=module_name,
                success=False,
                error=str(e),
                execution_time_seconds=execution_time
            )
    
    async def _run_health_trajectory(self, ctx: PipelineContext) -> Dict[str, Any]:
        """Run Module 1: Health & Trajectory Analysis."""
        if 'daily' not in ctx.gsc_data or ctx.gsc_data['daily'] is None:
            raise ValueError("No daily data available for health trajectory analysis")
        
        return await analyze_health_trajectory(ctx.gsc_data['daily'])
    
    async def _run_page_triage(self, ctx: PipelineContext) -> Dict[str, Any]:
        """Run Module 2: Page-Level Triage."""
        page_daily = ctx.gsc_data.get('page_date')
        page_summary = ctx.gsc_data.get('pages')
        ga4_landing = ctx.ga4_data.get('landing_pages')
        
        if page_summary is None:
            raise ValueError("No page data available for triage analysis")
        
        # Page-daily data is optional but valuable
        if page_daily is None:
            ctx.warnings.append(
                "Page-level time series unavailable - trend analysis will be limited to summary data"
            )
        
        return await analyze_page_triage(
            page_daily_data=page_daily,
            page_summary_data=page_summary,
            ga4_landing_data=ga4_landing
        )
    
    async def _run_gameplan(self, ctx: PipelineContext) -> Dict[str, Any]:
        """Run Module 5: The Gameplan (synthesis of prior modules)."""
        # Ensure required modules completed successfully
        health = ctx.results.get('health_trajectory')
        triage = ctx.results.get('page_triage')
        
        if not health or not health.success or not health.data:
            raise ValueError("Health trajectory module did not complete successfully")
        
        if not triage or not triage.success or not triage.data:
            raise ValueError("Page triage module did not complete successfully")
        
        # SERP and content modules are optional (Phase 2)
        serp_data = None
        content_data = None
        
        return await generate_gameplan(
            health_data=health.data,
            triage_data=triage.data,
            serp_data=serp_data,
            content_data=content_data
        )
    
    async def _assemble_report(self, ctx: PipelineContext) -> Dict[str, Any]:
        """
        Assemble the final report structure from all module results.
        """
        execution_time = (datetime.now(timezone.utc) - ctx.start_time).total_seconds()
        
        report = {
            "report_id": ctx.report_id,
            "user_id": ctx.user_id,
            "gsc_property": ctx.gsc_property,
            "ga4_property": ctx.ga4_property,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "execution_time_seconds": execution_time,
            "data_sources": {
                "gsc": True,
                "ga4": bool(ctx.ga4_data and ctx.ga4_data.get('landing_pages') is not None),
                "serp": bool(ctx.serp_data and ctx.serp_data.get('keywords')),
            },
            "warnings": ctx.warnings,
            "errors": ctx.errors,
            "modules": {}
        }
        
        # Add each module's results
        for module_name, result in ctx.results.items():
            report["modules"][module_name] = {
                "success": result.success,
                "execution_time_seconds": result.execution_time_seconds,
                "data": result.data if result.success else None,
                "error": result.error,
                "warning": result.warning
            }
        
        # Calculate completeness score
        total_modules = len(ctx.results)
        successful_modules = sum(1 for r in ctx.results.values() if r.success)
        report["completeness_score"] = successful_modules / total_modules if total_modules > 0 else 0
        
        return report
    
    async def _retry_with_backoff(
        self,
        func: Any,
        ctx: PipelineContext,
        operation_name: str
    ) -> bool:
        """
        Retry a function with exponential backoff.
        
        Args:
            func: Async function to retry
            ctx: Pipeline context
            operation_name: Human-readable name for logging
            
        Returns:
            True if operation succeeded, False if all retries exhausted
        """
        for attempt in range(self.max_retries):
            try:
                result = await func(ctx)
                if result:
                    return True
                
                logger.warning(
                    f"Report {ctx.report_id}: {operation_name} returned False, "
                    f"attempt {attempt + 1}/{self.max_retries}"
                )
                
            except Exception as