"""
Report generation endpoints.

Orchestrates the async report generation pipeline, including:
1. Data ingestion from GSC, GA4, DataForSEO, site crawl
2. Sequential execution of 12 analysis modules
3. LLM synthesis pass for narrative generation
4. Final report assembly and storage

All heavy computation is async via background tasks.
Frontend polls for progress updates via /api/v1/reports/{report_id}/status
"""

from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks, Query
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
import asyncio
import logging
import traceback
import uuid

from ..database import get_supabase_client
from ..auth.dependencies import get_current_user
from ..modules.module1_health_trajectory import analyze_health_trajectory
from ..modules.module2_page_triage import analyze_page_triage
from ..modules.module3_serp_landscape import analyze_serp_landscape
from ..modules.module4_content_intelligence import analyze_content_intelligence
from ..modules.module5_technical_health import analyze_technical_health
from ..modules.module6_gameplan import generate_gameplan
from ..services.gsc_service import GSCService
from ..services.ga4_service import GA4Service
from ..services.dataforseao_service import DataForSEOService
from ..services.crawler_service import CrawlerService

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/reports", tags=["reports"])


# ============================================================================
# Request/Response Models
# ============================================================================

class ReportGenerationRequest(BaseModel):
    """Request to generate a new report."""
    site_url: str = Field(..., description="Full site URL (must match GSC property)")
    gsc_property_url: str = Field(..., description="Exact GSC property URL")
    ga4_property_id: str = Field(..., description="GA4 property ID (format: properties/123456789)")
    report_name: Optional[str] = Field(None, description="Custom name for report")
    lookback_months: int = Field(16, ge=3, le=24, description="Months of historical data to analyze")
    top_keywords_count: int = Field(100, ge=20, le=200, description="Number of top keywords to analyze")


class ReportStatusResponse(BaseModel):
    """Current status of report generation."""
    report_id: str
    status: str  # queued, running, completed, failed
    progress_pct: float
    current_step: Optional[str]
    steps_completed: List[str]
    steps_total: int
    estimated_completion_time: Optional[datetime]
    error_message: Optional[str]
    created_at: datetime
    updated_at: datetime


class ReportSummary(BaseModel):
    """Summary of a completed report."""
    report_id: str
    report_name: str
    site_url: str
    status: str
    created_at: datetime
    completed_at: Optional[datetime]
    overall_direction: Optional[str]
    total_pages_analyzed: Optional[int]
    total_keywords_analyzed: Optional[int]
    total_recoverable_clicks: Optional[int]


# ============================================================================
# Report Generation Pipeline
# ============================================================================

class ReportGenerator:
    """
    Orchestrates the full report generation pipeline.
    Each step updates progress in the database for frontend polling.
    """
    
    def __init__(self, report_id: str, user_id: str, config: ReportGenerationRequest):
        self.report_id = report_id
        self.user_id = user_id
        self.config = config
        self.supabase = get_supabase_client()
        
        # Define pipeline steps
        self.steps = [
            "init",
            "fetch_gsc_data",
            "fetch_ga4_data",
            "fetch_serp_data",
            "crawl_site",
            "module_1_health_trajectory",
            "module_2_page_triage",
            "module_3_serp_landscape",
            "module_4_content_intelligence",
            "module_5_technical_health",
            "module_6_gameplan",
            "finalize_report"
        ]
        
        self.current_step_idx = 0
        self.data_cache = {}
        self.results = {}
        
    async def update_progress(self, step: str, progress_pct: float, error: Optional[str] = None):
        """Update report generation progress in database."""
        try:
            status = "failed" if error else ("completed" if progress_pct >= 100 else "running")
            
            update_data = {
                "status": status,
                "progress_pct": progress_pct,
                "current_step": step,
                "steps_completed": self.steps[:self.current_step_idx],
                "updated_at": datetime.utcnow().isoformat(),
            }
            
            if error:
                update_data["error_message"] = error
            
            if progress_pct >= 100:
                update_data["completed_at"] = datetime.utcnow().isoformat()
            
            self.supabase.table("reports").update(update_data).eq("id", self.report_id).execute()
            
            logger.info(f"Report {self.report_id}: {step} - {progress_pct}%")
            
        except Exception as e:
            logger.error(f"Failed to update progress for report {self.report_id}: {e}")
    
    async def run(self):
        """Execute the full report generation pipeline."""
        try:
            await self.update_progress("init", 0)
            
            # Step 1: Fetch GSC data
            self.current_step_idx = 1
            await self.update_progress("fetch_gsc_data", 5)
            await self.fetch_gsc_data()
            await self.update_progress("fetch_gsc_data", 15)
            
            # Step 2: Fetch GA4 data
            self.current_step_idx = 2
            await self.update_progress("fetch_ga4_data", 15)
            await self.fetch_ga4_data()
            await self.update_progress("fetch_ga4_data", 25)
            
            # Step 3: Fetch SERP data
            self.current_step_idx = 3
            await self.update_progress("fetch_serp_data", 25)
            await self.fetch_serp_data()
            await self.update_progress("fetch_serp_data", 35)
            
            # Step 4: Crawl site
            self.current_step_idx = 4
            await self.update_progress("crawl_site", 35)
            await self.crawl_site()
            await self.update_progress("crawl_site", 45)
            
            # Step 5: Module 1 - Health & Trajectory
            self.current_step_idx = 5
            await self.update_progress("module_1_health_trajectory", 45)
            await self.run_module_1()
            await self.update_progress("module_1_health_trajectory", 55)
            
            # Step 6: Module 2 - Page Triage
            self.current_step_idx = 6
            await self.update_progress("module_2_page_triage", 55)
            await self.run_module_2()
            await self.update_progress("module_2_page_triage", 65)
            
            # Step 7: Module 3 - SERP Landscape
            self.current_step_idx = 7
            await self.update_progress("module_3_serp_landscape", 65)
            await self.run_module_3()
            await self.update_progress("module_3_serp_landscape", 72)
            
            # Step 8: Module 4 - Content Intelligence
            self.current_step_idx = 8
            await self.update_progress("module_4_content_intelligence", 72)
            await self.run_module_4()
            await self.update_progress("module_4_content_intelligence", 80)
            
            # Step 9: Module 5 - Technical Health
            self.current_step_idx = 9
            await self.update_progress("module_5_technical_health", 80)
            await self.run_module_5()
            await self.update_progress("module_5_technical_health", 88)
            
            # Step 10: Module 6 - Gameplan
            self.current_step_idx = 10
            await self.update_progress("module_6_gameplan", 88)
            await self.run_module_6()
            await self.update_progress("module_6_gameplan", 95)
            
            # Step 11: Finalize report
            self.current_step_idx = 11
            await self.update_progress("finalize_report", 95)
            await self.finalize_report()
            await self.update_progress("finalize_report", 100)
            
            logger.info(f"Report {self.report_id} completed successfully")
            
        except Exception as e:
            error_msg = f"Report generation failed: {str(e)}\n{traceback.format_exc()}"
            logger.error(f"Report {self.report_id} failed: {error_msg}")
            await self.update_progress(self.steps[self.current_step_idx], self.current_step_idx * 8, error=error_msg)
            raise
    
    async def fetch_gsc_data(self):
        """Fetch all required GSC data."""
        try:
            gsc_service = GSCService(self.supabase, self.user_id)
            
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=self.config.lookback_months * 30)
            
            # Fetch daily time series
            daily_data = await gsc_service.fetch_performance_data(
                property_url=self.config.gsc_property_url,
                start_date=start_date,
                end_date=end_date,
                dimensions=["date"]
            )
            
            # Fetch per-query data
            query_data = await gsc_service.fetch_performance_data(
                property_url=self.config.gsc_property_url,
                start_date=start_date,
                end_date=end_date,
                dimensions=["query"]
            )
            
            # Fetch per-page data
            page_data = await gsc_service.fetch_performance_data(
                property_url=self.config.gsc_property_url,
                start_date=start_date,
                end_date=end_date,
                dimensions=["page"]
            )
            
            # Fetch query+page mapping
            query_page_data = await gsc_service.fetch_performance_data(
                property_url=self.config.gsc_property_url,
                start_date=start_date,
                end_date=end_date,
                dimensions=["query", "page"]
            )
            
            # Fetch per-page daily time series (for trend analysis)
            page_daily_data = await gsc_service.fetch_performance_data(
                property_url=self.config.gsc_property_url,
                start_date=start_date,
                end_date=end_date,
                dimensions=["page", "date"]
            )
            
            self.data_cache["gsc_daily"] = daily_data
            self.data_cache["gsc_queries"] = query_data
            self.data_cache["gsc_pages"] = page_data
            self.data_cache["gsc_query_page"] = query_page_data
            self.data_cache["gsc_page_daily"] = page_daily_data
            
            logger.info(f"GSC data fetched: {len(daily_data)} daily records, {len(query_data)} queries, {len(page_data)} pages")
            
        except Exception as e:
            logger.error(f"Failed to fetch GSC data: {e}")
            raise
    
    async def fetch_ga4_data(self):
        """Fetch all required GA4 data."""
        try:
            ga4_service = GA4Service(self.supabase, self.user_id)
            
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=self.config.lookback_months * 30)
            
            # Fetch landing page engagement metrics
            landing_pages = await ga4_service.fetch_landing_page_metrics(
                property_id=self.config.ga4_property_id,
                start_date=start_date,
                end_date=end_date
            )
            
            # Fetch traffic overview
            traffic_overview = await ga4_service.fetch_traffic_overview(
                property_id=self.config.ga4_property_id,
                start_date=start_date,
                end_date=end_date
            )
            
            self.data_cache["ga4_landing_pages"] = landing_pages
            self.data_cache["ga4_traffic_overview"] = traffic_overview
            
            logger.info(f"GA4 data fetched: {len(landing_pages)} landing pages")
            
        except Exception as e:
            logger.error(f"Failed to fetch GA4 data: {e}")
            raise
    
    async def fetch_serp_data(self):
        """Fetch SERP data for top keywords."""
        try:
            serp_service = DataForSEOService(self.supabase)
            
            # Get top N keywords from GSC data
            gsc_queries = self.data_cache.get("gsc_queries", [])
            
            # Filter out branded queries
            site_domain = self.config.site_url.replace("https://", "").replace("http://", "").split("/")[0]
            brand_terms = site_domain.split(".")[0].lower()
            
            non_branded = [
                q for q in gsc_queries 
                if brand_terms not in q.get("query", "").lower()
            ]
            
            # Sort by impressions and take top N
            top_keywords = sorted(
                non_branded,
                key=lambda x: x.get("impressions", 0),
                reverse=True
            )[:self.config.top_keywords_count]
            
            # Fetch SERP data for each keyword
            serp_results = []
            for kw in top_keywords:
                try:
                    serp_data = await serp_service.fetch_serp_data(
                        keyword=kw["query"],
                        location="United States"
                    )
                    serp_results.append({
                        "keyword": kw["query"],
                        "impressions": kw.get("impressions", 0),
                        "position": kw.get("position", 0),
                        "serp_data": serp_data
                    })
                except Exception as e:
                    logger.warning(f"Failed to fetch SERP data for '{kw['query']}': {e}")
                    continue
            
            self.data_cache["serp_results"] = serp_results
            
            logger.info(f"SERP data fetched for {len(serp_results)} keywords")
            
        except Exception as e:
            logger.error(f"Failed to fetch SERP data: {e}")
            raise
    
    async def crawl_site(self):
        """Crawl site for internal link graph and technical data."""
        try:
            crawler_service = CrawlerService(self.supabase)
            
            crawl_results = await crawler_service.crawl_site(
                site_url=self.config.site_url,
                max_pages=5000
            )
            
            self.data_cache["crawl_results"] = crawl_results
            
            logger.info(f"Site crawl completed: {len(crawl_results.get('pages', []))} pages")
            
        except Exception as e:
            logger.error(f"Failed to crawl site: {e}")
            raise
    
    async def run_module_1(self):
        """Run Module 1: Health & Trajectory analysis."""
        try:
            daily_data = self.data_cache.get("gsc_daily", [])
            
            if not daily_data:
                raise ValueError("No daily GSC data available for health trajectory analysis")
            
            result = await analyze_health_trajectory(daily_data)
            self.results["module_1_health_trajectory"] = result
            
            logger.info(f"Module 1 completed: direction={result.get('overall_direction')}")
            
        except Exception as e:
            logger.error(f"Module 1 failed: {e}")
            self.results["module_1_health_trajectory"] = {"error": str(e)}
    
    async def run_module_2(self):
        """Run Module 2: Page-level triage analysis."""
        try:
            page_daily_data = self.data_cache.get("gsc_page_daily", [])
            ga4_landing_data = self.data_cache.get("ga4_landing_pages", [])
            gsc_page_summary = self.data_cache.get("gsc_pages", [])
            
            if not page_daily_data:
                raise ValueError("No page-level GSC data available for triage analysis")
            
            result = await analyze_page_triage(
                page_daily_data,
                ga4_landing_data,
                gsc_page_summary
            )
            self.results["module_2_page_triage"] = result
            
            logger.info(f"Module 2 completed: {result.get('summary', {}).get('total_pages_analyzed', 0)} pages analyzed")
            
        except Exception as e:
            logger.error(f"Module 2 failed: {e}")
            self.results["module_2_page_triage"] = {"error": str(e)}
    
    async def run_module_3(self):
        """Run Module 3: SERP landscape analysis."""
        try:
            serp_data = self.data_cache.get("serp_results", [])
            gsc_keyword_data = self.data_cache.get("gsc_queries", [])
            
            if not serp_data:
                raise ValueError("No SERP data available for landscape analysis")
            
            result = await analyze_serp_landscape(serp_data, gsc_keyword_data)
            self.results["module_3_serp_landscape"] = result
            
            logger.info(f"Module 3 completed: {result.get('keywords_analyzed', 0)} keywords analyzed")
            
        except Exception as e:
            logger.error(f"Module 3 failed: {e}")
            self.results["module_3_serp_landscape"] = {"error": str(e)}
    
    async def run_module_4(self):
        """Run Module 4: Content intelligence analysis."""
        try:
            gsc_query_page = self.data_cache.get("gsc_query_page", [])
            page_data = self.data_cache.get("crawl_results", {}).get("pages", [])
            ga4_engagement = self.data_cache.get("ga4_landing_pages", [])
            
            if not gsc_query_page:
                raise ValueError("No query-page mapping data available for content intelligence")
            
            result = await analyze_content_intelligence(
                gsc_query_page,
                page_data,
                ga4_engagement
            )
            self.results["module_4_content_intelligence"] = result
            
            logger.info(f"Module 4 completed: {len(result.get('cannibalization_clusters', []))} cannibalization issues found")
            
        except Exception as e:
            logger.error(f"Module 4 failed: {e}")
            self.results["module_4_content_intelligence"] = {"error": str(e)}
    
    async def run_module_5(self):
        """Run Module 5: Technical health analysis."""
        try:
            crawl_results = self.data_cache.get("crawl_results", {})
            gsc_pages = self.data_cache.get("gsc_pages", [])
            
            if not crawl_results:
                raise ValueError("No crawl data available for technical health analysis")
            
            result = await analyze_technical_health(crawl_results, gsc_pages)
            self.results["module_5_technical_health"] = result
            
            logger.info(f"Module 5 completed: {result.get('total_issues', 0)} technical issues found")
            
        except Exception as e:
            logger.error(f"Module 5 failed: {e}")
            self.results["module_5_technical_health"] = {"error": str(e)}
    
    async def run_module_6(self):
        """Run Module 6: Gameplan synthesis."""
        try:
            health = self.results.get("module_1_health_trajectory", {})
            triage = self.results.get("module_2_page_triage", {})
            serp = self.results.get("module_3_serp_landscape", {})
            content = self.results.get("module_4_content_intelligence", {})
            technical = self.results.get("module_5_technical_health", {})
            
            result = await generate_gameplan(health, triage, serp, content, technical)
            self.results["module_6_gameplan"] = result
            
            logger.info(f"Module 6 completed: {len(result.get('critical', []))} critical actions identified")
            
        except Exception as e:
            logger.error(f"Module 6 failed: {e}")
            self.results["module_6_gameplan"] = {"error": str(e)}
    
    async def finalize_report(self):
        """Assemble final report and save to database."""
        try:
            # Build final report structure
            report_data = {
                "report_id": self.report_id,
                "user_id": self.user_id,
                "site_url": self.config.site_url,
                "report_name": self.config.report_name or f"Report for {self.config.site_url}",
                "generated_at": datetime.utcnow().isoformat(),
                "config": self.config.dict(),
                "results": self.results,
                "summary": self._generate_summary()
            }
            
            # Save to database
            self.supabase.table("reports").update({
                "report_data": report_data,
                "status": "completed",
                "completed_at": datetime.utcnow().isoformat()
            }).eq("id", self.report_id).execute()
            
            logger.info(f"Report {self.report_id} finalized and saved")
            
        except Exception as e:
            logger.error(f"Failed to finalize report: {e}")
            raise
    
    def _generate_summary(self) -> Dict[str, Any]:
        """Generate report summary statistics."""
        health = self.results.get("module_1_health_trajectory", {})
        triage = self.results.get("module_2_page_triage", {})
        serp = self.results.get("module_3_serp_landscape", {})
        gameplan = self.results.get("module_6_gameplan", {})
        
        return {
            "overall_direction": health.get("overall_direction"),
            "trend_slope_pct_per_month": health.get("trend_slope_pct_per_month"),
            "total_pages_analyzed": triage.get("summary", {}).get("total_pages_analyzed"),
            "total_keywords_analyzed": serp.get("keywords_analyzed"),
            "total_recoverable_clicks": gameplan.get("total_estimated_monthly_click_recovery"),
            "total_growth_opportunity": gameplan.get("total_estimated_monthly_click_growth"),
            "critical_actions_count": len(gameplan.get("critical", [])),
            "quick_wins_count": len(gameplan.get("quick_wins", []))
        }


# ============================================================================
# API Endpoints
# ============================================================================

@router.post("/generate", response_model=Dict[str, str])
async def generate_report(
    request: ReportGenerationRequest,
    background_tasks: BackgroundTasks,
    current_user: Dict = Depends(get_current_user)
):
    """
    Start async report generation.
    
    Returns immediately with report_id.
    Frontend polls /api/v1/reports/{report_id}/status for progress.
    """
    try:
        report_id = str(uuid.uuid4())
        
        # Create report record
        supabase = get_supabase_client()
        supabase.table("reports").insert({
            "id": report_id,
            "user_id": current_user["id"],
            "site_url": request.site_url,
            "report_name": request.report_name or f"Report for {request.site_url}",
            "status": "queued",
            "progress_pct": 0,
            "current_step": "init",
            "steps_completed": [],
            "steps_total": 12,
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat()
        }).execute()
        
        # Start background task
        generator = ReportGenerator(report_id, current_user["id"], request)
        background_tasks.add_task(generator.run)
        
        logger.info(f"Report generation started: {report_id} for user {current_user['id']}")
        
        return {"report_id": report_id}
        
    except Exception as e:
        logger.error(f"Failed to start report generation: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to start report generation: {str(e)}")


@router.get("/{report_id}/status", response_model=ReportStatusResponse)
async def get_report_status(
    report_id: str,
    current_user: Dict = Depends(get_current_user)
):
    """Get current status of report generation."""
    try:
        supabase = get_supabase_client()
        
        result = supabase.table("reports").select("*").eq("id", report_id).eq("user_id", current_user["id"]).single().execute()
        
        if not result.data:
            raise HTTPException(status_code=404, detail="Report not found")
        
        report = result.data
        
        # Calculate estimated completion time
        estimated_completion = None
        if report["status"] == "running" and report["progress_pct"] > 0:
            elapsed_seconds = (datetime.utcnow() - datetime.fromisoformat(report["created_at"])).total_seconds()
            estimated_total_seconds = (elapsed_seconds / report["progress_pct"]) * 100
            estimated_completion = datetime.fromisoformat(report["created_at"]) + timedelta(seconds=estimated_total_seconds)
        
        return ReportStatusResponse(
            report_id=report["id"],
            status=report["status"],
            progress_pct=report["progress_pct"],
            current_step=report.get("current_step"),
            steps_completed=report.get("steps_completed", []),
            steps_total=report.get("steps_total", 12),
            estimated_completion_time=estimated_completion,
            error_message=report.get("error_message"),
            created_at=datetime.fromisoformat(report["created_at"]),
            updated_at=datetime.fromisoformat(report["updated_at"])
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get report status: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get report status: {str(e)}")


@router.get("/{report_id}", response_model=Dict[str, Any])
async def get_report(
    report_id: str,
    current_user: Dict = Depends(get_current_user)
):
    """Get completed report data."""
    try:
        supabase = get_supabase_client()
        
        result = supabase.table("reports").select("*").eq("id", report_id).eq("user_id", current_user["id"]).single().execute()
        
        if not result.data:
            raise HTTPException(status_code=404, detail="Report not found")
        
        report = result.data
        
        if report["status"] != "completed":
            raise HTTPException(status_code=400, detail=f"Report is not completed yet (status: {report['status']})")
        
        return report.get("report_data", {})
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get report: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get report: {str(e)}")


@router.get("", response_model=List[ReportSummary])
async def list_reports(
    current_user: Dict = Depends(get_current_user),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0)
):
    """List all reports for current user."""
    try:
        supabase = get_supabase_client()
        
        result = supabase.table("reports").select(
            "id, site_url, report_name, status, created_at, completed_at, report_data"
        ).eq("user_id", current_user["id"]).order("created_at", desc=True).range(offset, offset + limit - 1).execute()
        
        reports = []
        for report in result.data:
            summary_data = report.get("report_data", {}).get("summary", {})
            reports.append(ReportSummary(
                report_id=report["id"],
                report_name=report["report_name"],
                site_url=report["site_url"],
                status=report["status"],
                created_at=datetime.fromisoformat(report["created_at"]),
                completed_at=datetime.fromisoformat(report["completed_at"]) if report.get("completed_at") else None,
                overall_direction=summary_data.get("overall_direction"),
                total_pages_analyzed=summary_data.get("total_pages_analyzed"),
                total_keywords_analyzed=summary_data.get("total_keywords_analyzed"),
                total_recoverable_clicks=summary_data.get("total_recoverable_clicks")
            ))
        
        return reports
        
    except Exception as e:
        logger.error(f"Failed to list reports: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to list reports: {str(e)}")


@router.delete("/{report_id}")
async def delete_report(
    report_id: str,
    current_user: Dict = Depends(get_current_user)
):
    """Delete a report."""
    try:
        supabase = get_supabase_client()
        
        # Verify ownership
        result = supabase.table("reports").select("id").eq("id", report_id).eq("user_id", current_user["id"]).single().execute()
        
        if not result.data:
            raise HTTPException(status_code=404, detail="Report not found")
        
        # Delete
        supabase.table("reports").delete().eq("id", report_id).execute()
        
        logger.info(f"Report {report_id} deleted by user {current_user['id']}")
        
        return {"message": "Report deleted successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete report: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to delete report: {str(e)}")
