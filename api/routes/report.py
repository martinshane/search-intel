# api/routes/report.py

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

from ..database import get_supabase_client
from ..auth.dependencies import get_current_user
from ..modules.module1_health_trajectory import analyze_health_trajectory
from ..modules.module2_page_triage import analyze_page_triage
from ..modules.module5_gameplan import generate_gameplan
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

async def run_report_generation_pipeline(
    report_id: str,
    user_id: str,
    request_data: ReportGenerationRequest
):
    """
    Main async pipeline for report generation.
    Runs as a background task, updates progress in Supabase.
    """
    supabase = get_supabase_client()
    
    # Initialize services
    gsc_service = GSCService(supabase, user_id)
    ga4_service = GA4Service(supabase, user_id)
    serp_service = DataForSEOService()
    crawler_service = CrawlerService()
    
    steps = [
        "fetch_gsc_data",
        "fetch_ga4_data",
        "fetch_serp_data",
        "crawl_site",
        "module_1_health",
        "module_2_triage",
        "module_3_serp",
        "module_4_content",
        "module_5_gameplan",
        "module_6_algorithm",
        "module_7_intent",
        "module_8_link_authority",
        "module_9_technical",
        "module_10_conversion",
        "module_11_seasonality",
        "module_12_competitive",
        "synthesis"
    ]
    
    async def update_progress(step: str, progress_pct: float, error: Optional[str] = None):
        """Update report status in database."""
        try:
            status = "failed" if error else ("completed" if progress_pct >= 100 else "running")
            
            update_data = {
                "status": status,
                "progress_pct": progress_pct,
                "current_step": step,
                "updated_at": datetime.utcnow().isoformat(),
            }
            
            if error:
                update_data["error_message"] = error
            
            if status == "completed":
                update_data["completed_at"] = datetime.utcnow().isoformat()
            
            supabase.table("reports").update(update_data).eq("id", report_id).execute()
            
        except Exception as e:
            logger.error(f"Failed to update report progress: {e}")
    
    try:
        # Step 1: Fetch GSC data
        await update_progress("fetch_gsc_data", 5.0)
        logger.info(f"Report {report_id}: Fetching GSC data")
        
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=request_data.lookback_months * 30)
        
        gsc_daily_data = await gsc_service.fetch_daily_performance(
            property_url=request_data.gsc_property_url,
            start_date=start_date,
            end_date=end_date
        )
        
        gsc_query_data = await gsc_service.fetch_query_performance(
            property_url=request_data.gsc_property_url,
            start_date=start_date,
            end_date=end_date,
            limit=request_data.top_keywords_count * 2  # Fetch extra for filtering
        )
        
        gsc_page_data = await gsc_service.fetch_page_performance(
            property_url=request_data.gsc_property_url,
            start_date=start_date,
            end_date=end_date
        )
        
        gsc_query_page_data = await gsc_service.fetch_query_page_performance(
            property_url=request_data.gsc_property_url,
            start_date=start_date,
            end_date=end_date
        )
        
        # Cache raw data
        await supabase.table("report_data_cache").insert({
            "report_id": report_id,
            "data_type": "gsc_daily",
            "data": gsc_daily_data,
            "fetched_at": datetime.utcnow().isoformat()
        }).execute()
        
        # Step 2: Fetch GA4 data
        await update_progress("fetch_ga4_data", 15.0)
        logger.info(f"Report {report_id}: Fetching GA4 data")
        
        ga4_landing_pages = await ga4_service.fetch_landing_page_performance(
            property_id=request_data.ga4_property_id,
            start_date=start_date,
            end_date=end_date
        )
        
        ga4_traffic_sources = await ga4_service.fetch_traffic_sources(
            property_id=request_data.ga4_property_id,
            start_date=start_date,
            end_date=end_date
        )
        
        ga4_conversions = await ga4_service.fetch_conversion_data(
            property_id=request_data.ga4_property_id,
            start_date=start_date,
            end_date=end_date
        )
        
        await supabase.table("report_data_cache").insert({
            "report_id": report_id,
            "data_type": "ga4_landing_pages",
            "data": ga4_landing_pages,
            "fetched_at": datetime.utcnow().isoformat()
        }).execute()
        
        # Step 3: Fetch SERP data
        await update_progress("fetch_serp_data", 25.0)
        logger.info(f"Report {report_id}: Fetching SERP data")
        
        # Filter to top non-branded keywords
        top_keywords = await filter_top_keywords(
            gsc_query_data,
            request_data.site_url,
            limit=request_data.top_keywords_count
        )
        
        serp_data = await serp_service.fetch_serp_data(
            keywords=top_keywords,
            location="United States"  # TODO: Make configurable
        )
        
        await supabase.table("report_data_cache").insert({
            "report_id": report_id,
            "data_type": "serp_data",
            "data": serp_data,
            "fetched_at": datetime.utcnow().isoformat()
        }).execute()
        
        # Step 4: Crawl site
        await update_progress("crawl_site", 35.0)
        logger.info(f"Report {report_id}: Crawling site")
        
        crawl_data = await crawler_service.crawl_site(
            site_url=request_data.site_url,
            max_pages=5000,
            respect_robots=True
        )
        
        await supabase.table("report_data_cache").insert({
            "report_id": report_id,
            "data_type": "crawl_data",
            "data": crawl_data,
            "fetched_at": datetime.utcnow().isoformat()
        }).execute()
        
        # Step 5-16: Run analysis modules
        module_results = {}
        
        # Module 1: Health & Trajectory
        await update_progress("module_1_health", 40.0)
        logger.info(f"Report {report_id}: Running Module 1 - Health & Trajectory")
        
        try:
            module1_result = analyze_health_trajectory(
                daily_data=gsc_daily_data,
                lookback_months=request_data.lookback_months
            )
            module_results["module_1"] = module1_result
            
            # Store module result
            await supabase.table("report_modules").insert({
                "report_id": report_id,
                "module_number": 1,
                "module_name": "Health & Trajectory",
                "status": "completed",
                "result_data": module1_result,
                "completed_at": datetime.utcnow().isoformat()
            }).execute()
            
        except Exception as e:
            logger.error(f"Module 1 failed: {e}\n{traceback.format_exc()}")
            module_results["module_1"] = {"error": str(e), "status": "failed"}
            await supabase.table("report_modules").insert({
                "report_id": report_id,
                "module_number": 1,
                "module_name": "Health & Trajectory",
                "status": "failed",
                "error_message": str(e),
                "completed_at": datetime.utcnow().isoformat()
            }).execute()
        
        # Module 2: Page-Level Triage
        await update_progress("module_2_triage", 50.0)
        logger.info(f"Report {report_id}: Running Module 2 - Page-Level Triage")
        
        try:
            module2_result = analyze_page_triage(
                page_daily_data=gsc_page_data,
                ga4_landing_data=ga4_landing_pages,
                gsc_page_summary=gsc_page_data
            )
            module_results["module_2"] = module2_result
            
            await supabase.table("report_modules").insert({
                "report_id": report_id,
                "module_number": 2,
                "module_name": "Page-Level Triage",
                "status": "completed",
                "result_data": module2_result,
                "completed_at": datetime.utcnow().isoformat()
            }).execute()
            
        except Exception as e:
            logger.error(f"Module 2 failed: {e}\n{traceback.format_exc()}")
            module_results["module_2"] = {"error": str(e), "status": "failed"}
            await supabase.table("report_modules").insert({
                "report_id": report_id,
                "module_number": 2,
                "module_name": "Page-Level Triage",
                "status": "failed",
                "error_message": str(e),
                "completed_at": datetime.utcnow().isoformat()
            }).execute()
        
        # Module 3: SERP Landscape (placeholder - full implementation needed)
        await update_progress("module_3_serp", 60.0)
        logger.info(f"Report {report_id}: Running Module 3 - SERP Landscape")
        
        module_results["module_3"] = {
            "status": "not_implemented",
            "message": "SERP Landscape analysis pending full implementation"
        }
        
        # Module 4: Content Intelligence (placeholder)
        await update_progress("module_4_content", 65.0)
        logger.info(f"Report {report_id}: Running Module 4 - Content Intelligence")
        
        module_results["module_4"] = {
            "status": "not_implemented",
            "message": "Content Intelligence analysis pending full implementation"
        }
        
        # Module 5: The Gameplan
        await update_progress("module_5_gameplan", 75.0)
        logger.info(f"Report {report_id}: Running Module 5 - The Gameplan")
        
        try:
            module5_result = generate_gameplan(
                health=module_results.get("module_1", {}),
                triage=module_results.get("module_2", {}),
                serp=module_results.get("module_3", {}),
                content=module_results.get("module_4", {})
            )
            module_results["module_5"] = module5_result
            
            await supabase.table("report_modules").insert({
                "report_id": report_id,
                "module_number": 5,
                "module_name": "The Gameplan",
                "status": "completed",
                "result_data": module5_result,
                "completed_at": datetime.utcnow().isoformat()
            }).execute()
            
        except Exception as e:
            logger.error(f"Module 5 failed: {e}\n{traceback.format_exc()}")
            module_results["module_5"] = {"error": str(e), "status": "failed"}
            await supabase.table("report_modules").insert({
                "report_id": report_id,
                "module_number": 5,
                "module_name": "The Gameplan",
                "status": "failed",
                "error_message": str(e),
                "completed_at": datetime.utcnow().isoformat()
            }).execute()
        
        # Modules 6-12: Placeholders for now
        for i, module_name in enumerate([
            "Algorithm Update Impact",
            "Query Intent Migration",
            "Link Authority Flow",
            "Technical Health",
            "Conversion Funnel",
            "Seasonality Intelligence",
            "Competitive Dynamics"
        ], start=6):
            progress_pct = 75.0 + (i - 5) * 3.0
            await update_progress(f"module_{i}_{module_name.lower().replace(' ', '_')}", progress_pct)
            module_results[f"module_{i}"] = {
                "status": "not_implemented",
                "message": f"{module_name} analysis pending full implementation"
            }
        
        # Final synthesis
        await update_progress("synthesis", 95.0)
        logger.info(f"Report {report_id}: Running final synthesis")
        
        # Assemble final report
        final_report = {
            "report_id": report_id,
            "report_name": request_data.report_name or f"Report for {request_data.site_url}",
            "site_url": request_data.site_url,
            "generated_at": datetime.utcnow().isoformat(),
            "parameters": {
                "lookback_months": request_data.lookback_months,
                "top_keywords_count": request_data.top_keywords_count,
                "date_range": {
                    "start": start_date.isoformat(),
                    "end": end_date.isoformat()
                }
            },
            "modules": module_results,
            "summary": extract_report_summary(module_results)
        }
        
        # Store final report
        await supabase.table("reports").update({
            "report_data": final_report,
            "status": "completed",
            "progress_pct": 100.0,
            "completed_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat()
        }).eq("id", report_id).execute()
        
        logger.info(f"Report {report_id}: Generation completed successfully")
        
    except Exception as e:
        error_msg = f"Pipeline failed: {str(e)}\n{traceback.format_exc()}"
        logger.error(f"Report {report_id}: {error_msg}")
        await update_progress("failed", 0.0, error=error_msg)


async def filter_top_keywords(
    gsc_query_data: List[Dict],
    site_url: str,
    limit: int
) -> List[str]:
    """
    Filter to top non-branded keywords.
    
    Args:
        gsc_query_data: Raw GSC query data
        site_url: Site URL to detect branded queries
        limit: Number of keywords to return
    
    Returns:
        List of top keyword strings
    """
    # Extract domain name for brand detection
    domain = site_url.replace("https://", "").replace("http://", "").split("/")[0]
    brand_terms = domain.split(".")[0].lower().split("-")
    
    # Filter out branded queries
    non_branded = []
    for query_row in gsc_query_data:
        query = query_row.get("keys", [""])[0].lower() if "keys" in query_row else query_row.get("query", "").lower()
        
        # Skip if any brand term appears in query
        if any(term in query for term in brand_terms if len(term) > 2):
            continue
        
        non_branded.append({
            "query": query,
            "impressions": query_row.get("impressions", 0),
            "clicks": query_row.get("clicks", 0),
            "position": query_row.get("position", 100)
        })
    
    # Sort by impressions descending
    non_branded.sort(key=lambda x: x["impressions"], reverse=True)
    
    # Also include queries with significant position changes
    # TODO: Implement position change detection from historical data
    
    return [q["query"] for q in non_branded[:limit]]


def extract_report_summary(module_results: Dict[str, Any]) -> Dict[str, Any]:
    """Extract high-level summary from module results."""
    summary = {
        "overall_direction": None,
        "total_pages_analyzed": None,
        "total_keywords_analyzed": None,
        "total_recoverable_clicks": None,
        "critical_issues_count": 0,
        "quick_wins_count": 0
    }
    
    # Extract from Module 1 (Health)
    if "module_1" in module_results and "error" not in module_results["module_1"]:
        m1 = module_results["module_1"]
        summary["overall_direction"] = m1.get("overall_direction")
    
    # Extract from Module 2 (Triage)
    if "module_2" in module_results and "error" not in module_results["module_2"]:
        m2 = module_results["module_2"]
        summary["total_pages_analyzed"] = m2.get("summary", {}).get("total_pages_analyzed")
        summary["total_recoverable_clicks"] = m2.get("summary", {}).get("total_recoverable_clicks_monthly")
    
    # Extract from Module 5 (Gameplan)
    if "module_5" in module_results and "error" not in module_results["module_5"]:
        m5 = module_results["module_5"]
        summary["critical_issues_count"] = len(m5.get("critical", []))
        summary["quick_wins_count"] = len(m5.get("quick_wins", []))
    
    return summary


# ============================================================================
# API Endpoints
# ============================================================================

@router.post("/generate", response_model=Dict[str, str])
async def generate_report(
    request: ReportGenerationRequest,
    background_tasks: BackgroundTasks,
    user = Depends(get_current_user)
):
    """
    Initiate report generation for a site.
    
    This endpoint:
    1. Creates a report record in the database
    2. Queues a background task to run the full analysis pipeline
    3. Returns immediately with a report_id for status polling
    
    The frontend should poll GET /reports/{report_id}/status for progress updates.
    """
    supabase = get_supabase_client()
    
    # Verify user has connected GSC + GA4
    auth_result = supabase.table("oauth_tokens").select("*").eq("user_id", user.id).execute()
    
    if not auth_result.data:
        raise HTTPException(status_code=400, detail="Please connect Google Search Console and GA4 first")
    
    # Check for GSC and GA4 tokens
    has_gsc = any(token["provider"] == "google_search_console" for token in auth_result.data)
    has_ga4 = any(token["provider"] == "google_analytics_4" for token in auth_result.data)
    
    if not has_gsc or not has_ga4:
        raise HTTPException(
            status_code=400,
            detail="Both Google Search Console and GA4 connections required"
        )
    
    # Create report record
    report_name = request.report_name or f"Report for {request.site_url}"
    
    report_data = {
        "user_id": user.id,
        "report_name": report_name,
        "site_url": request.site_url,
        "gsc_property_url": request.gsc_property_url,
        "ga4_property_id": request.ga4_property_id,
        "status": "queued",
        "progress_pct": 0.0,
        "current_step": "initializing",
        "created_at": datetime.utcnow().isoformat(),
        "updated_at": datetime.utcnow().isoformat(),
        "parameters": {
            "lookback_months": request.lookback_months,
            "top_keywords_count": request.top_keywords_count
        }
    }
    
    result = supabase.table("reports").insert(report_data).execute()
    report_id = result.data[0]["id"]
    
    # Queue background task
    background_tasks.add_task(
        run_report_generation_pipeline,
        report_id=report_id,
        user_id=user.id,
        request_data=request
    )
    
    logger.info(f"Report generation queued: {report_id} for user {user.id}")
    
    return {
        "report_id": report_id,
        "message": "Report generation started. Poll /reports/{report_id}/status for progress."
    }


@router.get("/{report_id}/status", response_model=ReportStatusResponse)
async def get_report_status(
    report_id: str,
    user = Depends(get_current_user)
):
    """Get current status of a report generation job."""
    supabase = get_supabase_client()
    
    result = supabase.table("reports").select("*").eq("id", report_id).eq("user_id", user.id).execute()
    
    if not result.data:
        raise HTTPException(status_code=404, detail="Report not found")
    
    report = result.data[0]
    
    # Calculate estimated completion time
    estimated_completion = None
    if report["status"] == "running" and report["progress_pct"] > 5:
        # Rough estimate: if we're at X% after Y minutes, extrapolate to 100%
        elapsed = (datetime.utcnow() - datetime.fromisoformat(report["created_at"])).total_seconds() / 60
        remaining_pct = 100 - report["progress_pct"]
        rate = report["progress_pct"] / elapsed if elapsed > 0 else 0
        if rate > 0:
            remaining_minutes = remaining_pct / rate
            estimated_completion = datetime.utcnow() + timedelta(minutes=remaining_minutes)
    
    # Get completed steps
    module_results = supabase.table("report_modules").select("module_number").eq("report_id", report_id).eq("status", "completed").execute()
    steps_completed = [f"module_{m['module_number']}" for m in module_results.data]
    
    return ReportStatusResponse(
        report_id=report_id,
        status=report["status"],
        progress_pct=report["progress_pct"],
        current_step=report.get("current_step"),
        steps_completed=steps_completed,
        steps_total=17,  # 4 data fetching + 12 modules + 1 synthesis
        estimated_completion_time=estimated_completion,
        error_message=report.get("error_message"),
        created_at=datetime.fromisoformat(report["created_at"]),
        updated_at=datetime.fromisoformat(report["updated_at"])
    )


@router.get("/{report_id}", response_model=Dict[str, Any])
async def get_report(
    report_id: str,
    user = Depends(get_current_user)
):
    """Get the full report data (only available when status=completed)."""
    supabase = get_supabase_client()
    
    result = supabase.table("reports").select("*").eq("id", report_id).eq("user_id", user.id).execute()
    
    if not result.data:
        raise HTTPException(status_code=404, detail="Report not found")
    
    report = result.data[0]
    
    if report["status"] != "completed":
        raise HTTPException(
            status_code=400,
            detail=f"Report not ready yet. Current status: {report['status']}"
        )
    
    return report.get("report_data", {})


@router.get("/", response_model=List[ReportSummary])
async def list_reports(
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    user = Depends(get_current_user)
):
    """List all reports for the current user."""
    supabase = get_supabase_client()
    
    result = supabase.table("reports") \
        .select("id, report_name, site_url, status, created_at, completed_at, report_data") \
        .eq("user_id", user.id) \
        .order("created_at", desc=True) \
        .range(offset, offset + limit - 1) \
        .execute()
    
    summaries = []
    for report in result.data:
        summary_data = {
            "report_id": report["id"],
            "report_name": report["report_name"],
            "site_url": report["site_url"],
            "status": report["status"],
            "created_at": datetime.fromisoformat(report["created_at"]),
            "completed_at": datetime.fromisoformat(report["completed_at"]) if report.get("completed_at") else None
        }
        
        # Extract summary metrics if available
        if report.get("report_data") and "summary" in report["report_data"]:
            summary = report["report_data"]["summary"]
            summary_data.update({
                "overall_direction": summary.get("overall_direction"),
                "total_pages_analyzed": summary.get("total_pages_analyzed"),
                "total_keywords_analyzed": summary.get("total_keywords_analyzed"),
                "total_recoverable_clicks": summary.get("total_recoverable_clicks")
            })
        
        summaries.append(ReportSummary(**summary_data))
    
    return summaries


@router.delete("/{report_id}")
async def delete_report(
    report_id: str,
    user = Depends(get_current_user)
):
    """Delete a report and all associated data."""
    supabase = get_supabase_client()
    
    # Verify ownership
    result = supabase.table("reports").select("id").eq("id", report_id).eq("user_id", user.id).execute()
    
    if not result.data:
        raise HTTPException(status_code=404, detail="Report not found")
    
    # Delete associated module results
    supabase.table("report_modules").delete().eq("report_id", report_id).execute()
    
    # Delete cached data
    supabase.table("report_data_cache").delete().eq("report_id", report_id).execute()
    
    # Delete report
    supabase.table("reports").delete().eq("id", report_id).execute()
    
    logger.info(f"Report deleted: {report_id} by user {user.id}")
    
    return {"message": "Report deleted successfully"}


@router.post("/{report_id}/regenerate")
async def regenerate_report(
    report_id: str,
    background_tasks: BackgroundTasks,
    user = Depends(get_current_user)
):
    """Regenerate an existing report (re-fetches data and re-runs all modules)."""
    supabase = get_supabase_client()
    
    # Get existing report
    result = supabase.table("reports").select("*").eq("id", report_id).eq("user_id", user.id).execute()
    
    if not result.data:
        raise HTTPException(status_code=404, detail="Report not found")
    
    report = result.data[0]
    
    # Reset report status
    supabase.table("reports").update({
        "status": "queued",
        "progress_pct": 0.0,
        "current_step": "initializing",
        "error_message": None,
        "updated_at": datetime.utcnow().isoformat()
    }).eq("id", report_id).execute()
    
    # Clear old module results
    supabase.table("report_modules").delete().eq("report_id", report_id).execute()
    
    # Queue regeneration
    request_data = ReportGenerationRequest(
        site_url=report["site_url"],
        gsc_property_url=report["gsc_property_url"],
        ga4_property_id=report["ga4_property_id"],
        report_name=report["report_name"],
        lookback_months=report.get("parameters", {}).get("lookback_months", 16),
        top_keywords_count=report.get("parameters", {}).get("top_keywords_count", 100)
    )
    
    background_tasks.add_task(
        run_report_generation_pipeline,
        report_id=report_id,
        user_id=user.id,
        request_data=request_data
    )
    
    logger.info(f"Report regeneration queued: {report_id}")
    
    return {"message": "Report regeneration started"}