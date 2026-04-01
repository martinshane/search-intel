import os
import sys
import time
import traceback
from typing import Dict, Any, Optional, List
from datetime import datetime
from fastapi import APIRouter, HTTPException, BackgroundTasks, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
import logging
from supabase import create_client, Client
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Supabase client
supabase_url = os.getenv("SUPABASE_URL")
supabase_key = os.getenv("SUPABASE_ANON_KEY")
supabase: Client = create_client(supabase_url, supabase_key)

router = APIRouter()

# Import analysis modules
try:
    from modules.health_trajectory import analyze_health_trajectory
    from modules.page_triage import analyze_page_triage
    from modules.serp_landscape import analyze_serp_landscape
    from modules.content_intelligence import analyze_content_intelligence
    from modules.gameplan import generate_gameplan
    from modules.algorithm_impact import analyze_algorithm_impacts
    from modules.intent_migration import analyze_intent_migration
    from modules.backlink_analysis import analyze_backlinks
    from modules.technical_seo import analyze_technical_seo
    from modules.internal_linking import analyze_internal_linking
    from modules.keyword_clusters import analyze_keyword_clusters
    from modules.revenue_attribution import analyze_revenue_attribution
    from data_ingestion.gsc_data import fetch_gsc_data
    from data_ingestion.ga4_data import fetch_ga4_data
    from data_ingestion.serp_data import fetch_serp_data
    from data_ingestion.site_crawl import crawl_site
except ImportError as e:
    logger.error(f"Failed to import required modules: {e}")
    raise


class ReportRequest(BaseModel):
    user_id: str = Field(..., description="User ID from authentication")
    site_url: str = Field(..., description="Site URL to analyze")
    report_type: str = Field(default="full", description="Report type: full or quick")
    force_refresh: bool = Field(default=False, description="Force fresh data pull")


class ModuleResult(BaseModel):
    module_name: str
    status: str  # success, failed, partial
    execution_time: float
    error: Optional[str] = None
    data: Optional[Dict[str, Any]] = None
    warning: Optional[str] = None


class ReportResponse(BaseModel):
    report_id: str
    status: str  # completed, partial, failed
    modules: List[ModuleResult]
    total_execution_time: float
    generated_at: str
    site_url: str
    user_id: str
    report_data: Optional[Dict[str, Any]] = None
    errors: Optional[List[str]] = None
    warnings: Optional[List[str]] = None


def log_module_execution(module_name: str, status: str, execution_time: float, 
                         error: Optional[str] = None, warning: Optional[str] = None):
    """Log module execution details to database and logger"""
    log_data = {
        "module_name": module_name,
        "status": status,
        "execution_time": execution_time,
        "error": error,
        "warning": warning,
        "timestamp": datetime.utcnow().isoformat()
    }
    logger.info(f"Module {module_name}: {status} in {execution_time:.2f}s")
    if error:
        logger.error(f"Module {module_name} error: {error}")
    if warning:
        logger.warning(f"Module {module_name} warning: {warning}")
    
    return log_data


def execute_module_safely(module_func, module_name: str, *args, **kwargs) -> ModuleResult:
    """
    Execute a module with comprehensive error handling and timing
    Returns ModuleResult with status, data, and timing info
    """
    start_time = time.time()
    logger.info(f"Starting module: {module_name}")
    
    try:
        result = module_func(*args, **kwargs)
        execution_time = time.time() - start_time
        
        # Validate result
        if result is None:
            raise ValueError(f"Module {module_name} returned None")
        
        if not isinstance(result, dict):
            logger.warning(f"Module {module_name} returned non-dict result, wrapping it")
            result = {"data": result}
        
        log_module_execution(module_name, "success", execution_time)
        
        return ModuleResult(
            module_name=module_name,
            status="success",
            execution_time=execution_time,
            data=result
        )
    
    except Exception as e:
        execution_time = time.time() - start_time
        error_msg = f"{str(e)}\n{traceback.format_exc()}"
        
        log_module_execution(module_name, "failed", execution_time, error=error_msg)
        
        return ModuleResult(
            module_name=module_name,
            status="failed",
            execution_time=execution_time,
            error=str(e),
            data={"error_details": error_msg}
        )


def execute_data_ingestion(user_id: str, site_url: str, force_refresh: bool = False) -> Dict[str, Any]:
    """
    Execute all data ingestion steps with error handling
    Returns dict with ingested data and status for each source
    """
    ingestion_results = {
        "gsc_data": None,
        "ga4_data": None,
        "serp_data": None,
        "crawl_data": None,
        "errors": [],
        "warnings": []
    }
    
    # GSC Data Ingestion
    logger.info("Starting GSC data ingestion")
    try:
        gsc_result = execute_module_safely(
            fetch_gsc_data,
            "GSC Data Ingestion",
            user_id=user_id,
            site_url=site_url,
            force_refresh=force_refresh
        )
        if gsc_result.status == "success":
            ingestion_results["gsc_data"] = gsc_result.data
        else:
            ingestion_results["errors"].append(f"GSC ingestion failed: {gsc_result.error}")
    except Exception as e:
        logger.error(f"GSC ingestion error: {e}")
        ingestion_results["errors"].append(f"GSC ingestion error: {str(e)}")
    
    # GA4 Data Ingestion
    logger.info("Starting GA4 data ingestion")
    try:
        ga4_result = execute_module_safely(
            fetch_ga4_data,
            "GA4 Data Ingestion",
            user_id=user_id,
            site_url=site_url,
            force_refresh=force_refresh
        )
        if ga4_result.status == "success":
            ingestion_results["ga4_data"] = ga4_result.data
        else:
            ingestion_results["errors"].append(f"GA4 ingestion failed: {ga4_result.error}")
            ingestion_results["warnings"].append("Some features will have limited data without GA4")
    except Exception as e:
        logger.error(f"GA4 ingestion error: {e}")
        ingestion_results["errors"].append(f"GA4 ingestion error: {str(e)}")
        ingestion_results["warnings"].append("Proceeding without GA4 data")
    
    # SERP Data Ingestion (only if GSC data available)
    if ingestion_results["gsc_data"]:
        logger.info("Starting SERP data ingestion")
        try:
            serp_result = execute_module_safely(
                fetch_serp_data,
                "SERP Data Ingestion",
                gsc_data=ingestion_results["gsc_data"],
                site_url=site_url,
                force_refresh=force_refresh
            )
            if serp_result.status == "success":
                ingestion_results["serp_data"] = serp_result.data
            else:
                ingestion_results["warnings"].append(f"SERP ingestion incomplete: {serp_result.error}")
        except Exception as e:
            logger.error(f"SERP ingestion error: {e}")
            ingestion_results["warnings"].append(f"SERP data unavailable: {str(e)}")
    
    # Site Crawl
    logger.info("Starting site crawl")
    try:
        crawl_result = execute_module_safely(
            crawl_site,
            "Site Crawl",
            site_url=site_url,
            force_refresh=force_refresh
        )
        if crawl_result.status == "success":
            ingestion_results["crawl_data"] = crawl_result.data
        else:
            ingestion_results["warnings"].append(f"Site crawl incomplete: {crawl_result.error}")
    except Exception as e:
        logger.error(f"Site crawl error: {e}")
        ingestion_results["warnings"].append(f"Site crawl unavailable: {str(e)}")
    
    return ingestion_results


def generate_report_sync(user_id: str, site_url: str, report_type: str, force_refresh: bool) -> ReportResponse:
    """
    Synchronous report generation with comprehensive error handling
    Each module can fail independently without breaking the entire report
    """
    report_start_time = time.time()
    report_id = f"{user_id}_{int(time.time())}"
    
    logger.info(f"Starting report generation: {report_id} for {site_url}")
    
    module_results = []
    errors = []
    warnings = []
    report_data = {}
    
    # Step 1: Data Ingestion
    logger.info("=" * 80)
    logger.info("STEP 1: DATA INGESTION")
    logger.info("=" * 80)
    
    try:
        ingestion_results = execute_data_ingestion(user_id, site_url, force_refresh)
        
        if ingestion_results["errors"]:
            errors.extend(ingestion_results["errors"])
        if ingestion_results["warnings"]:
            warnings.extend(ingestion_results["warnings"])
        
        gsc_data = ingestion_results.get("gsc_data")
        ga4_data = ingestion_results.get("ga4_data")
        serp_data = ingestion_results.get("serp_data")
        crawl_data = ingestion_results.get("crawl_data")
        
        # Critical check: Must have at least GSC data to proceed
        if not gsc_data:
            logger.error("No GSC data available - cannot generate report")
            return ReportResponse(
                report_id=report_id,
                status="failed",
                modules=[],
                total_execution_time=time.time() - report_start_time,
                generated_at=datetime.utcnow().isoformat(),
                site_url=site_url,
                user_id=user_id,
                errors=["Failed to fetch GSC data - report cannot be generated"],
                warnings=warnings
            )
    
    except Exception as e:
        logger.error(f"Data ingestion failed catastrophically: {e}")
        return ReportResponse(
            report_id=report_id,
            status="failed",
            modules=[],
            total_execution_time=time.time() - report_start_time,
            generated_at=datetime.utcnow().isoformat(),
            site_url=site_url,
            user_id=user_id,
            errors=[f"Data ingestion failed: {str(e)}"],
            warnings=[]
        )
    
    # Step 2: Analysis Modules
    logger.info("=" * 80)
    logger.info("STEP 2: ANALYSIS MODULES")
    logger.info("=" * 80)
    
    # Module 1: Health & Trajectory
    logger.info("-" * 80)
    health_result = execute_module_safely(
        analyze_health_trajectory,
        "Module 1: Health & Trajectory",
        daily_data=gsc_data.get("daily_data")
    )
    module_results.append(health_result)
    if health_result.status == "success":
        report_data["health_trajectory"] = health_result.data
    else:
        warnings.append(f"Health & Trajectory analysis failed: {health_result.error}")
    
    # Module 2: Page-Level Triage
    logger.info("-" * 80)
    triage_result = execute_module_safely(
        analyze_page_triage,
        "Module 2: Page-Level Triage",
        page_daily_data=gsc_data.get("page_daily_data"),
        ga4_landing_data=ga4_data.get("landing_pages") if ga4_data else None,
        gsc_page_summary=gsc_data.get("page_summary")
    )
    module_results.append(triage_result)
    if triage_result.status == "success":
        report_data["page_triage"] = triage_result.data
    else:
        warnings.append(f"Page Triage analysis failed: {triage_result.error}")
    
    # Module 3: SERP Landscape Analysis
    logger.info("-" * 80)
    if serp_data:
        serp_result = execute_module_safely(
            analyze_serp_landscape,
            "Module 3: SERP Landscape",
            serp_data=serp_data,
            gsc_keyword_data=gsc_data.get("query_summary")
        )
        module_results.append(serp_result)
        if serp_result.status == "success":
            report_data["serp_landscape"] = serp_result.data
        else:
            warnings.append(f"SERP Landscape analysis failed: {serp_result.error}")
    else:
        warnings.append("SERP Landscape analysis skipped - no SERP data available")
        report_data["serp_landscape"] = {"status": "skipped", "reason": "no_serp_data"}
    
    # Module 4: Content Intelligence
    logger.info("-" * 80)
    content_result = execute_module_safely(
        analyze_content_intelligence,
        "Module 4: Content Intelligence",
        gsc_query_page=gsc_data.get("query_page_mapping"),
        page_data=crawl_data.get("pages") if crawl_data else None,
        ga4_engagement=ga4_data.get("landing_pages") if ga4_data else None
    )
    module_results.append(content_result)
    if content_result.status == "success":
        report_data["content_intelligence"] = content_result.data
    else:
        warnings.append(f"Content Intelligence analysis failed: {content_result.error}")
    
    # Module 5: The Gameplan (depends on modules 1-4)
    logger.info("-" * 80)
    try:
        gameplan_result = execute_module_safely(
            generate_gameplan,
            "Module 5: The Gameplan",
            health=report_data.get("health_trajectory", {}),
            triage=report_data.get("page_triage", {}),
            serp=report_data.get("serp_landscape", {}),
            content=report_data.get("content_intelligence", {})
        )
        module_results.append(gameplan_result)
        if gameplan_result.status == "success":
            report_data["gameplan"] = gameplan_result.data
        else:
            warnings.append(f"Gameplan generation failed: {gameplan_result.error}")
    except Exception as e:
        logger.error(f"Gameplan generation error: {e}")
        warnings.append(f"Gameplan generation failed: {str(e)}")
    
    # Module 6: Algorithm Update Impact Analysis
    logger.info("-" * 80)
    algorithm_result = execute_module_safely(
        analyze_algorithm_impacts,
        "Module 6: Algorithm Impact",
        daily_data=gsc_data.get("daily_data"),
        change_points=report_data.get("health_trajectory", {}).get("change_points", [])
    )
    module_results.append(algorithm_result)
    if algorithm_result.status == "success":
        report_data["algorithm_impact"] = algorithm_result.data
    else:
        warnings.append(f"Algorithm Impact analysis failed: {algorithm_result.error}")
    
    # Module 7: Query Intent Migration Tracking
    logger.info("-" * 80)
    intent_result = execute_module_safely(
        analyze_intent_migration,
        "Module 7: Intent Migration",
        query_daily_data=gsc_data.get("query_daily_data"),
        serp_data=serp_data
    )
    module_results.append(intent_result)
    if intent_result.status == "success":
        report_data["intent_migration"] = intent_result.data
    else:
        warnings.append(f"Intent Migration analysis failed: {intent_result.error}")
    
    # Module 8: Backlink Analysis (optional, requires external API)
    logger.info("-" * 80)
    try:
        backlink_result = execute_module_safely(
            analyze_backlinks,
            "Module 8: Backlink Analysis",
            site_url=site_url,
            gsc_page_data=gsc_data.get("page_summary")
        )
        module_results.append(backlink_result)
        if backlink_result.status == "success":
            report_data["backlink_analysis"] = backlink_result.data
        elif backlink_result.status == "failed":
            warnings.append(f"Backlink analysis unavailable: {backlink_result.error}")
    except Exception as e:
        logger.warning(f"Backlink analysis skipped: {e}")
        warnings.append("Backlink analysis skipped - external API unavailable")
    
    # Module 9: Technical SEO Analysis
    logger.info("-" * 80)
    if crawl_data:
        technical_result = execute_module_safely(
            analyze_technical_seo,
            "Module 9: Technical SEO",
            crawl_data=crawl_data,
            gsc_data=gsc_data
        )
        module_results.append(technical_result)
        if technical_result.status == "success":
            report_data["technical_seo"] = technical_result.data
        else:
            warnings.append(f"Technical SEO analysis failed: {technical_result.error}")
    else:
        warnings.append("Technical SEO analysis skipped - no crawl data available")
        report_data["technical_seo"] = {"status": "skipped", "reason": "no_crawl_data"}
    
    # Module 10: Internal Linking Analysis
    logger.info("-" * 80)
    if crawl_data:
        linking_result = execute_module_safely(
            analyze_internal_linking,
            "Module 10: Internal Linking",
            crawl_data=crawl_data,
            gsc_page_data=gsc_data.get("page_summary")
        )
        module_results.append(linking_result)
        if linking_result.status == "success":
            report_data["internal_linking"] = linking_result.data
        else:
            warnings.append(f"Internal Linking analysis failed: {linking_result.error}")
    else:
        warnings.append("Internal Linking analysis skipped - no crawl data available")
        report_data["internal_linking"] = {"status": "skipped", "reason": "no_crawl_data"}
    
    # Module 11: Keyword Clusters
    logger.info("-" * 80)
    cluster_result = execute_module_safely(
        analyze_keyword_clusters,
        "Module 11: Keyword Clusters",
        query_data=gsc_data.get("query_summary"),
        query_page_mapping=gsc_data.get("query_page_mapping")
    )
    module_results.append(cluster_result)
    if cluster_result.status == "success":
        report_data["keyword_clusters"] = cluster_result.data
    else:
        warnings.append(f"Keyword Clustering analysis failed: {cluster_result.error}")
    
    # Module 12: Revenue Attribution (if GA4 conversion data available)
    logger.info("-" * 80)
    if ga4_data and ga4_data.get("conversions"):
        revenue_result = execute_module_safely(
            analyze_revenue_attribution,
            "Module 12: Revenue Attribution",
            ga4_data=ga4_data,
            gsc_data=gsc_data
        )
        module_results.append(revenue_result)
        if revenue_result.status == "success":
            report_data["revenue_attribution"] = revenue_result.data
        else:
            warnings.append(f"Revenue Attribution analysis failed: {revenue_result.error}")
    else:
        warnings.append("Revenue Attribution skipped - no conversion data available")
        report_data["revenue_attribution"] = {"status": "skipped", "reason": "no_conversion_data"}
    
    # Calculate final status
    total_execution_time = time.time() - report_start_time
    successful_modules = sum(1 for r in module_results if r.status == "success")
    failed_modules = sum(1 for r in module_results if r.status == "failed")
    
    if failed_modules == len(module_results):
        final_status = "failed"
    elif failed_modules > 0:
        final_status = "partial"
    else:
        final_status = "completed"
    
    logger.info("=" * 80)
    logger.info(f"REPORT GENERATION COMPLETE: {final_status}")
    logger.info(f"Total time: {total_execution_time:.2f}s")
    logger.info(f"Successful modules: {successful_modules}/{len(module_results)}")
    logger.info(f"Failed modules: {failed_modules}/{len(module_results)}")
    logger.info("=" * 80)
    
    # Save report to database
    try:
        report_record = {
            "report_id": report_id,
            "user_id": user_id,
            "site_url": site_url,
            "status": final_status,
            "report_data": report_data,
            "module_results": [r.dict() for r in module_results],
            "execution_time": total_execution_time,
            "generated_at": datetime.utcnow().isoformat(),
            "errors": errors,
            "warnings": warnings
        }
        
        supabase.table("reports").insert(report_record).execute()
        logger.info(f"Report saved to database: {report_id}")
    except Exception as e:
        logger.error(f"Failed to save report to database: {e}")
        warnings.append(f"Report generated but not saved to database: {str(e)}")
    
    return ReportResponse(
        report_id=report_id,
        status=final_status,
        modules=module_results,
        total_execution_time=total_execution_time,
        generated_at=datetime.utcnow().isoformat(),
        site_url=site_url,
        user_id=user_id,
        report_data=report_data if final_status != "failed" else None,
        errors=errors if errors else None,
        warnings=warnings if warnings else None
    )


@router.post("/generate-report", response_model=ReportResponse)
async def generate_report(
    request: ReportRequest,
    background_tasks: BackgroundTasks
):
    """
    Generate a comprehensive Search Intelligence Report
    
    This endpoint orchestrates all 12 analysis modules with comprehensive error handling.
    Each module can fail independently without breaking the entire report generation.
    
    Returns a partial report if some modules fail, or a full report if all succeed.
    """
    try:
        logger.info(f"Received report generation request for {request.site_url} from user {request.user_id}")
        
        # Validate request
        if not request.user_id or not request.site_url:
            raise HTTPException(
                status_code=400,
                detail="user_id and site_url are required"
            )
        
        # For quick reports, run synchronously
        # For full reports, could be run in background, but for now run sync
        result = generate_report_sync(
            user_id=request.user_id,
            site_url=request.site_url,
            report_type=request.report_type,
            force_refresh=request.force_refresh
        )
        
        # Return appropriate status code based on result
        if result.status == "failed":
            return JSONResponse(
                status_code=500,
                content=result.dict()
            )
        elif result.status == "partial":
            return JSONResponse(
                status_code=206,  # Partial Content
                content=result.dict()
            )
        else:
            return result
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in report generation endpoint: {e}\n{traceback.format_exc()}")
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected error during report generation: {str(e)}"
        )


@router.get("/report/{report_id}", response_model=ReportResponse)
async def get_report(report_id: str, user_id: str):
    """
    Retrieve a previously generated report
    """
    try:
        logger.info(f"Fetching report {report_id} for user {user_id}")
        
        # Fetch from database
        result = supabase.table("reports").select("*").eq("report_id", report_id).eq("user_id", user_id).execute()
        
        if not result.data:
            raise HTTPException(
                status_code=404,
                detail=f"Report {report_id} not found"
            )
        
        report_data = result.data[0]
        
        # Reconstruct ModuleResult objects
        module_results = [ModuleResult(**m) for m in report_data.get("module_results", [])]
        
        return ReportResponse(
            report_id=report_data["report_id"],
            status=report_data["status"],
            modules=module_results,
            total_execution_time=report_data["execution_time"],
            generated_at=report_data["generated_at"],
            site_url=report_data["site_url"],
            user_id=report_data["user_id"],
            report_data=report_data.get("report_data"),
            errors=report_data.get("errors"),
            warnings=report_data.get("warnings")
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching report: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching report: {str(e)}"
        )


@router.get("/reports", response_model=List[Dict[str, Any]])
async def list_reports(user_id: str, limit: int = 10, offset: int = 0):
    """
    List all reports for a user
    """
    try:
        logger.info(f"Listing reports for user {user_id}")
        
        result = supabase.table("reports") \
            .select("report_id, site_url, status, generated_at, execution_time") \
            .eq("user_id", user_id) \
            .order("generated_at", desc=True) \
            .limit(limit) \
            .offset(offset) \
            .execute()
        
        return result.data
    
    except Exception as e:
        logger.error(f"Error listing reports: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error listing reports: {str(e)}"
        )


@router.delete("/report/{report_id}")
async def delete_report(report_id: str, user_id: str):
    """
    Delete a report
    """
    try:
        logger.info(f"Deleting report {report_id} for user {user_id}")
        
        result = supabase.table("reports") \
            .delete() \
            .eq("report_id", report_id) \
            .eq("user_id", user_id) \
            .execute()
        
        if not result.data:
            raise HTTPException(
                status_code=404,
                detail=f"Report {report_id} not found"
            )
        
        return {"message": f"Report {report_id} deleted successfully"}
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting report: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error deleting report: {str(e)}"
        )
