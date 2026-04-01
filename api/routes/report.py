from fastapi import APIRouter, HTTPException, BackgroundTasks, Depends
from pydantic import BaseModel
from typing import Optional
import logging
from datetime import datetime

from ..services.gsc_service import GSCService
from ..services.ga4_service import GA4Service
from ..services.dataforseo_service import DataForSEOService
from ..services.crawl_service import CrawlService
from ..models.user import User
from ..auth.dependencies import get_current_user
from ..database import get_db

from ..analysis.module_1_health_trajectory import analyze_health_trajectory
from ..analysis.module_2_page_triage import analyze_page_triage
from ..analysis.module_3_serp_landscape import analyze_serp_landscape
from ..analysis.module_4_content_intelligence import analyze_content_intelligence
from ..analysis.module_5_gameplan import generate_gameplan
from ..utils.report_compiler import compile_report
from ..utils.error_handler import handle_module_error

logger = logging.getLogger(__name__)

router = APIRouter()


class ReportRequest(BaseModel):
    property_url: str
    ga4_property_id: str
    date_range_months: Optional[int] = 16


class ReportStatus(BaseModel):
    job_id: str
    status: str  # pending, processing, completed, failed
    progress: Optional[float] = None
    message: Optional[str] = None
    report_url: Optional[str] = None
    error: Optional[str] = None


async def generate_report_task(
    job_id: str,
    property_url: str,
    ga4_property_id: str,
    date_range_months: int,
    user_id: str,
    gsc_credentials: dict,
    ga4_credentials: dict,
    db
):
    """
    Background task to generate the full Search Intelligence Report.
    Runs all analysis modules sequentially and compiles the final report.
    """
    try:
        logger.info(f"Starting report generation for job {job_id}")
        
        # Update job status to processing
        db.update_job_status(job_id, "processing", 0.0, "Starting data ingestion...")
        
        # Initialize services
        gsc_service = GSCService(gsc_credentials)
        ga4_service = GA4Service(ga4_credentials)
        dataforseo_service = DataForSEOService()
        crawl_service = CrawlService()
        
        # Data Ingestion Phase (0-30%)
        logger.info(f"Job {job_id}: Fetching GSC data")
        db.update_job_status(job_id, "processing", 5.0, "Fetching Google Search Console data...")
        
        gsc_data = await gsc_service.fetch_comprehensive_data(
            property_url=property_url,
            months=date_range_months
        )
        
        logger.info(f"Job {job_id}: Fetching GA4 data")
        db.update_job_status(job_id, "processing", 15.0, "Fetching Google Analytics 4 data...")
        
        ga4_data = await ga4_service.fetch_comprehensive_data(
            property_id=ga4_property_id,
            months=date_range_months
        )
        
        logger.info(f"Job {job_id}: Fetching SERP data")
        db.update_job_status(job_id, "processing", 20.0, "Fetching SERP data for top keywords...")
        
        # Get top keywords from GSC data
        top_keywords = gsc_service.extract_top_keywords(
            gsc_data,
            limit=100,
            exclude_branded=True,
            domain=property_url
        )
        
        serp_data = await dataforseo_service.fetch_serp_data(top_keywords)
        
        logger.info(f"Job {job_id}: Crawling site for internal link graph")
        db.update_job_status(job_id, "processing", 25.0, "Analyzing site structure...")
        
        crawl_data = await crawl_service.crawl_site(
            property_url,
            max_pages=5000
        )
        
        db.update_job_status(job_id, "processing", 30.0, "Data ingestion complete. Starting analysis...")
        
        # Analysis Phase - Module 1 (30-40%)
        logger.info(f"Job {job_id}: Running Module 1 - Health & Trajectory")
        db.update_job_status(job_id, "processing", 35.0, "Analyzing traffic health and trajectory...")
        
        try:
            module_1_results = analyze_health_trajectory(gsc_data['daily_time_series'])
        except Exception as e:
            logger.error(f"Job {job_id}: Module 1 failed - {str(e)}")
            module_1_results = handle_module_error("module_1", e)
        
        # Analysis Phase - Module 2 (40-50%)
        logger.info(f"Job {job_id}: Running Module 2 - Page-Level Triage")
        db.update_job_status(job_id, "processing", 45.0, "Analyzing page-level performance...")
        
        try:
            module_2_results = analyze_page_triage(
                page_daily_data=gsc_data['page_daily_data'],
                ga4_landing_data=ga4_data['landing_pages'],
                gsc_page_summary=gsc_data['page_summary']
            )
        except Exception as e:
            logger.error(f"Job {job_id}: Module 2 failed - {str(e)}")
            module_2_results = handle_module_error("module_2", e)
        
        # Analysis Phase - Module 3 (50-60%)
        logger.info(f"Job {job_id}: Running Module 3 - SERP Landscape Analysis")
        db.update_job_status(job_id, "processing", 55.0, "Analyzing SERP landscape and competition...")
        
        try:
            module_3_results = analyze_serp_landscape(
                serp_data=serp_data,
                gsc_keyword_data=gsc_data['keyword_data']
            )
        except Exception as e:
            logger.error(f"Job {job_id}: Module 3 failed - {str(e)}")
            module_3_results = handle_module_error("module_3", e)
        
        # Analysis Phase - Module 4 (60-70%)
        logger.info(f"Job {job_id}: Running Module 4 - Content Intelligence")
        db.update_job_status(job_id, "processing", 65.0, "Analyzing content opportunities and issues...")
        
        try:
            module_4_results = analyze_content_intelligence(
                gsc_query_page=gsc_data['query_page_mapping'],
                page_data=crawl_data,
                ga4_engagement=ga4_data['engagement_metrics']
            )
        except Exception as e:
            logger.error(f"Job {job_id}: Module 4 failed - {str(e)}")
            module_4_results = handle_module_error("module_4", e)
        
        # Analysis Phase - Module 5 (70-80%)
        logger.info(f"Job {job_id}: Running Module 5 - The Gameplan")
        db.update_job_status(job_id, "processing", 75.0, "Generating prioritized action plan...")
        
        try:
            module_5_results = generate_gameplan(
                health=module_1_results,
                triage=module_2_results,
                serp=module_3_results,
                content=module_4_results
            )
        except Exception as e:
            logger.error(f"Job {job_id}: Module 5 failed - {str(e)}")
            module_5_results = handle_module_error("module_5", e)
        
        # Report Compilation Phase (80-100%)
        logger.info(f"Job {job_id}: Compiling final report")
        db.update_job_status(job_id, "processing", 85.0, "Compiling report sections...")
        
        report_data = {
            "metadata": {
                "job_id": job_id,
                "property_url": property_url,
                "ga4_property_id": ga4_property_id,
                "generated_at": datetime.utcnow().isoformat(),
                "date_range_months": date_range_months,
                "user_id": user_id
            },
            "modules": {
                "module_1_health_trajectory": module_1_results,
                "module_2_page_triage": module_2_results,
                "module_3_serp_landscape": module_3_results,
                "module_4_content_intelligence": module_4_results,
                "module_5_gameplan": module_5_results
            },
            "data_summary": {
                "gsc_queries_analyzed": len(gsc_data.get('query_data', [])),
                "gsc_pages_analyzed": len(gsc_data.get('page_summary', [])),
                "ga4_landing_pages": len(ga4_data.get('landing_pages', [])),
                "serp_keywords_analyzed": len(serp_data),
                "pages_crawled": len(crawl_data.get('pages', []))
            }
        }
        
        db.update_job_status(job_id, "processing", 90.0, "Generating visualizations...")
        
        # Compile the final report with charts, graphs, and narrative
        compiled_report = compile_report(report_data)
        
        db.update_job_status(job_id, "processing", 95.0, "Saving report...")
        
        # Store the compiled report in the database
        report_id = db.store_report(
            job_id=job_id,
            user_id=user_id,
            report_data=compiled_report
        )
        
        report_url = f"/api/reports/{report_id}"
        
        logger.info(f"Job {job_id}: Report generation completed successfully")
        db.update_job_status(
            job_id, 
            "completed", 
            100.0, 
            "Report generation complete!",
            report_url=report_url
        )
        
    except Exception as e:
        logger.error(f"Job {job_id}: Fatal error - {str(e)}", exc_info=True)
        db.update_job_status(
            job_id, 
            "failed", 
            None, 
            "Report generation failed",
            error=str(e)
        )


@router.post("/generate", response_model=dict)
async def generate_report(
    request: ReportRequest,
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_user),
    db = Depends(get_db)
):
    """
    Initiate generation of a comprehensive Search Intelligence Report.
    
    This endpoint:
    1. Validates user has connected GSC and GA4
    2. Creates a job record
    3. Queues the report generation as a background task
    4. Returns job ID for status polling
    """
    try:
        # Validate user has necessary OAuth tokens
        if not current_user.gsc_token or not current_user.ga4_token:
            raise HTTPException(
                status_code=400,
                detail="User must connect both Google Search Console and Google Analytics 4"
            )
        
        # Validate property access
        gsc_service = GSCService(current_user.gsc_token)
        if not await gsc_service.validate_property_access(request.property_url):
            raise HTTPException(
                status_code=403,
                detail=f"User does not have access to property: {request.property_url}"
            )
        
        ga4_service = GA4Service(current_user.ga4_token)
        if not await ga4_service.validate_property_access(request.ga4_property_id):
            raise HTTPException(
                status_code=403,
                detail=f"User does not have access to GA4 property: {request.ga4_property_id}"
            )
        
        # Create job record
        job_id = db.create_job(
            user_id=current_user.id,
            property_url=request.property_url,
            ga4_property_id=request.ga4_property_id,
            date_range_months=request.date_range_months
        )
        
        logger.info(f"Created job {job_id} for user {current_user.id}")
        
        # Queue background task
        background_tasks.add_task(
            generate_report_task,
            job_id=job_id,
            property_url=request.property_url,
            ga4_property_id=request.ga4_property_id,
            date_range_months=request.date_range_months,
            user_id=current_user.id,
            gsc_credentials=current_user.gsc_token,
            ga4_credentials=current_user.ga4_token,
            db=db
        )
        
        return {
            "job_id": job_id,
            "status": "pending",
            "message": "Report generation initiated. Use the job_id to check status.",
            "status_endpoint": f"/api/reports/status/{job_id}"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error initiating report generation: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to initiate report generation: {str(e)}"
        )


@router.get("/status/{job_id}", response_model=ReportStatus)
async def get_report_status(
    job_id: str,
    current_user: User = Depends(get_current_user),
    db = Depends(get_db)
):
    """
    Get the status of a report generation job.
    
    Returns:
    - status: pending, processing, completed, failed
    - progress: 0-100 percentage
    - message: current step description
    - report_url: if completed, URL to fetch the report
    - error: if failed, error message
    """
    try:
        job = db.get_job(job_id)
        
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        
        # Verify job belongs to current user
        if job['user_id'] != current_user.id:
            raise HTTPException(status_code=403, detail="Access denied")
        
        return ReportStatus(
            job_id=job_id,
            status=job['status'],
            progress=job.get('progress'),
            message=job.get('message'),
            report_url=job.get('report_url'),
            error=job.get('error')
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching job status: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch job status: {str(e)}"
        )


@router.get("/reports/{report_id}")
async def get_report(
    report_id: str,
    current_user: User = Depends(get_current_user),
    db = Depends(get_db)
):
    """
    Retrieve a completed report by ID.
    
    Returns the full compiled report including:
    - All module results
    - Generated visualizations
    - Narrative summaries
    - Actionable recommendations
    """
    try:
        report = db.get_report(report_id)
        
        if not report:
            raise HTTPException(status_code=404, detail="Report not found")
        
        # Verify report belongs to current user
        if report['user_id'] != current_user.id:
            raise HTTPException(status_code=403, detail="Access denied")
        
        return report['data']
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching report: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch report: {str(e)}"
        )


@router.get("/reports")
async def list_reports(
    current_user: User = Depends(get_current_user),
    db = Depends(get_db),
    limit: int = 20,
    offset: int = 0
):
    """
    List all reports for the current user.
    
    Returns a paginated list of report metadata (not full report data).
    """
    try:
        reports = db.list_user_reports(
            user_id=current_user.id,
            limit=limit,
            offset=offset
        )
        
        return {
            "reports": reports,
            "total": len(reports),
            "limit": limit,
            "offset": offset
        }
        
    except Exception as e:
        logger.error(f"Error listing reports: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list reports: {str(e)}"
        )


@router.delete("/reports/{report_id}")
async def delete_report(
    report_id: str,
    current_user: User = Depends(get_current_user),
    db = Depends(get_db)
):
    """
    Delete a report by ID.
    """
    try:
        report = db.get_report(report_id)
        
        if not report:
            raise HTTPException(status_code=404, detail="Report not found")
        
        # Verify report belongs to current user
        if report['user_id'] != current_user.id:
            raise HTTPException(status_code=403, detail="Access denied")
        
        db.delete_report(report_id)
        
        return {"message": "Report deleted successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting report: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to delete report: {str(e)}"
        )

