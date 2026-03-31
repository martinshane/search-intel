from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import Dict, Any
import logging
from datetime import datetime

from api.database import get_db
from api.auth import get_current_user
from api.models import User, Report, ReportModule
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
from api.helpers.ga4_helper import get_ga4_data
from api.helpers.gsc_helper import get_gsc_data
from api.helpers.serp_helper import get_serp_data
from api.helpers.crawl_helper import get_crawl_data

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/modules/1")
async def run_module_1(
    report_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Module 1: Health & Trajectory Analysis
    
    Analyzes overall site health using GSC time series data:
    - MSTL decomposition for trend and seasonality
    - Change point detection
    - Anomaly detection with STUMPY
    - Forward projection with ARIMA/Prophet
    """
    try:
        # Verify report exists and belongs to user
        report = db.query(Report).filter(
            Report.id == report_id,
            Report.user_id == current_user.id
        ).first()
        
        if not report:
            raise HTTPException(status_code=404, detail="Report not found")
        
        # Update report status
        report.status = "running"
        report.current_module = 1
        db.commit()
        
        # Fetch GSC daily time series data
        gsc_daily_data = await get_gsc_data(
            report_id=report_id,
            data_type="daily_timeseries",
            db=db
        )
        
        if gsc_daily_data is None or gsc_daily_data.empty:
            raise HTTPException(
                status_code=400,
                detail="Insufficient GSC data for analysis"
            )
        
        # Run analysis
        results = analyze_health_trajectory(gsc_daily_data)
        
        # Store results
        module_result = ReportModule(
            report_id=report_id,
            module_number=1,
            module_name="health_trajectory",
            results=results,
            status="completed",
            completed_at=datetime.utcnow()
        )
        db.add(module_result)
        db.commit()
        
        return {
            "status": "success",
            "module": 1,
            "results": results
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in module 1 for report {report_id}: {str(e)}", exc_info=True)
        
        # Update report status
        if 'report' in locals():
            report.status = "error"
            report.error_message = f"Module 1 failed: {str(e)}"
            db.commit()
        
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")


@router.get("/modules/2")
async def run_module_2(
    report_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Module 2: Page-Level Triage
    
    Analyzes individual page performance:
    - Per-page trend fitting
    - CTR anomaly detection
    - Engagement cross-reference with GA4
    - Priority scoring for page-level actions
    """
    try:
        # Verify report exists and belongs to user
        report = db.query(Report).filter(
            Report.id == report_id,
            Report.user_id == current_user.id
        ).first()
        
        if not report:
            raise HTTPException(status_code=404, detail="Report not found")
        
        # Update report status
        report.status = "running"
        report.current_module = 2
        db.commit()
        
        # Fetch GSC page-level daily data
        page_daily_data = await get_gsc_data(
            report_id=report_id,
            data_type="page_daily_timeseries",
            db=db
        )
        
        # Fetch GA4 landing page engagement data
        ga4_landing_data = await get_ga4_data(
            report_id=report_id,
            data_type="landing_pages",
            db=db
        )
        
        # Fetch GSC page summary
        gsc_page_summary = await get_gsc_data(
            report_id=report_id,
            data_type="page_summary",
            db=db
        )
        
        if page_daily_data is None or page_daily_data.empty:
            raise HTTPException(
                status_code=400,
                detail="Insufficient page-level data for analysis"
            )
        
        # Run analysis
        results = analyze_page_triage(
            page_daily_data=page_daily_data,
            ga4_landing_data=ga4_landing_data,
            gsc_page_summary=gsc_page_summary
        )
        
        # Store results
        module_result = ReportModule(
            report_id=report_id,
            module_number=2,
            module_name="page_triage",
            results=results,
            status="completed",
            completed_at=datetime.utcnow()
        )
        db.add(module_result)
        db.commit()
        
        return {
            "status": "success",
            "module": 2,
            "results": results
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in module 2 for report {report_id}: {str(e)}", exc_info=True)
        
        # Update report status
        if 'report' in locals():
            report.status = "error"
            report.error_message = f"Module 2 failed: {str(e)}"
            db.commit()
        
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")


@router.get("/modules/3")
async def run_module_3(
    report_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Module 3: SERP Landscape Analysis
    
    Analyzes search result pages:
    - SERP feature displacement analysis
    - Competitor mapping
    - Intent classification
    - Click share estimation
    """
    try:
        # Verify report exists and belongs to user
        report = db.query(Report).filter(
            Report.id == report_id,
            Report.user_id == current_user.id
        ).first()
        
        if not report:
            raise HTTPException(status_code=404, detail="Report not found")
        
        # Update report status
        report.status = "running"
        report.current_module = 3
        db.commit()
        
        # Fetch SERP data from DataForSEO
        serp_data = await get_serp_data(
            report_id=report_id,
            db=db
        )
        
        # Fetch GSC keyword data
        gsc_keyword_data = await get_gsc_data(
            report_id=report_id,
            data_type="query_summary",
            db=db
        )
        
        if serp_data is None or len(serp_data) == 0:
            raise HTTPException(
                status_code=400,
                detail="No SERP data available for analysis"
            )
        
        # Run analysis
        results = analyze_serp_landscape(
            serp_data=serp_data,
            gsc_keyword_data=gsc_keyword_data
        )
        
        # Store results
        module_result = ReportModule(
            report_id=report_id,
            module_number=3,
            module_name="serp_landscape",
            results=results,
            status="completed",
            completed_at=datetime.utcnow()
        )
        db.add(module_result)
        db.commit()
        
        return {
            "status": "success",
            "module": 3,
            "results": results
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in module 3 for report {report_id}: {str(e)}", exc_info=True)
        
        # Update report status
        if 'report' in locals():
            report.status = "error"
            report.error_message = f"Module 3 failed: {str(e)}"
            db.commit()
        
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")


@router.get("/modules/4")
async def run_module_4(
    report_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Module 4: Content Intelligence
    
    Analyzes content strategy:
    - Cannibalization detection
    - Striking distance opportunities
    - Thin content flagging
    - Content age vs performance matrix
    """
    try:
        # Verify report exists and belongs to user
        report = db.query(Report).filter(
            Report.id == report_id,
            Report.user_id == current_user.id
        ).first()
        
        if not report:
            raise HTTPException(status_code=404, detail="Report not found")
        
        # Update report status
        report.status = "running"
        report.current_module = 4
        db.commit()
        
        # Fetch GSC query-page mapping
        gsc_query_page = await get_gsc_data(
            report_id=report_id,
            data_type="query_page_mapping",
            db=db
        )
        
        # Fetch crawl data
        page_data = await get_crawl_data(
            report_id=report_id,
            db=db
        )
        
        # Fetch GA4 engagement data
        ga4_engagement = await get_ga4_data(
            report_id=report_id,
            data_type="landing_pages",
            db=db
        )
        
        if gsc_query_page is None or gsc_query_page.empty:
            raise HTTPException(
                status_code=400,
                detail="Insufficient query-page mapping data"
            )
        
        # Run analysis
        results = analyze_content_intelligence(
            gsc_query_page=gsc_query_page,
            page_data=page_data,
            ga4_engagement=ga4_engagement
        )
        
        # Store results
        module_result = ReportModule(
            report_id=report_id,
            module_number=4,
            module_name="content_intelligence",
            results=results,
            status="completed",
            completed_at=datetime.utcnow()
        )
        db.add(module_result)
        db.commit()
        
        return {
            "status": "success",
            "module": 4,
            "results": results
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in module 4 for report {report_id}: {str(e)}", exc_info=True)
        
        # Update report status
        if 'report' in locals():
            report.status = "error"
            report.error_message = f"Module 4 failed: {str(e)}"
            db.commit()
        
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")


@router.get("/modules/5")
async def run_module_5(
    report_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Module 5: The Gameplan
    
    Synthesizes all prior modules into actionable priorities:
    - Critical fixes
    - Quick wins
    - Strategic plays
    - Structural improvements
    """
    try:
        # Verify report exists and belongs to user
        report = db.query(Report).filter(
            Report.id == report_id,
            Report.user_id == current_user.id
        ).first()
        
        if not report:
            raise HTTPException(status_code=404, detail="Report not found")
        
        # Update report status
        report.status = "running"
        report.current_module = 5
        db.commit()
        
        # Fetch results from previous modules
        module_1 = db.query(ReportModule).filter(
            ReportModule.report_id == report_id,
            ReportModule.module_number == 1
        ).first()
        
        module_2 = db.query(ReportModule).filter(
            ReportModule.report_id == report_id,
            ReportModule.module_number == 2
        ).first()
        
        module_3 = db.query(ReportModule).filter(
            ReportModule.report_id == report_id,
            ReportModule.module_number == 3
        ).first()
        
        module_4 = db.query(ReportModule).filter(
            ReportModule.report_id == report_id,
            ReportModule.module_number == 4
        ).first()
        
        if not all([module_1, module_2, module_3, module_4]):
            raise HTTPException(
                status_code=400,
                detail="Cannot generate gameplan: previous modules not completed"
            )
        
        # Run synthesis
        results = generate_gameplan(
            health=module_1.results,
            triage=module_2.results,
            serp=module_3.results,
            content=module_4.results
        )
        
        # Store results
        module_result = ReportModule(
            report_id=report_id,
            module_number=5,
            module_name="gameplan",
            results=results,
            status="completed",
            completed_at=datetime.utcnow()
        )
        db.add(module_result)
        db.commit()
        
        return {
            "status": "success",
            "module": 5,
            "results": results
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in module 5 for report {report_id}: {str(e)}", exc_info=True)
        
        # Update report status
        if 'report' in locals():
            report.status = "error"
            report.error_message = f"Module 5 failed: {str(e)}"
            db.commit()
        
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")


@router.get("/modules/6")
async def run_module_6(
    report_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Module 6: Algorithm Update Impact Analysis
    
    Analyzes algorithm update impacts:
    - Matches traffic changes to known algorithm updates
    - Identifies affected pages
    - Assesses vulnerability
    - Provides recovery recommendations
    """
    try:
        # Verify report exists and belongs to user
        report = db.query(Report).filter(
            Report.id == report_id,
            Report.user_id == current_user.id
        ).first()
        
        if not report:
            raise HTTPException(status_code=404, detail="Report not found")
        
        # Update report status
        report.status = "running"
        report.current_module = 6
        db.commit()
        
        # Fetch GSC daily data
        daily_data = await get_gsc_data(
            report_id=report_id,
            data_type="daily_timeseries",
            db=db
        )
        
        # Fetch change points from Module 1
        module_1 = db.query(ReportModule).filter(
            ReportModule.report_id == report_id,
            ReportModule.module_number == 1
        ).first()
        
        if not module_1:
            raise HTTPException(
                status_code=400,
                detail="Module 1 must be completed first"
            )
        
        change_points = module_1.results.get("change_points", [])
        
        # Run analysis
        results = analyze_algorithm_impacts(
            daily_data=daily_data,
            change_points_from_module1=change_points
        )
        
        # Store results
        module_result = ReportModule(
            report_id=report_id,
            module_number=6,
            module_name="algorithm_updates",
            results=results,
            status="completed",
            completed_at=datetime.utcnow()
        )
        db.add(module_result)
        db.commit()
        
        return {
            "status": "success",
            "module": 6,
            "results": results
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in module 6 for report {report_id}: {str(e)}", exc_info=True)
        
        # Update report status
        if 'report' in locals():
            report.status = "error"
            report.error_message = f"Module 6 failed: {str(e)}"
            db.commit()
        
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")


@router.get("/modules/7")
async def run_module_7(
    report_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Module 7: Query Intent Migration Tracking
    
    Analyzes how search intent evolves over time:
    - Tracks changes in SERP composition for queries
    - Identifies intent shifts
    - Maps content alignment needs
    - Recommends content strategy adjustments
    """
    try:
        # Verify report exists and belongs to user
        report = db.query(Report).filter(
            Report.id == report_id,
            Report.user_id == current_user.id
        ).first()
        
        if not report:
            raise HTTPException(status_code=404, detail="Report not found")
        
        # Update report status
        report.status = "running"
        report.current_module = 7
        db.commit()
        
        # Fetch GSC query time series data
        query_timeseries = await get_gsc_data(
            report_id=report_id,
            data_type="query_daily_timeseries",
            db=db
        )
        
        # Fetch SERP data
        serp_data = await get_serp_data(
            report_id=report_id,
            db=db
        )
        
        # Fetch page data for content type mapping
        page_data = await get_crawl_data(
            report_id=report_id,
            db=db
        )
        
        if query_timeseries is None or query_timeseries.empty:
            raise HTTPException(
                status_code=400,
                detail="Insufficient query time series data"
            )
        
        # Run analysis
        results = analyze_intent_migration(
            query_timeseries=query_timeseries,
            serp_data=serp_data,
            page_data=page_data
        )
        
        # Store results
        module_result = ReportModule(
            report_id=report_id,
            module_number=7,
            module_name="intent_migration",
            results=results,
            status="completed",
            completed_at=datetime.utcnow()
        )
        db.add(module_result)
        db.commit()
        
        return {
            "status": "success",
            "module": 7,
            "results": results
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in module 7 for report {report_id}: {str(e)}", exc_info=True)
        
        # Update report status
        if 'report' in locals():
            report.status = "error"
            report.error_message = f"Module 7 failed: {str(e)}"
            db.commit()
        
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")


@router.get("/modules/8")
async def run_module_8(
    report_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Module 8: Technical Health & Core Web Vitals
    
    Analyzes technical SEO factors:
    - Core Web Vitals from GA4
    - Indexing coverage from GSC
    - Mobile usability issues
    - Technical debt assessment
    """
    try:
        # Verify report exists and belongs to user
        report = db.query(Report).filter(
            Report.id == report_id,
            Report.user_id == current_user.id
        ).first()
        
        if not report:
            raise HTTPException(status_code=404, detail="Report not found")
        
        # Update report status
        report.status = "running"
        report.current_module = 8
        db.commit()
        
        # Fetch GA4 Core Web Vitals data
        ga4_cwv_data = await get_ga4_data(
            report_id=report_id,
            data_type="core_web_vitals",
            db=db
        )
        
        # Fetch GSC URL inspection data
        gsc_coverage = await get_gsc_data(
            report_id=report_id,
            data_type="index_coverage",
            db=db
        )
        
        # Fetch mobile usability from GSC
        gsc_mobile = await get_gsc_data(
            report_id=report_id,
            data_type="mobile_usability",
            db=db
        )
        
        # Fetch crawl data for technical issues
        crawl_technical = await get_crawl_data(
            report_id=report_id,
            data_type="technical",
            db=db
        )
        
        # Run analysis
        results = analyze_technical_health(
            ga4_cwv_data=ga4_cwv_data,
            gsc_coverage=gsc_coverage,
            gsc_mobile=gsc_mobile,
            crawl_technical=crawl_technical
        )
        
        # Store results
        module_result = ReportModule(
            report_id=report_id,
            module_number=8,
            module_name="technical_health",
            results=results,
            status="completed",
            completed_at=datetime.utcnow()
        )
        db.add(module_result)
        db.commit()
        
        return {
            "status": "success",
            "module": 8,
            "results": results
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in module 8 for report {report_id}: {str(e)}", exc_info=True)
        
        # Update report status
        if 'report' in locals():
            report.status = "error"
            report.error_message = f"Module 8 failed: {str(e)}"
            db.commit()
        
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")

@router.get("/modules/9")
async def run_module_9(
    report_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Module 9: Site Architecture & Internal Linking
    
    Analyzes site structure and internal link optimization:
    - Internal link graph analysis
    - PageRank distribution
    - Conversion path optimization
    - Orphan page detection
    - Hub/spoke structure evaluation
    """
    try:
        # Verify report exists and belongs to user
        report = db.query(Report).filter(
            Report.id == report_id,
            Report.user_id == current_user.id
        ).first()
        
        if not report:
            raise HTTPException(status_code=404, detail="Report not found")
        
        # Update report status
        report.status = "running"
        report.current_module = 9
        db.commit()
        
        # Fetch crawl link graph data
        link_graph = await get_crawl_data(
            report_id=report_id,
            data_type="link_graph",
            db=db
        )
        
        # Fetch page performance data from GSC
        page_performance = await get_gsc_data(
            report_id=report_id,
            data_type="page_performance",
            db=db
        )
        
        # Fetch sitemap URLs
        crawl_sitemap = await get_crawl_data(
            report_id=report_id,
            data_type="sitemap_urls",
            db=db
        )
        
        # Fetch GSC query data for intent mapping
        gsc_query_data = await get_gsc_data(
            report_id=report_id,
            data_type="queries",
            db=db
        )
        
        # Run analysis
        results = analyze_site_architecture(
            link_graph=link_graph,
            page_performance=page_performance,
            sitemap_urls=crawl_sitemap,
            query_data=gsc_query_data
        )
        
        # Store results
        module_result = ReportModule(
            report_id=report_id,
            module_number=9,
            module_name="site_architecture",
            results=results,
            status="completed",
            completed_at=datetime.utcnow()
        )
        db.add(module_result)
        db.commit()
        
        return {
            "status": "success",
            "module": 9,
            "results": results
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in module 9 for report {report_id}: {str(e)}", exc_info=True)
        
        # Update report status
        if 'report' in locals():
            report.status = "error"
            report.error_message = f"Module 9 failed: {str(e)}"
            db.commit()
        
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")


@router.get("/modules/10")
async def run_module_10(
    report_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Module 10: Branded vs Non-Branded Split
    
    Analyzes brand dependency and non-branded growth opportunities:
    - Branded vs non-branded traffic segmentation
    - Brand dependency risk assessment
    - Non-branded keyword opportunity analysis
    - Segment trend analysis over time
    """
    try:
        # Verify report exists and belongs to user
        report = db.query(Report).filter(
            Report.id == report_id,
            Report.user_id == current_user.id
        ).first()
        
        if not report:
            raise HTTPException(status_code=404, detail="Report not found")
        
        # Update report status
        report.status = "running"
        report.current_module = 10
        db.commit()
        
        # Fetch GSC query data
        gsc_query_data = await get_gsc_data(
            report_id=report_id,
            data_type="queries",
            db=db
        )
        
        # Get brand terms from report configuration
        brand_terms = report.config.get("brand_terms", []) if report.config else []
        if not brand_terms and report.domain:
            # Auto-derive brand terms from domain
            domain_parts = report.domain.replace("www.", "").split(".")
            brand_terms = [domain_parts[0]] if domain_parts else []
        
        # Run analysis
        results = analyze_branded_split(
            gsc_query_data=gsc_query_data,
            brand_terms=brand_terms
        )
        
        # Store results
        module_result = ReportModule(
            report_id=report_id,
            module_number=10,
            module_name="branded_split",
            results=results,
            status="completed",
            completed_at=datetime.utcnow()
        )
        db.add(module_result)
        db.commit()
        
        return {
            "status": "success",
            "module": 10,
            "results": results
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in module 10 for report {report_id}: {str(e)}", exc_info=True)
        
        if 'report' in locals():
            report.status = "error"
            report.error_message = f"Module 10 failed: {str(e)}"
            db.commit()
        
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")


@router.get("/modules/11")
async def run_module_11(
    report_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Module 11: Competitive Intelligence
    
    Analyzes competitive landscape from SERP data:
    - Primary competitor identification and profiling
    - Emerging threat detection
    - Keyword vulnerability assessment
    - Competitor content velocity estimation
    - Competitive pressure scoring
    """
    try:
        # Verify report exists and belongs to user
        report = db.query(Report).filter(
            Report.id == report_id,
            Report.user_id == current_user.id
        ).first()
        
        if not report:
            raise HTTPException(status_code=404, detail="Report not found")
        
        # Update report status
        report.status = "running"
        report.current_module = 11
        db.commit()
        
        # Fetch SERP data from DataForSEO
        serp_data = await get_serp_data(
            report_id=report_id,
            db=db
        )
        
        # Fetch GSC data for user performance context
        gsc_data = await get_gsc_data(
            report_id=report_id,
            data_type="queries",
            db=db
        )
        
        # Get user domain from report
        user_domain = report.domain or ""
        
        # Run analysis
        results = analyze_competitive_threats(
            serp_data=serp_data,
            gsc_data=gsc_data,
            user_domain=user_domain
        )
        
        # Store results
        module_result = ReportModule(
            report_id=report_id,
            module_number=11,
            module_name="competitive_intelligence",
            results=results,
            status="completed",
            completed_at=datetime.utcnow()
        )
        db.add(module_result)
        db.commit()
        
        return {
            "status": "success",
            "module": 11,
            "results": results
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in module 11 for report {report_id}: {str(e)}", exc_info=True)
        
        if 'report' in locals():
            report.status = "error"
            report.error_message = f"Module 11 failed: {str(e)}"
            db.commit()
        
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")


@router.get("/modules/12")
async def run_module_12(
    report_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Module 12: Revenue Attribution & ROI Modeling
    
    Models the revenue impact of search performance:
    - Search traffic to conversion attribution
    - Revenue-at-risk from declining pages
    - Position improvement ROI estimates
    - Top revenue keyword identification
    - Action-level ROI projections
    """
    try:
        # Verify report exists and belongs to user
        report = db.query(Report).filter(
            Report.id == report_id,
            Report.user_id == current_user.id
        ).first()
        
        if not report:
            raise HTTPException(status_code=404, detail="Report not found")
        
        # Update report status
        report.status = "running"
        report.current_module = 12
        db.commit()
        
        # Fetch GSC performance data
        gsc_data = await get_gsc_data(
            report_id=report_id,
            data_type="queries",
            db=db
        )
        
        # Fetch GA4 conversion data
        ga4_conversions = await get_ga4_data(
            report_id=report_id,
            data_type="conversions",
            db=db
        )
        
        # Fetch GA4 engagement data
        ga4_engagement = await get_ga4_data(
            report_id=report_id,
            data_type="engagement",
            db=db
        )
        
        # Fetch GA4 ecommerce data (optional)
        ga4_ecommerce = await get_ga4_data(
            report_id=report_id,
            data_type="ecommerce",
            db=db
        )
        
        # Run analysis
        results = estimate_revenue_attribution(
            gsc_data=gsc_data,
            ga4_conversions=ga4_conversions,
            ga4_engagement=ga4_engagement,
            ga4_ecommerce=ga4_ecommerce
        )
        
        # Store results
        module_result = ReportModule(
            report_id=report_id,
            module_number=12,
            module_name="revenue_attribution",
            results=results,
            status="completed",
            completed_at=datetime.utcnow()
        )
        db.add(module_result)
        db.commit()
        
        return {
            "status": "success",
            "module": 12,
            "results": results
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in module 12 for report {report_id}: {str(e)}", exc_info=True)
        
        if 'report' in locals():
            report.status = "error"
            report.error_message = f"Module 12 failed: {str(e)}"
            db.commit()
        
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")

