from fastapi import APIRouter, HTTPException, Depends
from typing import Dict, Any
import logging
from datetime import datetime
import traceback

from ..auth.dependencies import get_current_user
from ..services.supabase_client import get_supabase_client
from ..modules import module_10

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/api/module-10/{report_id}")
async def get_module_10_analysis(
    report_id: str,
    current_user: Dict[str, Any] = Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Module 10: Revenue Attribution Analysis
    
    Fetches GA4 revenue/conversion data, analyzes revenue attribution
    by channel/source/page, identifies high-value pages and conversion
    path patterns, calculates SEO revenue contribution.
    
    Args:
        report_id: The unique identifier for the report
        current_user: Authenticated user from dependency injection
        
    Returns:
        Dict containing revenue attribution insights:
        - revenue_by_channel: Revenue breakdown by traffic channel
        - revenue_by_source: Top revenue-driving sources
        - high_value_pages: Pages with highest revenue/conversion rates
        - seo_revenue_attribution: Organic search revenue contribution
        - conversion_funnel: Multi-touch attribution insights
        - revenue_trends: Historical revenue patterns
        - assisted_conversions: Pages that assist in conversion path
    """
    supabase = get_supabase_client()
    
    try:
        # Verify report belongs to user
        report_response = supabase.table("reports").select("*").eq("id", report_id).eq("user_id", current_user["id"]).execute()
        
        if not report_response.data:
            raise HTTPException(status_code=404, detail="Report not found or access denied")
        
        report = report_response.data[0]
        
        # Check for cached results
        cached_response = supabase.table("modules").select("*").eq("report_id", report_id).eq("module_number", 10).execute()
        
        if cached_response.data and cached_response.data[0].get("results"):
            cached_result = cached_response.data[0]
            logger.info(f"Returning cached Module 10 results for report {report_id}")
            return {
                "status": "completed",
                "cached": True,
                "data": cached_result["results"],
                "generated_at": cached_result["updated_at"]
            }
        
        # Get GSC data
        gsc_response = supabase.table("gsc_data").select("*").eq("report_id", report_id).execute()
        
        if not gsc_response.data:
            raise HTTPException(status_code=404, detail="GSC data not found. Please run data ingestion first.")
        
        gsc_data = gsc_response.data[0].get("data", {})
        
        # Get GA4 data
        ga4_response = supabase.table("ga4_data").select("*").eq("report_id", report_id).execute()
        
        if not ga4_response.data:
            raise HTTPException(status_code=404, detail="GA4 data not found. Please run data ingestion first.")
        
        ga4_data = ga4_response.data[0].get("data", {})
        
        # Check if GA4 data contains revenue/conversion information
        if not ga4_data.get("revenue_data") and not ga4_data.get("conversions_data"):
            logger.warning(f"No revenue or conversion data available for report {report_id}")
            return {
                "status": "completed",
                "cached": False,
                "data": {
                    "error": "no_revenue_data",
                    "message": "No revenue or conversion data available in GA4. Please ensure ecommerce tracking or conversion events are configured.",
                    "revenue_by_channel": {},
                    "revenue_by_source": [],
                    "high_value_pages": [],
                    "seo_revenue_attribution": {},
                    "conversion_funnel": {},
                    "revenue_trends": [],
                    "assisted_conversions": []
                },
                "generated_at": datetime.utcnow().isoformat()
            }
        
        # Get previous module results for context
        module_dependencies = [2, 4]  # Page triage and content intelligence
        previous_modules = {}
        
        for module_num in module_dependencies:
            mod_response = supabase.table("modules").select("results").eq("report_id", report_id).eq("module_number", module_num).execute()
            if mod_response.data and mod_response.data[0].get("results"):
                previous_modules[f"module_{module_num}"] = mod_response.data[0]["results"]
        
        logger.info(f"Starting Module 10 analysis for report {report_id}")
        
        # Run Module 10 analysis
        try:
            analysis_results = module_10.analyze_revenue_attribution(
                gsc_data=gsc_data,
                ga4_data=ga4_data,
                previous_modules=previous_modules
            )
        except Exception as analysis_error:
            logger.error(f"Module 10 analysis failed: {str(analysis_error)}")
            logger.error(traceback.format_exc())
            raise HTTPException(
                status_code=500,
                detail=f"Revenue attribution analysis failed: {str(analysis_error)}"
            )
        
        # Cache results in database
        module_record = {
            "report_id": report_id,
            "module_number": 10,
            "module_name": "Revenue Attribution Analysis",
            "status": "completed",
            "results": analysis_results,
            "updated_at": datetime.utcnow().isoformat()
        }
        
        # Check if record exists
        if cached_response.data:
            # Update existing record
            supabase.table("modules").update(module_record).eq("id", cached_response.data[0]["id"]).execute()
            logger.info(f"Updated Module 10 cache for report {report_id}")
        else:
            # Insert new record
            module_record["created_at"] = datetime.utcnow().isoformat()
            supabase.table("modules").insert(module_record).execute()
            logger.info(f"Created Module 10 cache for report {report_id}")
        
        # Update report progress
        current_progress = report.get("progress", {})
        current_progress["module_10"] = "completed"
        completed_modules = sum(1 for v in current_progress.values() if v == "completed")
        total_modules = 12
        progress_percentage = int((completed_modules / total_modules) * 100)
        
        supabase.table("reports").update({
            "progress": current_progress,
            "progress_percentage": progress_percentage,
            "updated_at": datetime.utcnow().isoformat()
        }).eq("id", report_id).execute()
        
        logger.info(f"Module 10 analysis completed successfully for report {report_id}")
        
        return {
            "status": "completed",
            "cached": False,
            "data": analysis_results,
            "generated_at": datetime.utcnow().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in Module 10 endpoint: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(
            status_code=500,
            detail=f"An unexpected error occurred: {str(e)}"
        )


@router.delete("/api/module-10/{report_id}/cache")
async def clear_module_10_cache(
    report_id: str,
    current_user: Dict[str, Any] = Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Clear cached Module 10 results for a report.
    
    Args:
        report_id: The unique identifier for the report
        current_user: Authenticated user from dependency injection
        
    Returns:
        Dict with status message
    """
    supabase = get_supabase_client()
    
    try:
        # Verify report belongs to user
        report_response = supabase.table("reports").select("id").eq("id", report_id).eq("user_id", current_user["id"]).execute()
        
        if not report_response.data:
            raise HTTPException(status_code=404, detail="Report not found or access denied")
        
        # Delete cached module results
        supabase.table("modules").delete().eq("report_id", report_id).eq("module_number", 10).execute()
        
        logger.info(f"Cleared Module 10 cache for report {report_id}")
        
        return {
            "status": "success",
            "message": "Module 10 cache cleared successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error clearing Module 10 cache: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(
            status_code=500,
            detail=f"Failed to clear cache: {str(e)}"
        )
