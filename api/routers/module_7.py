import logging
from datetime import datetime, timedelta
from typing import Optional
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field

from api.auth import get_current_user, check_site_access
from api.database import get_supabase_client
from api.analysis import module_7_algorithm_impact

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/module-7", tags=["module-7"])


class AnalyzeRequest(BaseModel):
    site_id: str = Field(..., description="Site identifier")
    date_range: Optional[int] = Field(
        default=365,
        description="Number of days to analyze (default 365)",
        ge=30,
        le=730
    )


class AlgorithmImpact(BaseModel):
    update_name: str
    date: str
    site_impact: str
    click_change_pct: float
    impression_change_pct: float
    pages_most_affected: list[str]
    common_characteristics: list[str]
    recovery_status: str
    recovery_days: Optional[int]
    severity: str


class AnalysisResponse(BaseModel):
    status: str
    site_id: str
    analysis_date: str
    date_range_days: int
    updates_impacting_site: list[AlgorithmImpact]
    vulnerability_score: float
    total_updates_analyzed: int
    updates_with_impact: int
    average_recovery_days: Optional[float]
    recommendation: str
    most_vulnerable_page_types: list[dict]


async def log_error_to_supabase(
    supabase,
    site_id: str,
    module_name: str,
    error_type: str,
    error_message: str,
    stack_trace: Optional[str] = None
):
    """Log error to Supabase error_logs table"""
    try:
        supabase.table("error_logs").insert({
            "site_id": site_id,
            "module_name": module_name,
            "error_type": error_type,
            "error_message": error_message,
            "stack_trace": stack_trace,
            "timestamp": datetime.utcnow().isoformat()
        }).execute()
    except Exception as e:
        logger.error(f"Failed to log error to Supabase: {str(e)}")


@router.post("/analyze", response_model=AnalysisResponse)
async def analyze_algorithm_impact(
    request: AnalyzeRequest,
    current_user: dict = Depends(get_current_user)
):
    """
    Analyze algorithm update impacts on site performance.
    
    This endpoint:
    1. Validates site ownership via check_site_access
    2. Calls module_7_algorithm_impact.analyze_algorithm_impact()
    3. Stores results in supabase module_results table
    4. Returns formatted response with impacts sorted by severity
    5. Includes proper error handling and logs failures
    
    Args:
        request: AnalyzeRequest with site_id and optional date_range
        current_user: Authenticated user from dependency
        
    Returns:
        AnalysisResponse with algorithm impact analysis results
        
    Raises:
        HTTPException: On validation, access, or processing errors
    """
    supabase = get_supabase_client()
    site_id = request.site_id
    date_range = request.date_range or 365
    
    try:
        # Validate site ownership
        logger.info(f"Validating access for user {current_user['id']} to site {site_id}")
        has_access = await check_site_access(
            supabase=supabase,
            user_id=current_user["id"],
            site_id=site_id
        )
        
        if not has_access:
            logger.warning(f"Access denied for user {current_user['id']} to site {site_id}")
            raise HTTPException(
                status_code=403,
                detail="You do not have access to this site"
            )
        
        # Calculate date range
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=date_range)
        
        logger.info(f"Starting algorithm impact analysis for site {site_id}")
        logger.info(f"Date range: {start_date.date()} to {end_date.date()}")
        
        # Call analysis algorithm
        try:
            analysis_results = await module_7_algorithm_impact.analyze_algorithm_impact(
                supabase=supabase,
                site_id=site_id,
                start_date=start_date,
                end_date=end_date
            )
        except ValueError as e:
            logger.error(f"Validation error in analysis: {str(e)}")
            await log_error_to_supabase(
                supabase=supabase,
                site_id=site_id,
                module_name="algorithm_impact",
                error_type="validation_error",
                error_message=str(e)
            )
            raise HTTPException(
                status_code=400,
                detail=f"Invalid data for analysis: {str(e)}"
            )
        except Exception as e:
            logger.error(f"Analysis algorithm error: {str(e)}", exc_info=True)
            import traceback
            await log_error_to_supabase(
                supabase=supabase,
                site_id=site_id,
                module_name="algorithm_impact",
                error_type="analysis_error",
                error_message=str(e),
                stack_trace=traceback.format_exc()
            )
            raise HTTPException(
                status_code=500,
                detail="Error during algorithm impact analysis"
            )
        
        # Store results in module_results table
        analysis_date = datetime.utcnow().isoformat()
        
        try:
            result_record = {
                "site_id": site_id,
                "module_name": "algorithm_impact",
                "analysis_date": analysis_date,
                "results": analysis_results,
                "parameters": {
                    "date_range_days": date_range,
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat()
                },
                "status": "completed",
                "created_at": analysis_date,
                "updated_at": analysis_date
            }
            
            # Check if a record exists for this site and module
            existing = supabase.table("module_results").select("id").eq(
                "site_id", site_id
            ).eq(
                "module_name", "algorithm_impact"
            ).execute()
            
            if existing.data and len(existing.data) > 0:
                # Update existing record
                supabase.table("module_results").update({
                    "results": analysis_results,
                    "parameters": result_record["parameters"],
                    "analysis_date": analysis_date,
                    "updated_at": analysis_date,
                    "status": "completed"
                }).eq("id", existing.data[0]["id"]).execute()
                logger.info(f"Updated existing module_results record for site {site_id}")
            else:
                # Insert new record
                supabase.table("module_results").insert(result_record).execute()
                logger.info(f"Inserted new module_results record for site {site_id}")
                
        except Exception as e:
            logger.error(f"Failed to store results in database: {str(e)}", exc_info=True)
            import traceback
            await log_error_to_supabase(
                supabase=supabase,
                site_id=site_id,
                module_name="algorithm_impact",
                error_type="database_error",
                error_message=f"Failed to store results: {str(e)}",
                stack_trace=traceback.format_exc()
            )
            # Continue - don't fail the request if storage fails
        
        # Sort impacts by severity (critical > high > medium > low)
        severity_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
        sorted_impacts = sorted(
            analysis_results.get("updates_impacting_site", []),
            key=lambda x: severity_order.get(x.get("severity", "low"), 99)
        )
        
        # Format response
        response = AnalysisResponse(
            status="success",
            site_id=site_id,
            analysis_date=analysis_date,
            date_range_days=date_range,
            updates_impacting_site=[
                AlgorithmImpact(
                    update_name=impact["update_name"],
                    date=impact["date"],
                    site_impact=impact["site_impact"],
                    click_change_pct=impact["click_change_pct"],
                    impression_change_pct=impact.get("impression_change_pct", 0.0),
                    pages_most_affected=impact["pages_most_affected"],
                    common_characteristics=impact["common_characteristics"],
                    recovery_status=impact["recovery_status"],
                    recovery_days=impact.get("recovery_days"),
                    severity=impact["severity"]
                )
                for impact in sorted_impacts
            ],
            vulnerability_score=analysis_results.get("vulnerability_score", 0.0),
            total_updates_analyzed=analysis_results.get("total_updates_analyzed", 0),
            updates_with_impact=len(sorted_impacts),
            average_recovery_days=analysis_results.get("average_recovery_days"),
            recommendation=analysis_results.get("recommendation", ""),
            most_vulnerable_page_types=analysis_results.get("most_vulnerable_page_types", [])
        )
        
        logger.info(f"Successfully completed algorithm impact analysis for site {site_id}")
        logger.info(f"Found {len(sorted_impacts)} impactful updates out of {response.total_updates_analyzed} analyzed")
        
        return response
        
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        logger.error(f"Unexpected error in analyze_algorithm_impact endpoint: {str(e)}", exc_info=True)
        import traceback
        await log_error_to_supabase(
            supabase=supabase,
            site_id=site_id,
            module_name="algorithm_impact",
            error_type="unexpected_error",
            error_message=str(e),
            stack_trace=traceback.format_exc()
        )
        raise HTTPException(
            status_code=500,
            detail="An unexpected error occurred during analysis"
        )
