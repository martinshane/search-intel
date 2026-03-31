from fastapi import APIRouter, HTTPException, Depends
from typing import Dict, Any, Optional
from datetime import datetime
import time
from pydantic import BaseModel

from core.report_generator import ReportGenerator
from core.auth import get_current_user
from database.supabase_client import get_supabase_client

router = APIRouter()


class PerformanceMetrics(BaseModel):
    """Performance metrics for report generation"""
    total_duration_seconds: float
    modules: Dict[str, Dict[str, Any]]
    data_ingestion: Dict[str, Any]
    report_id: str
    timestamp: str
    bottlenecks: list[Dict[str, Any]]


class ModuleMetrics(BaseModel):
    """Metrics for a single module"""
    name: str
    duration_seconds: float
    status: str
    error: Optional[str] = None
    memory_mb: Optional[float] = None
    rows_processed: Optional[int] = None


def identify_bottlenecks(
    module_timings: Dict[str, float], 
    data_timings: Dict[str, float],
    threshold_seconds: float = 5.0
) -> list[Dict[str, Any]]:
    """
    Identify performance bottlenecks based on execution time.
    
    Args:
        module_timings: Dict of module_name -> duration_seconds
        data_timings: Dict of data_source -> duration_seconds
        threshold_seconds: Minimum duration to be considered a bottleneck
    
    Returns:
        List of bottleneck descriptions with optimization suggestions
    """
    bottlenecks = []
    
    # Check module timings
    for module_name, duration in module_timings.items():
        if duration >= threshold_seconds:
            bottleneck = {
                "type": "module",
                "name": module_name,
                "duration_seconds": duration,
                "severity": "high" if duration >= 30 else "medium" if duration >= 10 else "low",
                "suggestions": []
            }
            
            # Module-specific optimization suggestions
            if "health_trajectory" in module_name.lower():
                bottleneck["suggestions"].extend([
                    "Consider caching MSTL decomposition results",
                    "Reduce matrix profile window size for large datasets",
                    "Use parallel processing for multiple time series"
                ])
            elif "page_triage" in module_name.lower():
                bottleneck["suggestions"].extend([
                    "Batch process page trend fitting",
                    "Optimize PyOD Isolation Forest parameters (n_estimators)",
                    "Pre-filter pages with < 30 days data before processing"
                ])
            elif "serp_landscape" in module_name.lower():
                bottleneck["suggestions"].extend([
                    "Cache SERP feature parsing results",
                    "Reduce number of keywords analyzed (focus on top performers)",
                    "Parallelize SERP feature extraction"
                ])
            elif "content_intelligence" in module_name.lower():
                bottleneck["suggestions"].extend([
                    "Use sparse matrix operations for TF-IDF",
                    "Limit cannibalization detection to pages with > threshold impressions",
                    "Cache semantic similarity calculations"
                ])
            elif "architecture" in module_name.lower():
                bottleneck["suggestions"].extend([
                    "Use approximate PageRank for large graphs (> 1000 nodes)",
                    "Limit graph depth for authority flow analysis",
                    "Cache community detection results"
                ])
            elif "ctr_modeling" in module_name.lower():
                bottleneck["suggestions"].extend([
                    "Use pre-trained model with incremental updates",
                    "Reduce gradient boosting iterations or max_depth",
                    "Sample training data for large keyword sets"
                ])
            
            bottlenecks.append(bottleneck)
    
    # Check data ingestion timings
    for source, duration in data_timings.items():
        if duration >= threshold_seconds:
            bottleneck = {
                "type": "data_ingestion",
                "name": source,
                "duration_seconds": duration,
                "severity": "high" if duration >= 60 else "medium" if duration >= 20 else "low",
                "suggestions": []
            }
            
            if "gsc" in source.lower():
                bottleneck["suggestions"].extend([
                    "Implement request batching and pagination optimization",
                    "Use concurrent API calls with rate limiting",
                    "Cache results with 24h TTL",
                    "Reduce date range for initial analysis (e.g., 12 months instead of 16)"
                ])
            elif "ga4" in source.lower():
                bottleneck["suggestions"].extend([
                    "Batch dimension combinations in single requests",
                    "Use report snapshots instead of raw data queries",
                    "Implement progressive loading (recent data first)"
                ])
            elif "dataforseo" in source.lower():
                bottleneck["suggestions"].extend([
                    "Reduce number of keywords queried (top 50 instead of 100)",
                    "Use cached SERP data where available",
                    "Implement tiered keyword analysis (critical keywords first)"
                ])
            elif "crawl" in source.lower():
                bottleneck["suggestions"].extend([
                    "Use sitemap-based extraction instead of full crawl",
                    "Limit crawl depth and page count",
                    "Accept user-uploaded Screaming Frog export",
                    "Parallelize page fetching with connection pooling"
                ])
            
            bottlenecks.append(bottleneck)
    
    # Sort by duration (longest first)
    bottlenecks.sort(key=lambda x: x["duration_seconds"], reverse=True)
    
    return bottlenecks


@router.get("/api/metrics/{report_id}", response_model=PerformanceMetrics)
async def get_report_metrics(
    report_id: str,
    user = Depends(get_current_user)
):
    """
    Get performance metrics for a specific report generation run.
    
    Args:
        report_id: UUID of the report
        user: Authenticated user (from dependency)
    
    Returns:
        PerformanceMetrics object with detailed timing information
    
    Raises:
        HTTPException: If report not found or user not authorized
    """
    try:
        supabase = get_supabase_client()
        
        # Fetch report with metrics
        response = supabase.table("reports").select("*").eq("id", report_id).eq("user_id", user.id).execute()
        
        if not response.data or len(response.data) == 0:
            raise HTTPException(status_code=404, detail="Report not found")
        
        report = response.data[0]
        
        # Extract metrics from report progress field
        progress = report.get("progress", {})
        
        # Calculate total duration
        created_at = datetime.fromisoformat(report["created_at"].replace("Z", "+00:00"))
        completed_at = report.get("completed_at")
        
        if completed_at:
            completed_at = datetime.fromisoformat(completed_at.replace("Z", "+00:00"))
            total_duration = (completed_at - created_at).total_seconds()
        else:
            total_duration = (datetime.now().astimezone() - created_at).total_seconds()
        
        # Extract module timings
        modules = {}
        module_timings = {}
        for key, value in progress.items():
            if key.startswith("module_"):
                module_data = value if isinstance(value, dict) else {"status": value}
                modules[key] = {
                    "name": key.replace("module_", "").replace("_", " ").title(),
                    "status": module_data.get("status", "unknown"),
                    "duration_seconds": module_data.get("duration_seconds", 0),
                    "rows_processed": module_data.get("rows_processed"),
                    "error": module_data.get("error")
                }
                module_timings[key] = module_data.get("duration_seconds", 0)
        
        # Extract data ingestion timings
        data_ingestion = progress.get("data_ingestion", {})
        data_timings = {}
        for source, value in data_ingestion.items():
            if isinstance(value, dict) and "duration_seconds" in value:
                data_timings[source] = value["duration_seconds"]
        
        # Identify bottlenecks
        bottlenecks = identify_bottlenecks(module_timings, data_timings)
        
        return PerformanceMetrics(
            total_duration_seconds=round(total_duration, 2),
            modules=modules,
            data_ingestion=data_ingestion,
            report_id=report_id,
            timestamp=datetime.now().isoformat(),
            bottlenecks=bottlenecks
        )
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving metrics: {str(e)}")


@router.get("/api/metrics", response_model=Dict[str, Any])
async def get_aggregate_metrics(
    user = Depends(get_current_user),
    limit: int = 10
):
    """
    Get aggregate performance metrics across recent reports.
    
    Args:
        user: Authenticated user (from dependency)
        limit: Number of recent reports to analyze
    
    Returns:
        Aggregate metrics including average timings and common bottlenecks
    """
    try:
        supabase = get_supabase_client()
        
        # Fetch recent completed reports
        response = supabase.table("reports").select("*").eq(
            "user_id", user.id
        ).eq(
            "status", "complete"
        ).order(
            "completed_at", desc=True
        ).limit(limit).execute()
        
        if not response.data or len(response.data) == 0:
            return {
                "message": "No completed reports found",
                "reports_analyzed": 0
            }
        
        reports = response.data
        
        # Aggregate metrics
        total_durations = []
        module_durations = {}
        data_source_durations = {}
        all_bottlenecks = []
        
        for report in reports:
            # Calculate duration
            created_at = datetime.fromisoformat(report["created_at"].replace("Z", "+00:00"))
            completed_at = datetime.fromisoformat(report["completed_at"].replace("Z", "+00:00"))
            duration = (completed_at - created_at).total_seconds()
            total_durations.append(duration)
            
            progress = report.get("progress", {})
            
            # Aggregate module timings
            module_timings = {}
            for key, value in progress.items():
                if key.startswith("module_"):
                    module_data = value if isinstance(value, dict) else {}
                    duration_sec = module_data.get("duration_seconds", 0)
                    module_timings[key] = duration_sec
                    
                    if key not in module_durations:
                        module_durations[key] = []
                    module_durations[key].append(duration_sec)
            
            # Aggregate data source timings
            data_timings = {}
            data_ingestion = progress.get("data_ingestion", {})
            for source, value in data_ingestion.items():
                if isinstance(value, dict) and "duration_seconds" in value:
                    duration_sec = value["duration_seconds"]
                    data_timings[source] = duration_sec
                    
                    if source not in data_source_durations:
                        data_source_durations[source] = []
                    data_source_durations[source].append(duration_sec)
            
            # Collect bottlenecks
            bottlenecks = identify_bottlenecks(module_timings, data_timings, threshold_seconds=3.0)
            all_bottlenecks.extend(bottlenecks)
        
        # Calculate averages
        avg_total_duration = sum(total_durations) / len(total_durations)
        
        avg_module_durations = {}
        for module, durations in module_durations.items():
            avg_module_durations[module] = {
                "avg_seconds": round(sum(durations) / len(durations), 2),
                "min_seconds": round(min(durations), 2),
                "max_seconds": round(max(durations), 2),
                "sample_size": len(durations)
            }
        
        avg_data_source_durations = {}
        for source, durations in data_source_durations.items():
            avg_data_source_durations[source] = {
                "avg_seconds": round(sum(durations) / len(durations), 2),
                "min_seconds": round(min(durations), 2),
                "max_seconds": round(max(durations), 2),
                "sample_size": len(durations)
            }
        
        # Find most common bottlenecks
        bottleneck_frequency = {}
        for bottleneck in all_bottlenecks:
            key = f"{bottleneck['type']}:{bottleneck['name']}"
            if key not in bottleneck_frequency:
                bottleneck_frequency[key] = {
                    "count": 0,
                    "type": bottleneck["type"],
                    "name": bottleneck["name"],
                    "avg_duration": 0,
                    "durations": [],
                    "suggestions": bottleneck["suggestions"]
                }
            bottleneck_frequency[key]["count"] += 1
            bottleneck_frequency[key]["durations"].append(bottleneck["duration_seconds"])
        
        # Calculate average duration for each bottleneck
        common_bottlenecks = []
        for key, data in bottleneck_frequency.items():
            data["avg_duration"] = round(sum(data["durations"]) / len(data["durations"]), 2)
            del data["durations"]  # Remove raw data
            common_bottlenecks.append(data)
        
        # Sort by frequency
        common_bottlenecks.sort(key=lambda x: (x["count"], x["avg_duration"]), reverse=True)
        
        return {
            "reports_analyzed": len(reports),
            "avg_total_duration_seconds": round(avg_total_duration, 2),
            "avg_total_duration_minutes": round(avg_total_duration / 60, 2),
            "target_duration_minutes": 3.0,
            "meets_target": avg_total_duration <= 180,  # 3 minutes = 180 seconds
            "module_performance": avg_module_durations,
            "data_source_performance": avg_data_source_durations,
            "common_bottlenecks": common_bottlenecks[:5],  # Top 5
            "optimization_priority": common_bottlenecks[0]["name"] if common_bottlenecks else None,
            "timestamp": datetime.now().isoformat()
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error calculating aggregate metrics: {str(e)}")


@router.post("/api/metrics/{report_id}/record")
async def record_module_timing(
    report_id: str,
    module_name: str,
    duration_seconds: float,
    status: str = "complete",
    rows_processed: Optional[int] = None,
    error: Optional[str] = None,
    user = Depends(get_current_user)
):
    """
    Record timing for a specific module execution.
    This is called by the report generator during execution.
    
    Args:
        report_id: UUID of the report
        module_name: Name of the module (e.g., "module_1_health_trajectory")
        duration_seconds: Execution time in seconds
        status: Module status (complete, failed, etc.)
        rows_processed: Number of rows/items processed
        error: Error message if status is failed
        user: Authenticated user
    
    Returns:
        Success confirmation
    """
    try:
        supabase = get_supabase_client()
        
        # Fetch current progress
        response = supabase.table("reports").select("progress").eq("id", report_id).eq("user_id", user.id).execute()
        
        if not response.data or len(response.data) == 0:
            raise HTTPException(status_code=404, detail="Report not found")
        
        progress = response.data[0].get("progress", {})
        
        # Update module timing
        progress[module_name] = {
            "status": status,
            "duration_seconds": round(duration_seconds, 2),
            "rows_processed": rows_processed,
            "error": error,
            "timestamp": datetime.now().isoformat()
        }
        
        # Save back to database
        supabase.table("reports").update({
            "progress": progress
        }).eq("id", report_id).execute()
        
        return {
            "success": True,
            "module": module_name,
            "duration_seconds": round(duration_seconds, 2)
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error recording timing: {str(e)}")
