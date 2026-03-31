"""
Analysis pipeline orchestrator.
Runs all 12 analysis modules in sequence and writes progress updates to the reports table.
"""

import logging
from typing import Dict, Any, Optional
from datetime import datetime
import traceback

from .modules import health_trajectory
from .modules import page_triage
from .modules import serp_landscape
from .modules import content_intelligence
from .modules import gameplan
from .modules import algorithm_impacts
from .modules import intent_migration
from .modules import ctr_modeling
from .modules import site_architecture
from .modules import branded_split
from .modules import competitive_threats
from .modules import revenue_attribution
from ..database import get_supabase_client

logger = logging.getLogger(__name__)


class AnalysisPipeline:
    """Orchestrates the execution of all analysis modules."""
    
    def __init__(self, report_id: str):
        self.report_id = report_id
        self.supabase = get_supabase_client()
        self.results = {}
        
    async def run(self) -> Dict[str, Any]:
        """
        Execute all analysis modules in sequence.
        Updates progress in the reports table after each module.
        
        Returns:
            Complete report data dictionary
        """
        modules = [
            ("module_1_health", "Health & Trajectory", self._run_module_1),
            ("module_2_triage", "Page-Level Triage", self._run_module_2),
            ("module_3_serp", "SERP Landscape Analysis", self._run_module_3),
            ("module_4_content", "Content Intelligence", self._run_module_4),
            ("module_5_gameplan", "The Gameplan", self._run_module_5),
            ("module_6_algorithm", "Algorithm Update Impact", self._run_module_6),
            ("module_7_intent", "Query Intent Migration", self._run_module_7),
            ("module_8_ctr", "CTR Modeling by SERP Context", self._run_module_8),
            ("module_9_architecture", "Site Architecture & Authority Flow", self._run_module_9),
            ("module_10_branded", "Branded vs Non-Branded Health", self._run_module_10),
            ("module_11_competitive", "Competitive Threat Radar", self._run_module_11),
            ("module_12_revenue", "Revenue Attribution", self._run_module_12),
        ]
        
        try:
            for module_key, module_name, module_func in modules:
                logger.info(f"Starting {module_name} for report {self.report_id}")
                
                # Update progress to show module is running
                await self._update_progress(module_key, "running", module_name)
                
                try:
                    # Execute the module
                    result = await module_func()
                    self.results[module_key] = result
                    
                    # Mark module as complete
                    await self._update_progress(module_key, "complete", module_name)
                    logger.info(f"Completed {module_name} for report {self.report_id}")
                    
                except Exception as e:
                    logger.error(f"Error in {module_name}: {str(e)}\n{traceback.format_exc()}")
                    await self._update_progress(module_key, "failed", module_name, str(e))
                    raise
            
            # All modules complete - compile final report
            final_report = self._compile_report()
            
            # Update report status to complete
            self.supabase.table("reports").update({
                "status": "complete",
                "report_data": final_report,
                "completed_at": datetime.utcnow().isoformat()
            }).eq("id", self.report_id).execute()
            
            return final_report
            
        except Exception as e:
            logger.error(f"Pipeline failed for report {self.report_id}: {str(e)}")
            
            # Mark report as failed
            self.supabase.table("reports").update({
                "status": "failed",
                "error": str(e)
            }).eq("id", self.report_id).execute()
            
            raise
    
    async def _update_progress(
        self, 
        module_key: str, 
        status: str, 
        module_name: str,
        error: Optional[str] = None
    ):
        """Update the progress field in the reports table."""
        try:
            # Fetch current report
            response = self.supabase.table("reports").select("progress").eq("id", self.report_id).single().execute()
            current_progress = response.data.get("progress", {})
            
            # Update module status
            current_progress[module_key] = {
                "status": status,
                "name": module_name,
                "timestamp": datetime.utcnow().isoformat()
            }
            
            if error:
                current_progress[module_key]["error"] = error
            
            # Write back to database
            self.supabase.table("reports").update({
                "progress": current_progress,
                "status": "analyzing"  # Overall status while modules are running
            }).eq("id", self.report_id).execute()
            
        except Exception as e:
            logger.error(f"Failed to update progress for {module_key}: {str(e)}")
            # Don't raise - progress updates are non-critical
    
    def _compile_report(self) -> Dict[str, Any]:
        """Compile all module results into final report structure."""
        return {
            "report_id": self.report_id,
            "generated_at": datetime.utcnow().isoformat(),
            "version": "1.0",
            "modules": self.results,
            "summary": self._generate_executive_summary()
        }
    
    def _generate_executive_summary(self) -> Dict[str, Any]:
        """Generate high-level summary metrics across all modules."""
        summary = {}
        
        # Extract key metrics from each module
        if "module_1_health" in self.results:
            health = self.results["module_1_health"]
            summary["overall_direction"] = health.get("overall_direction")
            summary["trend_slope_pct_per_month"] = health.get("trend_slope_pct_per_month")
        
        if "module_2_triage" in self.results:
            triage = self.results["module_2_triage"]
            summary["total_recoverable_clicks"] = triage.get("summary", {}).get("total_recoverable_clicks_monthly")
        
        if "module_5_gameplan" in self.results:
            gameplan = self.results["module_5_gameplan"]
            summary["total_action_items"] = (
                len(gameplan.get("critical", [])) +
                len(gameplan.get("quick_wins", [])) +
                len(gameplan.get("strategic", []))
            )
        
        if "module_12_revenue" in self.results:
            revenue = self.results["module_12_revenue"]
            summary["total_revenue_opportunity"] = revenue.get("action_roi", {}).get("total_opportunity")
        
        return summary
    
    async def _run_module_1(self) -> Dict[str, Any]:
        """Module 1: Health & Trajectory Analysis"""
        # Fetch GSC daily data for this report
        data = await self._get_gsc_daily_data()
        return health_trajectory.analyze(data)
    
    async def _run_module_2(self) -> Dict[str, Any]:
        """Module 2: Page-Level Triage"""
        page_data = await self._get_gsc_page_data()
        ga4_data = await self._get_ga4_landing_page_data()
        return page_triage.analyze(page_data, ga4_data)
    
    async def _run_module_3(self) -> Dict[str, Any]:
        """Module 3: SERP Landscape Analysis"""
        serp_data = await self._get_serp_data()
        gsc_keyword_data = await self._get_gsc_keyword_data()
        return serp_landscape.analyze(serp_data, gsc_keyword_data)
    
    async def _run_module_4(self) -> Dict[str, Any]:
        """Module 4: Content Intelligence"""
        query_page_data = await self._get_gsc_query_page_data()
        page_crawl = await self._get_page_crawl_data()
        ga4_engagement = await self._get_ga4_engagement_data()
        return content_intelligence.analyze(query_page_data, page_crawl, ga4_engagement)
    
    async def _run_module_5(self) -> Dict[str, Any]:
        """Module 5: The Gameplan (synthesizes modules 1-4)"""
        return gameplan.generate(
            self.results.get("module_1_health", {}),
            self.results.get("module_2_triage", {}),
            self.results.get("module_3_serp", {}),
            self.results.get("module_4_content", {})
        )
    
    async def _run_module_6(self) -> Dict[str, Any]:
        """Module 6: Algorithm Update Impact Analysis"""
        daily_data = await self._get_gsc_daily_data()
        change_points = self.results.get("module_1_health", {}).get("change_points", [])
        return algorithm_impacts.analyze(daily_data, change_points)
    
    async def _run_module_7(self) -> Dict[str, Any]:
        """Module 7: Query Intent Migration Tracking"""
        query_date_data = await self._get_gsc_query_date_data()
        return intent_migration.analyze(query_date_data)
    
    async def _run_module_8(self) -> Dict[str, Any]:
        """Module 8: CTR Modeling by SERP Context"""
        serp_data = await self._get_serp_data()
        gsc_data = await self._get_gsc_keyword_data()
        return ctr_modeling.analyze(serp_data, gsc_data)
    
    async def _run_module_9(self) -> Dict[str, Any]:
        """Module 9: Site Architecture & Authority Flow"""
        link_graph = await self._get_link_graph()
        page_performance = await self._get_gsc_page_data()
        return site_architecture.analyze(link_graph, page_performance)
    
    async def _run_module_10(self) -> Dict[str, Any]:
        """Module 10: Branded vs Non-Branded Health"""
        query_data = await self._get_gsc_query_data()
        brand_terms = await self._get_brand_terms()
        return branded_split.analyze(query_data, brand_terms)
    
    async def _run_module_11(self) -> Dict[str, Any]:
        """Module 11: Competitive Threat Radar"""
        serp_data = await self._get_serp_data()
        gsc_data = await self._get_gsc_keyword_data()
        return competitive_threats.analyze(serp_data, gsc_data)
    
    async def _run_module_12(self) -> Dict[str, Any]:
        """Module 12: Revenue Attribution"""
        gsc_data = await self._get_gsc_page_data()
        ga4_conversions = await self._get_ga4_conversion_data()
        ga4_engagement = await self._get_ga4_engagement_data()
        return revenue_attribution.analyze(gsc_data, ga4_conversions, ga4_engagement)
    
    # Data fetching helpers - these pull from cached ingestion results
    
    async def _get_gsc_daily_data(self) -> Dict[str, Any]:
        """Fetch GSC daily time series data for this report."""
        response = self.supabase.table("gsc_daily_data").select("*").eq("report_id", self.report_id).execute()
        return response.data
    
    async def _get_gsc_page_data(self) -> Dict[str, Any]:
        """Fetch GSC page-level data."""
        response = self.supabase.table("gsc_page_data").select("*").eq("report_id", self.report_id).execute()
        return response.data
    
    async def _get_gsc_keyword_data(self) -> Dict[str, Any]:
        """Fetch GSC keyword data."""
        response = self.supabase.table("gsc_query_data").select("*").eq("report_id", self.report_id).execute()
        return response.data
    
    async def _get_gsc_query_page_data(self) -> Dict[str, Any]:
        """Fetch GSC query+page mapping."""
        response = self.supabase.table("gsc_query_page_data").select("*").eq("report_id", self.report_id).execute()
        return response.data
    
    async def _get_gsc_query_date_data(self) -> Dict[str, Any]:
        """Fetch GSC query+date time series."""
        response = self.supabase.table("gsc_query_date_data").select("*").eq("report_id", self.report_id).execute()
        return response.data
    
    async def _get_ga4_landing_page_data(self) -> Dict[str, Any]:
        """Fetch GA4 landing page data."""
        response = self.supabase.table("ga4_landing_pages").select("*").eq("report_id", self.report_id).execute()
        return response.data
    
    async def _get_ga4_engagement_data(self) -> Dict[str, Any]:
        """Fetch GA4 engagement metrics."""
        response = self.supabase.table("ga4_engagement").select("*").eq("report_id", self.report_id).execute()
        return response.data
    
    async def _get_ga4_conversion_data(self) -> Dict[str, Any]:
        """Fetch GA4 conversion data."""
        response = self.supabase.table("ga4_conversions").select("*").eq("report_id", self.report_id).execute()
        return response.data
    
    async def _get_serp_data(self) -> Dict[str, Any]:
        """Fetch DataForSEO SERP data."""
        response = self.supabase.table("serp_data").select("*").eq("report_id", self.report_id).execute()
        return response.data
    
    async def _get_page_crawl_data(self) -> Dict[str, Any]:
        """Fetch site crawl data."""
        response = self.supabase.table("crawl_data").select("*").eq("report_id", self.report_id).execute()
        return response.data
    
    async def _get_link_graph(self) -> Dict[str, Any]:
        """Fetch internal link graph."""
        response = self.supabase.table("link_graph").select("*").eq("report_id", self.report_id).execute()
        return response.data
    
    async def _get_brand_terms(self) -> list:
        """Get brand terms for this report."""
        response = self.supabase.table("reports").select("brand_terms").eq("id", self.report_id).single().execute()
        return response.data.get("brand_terms", [])


async def run_analysis_pipeline(report_id: str) -> Dict[str, Any]:
    """
    Convenience function to run the complete analysis pipeline.
    
    Args:
        report_id: UUID of the report to analyze
        
    Returns:
        Complete report data dictionary
        
    Raises:
        Exception: If any module fails
    """
    pipeline = AnalysisPipeline(report_id)
    return await pipeline.run()
