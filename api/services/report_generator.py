import asyncio
from typing import Dict, Any, List
import logging
from datetime import datetime

from .modules.health_trajectory import analyze_health_trajectory
from .modules.page_triage import analyze_page_triage
from .modules.serp_landscape import analyze_serp_landscape
from .modules.content_intelligence import analyze_content_intelligence
from .modules.top_landing_pages import analyze_top_landing_pages
from .modules.gameplan import generate_gameplan
from .modules.algorithm_impact import analyze_algorithm_impacts
from .modules.query_intent_migration import analyze_query_intent_migration
from .modules.link_graph import analyze_link_graph
from .modules.technical_health import analyze_technical_health
from .modules.conversion_path import analyze_conversion_path
from .modules.forecasting import generate_forecasts
from .modules.executive_summary import generate_executive_summary

logger = logging.getLogger(__name__)


class ReportGenerator:
    """
    Orchestrates the generation of a complete Search Intelligence Report.
    
    Runs all 12 analysis modules in sequence, building up a comprehensive
    report data structure that combines statistical analysis, ML-based insights,
    and actionable recommendations.
    """
    
    def __init__(self, data_store):
        """
        Initialize the report generator.
        
        Args:
            data_store: DataStore instance containing all ingested data
        """
        self.data_store = data_store
        
    async def generate_report(self, site_id: str, user_id: str) -> Dict[str, Any]:
        """
        Generate a complete Search Intelligence Report for a site.
        
        This is the main orchestration function that:
        1. Retrieves all necessary data from the data store
        2. Runs all 12 analysis modules in sequence
        3. Assembles the final report structure
        4. Returns the complete report for storage/rendering
        
        Args:
            site_id: Unique identifier for the site being analyzed
            user_id: User who owns the site
            
        Returns:
            Complete report data structure as a dictionary
        """
        logger.info(f"Starting report generation for site_id={site_id}, user_id={user_id}")
        
        try:
            # Initialize report structure
            report = {
                "site_id": site_id,
                "user_id": user_id,
                "generated_at": datetime.utcnow().isoformat(),
                "status": "generating",
                "modules": {},
                "metadata": {}
            }
            
            # Fetch all required data from data store
            logger.info("Fetching data from data store...")
            data = await self._fetch_report_data(site_id)
            
            # Store metadata about the data
            report["metadata"] = {
                "data_range_start": data.get("date_range_start"),
                "data_range_end": data.get("date_range_end"),
                "total_gsc_queries": len(data.get("gsc_queries", [])),
                "total_pages": len(data.get("pages", [])),
                "ga4_connected": data.get("ga4_available", False),
                "serp_data_keywords": len(data.get("serp_data", [])),
            }
            
            # Module 1: Health & Trajectory
            logger.info("Running Module 1: Health & Trajectory...")
            try:
                health_results = await analyze_health_trajectory(
                    daily_data=data.get("gsc_daily_timeseries"),
                    site_id=site_id
                )
                report["modules"]["health_trajectory"] = health_results
                logger.info("Module 1 completed successfully")
            except Exception as e:
                logger.error(f"Module 1 failed: {str(e)}", exc_info=True)
                report["modules"]["health_trajectory"] = {"error": str(e)}
            
            # Module 2: Page-Level Triage
            logger.info("Running Module 2: Page-Level Triage...")
            try:
                triage_results = await analyze_page_triage(
                    page_daily_data=data.get("gsc_page_timeseries"),
                    ga4_landing_data=data.get("ga4_landing_pages"),
                    gsc_page_summary=data.get("gsc_page_summary"),
                    site_id=site_id
                )
                report["modules"]["page_triage"] = triage_results
                logger.info("Module 2 completed successfully")
            except Exception as e:
                logger.error(f"Module 2 failed: {str(e)}", exc_info=True)
                report["modules"]["page_triage"] = {"error": str(e)}
            
            # Module 3: SERP Landscape Analysis
            logger.info("Running Module 3: SERP Landscape Analysis...")
            try:
                serp_results = await analyze_serp_landscape(
                    serp_data=data.get("serp_data"),
                    gsc_keyword_data=data.get("gsc_queries"),
                    site_id=site_id
                )
                report["modules"]["serp_landscape"] = serp_results
                logger.info("Module 3 completed successfully")
            except Exception as e:
                logger.error(f"Module 3 failed: {str(e)}", exc_info=True)
                report["modules"]["serp_landscape"] = {"error": str(e)}
            
            # Module 4: Content Intelligence
            logger.info("Running Module 4: Content Intelligence...")
            try:
                content_results = await analyze_content_intelligence(
                    gsc_query_page=data.get("gsc_query_page_mapping"),
                    page_data=data.get("page_crawl_data"),
                    ga4_engagement=data.get("ga4_landing_pages"),
                    site_id=site_id
                )
                report["modules"]["content_intelligence"] = content_results
                logger.info("Module 4 completed successfully")
            except Exception as e:
                logger.error(f"Module 4 failed: {str(e)}", exc_info=True)
                report["modules"]["content_intelligence"] = {"error": str(e)}
            
            # Module 5: Top Landing Pages
            logger.info("Running Module 5: Top Landing Pages...")
            try:
                landing_pages_results = await analyze_top_landing_pages(
                    gsc_page_summary=data.get("gsc_page_summary"),
                    ga4_landing_data=data.get("ga4_landing_pages"),
                    gsc_page_timeseries=data.get("gsc_page_timeseries"),
                    site_id=site_id
                )
                report["modules"]["top_landing_pages"] = landing_pages_results
                logger.info("Module 5 completed successfully")
            except Exception as e:
                logger.error(f"Module 5 failed: {str(e)}", exc_info=True)
                report["modules"]["top_landing_pages"] = {"error": str(e)}
            
            # Module 6: Algorithm Update Impact Analysis
            logger.info("Running Module 6: Algorithm Update Impact Analysis...")
            try:
                algorithm_results = await analyze_algorithm_impacts(
                    daily_data=data.get("gsc_daily_timeseries"),
                    change_points=report["modules"].get("health_trajectory", {}).get("change_points", []),
                    site_id=site_id
                )
                report["modules"]["algorithm_impact"] = algorithm_results
                logger.info("Module 6 completed successfully")
            except Exception as e:
                logger.error(f"Module 6 failed: {str(e)}", exc_info=True)
                report["modules"]["algorithm_impact"] = {"error": str(e)}
            
            # Module 7: Query Intent Migration Tracking
            logger.info("Running Module 7: Query Intent Migration Tracking...")
            try:
                intent_results = await analyze_query_intent_migration(
                    gsc_query_timeseries=data.get("gsc_query_timeseries"),
                    serp_data=data.get("serp_data"),
                    gsc_queries=data.get("gsc_queries"),
                    site_id=site_id
                )
                report["modules"]["query_intent_migration"] = intent_results
                logger.info("Module 7 completed successfully")
            except Exception as e:
                logger.error(f"Module 7 failed: {str(e)}", exc_info=True)
                report["modules"]["query_intent_migration"] = {"error": str(e)}
            
            # Module 8: Internal Link Graph Analysis
            logger.info("Running Module 8: Internal Link Graph Analysis...")
            try:
                link_graph_results = await analyze_link_graph(
                    link_data=data.get("internal_links"),
                    page_data=data.get("page_crawl_data"),
                    gsc_page_summary=data.get("gsc_page_summary"),
                    site_id=site_id
                )
                report["modules"]["link_graph"] = link_graph_results
                logger.info("Module 8 completed successfully")
            except Exception as e:
                logger.error(f"Module 8 failed: {str(e)}", exc_info=True)
                report["modules"]["link_graph"] = {"error": str(e)}
            
            # Module 9: Technical Health Score
            logger.info("Running Module 9: Technical Health Score...")
            try:
                technical_results = await analyze_technical_health(
                    page_data=data.get("page_crawl_data"),
                    gsc_url_inspection=data.get("gsc_url_inspection"),
                    gsc_sitemaps=data.get("gsc_sitemaps"),
                    site_id=site_id
                )
                report["modules"]["technical_health"] = technical_results
                logger.info("Module 9 completed successfully")
            except Exception as e:
                logger.error(f"Module 9 failed: {str(e)}", exc_info=True)
                report["modules"]["technical_health"] = {"error": str(e)}
            
            # Module 10: Conversion Path Analysis
            logger.info("Running Module 10: Conversion Path Analysis...")
            try:
                conversion_results = await analyze_conversion_path(
                    ga4_conversion_data=data.get("ga4_conversions"),
                    ga4_landing_data=data.get("ga4_landing_pages"),
                    gsc_page_summary=data.get("gsc_page_summary"),
                    site_id=site_id
                )
                report["modules"]["conversion_path"] = conversion_results
                logger.info("Module 10 completed successfully")
            except Exception as e:
                logger.error(f"Module 10 failed: {str(e)}", exc_info=True)
                report["modules"]["conversion_path"] = {"error": str(e)}
            
            # Module 11: Forecasting & Scenario Planning
            logger.info("Running Module 11: Forecasting & Scenario Planning...")
            try:
                forecast_results = await generate_forecasts(
                    daily_data=data.get("gsc_daily_timeseries"),
                    page_timeseries=data.get("gsc_page_timeseries"),
                    health_data=report["modules"].get("health_trajectory", {}),
                    triage_data=report["modules"].get("page_triage", {}),
                    site_id=site_id
                )
                report["modules"]["forecasting"] = forecast_results
                logger.info("Module 11 completed successfully")
            except Exception as e:
                logger.error(f"Module 11 failed: {str(e)}", exc_info=True)
                report["modules"]["forecasting"] = {"error": str(e)}
            
            # Module 12: The Gameplan (synthesis)
            logger.info("Running Module 12: The Gameplan...")
            try:
                gameplan_results = await generate_gameplan(
                    health=report["modules"].get("health_trajectory", {}),
                    triage=report["modules"].get("page_triage", {}),
                    serp=report["modules"].get("serp_landscape", {}),
                    content=report["modules"].get("content_intelligence", {}),
                    top_landing_pages=report["modules"].get("top_landing_pages", {}),
                    algorithm=report["modules"].get("algorithm_impact", {}),
                    intent=report["modules"].get("query_intent_migration", {}),
                    link_graph=report["modules"].get("link_graph", {}),
                    technical=report["modules"].get("technical_health", {}),
                    conversion=report["modules"].get("conversion_path", {}),
                    forecasting=report["modules"].get("forecasting", {}),
                    site_id=site_id
                )
                report["modules"]["gameplan"] = gameplan_results
                logger.info("Module 12 completed successfully")
            except Exception as e:
                logger.error(f"Module 12 failed: {str(e)}", exc_info=True)
                report["modules"]["gameplan"] = {"error": str(e)}
            
            # Generate Executive Summary (meta-module that synthesizes everything)
            logger.info("Generating Executive Summary...")
            try:
                executive_summary = await generate_executive_summary(
                    report_modules=report["modules"],
                    metadata=report["metadata"],
                    site_id=site_id
                )
                report["executive_summary"] = executive_summary
                logger.info("Executive Summary completed successfully")
            except Exception as e:
                logger.error(f"Executive Summary failed: {str(e)}", exc_info=True)
                report["executive_summary"] = {"error": str(e)}
            
            # Mark report as complete
            report["status"] = "complete"
            report["completed_at"] = datetime.utcnow().isoformat()
            
            # Calculate overall success rate
            total_modules = 12
            successful_modules = sum(
                1 for module_data in report["modules"].values()
                if "error" not in module_data
            )
            report["metadata"]["success_rate"] = successful_modules / total_modules
            
            logger.info(
                f"Report generation complete for site_id={site_id}. "
                f"Success rate: {successful_modules}/{total_modules} modules"
            )
            
            return report
            
        except Exception as e:
            logger.error(f"Report generation failed for site_id={site_id}: {str(e)}", exc_info=True)
            return {
                "site_id": site_id,
                "user_id": user_id,
                "status": "failed",
                "error": str(e),
                "generated_at": datetime.utcnow().isoformat(),
            }
    
    async def _fetch_report_data(self, site_id: str) -> Dict[str, Any]:
        """
        Fetch all required data from the data store for report generation.
        
        Args:
            site_id: Site identifier
            
        Returns:
            Dictionary containing all data needed by analysis modules
        """
        logger.info(f"Fetching all data for site_id={site_id}")
        
        # Fetch all data concurrently for efficiency
        gsc_daily_task = self.data_store.get_gsc_daily_timeseries(site_id)
        gsc_page_timeseries_task = self.data_store.get_gsc_page_timeseries(site_id)
        gsc_page_summary_task = self.data_store.get_gsc_page_summary(site_id)
        gsc_queries_task = self.data_store.get_gsc_queries(site_id)
        gsc_query_page_task = self.data_store.get_gsc_query_page_mapping(site_id)
        gsc_query_timeseries_task = self.data_store.get_gsc_query_timeseries(site_id)
        ga4_landing_task = self.data_store.get_ga4_landing_pages(site_id)
        ga4_conversions_task = self.data_store.get_ga4_conversions(site_id)
        serp_data_task = self.data_store.get_serp_data(site_id)
        page_crawl_task = self.data_store.get_page_crawl_data(site_id)
        internal_links_task = self.data_store.get_internal_links(site_id)
        gsc_url_inspection_task = self.data_store.get_gsc_url_inspection(site_id)
        gsc_sitemaps_task = self.data_store.get_gsc_sitemaps(site_id)
        
        results = await asyncio.gather(
            gsc_daily_task,
            gsc_page_timeseries_task,
            gsc_page_summary_task,
            gsc_queries_task,
            gsc_query_page_task,
            gsc_query_timeseries_task,
            ga4_landing_task,
            ga4_conversions_task,
            serp_data_task,
            page_crawl_task,
            internal_links_task,
            gsc_url_inspection_task,
            gsc_sitemaps_task,
            return_exceptions=True
        )
        
        # Unpack results with error handling
        data = {
            "gsc_daily_timeseries": results[0] if not isinstance(results[0], Exception) else None,
            "gsc_page_timeseries": results[1] if not isinstance(results[1], Exception) else None,
            "gsc_page_summary": results[2] if not isinstance(results[2], Exception) else None,
            "gsc_queries": results[3] if not isinstance(results[3], Exception) else None,
            "gsc_query_page_mapping": results[4] if not isinstance(results[4], Exception) else None,
            "gsc_query_timeseries": results[5] if not isinstance(results[5], Exception) else None,
            "ga4_landing_pages": results[6] if not isinstance(results[6], Exception) else None,
            "ga4_conversions": results[7] if not isinstance(results[7], Exception) else None,
            "serp_data": results[8] if not isinstance(results[8], Exception) else None,
            "page_crawl_data": results[9] if not isinstance(results[9], Exception) else None,
            "internal_links": results[10] if not isinstance(results[10], Exception) else None,
            "gsc_url_inspection": results[11] if not isinstance(results[11], Exception) else None,
            "gsc_sitemaps": results[12] if not isinstance(results[12], Exception) else None,
        }
        
        # Log any data fetch failures
        for key, value in data.items():
            if value is None:
                logger.warning(f"Failed to fetch {key} for site_id={site_id}")
        
        # Determine date range from daily timeseries if available
        if data["gsc_daily_timeseries"] is not None and len(data["gsc_daily_timeseries"]) > 0:
            dates = [row.get("date") for row in data["gsc_daily_timeseries"] if "date" in row]
            if dates:
                data["date_range_start"] = min(dates)
                data["date_range_end"] = max(dates)
        
        # Check if GA4 data is available
        data["ga4_available"] = (
            data["ga4_landing_pages"] is not None and
            len(data.get("ga4_landing_pages", [])) > 0
        )
        
        logger.info(f"Data fetch complete for site_id={site_id}")
        return data
    
    async def regenerate_module(
        self,
        site_id: str,
        module_name: str,
        report_id: str
    ) -> Dict[str, Any]:
        """
        Regenerate a single module of an existing report.
        
        Useful for iterating on module logic without re-running the entire report.
        
        Args:
            site_id: Site identifier
            module_name: Name of the module to regenerate
            report_id: Existing report ID to update
            
        Returns:
            Updated module data
        """
        logger.info(f"Regenerating module {module_name} for site_id={site_id}")
        
        # Map module names to functions
        module_functions = {
            "health_trajectory": analyze_health_trajectory,
            "page_triage": analyze_page_triage,
            "serp_landscape": analyze_serp_landscape,
            "content_intelligence": analyze_content_intelligence,
            "top_landing_pages": analyze_top_landing_pages,
            "algorithm_impact": analyze_algorithm_impacts,
            "query_intent_migration": analyze_query_intent_migration,
            "link_graph": analyze_link_graph,
            "technical_health": analyze_technical_health,
            "conversion_path": analyze_conversion_path,
            "forecasting": generate_forecasts,
            "gameplan": generate_gameplan,
        }
        
        if module_name not in module_functions:
            raise ValueError(f"Unknown module: {module_name}")
        
        # Fetch necessary data
        data = await self._fetch_report_data(site_id)
        
        # Fetch existing report to get dependencies for synthesis modules
        existing_report = await self.data_store.get_report(report_id)
        
        # Run the specific module
        module_func = module_functions[module_name]
        
        # Build kwargs based on module requirements
        kwargs = {"site_id": site_id}
        
        if module_name == "health_trajectory":
            kwargs["daily_data"] = data.get("gsc_daily_timeseries")
        elif module_name == "page_triage":
            kwargs.update({
                "page_daily_data": data.get("gsc_page_timeseries"),
                "ga4_landing_data": data.get("ga4_landing_pages"),
                "gsc_page_summary": data.get("gsc_page_summary"),
            })
        elif module_name == "serp_landscape":
            kwargs.update({
                "serp_data": data.get("serp_data"),
                "gsc_keyword_data": data.get("gsc_queries"),
            })
        elif module_name == "content_intelligence":
            kwargs.update({
                "gsc_query_page": data.get("gsc_query_page_mapping"),
                "page_data": data.get("page_crawl_data"),
                "ga4_engagement": data.get("ga4_landing_pages"),
            })
        elif module_name == "top_landing_pages":
            kwargs.update({
                "gsc_page_summary": data.get("gsc_page_summary"),
                "ga4_landing_data": data.get("ga4_landing_pages"),
                "gsc_page_timeseries": data.get("gsc_page_timeseries"),
            })
        elif module_name == "algorithm_impact":
            kwargs.update({
                "daily_data": data.get("gsc_daily_timeseries"),
                "change_points": existing_report.get("modules", {}).get("health_trajectory", {}).get("change_points", []),
            })
        elif module_name == "query_intent_migration":
            kwargs.update({
                "gsc_query_timeseries": data.get("gsc_query_timeseries"),
                "serp_data": data.get("serp_data"),
                "gsc_queries": data.get("gsc_queries"),
            })
        elif module_name == "link_graph":
            kwargs.update({
                "link_data": data.get("internal_links"),
                "page_data": data.get("page_crawl_data"),
                "gsc_page_summary": data.get("gsc_page_summary"),
            })
        elif module_name == "technical_health":
            kwargs.update({
                "page_data": data.get("page_crawl_data"),
                "gsc_url_inspection": data.get("gsc_url_inspection"),
                "gsc_sitemaps": data.get("gsc_sitemaps"),
            })
        elif module_name == "conversion_path":
            kwargs.update({
                "ga4_conversion_data": data.get("ga4_conversions"),
                "ga4_landing_data": data.get("ga4_landing_pages"),
                "gsc_page_summary": data.get("gsc_page_summary"),
            })
        elif module_name == "forecasting":
            kwargs.update({
                "daily_data": data.get("gsc_daily_timeseries"),
                "page_timeseries": data.get("gsc_page_timeseries"),
                "health_data": existing_report.get("modules", {}).get("health_trajectory", {}),
                "triage_data": existing_report.get("modules", {}).get("page_triage", {}),
            })
        elif module_name == "gameplan":
            modules = existing_report.get("modules", {})
            kwargs.update({
                "health": modules.get("health_trajectory", {}),
                "triage": modules.get("page_triage", {}),
                "serp": modules.get("serp_landscape", {}),
                "content": modules.get("content_intelligence", {}),
                "top_landing_pages": modules.get("top_landing_pages", {}),
                "algorithm": modules.get("algorithm_impact", {}),
                "intent": modules.get("query_intent_migration", {}),
                "link_graph": modules.get("link_graph", {}),
                "technical": modules.get("technical_health", {}),
                "conversion": modules.get("conversion_path", {}),
                "forecasting": modules.get("forecasting", {}),
            })
        
        result = await module_func(**kwargs)
        
        # Update the report in the data store
        await self.data_store.update_report_module(report_id, module_name, result)
        
        logger.info(f"Module {module_name} regenerated successfully")
        return result
