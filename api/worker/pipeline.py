"""
Main async job pipeline that orchestrates report generation through 5 stages.

Stages:
1. Ingesting: Fetch data from GSC, GA4, DataForSEO, site crawl
2. Analyzing: Run all 12 analysis modules in sequence
3. Generating: LLM synthesis passes for narrative sections
4. Finalizing: Package complete report JSON
5. Complete: Mark as ready for display

Progress is tracked in the reports table with detailed status per module.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from uuid import UUID

from api.core.database import get_supabase_client
from api.services.gsc_service import GSCService
from api.services.ga4_service import GA4Service
from api.services.dataforseo_service import DataForSEOService
from api.services.crawler_service import CrawlerService
from api.analysis.health_trajectory import analyze_health_trajectory
from api.analysis.page_triage import analyze_page_triage
from api.analysis.serp_landscape import analyze_serp_landscape
from api.analysis.content_intelligence import analyze_content_intelligence
from api.analysis.gameplan import generate_gameplan
from api.analysis.algorithm_impact import analyze_algorithm_impacts
from api.analysis.intent_migration import analyze_intent_migration
from api.analysis.ctr_modeling import model_contextual_ctr
from api.analysis.site_architecture import analyze_site_architecture
from api.analysis.branded_split import analyze_branded_split
from api.analysis.competitive_threats import analyze_competitive_threats
from api.analysis.revenue_attribution import estimate_revenue_attribution

logger = logging.getLogger(__name__)


class ReportPipeline:
    """Orchestrates the full report generation pipeline."""
    
    def __init__(self, report_id: UUID, user_id: UUID, gsc_property: str, ga4_property: Optional[str] = None):
        self.report_id = report_id
        self.user_id = user_id
        self.gsc_property = gsc_property
        self.ga4_property = ga4_property
        self.supabase = get_supabase_client()
        
        # Services
        self.gsc_service: Optional[GSCService] = None
        self.ga4_service: Optional[GA4Service] = None
        self.dataforseo_service: Optional[DataForSEOService] = None
        self.crawler_service: Optional[CrawlerService] = None
        
        # Shared data store for analysis modules
        self.data_store: Dict[str, Any] = {}
        
        # Analysis results
        self.results: Dict[str, Any] = {}
    
    async def run(self) -> Dict[str, Any]:
        """
        Execute the full pipeline.
        
        Returns:
            Complete report data
        """
        try:
            logger.info(f"Starting pipeline for report {self.report_id}")
            
            # Initialize services
            await self._initialize_services()
            
            # Stage 1: Data ingestion
            await self._update_status("ingesting", {"stage": "ingesting", "progress": 0})
            await self._stage_ingestion()
            
            # Stage 2: Analysis
            await self._update_status("analyzing", {"stage": "analyzing", "progress": 0})
            await self._stage_analysis()
            
            # Stage 3: Generation (LLM synthesis)
            await self._update_status("generating", {"stage": "generating", "progress": 0})
            await self._stage_generation()
            
            # Stage 4: Finalization
            await self._update_status("finalizing", {"stage": "finalizing", "progress": 95})
            report_data = await self._stage_finalization()
            
            # Stage 5: Complete
            await self._update_status("complete", {"stage": "complete", "progress": 100})
            await self._mark_complete(report_data)
            
            logger.info(f"Pipeline completed for report {self.report_id}")
            return report_data
            
        except Exception as e:
            logger.error(f"Pipeline failed for report {self.report_id}: {str(e)}", exc_info=True)
            await self._update_status("failed", {"stage": "failed", "error": str(e)})
            raise
    
    async def _initialize_services(self):
        """Initialize API service clients with OAuth tokens."""
        try:
            # Fetch user tokens from database
            result = self.supabase.table("users").select("gsc_token, ga4_token").eq("id", str(self.user_id)).single().execute()
            
            if not result.data:
                raise ValueError(f"User {self.user_id} not found")
            
            user = result.data
            
            # Initialize GSC service
            if user.get("gsc_token"):
                self.gsc_service = GSCService(user["gsc_token"])
            else:
                raise ValueError("GSC token not found for user")
            
            # Initialize GA4 service (optional)
            if self.ga4_property and user.get("ga4_token"):
                self.ga4_service = GA4Service(user["ga4_token"])
            
            # Initialize DataForSEO service
            self.dataforseo_service = DataForSEOService()
            
            # Initialize crawler service
            self.crawler_service = CrawlerService()
            
            logger.info(f"Services initialized for report {self.report_id}")
            
        except Exception as e:
            logger.error(f"Failed to initialize services: {str(e)}")
            raise
    
    async def _stage_ingestion(self):
        """
        Stage 1: Fetch all required data.
        
        Pulls:
        - GSC data (16 months: by query, page, date, query+page, query+date, page+date)
        - GA4 data (engagement, landing pages, channels, conversions)
        - DataForSEO SERP data (top 50-100 non-branded keywords)
        - Site crawl (internal link graph)
        """
        try:
            logger.info(f"Starting ingestion stage for report {self.report_id}")
            
            # Progress checkpoints
            total_steps = 8
            current_step = 0
            
            def update_progress():
                nonlocal current_step
                current_step += 1
                progress = int((current_step / total_steps) * 25)  # Ingestion is 0-25%
                asyncio.create_task(self._update_progress({"stage": "ingesting", "progress": progress}))
            
            # 1. Fetch GSC data
            logger.info("Fetching GSC data...")
            gsc_data = await self.gsc_service.fetch_comprehensive_data(self.gsc_property)
            self.data_store["gsc"] = gsc_data
            update_progress()
            
            # 2. Fetch GA4 data (if available)
            if self.ga4_service:
                logger.info("Fetching GA4 data...")
                ga4_data = await self.ga4_service.fetch_comprehensive_data(self.ga4_property)
                self.data_store["ga4"] = ga4_data
            else:
                logger.info("GA4 not configured, skipping")
                self.data_store["ga4"] = None
            update_progress()
            
            # 3. Identify top non-branded keywords for SERP analysis
            logger.info("Identifying top keywords for SERP analysis...")
            top_keywords = await self._identify_top_keywords(gsc_data)
            self.data_store["top_keywords"] = top_keywords
            update_progress()
            
            # 4. Fetch live SERP data
            logger.info(f"Fetching SERP data for {len(top_keywords)} keywords...")
            serp_data = await self.dataforseo_service.fetch_serp_data(top_keywords)
            self.data_store["serp"] = serp_data
            update_progress()
            
            # 5. Perform site crawl
            logger.info("Starting site crawl...")
            crawl_data = await self.crawler_service.crawl_site(self.gsc_property)
            self.data_store["crawl"] = crawl_data
            update_progress()
            
            # 6. Fetch algorithm update history
            logger.info("Fetching algorithm update history...")
            algorithm_updates = await self._fetch_algorithm_updates()
            self.data_store["algorithm_updates"] = algorithm_updates
            update_progress()
            
            # 7. Load intent classification cache
            logger.info("Loading intent classification cache...")
            intent_cache = await self._load_intent_cache()
            self.data_store["intent_cache"] = intent_cache
            update_progress()
            
            # 8. Load SERP history (if available)
            logger.info("Loading SERP history...")
            serp_history = await self._load_serp_history(top_keywords)
            self.data_store["serp_history"] = serp_history
            update_progress()
            
            logger.info(f"Ingestion stage complete for report {self.report_id}")
            
        except Exception as e:
            logger.error(f"Ingestion stage failed: {str(e)}")
            raise
    
    async def _stage_analysis(self):
        """
        Stage 2: Run all 12 analysis modules in sequence.
        
        Each module reads from data_store and writes to results.
        Progress updates after each module (25-75%).
        """
        try:
            logger.info(f"Starting analysis stage for report {self.report_id}")
            
            modules = [
                ("module_1_health", analyze_health_trajectory, ["gsc"]),
                ("module_2_triage", analyze_page_triage, ["gsc", "ga4"]),
                ("module_3_serp", analyze_serp_landscape, ["serp", "gsc"]),
                ("module_4_content", analyze_content_intelligence, ["gsc", "crawl", "ga4"]),
                ("module_6_algorithm", analyze_algorithm_impacts, ["gsc", "algorithm_updates", "module_1_health"]),
                ("module_7_intent", analyze_intent_migration, ["gsc", "intent_cache"]),
                ("module_8_ctr", model_contextual_ctr, ["serp", "gsc"]),
                ("module_9_architecture", analyze_site_architecture, ["crawl", "gsc"]),
                ("module_10_branded", analyze_branded_split, ["gsc"]),
                ("module_11_competitive", analyze_competitive_threats, ["serp", "gsc", "serp_history"]),
                ("module_12_revenue", estimate_revenue_attribution, ["gsc", "ga4"]),
                ("module_5_gameplan", generate_gameplan, [
                    "module_1_health", "module_2_triage", "module_3_serp", "module_4_content"
                ]),
            ]
            
            total_modules = len(modules)
            
            for i, (module_name, module_func, dependencies) in enumerate(modules):
                try:
                    logger.info(f"Running {module_name}...")
                    
                    # Prepare module inputs
                    module_inputs = {}
                    for dep in dependencies:
                        if dep.startswith("module_"):
                            # Dependency is a prior module result
                            module_inputs[dep] = self.results.get(dep, {})
                        else:
                            # Dependency is data from ingestion
                            module_inputs[dep] = self.data_store.get(dep)
                    
                    # Run module
                    module_result = await self._run_module_safely(module_func, module_inputs)
                    self.results[module_name] = module_result
                    
                    # Update progress (25-75%)
                    progress = 25 + int((i + 1) / total_modules * 50)
                    await self._update_progress({
                        "stage": "analyzing",
                        "progress": progress,
                        "current_module": module_name
                    })
                    
                    logger.info(f"{module_name} complete")
                    
                except Exception as e:
                    logger.error(f"Module {module_name} failed: {str(e)}", exc_info=True)
                    # Store error in results but continue pipeline
                    self.results[module_name] = {
                        "error": str(e),
                        "status": "failed"
                    }
            
            logger.info(f"Analysis stage complete for report {self.report_id}")
            
        except Exception as e:
            logger.error(f"Analysis stage failed: {str(e)}")
            raise
    
    async def _stage_generation(self):
        """
        Stage 3: LLM synthesis passes for narrative sections.
        
        Uses Claude API to generate human-readable narratives for key sections.
        Progress: 75-90%
        """
        try:
            logger.info(f"Starting generation stage for report {self.report_id}")
            
            # Modules that need narrative generation
            narrative_modules = [
                "module_1_health",
                "module_5_gameplan",
                "module_6_algorithm",
                "module_10_branded"
            ]
            
            total = len(narrative_modules)
            
            for i, module_name in enumerate(narrative_modules):
                try:
                    if module_name in self.results and "error" not in self.results[module_name]:
                        logger.info(f"Generating narrative for {module_name}...")
                        
                        # Generate narrative using Claude
                        narrative = await self._generate_narrative(module_name, self.results[module_name])
                        
                        # Add narrative to results
                        self.results[module_name]["narrative"] = narrative
                        
                        # Update progress (75-90%)
                        progress = 75 + int((i + 1) / total * 15)
                        await self._update_progress({
                            "stage": "generating",
                            "progress": progress,
                            "current_module": module_name
                        })
                    
                except Exception as e:
                    logger.error(f"Narrative generation failed for {module_name}: {str(e)}")
                    # Continue even if narrative generation fails
            
            logger.info(f"Generation stage complete for report {self.report_id}")
            
        except Exception as e:
            logger.error(f"Generation stage failed: {str(e)}")
            raise
    
    async def _stage_finalization(self) -> Dict[str, Any]:
        """
        Stage 4: Package complete report JSON.
        
        Combines all module results into final report structure.
        Progress: 90-95%
        """
        try:
            logger.info(f"Starting finalization stage for report {self.report_id}")
            
            report_data = {
                "report_id": str(self.report_id),
                "user_id": str(self.user_id),
                "gsc_property": self.gsc_property,
                "ga4_property": self.ga4_property,
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "version": "1.0",
                "modules": self.results,
                "metadata": {
                    "total_keywords_analyzed": len(self.data_store.get("top_keywords", [])),
                    "date_range_months": 16,
                    "has_ga4_data": self.data_store.get("ga4") is not None,
                    "crawl_pages": len(self.data_store.get("crawl", {}).get("pages", [])),
                }
            }
            
            logger.info(f"Finalization stage complete for report {self.report_id}")
            return report_data
            
        except Exception as e:
            logger.error(f"Finalization stage failed: {str(e)}")
            raise
    
    async def _mark_complete(self, report_data: Dict[str, Any]):
        """Mark report as complete and store final data."""
        try:
            self.supabase.table("reports").update({
                "status": "complete",
                "report_data": report_data,
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "progress": {"stage": "complete", "progress": 100}
            }).eq("id", str(self.report_id)).execute()
            
            logger.info(f"Report {self.report_id} marked as complete")
            
        except Exception as e:
            logger.error(f"Failed to mark report complete: {str(e)}")
            raise
    
    async def _update_status(self, status: str, progress: Dict[str, Any]):
        """Update report status and progress in database."""
        try:
            self.supabase.table("reports").update({
                "status": status,
                "progress": progress
            }).eq("id", str(self.report_id)).execute()
            
        except Exception as e:
            logger.error(f"Failed to update status: {str(e)}")
            # Don't raise - status updates are non-critical
    
    async def _update_progress(self, progress: Dict[str, Any]):
        """Update just the progress field."""
        try:
            self.supabase.table("reports").update({
                "progress": progress
            }).eq("id", str(self.report_id)).execute()
            
        except Exception as e:
            logger.error(f"Failed to update progress: {str(e)}")
    
    async def _identify_top_keywords(self, gsc_data: Dict[str, Any]) -> list[str]:
        """
        Identify top N non-branded keywords for SERP analysis.
        
        Logic:
        - Filter out branded queries
        - Sort by impressions DESC
        - Take top 50-100
        - Include any with significant position changes
        """
        try:
            from api.utils.brand_detection import filter_branded_queries
            
            all_queries = gsc_data.get("queries", [])
            
            # Filter branded
            non_branded = filter_branded_queries(all_queries, self.gsc_property)
            
            # Sort by impressions
            non_branded.sort(key=lambda x: x.get("impressions", 0), reverse=True)
            
            # Take top 100
            top_keywords = [q["query"] for q in non_branded[:100]]
            
            # Add queries with significant position changes
            for query in non_branded[100:500]:  # Check next 400
                if query.get("position_change_30d", 0) > 3:
                    top_keywords.append(query["query"])
                    if len(top_keywords) >= 150:
                        break
            
            logger.info(f"Identified {len(top_keywords)} top keywords for SERP analysis")
            return top_keywords
            
        except Exception as e:
            logger.error(f"Failed to identify top keywords: {str(e)}")
            return []
    
    async def _fetch_algorithm_updates(self) -> list[Dict[str, Any]]:
        """Fetch algorithm update history from database."""
        try:
            result = self.supabase.table("algorithm_updates").select("*").order("date", desc=True).limit(50).execute()
            return result.data or []
        except Exception as e:
            logger.error(f"Failed to fetch algorithm updates: {str(e)}")
            return []
    
    async def _load_intent_cache(self) -> Dict[str, str]:
        """Load cached query intent classifications."""
        try:
            result = self.supabase.table("query_intents").select("query, intent").execute()
            return {row["query"]: row["intent"] for row in (result.data or [])}
        except Exception as e:
            logger.error(f"Failed to load intent cache: {str(e)}")
            return {}
    
    async def _load_serp_history(self, keywords: list[str]) -> Dict[str, list]:
        """Load historical SERP snapshots for keywords."""
        try:
            result = self.supabase.table("serp_snapshots").select("*").in_("keyword", keywords).order("snapshot_date", desc=True).execute()
            
            # Organize by keyword
            history = {}
            for row in (result.data or []):
                keyword = row["keyword"]
                if keyword not in history:
                    history[keyword] = []
                history[keyword].append(row)
            
            return history
        except Exception as e:
            logger.error(f"Failed to load SERP history: {str(e)}")
            return {}
    
    async def _run_module_safely(self, module_func, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Run an analysis module with error handling.
        
        If module is async, await it. Otherwise run in executor.
        """
        try:
            if asyncio.iscoroutinefunction(module_func):
                result = await module_func(**inputs)
            else:
                # Run CPU-bound analysis in thread pool
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(None, lambda: module_func(**inputs))
            
            return result
            
        except Exception as e:
            logger.error(f"Module {module_func.__name__} failed: {str(e)}", exc_info=True)
            raise
    
    async def _generate_narrative(self, module_name: str, module_data: Dict[str, Any]) -> str:
        """
        Generate human-readable narrative for a module using Claude API.
        
        Prompts are customized per module to generate consultant-grade prose.
        """
        try:
            from api.services.llm_service import LLMService
            
            llm_service = LLMService()
            
            prompts = {
                "module_1_health": self._get_health_narrative_prompt(module_data),
                "module_5_gameplan": self._get_gameplan_narrative_prompt(module_data),
                "module_6_algorithm": self._get_algorithm_narrative_prompt(module_data),
                "module_10_branded": self._get_branded_narrative_prompt(module_data)
            }
            
            prompt = prompts.get(module_name)
            if not prompt:
                return ""
            
            narrative = await llm_service.generate_narrative(prompt, module_data)
            return narrative
            
        except Exception as e:
            logger.error(f"Failed to generate narrative for {module_name}: {str(e)}")
            return ""
    
    def _get_health_narrative_prompt(self, data: Dict[str, Any]) -> str:
        """Generate prompt for health & trajectory narrative."""
        return f"""
You are a search marketing consultant analyzing a website's traffic health and trajectory.

Based on this analysis data, write a concise, direct assessment (2-3 paragraphs):

Overall Direction: {data.get('overall_direction')}
Trend Slope: {data.get('trend_slope_pct_per_month')}% per month
Change Points: {len(data.get('change_points', []))} detected
Forecast (90d): {data.get('forecast', {}).get('90d', {}).get('clicks')} clicks

Guidelines:
- Start with the bottom line (growing/declining/stable)
- Explain what's driving the trend
- Mention any significant change points or anomalies
- Give the 90-day forecast with confidence level
- Be direct and actionable - no fluff
"""
    
    def _get_gameplan_narrative_prompt(self, data: Dict[str, Any]) -> str:
        """Generate prompt for