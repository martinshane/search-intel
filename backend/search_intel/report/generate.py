"""Report generation orchestrator."""

import logging
from datetime import datetime
from typing import Dict, Any, Optional
import json

from ..data.gsc_client import GSCClient
from ..data.ga4_client import GA4Client
from ..data.dataforseo_client import DataForSEOClient
from ..analysis.health_trajectory import analyze_health_trajectory
from ..analysis.page_triage import analyze_page_triage
from ..analysis.serp_landscape import analyze_serp_landscape
from ..analysis.content_intelligence import analyze_content_intelligence
from ..analysis.gameplan import generate_gameplan
from ..analysis.algorithm_impact import analyze_algorithm_impacts
from ..analysis.intent_migration import analyze_intent_migration
from ..analysis.ctr_modeling import model_contextual_ctr
from ..analysis.site_architecture import analyze_site_architecture
from ..analysis.branded_split import analyze_branded_split

logger = logging.getLogger(__name__)


class ReportGenerator:
    """Orchestrates the full report generation pipeline."""

    def __init__(
        self,
        gsc_client: GSCClient,
        ga4_client: GA4Client,
        dataforseo_client: DataForSEOClient,
        db_connection: Any,
    ):
        self.gsc = gsc_client
        self.ga4 = ga4_client
        self.dataforseo = dataforseo_client
        self.db = db_connection

    async def generate_report(
        self,
        site_url: str,
        gsc_property: str,
        ga4_property: str,
        user_id: str,
        report_id: str,
    ) -> Dict[str, Any]:
        """Generate complete search intelligence report.

        Args:
            site_url: The website URL
            gsc_property: GSC property identifier
            ga4_property: GA4 property ID
            user_id: User identifier
            report_id: Unique report ID

        Returns:
            Complete report data structure
        """
        logger.info(f"Starting report generation for {site_url} (report_id: {report_id})")

        try:
            # Update status
            await self._update_status(report_id, "ingesting_data", 10)

            # Step 1: Data Ingestion
            logger.info("Step 1: Data ingestion")
            data = await self._ingest_data(site_url, gsc_property, ga4_property)

            # Step 2: Module 1 - Health & Trajectory
            await self._update_status(report_id, "analyzing_health", 20)
            logger.info("Step 2: Health & Trajectory analysis")
            health_trajectory = await analyze_health_trajectory(data["gsc_daily"])

            # Step 3: Module 2 - Page-Level Triage
            await self._update_status(report_id, "analyzing_pages", 30)
            logger.info("Step 3: Page-Level Triage")
            page_triage = await analyze_page_triage(
                data["gsc_page_daily"],
                data["ga4_landing_pages"],
                data["gsc_pages"],
            )

            # Step 4: Module 3 - SERP Landscape
            await self._update_status(report_id, "analyzing_serps", 40)
            logger.info("Step 4: SERP Landscape analysis")
            serp_landscape = await analyze_serp_landscape(
                data["serp_data"], data["gsc_queries"]
            )

            # Step 5: Module 4 - Content Intelligence
            await self._update_status(report_id, "analyzing_content", 50)
            logger.info("Step 5: Content Intelligence")
            content_intelligence = await analyze_content_intelligence(
                data["gsc_query_page"], data["page_crawl"], data["ga4_landing_pages"]
            )

            # Step 6: Module 5 - The Gameplan
            await self._update_status(report_id, "generating_gameplan", 60)
            logger.info("Step 6: Generating Gameplan")
            gameplan = await generate_gameplan(
                health_trajectory, page_triage, serp_landscape, content_intelligence
            )

            # Step 7: Module 6 - Algorithm Impact
            await self._update_status(report_id, "analyzing_algorithms", 65)
            logger.info("Step 7: Algorithm Impact analysis")
            algorithm_impact = await analyze_algorithm_impacts(
                data["gsc_daily"], health_trajectory.get("change_points", [])
            )

            # Step 8: Module 7 - Intent Migration
            await self._update_status(report_id, "analyzing_intent", 70)
            logger.info("Step 8: Intent Migration tracking")
            intent_migration = await analyze_intent_migration(data["gsc_query_date"])

            # Step 9: Module 8 - CTR Modeling
            await self._update_status(report_id, "modeling_ctr", 75)
            logger.info("Step 9: CTR Modeling")
            ctr_modeling = await model_contextual_ctr(
                data["serp_data"], data["gsc_queries"]
            )

            # Step 10: Module 9 - Site Architecture
            await self._update_status(report_id, "analyzing_architecture", 80)
            logger.info("Step 10: Site Architecture analysis")
            site_architecture = await analyze_site_architecture(
                data["link_graph"], data["gsc_pages"]
            )

            # Step 11: Module 10 - Branded vs Non-Branded
            await self._update_status(report_id, "analyzing_branded", 85)
            logger.info("Step 11: Branded vs Non-Branded analysis")
            branded_split = await analyze_branded_split(
                data["gsc_queries"], self._extract_brand_terms(site_url)
            )

            # Step 12: Compile final report
            await self._update_status(report_id, "compiling_report", 90)
            logger.info("Step 12: Compiling final report")
            report = self._compile_report(
                site_url=site_url,
                report_id=report_id,
                user_id=user_id,
                health_trajectory=health_trajectory,
                page_triage=page_triage,
                serp_landscape=serp_landscape,
                content_intelligence=content_intelligence,
                gameplan=gameplan,
                algorithm_impact=algorithm_impact,
                intent_migration=intent_migration,
                ctr_modeling=ctr_modeling,
                site_architecture=site_architecture,
                branded_split=branded_split,
            )

            # Step 13: Save to database
            await self._update_status(report_id, "saving", 95)
            logger.info("Step 13: Saving report to database")
            await self._save_report(report_id, report)

            # Step 14: Mark complete
            await self._update_status(report_id, "complete", 100)
            logger.info(f"Report generation complete for {site_url}")

            return report

        except Exception as e:
            logger.error(f"Report generation failed: {str(e)}", exc_info=True)
            await self._update_status(report_id, "failed", 0, error=str(e))
            raise

    async def _ingest_data(
        self, site_url: str, gsc_property: str, ga4_property: str
    ) -> Dict[str, Any]:
        """Ingest all required data from APIs."""
        logger.info("Starting data ingestion")

        # GSC data pulls
        logger.info("Fetching GSC data")
        gsc_daily = await self.gsc.get_daily_performance(gsc_property, months=16)
        gsc_queries = await self.gsc.get_query_performance(gsc_property, months=16)
        gsc_pages = await self.gsc.get_page_performance(gsc_property, months=16)
        gsc_page_daily = await self.gsc.get_page_date_performance(
            gsc_property, months=16
        )
        gsc_query_date = await self.gsc.get_query_date_performance(
            gsc_property, months=16
        )
        gsc_query_page = await self.gsc.get_query_page_performance(
            gsc_property, months=16
        )

        # GA4 data pulls
        logger.info("Fetching GA4 data")
        ga4_landing_pages = await self.ga4.get_landing_page_metrics(
            ga4_property, months=16
        )
        ga4_traffic_sources = await self.ga4.get_traffic_sources(
            ga4_property, months=16
        )

        # DataForSEO SERP data
        logger.info("Fetching SERP data")
        top_keywords = self._select_keywords_for_serp_analysis(gsc_queries, site_url)
        serp_data = await self.dataforseo.get_serp_data(top_keywords)

        # Site crawl (placeholder - would trigger crawl job)
        logger.info("Initiating site crawl")
        link_graph = await self._get_or_create_link_graph(site_url)
        page_crawl = await self._get_or_create_page_crawl(site_url)

        return {
            "gsc_daily": gsc_daily,
            "gsc_queries": gsc_queries,
            "gsc_pages": gsc_pages,
            "gsc_page_daily": gsc_page_daily,
            "gsc_query_date": gsc_query_date,
            "gsc_query_page": gsc_query_page,
            "ga4_landing_pages": ga4_landing_pages,
            "ga4_traffic_sources": ga4_traffic_sources,
            "serp_data": serp_data,
            "link_graph": link_graph,
            "page_crawl": page_crawl,
        }

    def _select_keywords_for_serp_analysis(
        self, gsc_queries: list, site_url: str, limit: int = 100
    ) -> list:
        """Select top keywords for SERP analysis."""
        # Extract brand terms
        brand_terms = self._extract_brand_terms(site_url)

        # Filter out branded queries
        non_branded = [
            q
            for q in gsc_queries
            if not any(brand.lower() in q["query"].lower() for brand in brand_terms)
        ]

        # Sort by impressions
        non_branded.sort(key=lambda x: x.get("impressions", 0), reverse=True)

        # Take top N
        top_queries = non_branded[:limit]

        # Also include queries with significant position changes
        position_movers = [
            q
            for q in gsc_queries
            if abs(q.get("position_change_30d", 0)) > 3 and q.get("impressions", 0) > 100
        ]

        # Combine and deduplicate
        selected = {q["query"]: q for q in top_queries + position_movers}
        return list(selected.values())

    def _extract_brand_terms(self, site_url: str) -> list:
        """Extract brand terms from site URL."""
        # Simple extraction from domain
        domain = site_url.replace("https://", "").replace("http://", "").replace("www.", "")
        domain_parts = domain.split(".")[0].split("-")

        brand_terms = [domain.split(".")[0]]
        brand_terms.extend(domain_parts)

        # Remove common terms
        stop_words = {"com", "net", "org", "io", "co", "uk", "app", "web"}
        brand_terms = [t for t in brand_terms if t.lower() not in stop_words]

        return brand_terms

    async def _get_or_create_link_graph(self, site_url: str) -> Dict[str, Any]:
        """Get existing link graph or trigger new crawl."""
        # Check if we have a recent crawl
        existing = await self.db.fetch_one(
            "SELECT * FROM link_graphs WHERE site_url = $1 ORDER BY created_at DESC LIMIT 1",
            site_url,
        )

        if existing and (datetime.now() - existing["created_at"]).days < 7:
            logger.info("Using existing link graph")
            return json.loads(existing["graph_data"])

        logger.info("Triggering new site crawl for link graph")
        # In production, this would trigger an async crawl job
        # For now, return empty structure
        return {"nodes": [], "edges": [], "status": "pending_crawl"}

    async def _get_or_create_page_crawl(self, site_url: str) -> Dict[str, Any]:
        """Get existing page crawl data or trigger new crawl."""
        existing = await self.db.fetch_one(
            "SELECT * FROM page_crawls WHERE site_url = $1 ORDER BY created_at DESC LIMIT 1",
            site_url,
        )

        if existing and (datetime.now() - existing["created_at"]).days < 7:
            logger.info("Using existing page crawl")
            return json.loads(existing["crawl_data"])

        logger.info("Triggering new page crawl")
        return {"pages": [], "status": "pending_crawl"}

    def _compile_report(
        self,
        site_url: str,
        report_id: str,
        user_id: str,
        health_trajectory: Dict,
        page_triage: Dict,
        serp_landscape: Dict,
        content_intelligence: Dict,
        gameplan: Dict,
        algorithm_impact: Dict,
        intent_migration: Dict,
        ctr_modeling: Dict,
        site_architecture: Dict,
        branded_split: Dict,
    ) -> Dict[str, Any]:
        """Compile all analysis modules into final report structure."""
        return {
            "report_id": report_id,
            "user_id": user_id,
            "site_url": site_url,
            "generated_at": datetime.now().isoformat(),
            "version": "1.0",
            "modules": {
                "health_trajectory": health_trajectory,
                "page_triage": page_triage,
                "serp_landscape": serp_landscape,
                "content_intelligence": content_intelligence,
                "gameplan": gameplan,
                "algorithm_impact": algorithm_impact,
                "intent_migration": intent_migration,
                "ctr_modeling": ctr_modeling,
                "site_architecture": site_architecture,
                "branded_split": branded_split,
            },
            "summary": self._generate_executive_summary(
                health_trajectory,
                page_triage,
                serp_landscape,
                content_intelligence,
                gameplan,
                branded_split,
            ),
        }

    def _generate_executive_summary(
        self,
        health_trajectory: Dict,
        page_triage: Dict,
        serp_landscape: Dict,
        content_intelligence: Dict,
        gameplan: Dict,
        branded_split: Dict,
    ) -> Dict[str, Any]:
        """Generate executive summary metrics."""
        return {
            "overall_health": health_trajectory.get("overall_direction", "unknown"),
            "trend_direction": health_trajectory.get("trend_slope_pct_per_month", 0),
            "total_recoverable_clicks": page_triage.get("summary", {}).get(
                "total_recoverable_clicks_monthly", 0
            ),
            "critical_pages_count": len(
                [p for p in page_triage.get("pages", []) if p.get("bucket") == "critical"]
            ),
            "quick_wins_count": len(gameplan.get("quick_wins", [])),
            "branded_ratio": branded_split.get("branded_ratio", 0),
            "dependency_level": branded_split.get("dependency_level", "unknown"),
            "total_estimated_impact": gameplan.get(
                "total_estimated_monthly_click_recovery", 0
            )
            + gameplan.get("total_estimated_monthly_click_growth", 0),
        }

    async def _update_status(
        self,
        report_id: str,
        status: str,
        progress: int,
        error: Optional[str] = None,
    ) -> None:
        """Update report generation status in database."""
        await self.db.execute(
            "UPDATE reports SET status = $1, progress = $2, error = $3, updated_at = NOW() WHERE id = $4",
            status,
            progress,
            error,
            report_id,
        )

    async def _save_report(self, report_id: str, report_data: Dict[str, Any]) -> None:
        """Save completed report to database."""
        await self.db.execute(
            "UPDATE reports SET report_data = $1, completed_at = NOW() WHERE id = $2",
            json.dumps(report_data),
            report_id,
        )
