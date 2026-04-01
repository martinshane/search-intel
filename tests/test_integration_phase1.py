"""
Integration tests for Phase 1 modules with real GSC/GA4 data.

Tests the complete pipeline:
1. Data ingestion (GSC + GA4)
2. Module 1: Health & Trajectory
3. Module 2: Page-Level Triage
4. Module 5: The Gameplan
5. Schema validation
6. Report rendering
"""

import pytest
import asyncio
import os
from datetime import datetime, timedelta
from typing import Dict, Any
import pandas as pd

from src.data_ingestion.gsc_client import GSCClient
from src.data_ingestion.ga4_client import GA4Client
from src.analysis.module1_health import analyze_health_trajectory
from src.analysis.module2_triage import analyze_page_triage
from src.analysis.module5_gameplan import generate_gameplan
from src.report.renderer import ReportRenderer
from src.report.schemas import ReportSchema


# Test configuration - use real credentials from environment
TEST_GSC_PROPERTY = os.getenv("TEST_GSC_PROPERTY", "sc-domain:kixie.com")
TEST_GA4_PROPERTY = os.getenv("TEST_GA4_PROPERTY", "properties/YOUR_GA4_PROPERTY_ID")


@pytest.fixture
def gsc_client():
    """Initialize GSC client with OAuth credentials."""
    return GSCClient()


@pytest.fixture
def ga4_client():
    """Initialize GA4 client with OAuth credentials."""
    return GA4Client()


@pytest.fixture
def date_range():
    """16-month date range for testing."""
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=480)  # ~16 months
    return start_date, end_date


class TestDataIngestion:
    """Test suite for data ingestion layer with real API calls."""
    
    @pytest.mark.asyncio
    async def test_gsc_data_fetch(self, gsc_client, date_range):
        """Test GSC data fetching for all required dimensions."""
        start_date, end_date = date_range
        
        # Test daily time series
        daily_data = await gsc_client.fetch_daily_timeseries(
            property_url=TEST_GSC_PROPERTY,
            start_date=start_date,
            end_date=end_date
        )
        
        assert daily_data is not None
        assert len(daily_data) > 0
        assert "date" in daily_data.columns
        assert "clicks" in daily_data.columns
        assert "impressions" in daily_data.columns
        assert "ctr" in daily_data.columns
        assert "position" in daily_data.columns
        
        # Verify date range coverage
        date_range_days = (end_date - start_date).days
        assert len(daily_data) >= date_range_days * 0.8  # Allow some missing days
        
        # Test per-page data
        page_data = await gsc_client.fetch_page_performance(
            property_url=TEST_GSC_PROPERTY,
            start_date=start_date,
            end_date=end_date
        )
        
        assert page_data is not None
        assert len(page_data) > 0
        assert "page" in page_data.columns
        assert "clicks" in page_data.columns
        
        # Test per-query data
        query_data = await gsc_client.fetch_query_performance(
            property_url=TEST_GSC_PROPERTY,
            start_date=start_date,
            end_date=end_date
        )
        
        assert query_data is not None
        assert len(query_data) > 0
        assert "query" in query_data.columns
        assert "clicks" in query_data.columns
        
        # Test query-page mapping
        query_page_data = await gsc_client.fetch_query_page_mapping(
            property_url=TEST_GSC_PROPERTY,
            start_date=start_date,
            end_date=end_date
        )
        
        assert query_page_data is not None
        assert len(query_page_data) > 0
        assert "query" in query_page_data.columns
        assert "page" in query_page_data.columns
        
        # Test page daily time series
        page_daily_data = await gsc_client.fetch_page_daily_timeseries(
            property_url=TEST_GSC_PROPERTY,
            start_date=start_date,
            end_date=end_date
        )
        
        assert page_daily_data is not None
        assert len(page_daily_data) > 0
        assert "page" in page_daily_data.columns
        assert "date" in page_daily_data.columns
        assert "clicks" in page_daily_data.columns
    
    @pytest.mark.asyncio
    async def test_ga4_data_fetch(self, ga4_client, date_range):
        """Test GA4 data fetching for engagement metrics."""
        start_date, end_date = date_range
        
        # Test landing page engagement
        landing_data = await ga4_client.fetch_landing_page_engagement(
            property_id=TEST_GA4_PROPERTY,
            start_date=start_date,
            end_date=end_date
        )
        
        assert landing_data is not None
        assert len(landing_data) > 0
        assert "landingPage" in landing_data.columns
        assert "sessions" in landing_data.columns
        assert "bounceRate" in landing_data.columns
        assert "avgSessionDuration" in landing_data.columns
        
        # Test conversion data if available
        conversion_data = await ga4_client.fetch_conversions(
            property_id=TEST_GA4_PROPERTY,
            start_date=start_date,
            end_date=end_date
        )
        
        assert conversion_data is not None
        # Note: Conversion data may be empty for test properties
        if len(conversion_data) > 0:
            assert "eventName" in conversion_data.columns
            assert "conversions" in conversion_data.columns


class TestModule1Health:
    """Test suite for Module 1: Health & Trajectory Analysis."""
    
    @pytest.mark.asyncio
    async def test_health_trajectory_analysis(self, gsc_client, date_range):
        """Test complete health trajectory analysis with real data."""
        start_date, end_date = date_range
        
        # Fetch real GSC data
        daily_data = await gsc_client.fetch_daily_timeseries(
            property_url=TEST_GSC_PROPERTY,
            start_date=start_date,
            end_date=end_date
        )
        
        # Run Module 1 analysis
        result = analyze_health_trajectory(daily_data)
        
        # Validate schema
        assert "overall_direction" in result
        assert result["overall_direction"] in [
            "strong_growth", "growth", "flat", "decline", "strong_decline"
        ]
        
        assert "trend_slope_pct_per_month" in result
        assert isinstance(result["trend_slope_pct_per_month"], (int, float))
        
        assert "change_points" in result
        assert isinstance(result["change_points"], list)
        for cp in result["change_points"]:
            assert "date" in cp
            assert "magnitude" in cp
            assert "direction" in cp
        
        assert "seasonality" in result
        assert "best_day" in result["seasonality"]
        assert "worst_day" in result["seasonality"]
        assert "monthly_cycle" in result["seasonality"]
        
        assert "anomalies" in result
        assert isinstance(result["anomalies"], list)
        
        assert "forecast" in result
        assert "30d" in result["forecast"]
        assert "60d" in result["forecast"]
        assert "90d" in result["forecast"]
        
        for period in ["30d", "60d", "90d"]:
            forecast = result["forecast"][period]
            assert "clicks" in forecast
            assert "ci_low" in forecast
            assert "ci_high" in forecast
            assert forecast["ci_low"] <= forecast["clicks"] <= forecast["ci_high"]
    
    def test_mstl_decomposition(self, gsc_client, date_range):
        """Test MSTL decomposition produces valid components."""
        # This would be tested as part of the health trajectory
        # but we can test the decomposition logic separately
        pass


class TestModule2Triage:
    """Test suite for Module 2: Page-Level Triage."""
    
    @pytest.mark.asyncio
    async def test_page_triage_analysis(self, gsc_client, ga4_client, date_range):
        """Test complete page triage analysis with real data."""
        start_date, end_date = date_range
        
        # Fetch required data
        page_daily_data = await gsc_client.fetch_page_daily_timeseries(
            property_url=TEST_GSC_PROPERTY,
            start_date=start_date,
            end_date=end_date
        )
        
        ga4_landing_data = await ga4_client.fetch_landing_page_engagement(
            property_id=TEST_GA4_PROPERTY,
            start_date=start_date,
            end_date=end_date
        )
        
        gsc_page_summary = await gsc_client.fetch_page_performance(
            property_url=TEST_GSC_PROPERTY,
            start_date=start_date,
            end_date=end_date
        )
        
        # Run Module 2 analysis
        result = analyze_page_triage(
            page_daily_data=page_daily_data,
            ga4_landing_data=ga4_landing_data,
            gsc_page_summary=gsc_page_summary
        )
        
        # Validate schema
        assert "pages" in result
        assert isinstance(result["pages"], list)
        
        for page in result["pages"]:
            assert "url" in page
            assert "bucket" in page
            assert page["bucket"] in ["growing", "stable", "decaying", "critical"]
            assert "current_monthly_clicks" in page
            assert "trend_slope" in page
            assert "priority_score" in page
            assert "recommended_action" in page
            
            # Optional fields
            if "ctr_anomaly" in page and page["ctr_anomaly"]:
                assert "ctr_expected" in page
                assert "ctr_actual" in page
        
        assert "summary" in result
        summary = result["summary"]
        assert "total_pages_analyzed" in summary
        assert "growing" in summary
        assert "stable" in summary
        assert "decaying" in summary
        assert "critical" in summary
        assert "total_recoverable_clicks_monthly" in summary
        
        # Validate totals add up
        total_buckets = (
            summary["growing"] + summary["stable"] + 
            summary["decaying"] + summary["critical"]
        )
        assert total_buckets == summary["total_pages_analyzed"]
    
    def test_ctr_anomaly_detection(self):
        """Test CTR anomaly detection using PyOD Isolation Forest."""
        # Test with synthetic data to verify the algorithm works
        pass
    
    def test_engagement_cross_reference(self):
        """Test GA4 engagement cross-referencing logic."""
        pass


class TestModule5Gameplan:
    """Test suite for Module 5: The Gameplan synthesis."""
    
    @pytest.mark.asyncio
    async def test_gameplan_generation(
        self, gsc_client, ga4_client, date_range
    ):
        """Test complete gameplan generation with real data."""
        start_date, end_date = date_range
        
        # Fetch all required data
        daily_data = await gsc_client.fetch_daily_timeseries(
            property_url=TEST_GSC_PROPERTY,
            start_date=start_date,
            end_date=end_date
        )
        
        page_daily_data = await gsc_client.fetch_page_daily_timeseries(
            property_url=TEST_GSC_PROPERTY,
            start_date=start_date,
            end_date=end_date
        )
        
        ga4_landing_data = await ga4_client.fetch_landing_page_engagement(
            property_id=TEST_GA4_PROPERTY,
            start_date=start_date,
            end_date=end_date
        )
        
        gsc_page_summary = await gsc_client.fetch_page_performance(
            property_url=TEST_GSC_PROPERTY,
            start_date=start_date,
            end_date=end_date
        )
        
        # Run prerequisite modules
        health_result = analyze_health_trajectory(daily_data)
        triage_result = analyze_page_triage(
            page_daily_data=page_daily_data,
            ga4_landing_data=ga4_landing_data,
            gsc_page_summary=gsc_page_summary
        )
        
        # Run Module 5
        result = generate_gameplan(
            health=health_result,
            triage=triage_result,
            serp=None,  # Not available in Phase 1
            content=None  # Not available in Phase 1
        )
        
        # Validate schema
        assert "critical" in result
        assert isinstance(result["critical"], list)
        
        assert "quick_wins" in result
        assert isinstance(result["quick_wins"], list)
        
        assert "strategic" in result
        assert isinstance(result["strategic"], list)
        
        assert "structural" in result
        assert isinstance(result["structural"], list)
        
        # Validate action items have required fields
        for action_list in [
            result["critical"], result["quick_wins"],
            result["strategic"], result["structural"]
        ]:
            for action in action_list:
                assert "action" in action
                assert "impact" in action
                assert "effort" in action
                assert action["effort"] in ["low", "medium", "high"]
                assert isinstance(action["impact"], (int, float))
        
        assert "total_estimated_monthly_click_recovery" in result
        assert "total_estimated_monthly_click_growth" in result
        
        assert "narrative" in result
        assert isinstance(result["narrative"], str)
        assert len(result["narrative"]) > 100  # Should be substantial


class TestSchemaValidation:
    """Test suite for schema validation across all modules."""
    
    def test_report_schema_validation(self):
        """Test that generated report conforms to ReportSchema."""
        # Create a sample report structure
        sample_report = {
            "meta": {
                "gsc_property": TEST_GSC_PROPERTY,
                "ga4_property": TEST_GA4_PROPERTY,
                "date_range": {
                    "start": "2024-10-01",
                    "end": "2026-02-01"
                },
                "generated_at": datetime.now().isoformat()
            },
            "module1_health": {
                "overall_direction": "growth",
                "trend_slope_pct_per_month": 2.5,
                "change_points": [],
                "seasonality": {
                    "best_day": "Tuesday",
                    "worst_day": "Sunday",
                    "monthly_cycle": True
                },
                "anomalies": [],
                "forecast": {
                    "30d": {"clicks": 10000, "ci_low": 9000, "ci_high": 11000},
                    "60d": {"clicks": 11000, "ci_low": 9500, "ci_high": 12500},
                    "90d": {"clicks": 12000, "ci_low": 10000, "ci_high": 14000}
                }
            },
            "module2_triage": {
                "pages": [],
                "summary": {
                    "total_pages_analyzed": 100,
                    "growing": 20,
                    "stable": 50,
                    "decaying": 25,
                    "critical": 5,
                    "total_recoverable_clicks_monthly": 1000
                }
            },
            "module5_gameplan": {
                "critical": [],
                "quick_wins": [],
                "strategic": [],
                "structural": [],
                "total_estimated_monthly_click_recovery": 1000,
                "total_estimated_monthly_click_growth": 2000,
                "narrative": "Test narrative"
            }
        }
        
        # Validate against schema
        is_valid, errors = ReportSchema.validate(sample_report)
        
        assert is_valid, f"Schema validation failed: {errors}"
    
    def test_module1_output_schema(self, gsc_client, date_range):
        """Test Module 1 output matches expected schema."""
        # Would validate the exact structure
        pass


class TestReportRendering:
    """Test suite for report rendering and output generation."""
    
    @pytest.mark.asyncio
    async def test_html_report_generation(
        self, gsc_client, ga4_client, date_range
    ):
        """Test HTML report renders correctly with real data."""
        start_date, end_date = date_range
        
        # Generate complete report data
        daily_data = await gsc_client.fetch_daily_timeseries(
            property_url=TEST_GSC_PROPERTY,
            start_date=start_date,
            end_date=end_date
        )
        
        page_daily_data = await gsc_client.fetch_page_daily_timeseries(
            property_url=TEST_GSC_PROPERTY,
            start_date=start_date,
            end_date=end_date
        )
        
        ga4_landing_data = await ga4_client.fetch_landing_page_engagement(
            property_id=TEST_GA4_PROPERTY,
            start_date=start_date,
            end_date=end_date
        )
        
        gsc_page_summary = await gsc_client.fetch_page_performance(
            property_url=TEST_GSC_PROPERTY,
            start_date=start_date,
            end_date=end_date
        )
        
        # Run all modules
        health_result = analyze_health_trajectory(daily_data)
        triage_result = analyze_page_triage(
            page_daily_data=page_daily_data,
            ga4_landing_data=ga4_landing_data,
            gsc_page_summary=gsc_page_summary
        )
        gameplan_result = generate_gameplan(
            health=health_result,
            triage=triage_result,
            serp=None,
            content=None
        )
        
        # Build complete report
        report_data = {
            "meta": {
                "gsc_property": TEST_GSC_PROPERTY,
                "ga4_property": TEST_GA4_PROPERTY,
                "date_range": {
                    "start": start_date.isoformat(),
                    "end": end_date.isoformat()
                },
                "generated_at": datetime.now().isoformat()
            },
            "module1_health": health_result,
            "module2_triage": triage_result,
            "module5_gameplan": gameplan_result
        }
        
        # Render to HTML
        renderer = ReportRenderer()
        html_output = renderer.render_html(report_data)
        
        # Validate HTML output
        assert html_output is not None
        assert len(html_output) > 1000  # Should be substantial
        assert "<!DOCTYPE html>" in html_output or "<html" in html_output
        assert TEST_GSC_PROPERTY in html_output
        
        # Check for key sections
        assert "Health & Trajectory" in html_output
        assert "Page-Level Triage" in html_output
        assert "Gameplan" in html_output or "The Gameplan" in html_output
        
        # Verify data is rendered
        assert str(health_result["overall_direction"]) in html_output
        assert str(triage_result["summary"]["total_pages_analyzed"]) in html_output
    
    def test_json_report_export(self):
        """Test JSON export is valid and complete."""
        pass
    
    def test_chart_data_generation(self):
        """Test chart data structures are correct for frontend rendering."""
        pass


class TestEndToEnd:
    """End-to-end integration test of the complete pipeline."""
    
    @pytest.mark.asyncio
    @pytest.mark.slow
    async def test_complete_pipeline_kixie(self):
        """
        Complete end-to-end test with kixie.com data.
        
        This test runs the entire pipeline:
        1. OAuth authentication (requires valid tokens)
        2. Data ingestion from GSC and GA4
        3. All Phase 1 analysis modules
        4. Report generation
        5. HTML rendering
        """
        # Initialize clients
        gsc_client = GSCClient()
        ga4_client = GA4Client()
        
        # Define date range
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=480)
        
        print(f"\n🚀 Starting complete pipeline test for {TEST_GSC_PROPERTY}")
        print(f"📅 Date range: {start_date} to {end_date}")
        
        # Step 1: Data Ingestion
        print("\n📥 Step 1: Data Ingestion")
        daily_data = await gsc_client.fetch_daily_timeseries(
            property_url=TEST_GSC_PROPERTY,
            start_date=start_date,
            end_date=end_date
        )
        print(f"  ✓ Fetched {len(daily_data)} days of GSC data")
        
        page_daily_data = await gsc_client.fetch_page_daily_timeseries(
            property_url=TEST_GSC_PROPERTY,
            start_date=start_date,
            end_date=end_date
        )
        print(f"  ✓ Fetched page-level daily data ({len(page_daily_data)} rows)")
        
        gsc_page_summary = await gsc_client.fetch_page_performance(
            property_url=TEST_GSC_PROPERTY,
            start_date=start_date,
            end_date=end_date
        )
        print(f"  ✓ Fetched page summary ({len(gsc_page_summary)} pages)")
        
        ga4_landing_data = await ga4_client.fetch_landing_page_engagement(
            property_id=TEST_GA4_PROPERTY,
            start_date=start_date,
            end_date=end_date
        )
        print(f"  ✓ Fetched GA4 landing page data ({len(ga4_landing_data)} pages)")
        
        # Step 2: Module 1 Analysis
        print("\n📊 Step 2: Health & Trajectory Analysis")
        health_result = analyze_health_trajectory(daily_data)
        print(f"  ✓ Overall direction: {health_result['overall_direction']}")
        print(f"  ✓ Trend: {health_result['trend_slope_pct_per_month']:.2f}% per month")
        print(f"  ✓ Change points detected: {len(health_result['change_points'])}")
        print(f"  ✓ 30-day forecast: {health_result['forecast']['30d']['clicks']} clicks")
        
        # Step 3: Module 2 Analysis
        print("\n📋 Step 3: Page Triage Analysis")
        triage_result = analyze_page_triage(gsc_page_summary, ga4_landing_data)
        print(f"  ✓ Pages analyzed: {triage_result['total_pages']}")
        print(f"  ✓ Priority pages: {len(triage_result['priority_pages'])}")
        
        # Step 4: Module 5 Analysis
        print("\n🎯 Step 4: Game Plan Generation")
        gameplan_result = generate_gameplan(
            health_result=health_result,
            triage_result=triage_result
        )
        print(f"  ✓ Recommendations generated: {len(gameplan_result['recommendations'])}")
        print(f"  ✓ Top priority: {gameplan_result['recommendations'][0]['title'] if gameplan_result['recommendations'] else 'None'}")
        
        # Verify all results are well-formed
        assert health_result is not None
        assert "overall_direction" in health_result
        assert triage_result is not None
        assert "total_pages" in triage_result
        assert gameplan_result is not None
        assert "recommendations" in gameplan_result
        
        print("\n✅ Complete pipeline test passed!")
