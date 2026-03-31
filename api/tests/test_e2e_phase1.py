"""
End-to-end integration test for Phase 1 pipeline.

This test connects to a real GSC+GA4 property (using credentials from environment),
triggers full report generation, polls status, and verifies all outputs.

Requirements:
- Environment variables: TEST_GSC_PROPERTY, TEST_GA4_PROPERTY, GOOGLE_CREDENTIALS_JSON
- Supabase connection configured
- FastAPI server running (or started by this test)

Run with: pytest api/tests/test_e2e_phase1.py -v -s
"""

import os
import sys
import json
import time
import pytest
import httpx
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

# Add parent directory to path to import from api modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from supabase import create_client, Client
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request


# ============================================================================
# Configuration & Fixtures
# ============================================================================

API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")
TEST_GSC_PROPERTY = os.getenv("TEST_GSC_PROPERTY")
TEST_GA4_PROPERTY = os.getenv("TEST_GA4_PROPERTY")
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Timeouts
INGESTION_TIMEOUT = 600  # 10 minutes for data ingestion
ANALYSIS_TIMEOUT = 600   # 10 minutes for analysis
POLL_INTERVAL = 5        # Poll status every 5 seconds


@pytest.fixture(scope="module")
def supabase_client() -> Client:
    """Create Supabase client for direct database verification."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        pytest.skip("Supabase credentials not configured")
    return create_client(SUPABASE_URL, SUPABASE_KEY)


@pytest.fixture(scope="module")
def test_credentials() -> Dict[str, Any]:
    """Parse and validate test credentials."""
    if not TEST_GSC_PROPERTY:
        pytest.skip("TEST_GSC_PROPERTY not set")
    if not TEST_GA4_PROPERTY:
        pytest.skip("TEST_GA4_PROPERTY not set")
    if not GOOGLE_CREDENTIALS_JSON:
        pytest.skip("GOOGLE_CREDENTIALS_JSON not set")
    
    try:
        creds_data = json.loads(GOOGLE_CREDENTIALS_JSON)
        return {
            "gsc_property": TEST_GSC_PROPERTY,
            "ga4_property": TEST_GA4_PROPERTY,
            "credentials": creds_data
        }
    except json.JSONDecodeError as e:
        pytest.skip(f"Invalid GOOGLE_CREDENTIALS_JSON: {e}")


@pytest.fixture(scope="module")
def api_client() -> httpx.Client:
    """Create HTTP client for API calls."""
    return httpx.Client(base_url=API_BASE_URL, timeout=30.0)


@pytest.fixture(scope="module")
def test_user_id(supabase_client: Client, test_credentials: Dict[str, Any]) -> str:
    """
    Create or retrieve test user in database.
    Stores OAuth tokens for GSC and GA4.
    """
    test_email = f"e2e_test_{int(time.time())}@searchintel.test"
    
    # Create user
    result = supabase_client.table("users").insert({
        "email": test_email,
        "gsc_token": test_credentials["credentials"],
        "ga4_token": test_credentials["credentials"],
        "created_at": datetime.utcnow().isoformat()
    }).execute()
    
    user_id = result.data[0]["id"]
    
    yield user_id
    
    # Cleanup: delete test user and associated reports
    supabase_client.table("reports").delete().eq("user_id", user_id).execute()
    supabase_client.table("api_cache").delete().eq("user_id", user_id).execute()
    supabase_client.table("users").delete().eq("id", user_id).execute()


# ============================================================================
# Test Cases
# ============================================================================

class TestE2EPhase1Pipeline:
    """End-to-end tests for Phase 1 report generation pipeline."""
    
    def test_01_health_check(self, api_client: httpx.Client):
        """Verify API server is running and healthy."""
        response = api_client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "version" in data
    
    def test_02_create_report_job(
        self, 
        api_client: httpx.Client,
        test_user_id: str,
        test_credentials: Dict[str, Any]
    ) -> str:
        """
        Trigger report generation and verify job is created.
        Returns report_id for subsequent tests.
        """
        payload = {
            "user_id": test_user_id,
            "gsc_property": test_credentials["gsc_property"],
            "ga4_property": test_credentials["ga4_property"]
        }
        
        response = api_client.post("/api/reports/generate", json=payload)
        assert response.status_code == 202  # Accepted
        
        data = response.json()
        assert "report_id" in data
        assert data["status"] == "pending"
        
        # Store for next tests
        self.report_id = data["report_id"]
        return self.report_id
    
    def test_03_poll_ingestion_status(
        self, 
        api_client: httpx.Client,
        supabase_client: Client
    ):
        """
        Poll report status until ingestion completes.
        Verify status transitions: pending -> ingesting -> analyzing
        """
        if not hasattr(self, 'report_id'):
            pytest.skip("Report not created in previous test")
        
        start_time = time.time()
        ingestion_complete = False
        last_status = None
        
        while time.time() - start_time < INGESTION_TIMEOUT:
            # Poll via API
            response = api_client.get(f"/api/reports/{self.report_id}/status")
            assert response.status_code == 200
            
            data = response.json()
            status = data["status"]
            progress = data.get("progress", {})
            
            # Log status change
            if status != last_status:
                print(f"\n[{time.time() - start_time:.1f}s] Status: {status}")
                print(f"Progress: {json.dumps(progress, indent=2)}")
                last_status = status
            
            # Check for failure
            if status == "failed":
                error_msg = data.get("error", "Unknown error")
                pytest.fail(f"Report generation failed: {error_msg}")
            
            # Wait for ingesting -> analyzing transition
            if status == "analyzing":
                ingestion_complete = True
                break
            
            time.sleep(POLL_INTERVAL)
        
        if not ingestion_complete:
            pytest.fail(f"Ingestion did not complete within {INGESTION_TIMEOUT}s")
        
        # Verify data was cached in api_cache table
        cache_result = supabase_client.table("api_cache") \
            .select("*") \
            .eq("user_id", data["user_id"]) \
            .execute()
        
        assert len(cache_result.data) > 0, "No API responses cached"
        
        # Verify we have both GSC and GA4 cached data
        cache_keys = [item["cache_key"] for item in cache_result.data]
        has_gsc = any("gsc" in key.lower() for key in cache_keys)
        has_ga4 = any("ga4" in key.lower() for key in cache_keys)
        
        assert has_gsc, "No GSC data in cache"
        assert has_ga4, "No GA4 data in cache"
        
        print(f"\n✓ Ingestion complete. {len(cache_result.data)} API responses cached.")
    
    def test_04_verify_module_execution(
        self, 
        api_client: httpx.Client,
        supabase_client: Client
    ):
        """
        Poll until all Phase 1 modules complete.
        Verify each module runs and produces output.
        
        Phase 1 modules:
        - Module 1: Health & Trajectory
        - Module 2: Page-Level Triage
        - Module 5: The Gameplan
        """
        if not hasattr(self, 'report_id'):
            pytest.skip("Report not created in previous test")
        
        required_modules = ["module_1", "module_2", "module_5"]
        start_time = time.time()
        last_progress = {}
        
        while time.time() - start_time < ANALYSIS_TIMEOUT:
            response = api_client.get(f"/api/reports/{self.report_id}/status")
            assert response.status_code == 200
            
            data = response.json()
            status = data["status"]
            progress = data.get("progress", {})
            
            # Log progress changes
            if progress != last_progress:
                completed = [k for k, v in progress.items() if v == "complete"]
                running = [k for k, v in progress.items() if v == "running"]
                print(f"\n[{time.time() - start_time:.1f}s] Completed: {completed}")
                if running:
                    print(f"Running: {running}")
                last_progress = progress.copy()
            
            # Check for failure
            if status == "failed":
                error_msg = data.get("error", "Unknown error")
                pytest.fail(f"Analysis failed: {error_msg}")
            
            # Check if all required modules are complete
            all_complete = all(
                progress.get(module) == "complete" 
                for module in required_modules
            )
            
            if all_complete and status == "complete":
                print(f"\n✓ All Phase 1 modules complete in {time.time() - start_time:.1f}s")
                break
            
            time.sleep(POLL_INTERVAL)
        else:
            pytest.fail(f"Analysis did not complete within {ANALYSIS_TIMEOUT}s")
        
        # Verify report data exists in database
        report_result = supabase_client.table("reports") \
            .select("report_data") \
            .eq("id", self.report_id) \
            .execute()
        
        assert len(report_result.data) > 0, "Report not found in database"
        report_data = report_result.data[0]["report_data"]
        assert report_data is not None, "report_data is null"
        
        # Store for detailed verification
        self.report_data = report_data
    
    def test_05_verify_module_1_output(self):
        """Verify Module 1 (Health & Trajectory) output structure and content."""
        if not hasattr(self, 'report_data'):
            pytest.skip("Report data not available")
        
        module_1 = self.report_data.get("module_1")
        assert module_1 is not None, "Module 1 output missing"
        
        # Required fields
        required_fields = [
            "overall_direction",
            "trend_slope_pct_per_month",
            "change_points",
            "seasonality",
            "anomalies",
            "forecast"
        ]
        
        for field in required_fields:
            assert field in module_1, f"Module 1 missing field: {field}"
        
        # Validate overall_direction is valid enum
        valid_directions = ["strong_growth", "growth", "flat", "decline", "strong_decline"]
        assert module_1["overall_direction"] in valid_directions, \
            f"Invalid overall_direction: {module_1['overall_direction']}"
        
        # Validate trend_slope is numeric
        assert isinstance(module_1["trend_slope_pct_per_month"], (int, float)), \
            "trend_slope_pct_per_month must be numeric"
        
        # Validate forecast structure
        forecast = module_1["forecast"]
        for period in ["30d", "60d", "90d"]:
            assert period in forecast, f"Forecast missing {period}"
            assert "clicks" in forecast[period], f"Forecast {period} missing clicks"
            assert "ci_low" in forecast[period], f"Forecast {period} missing ci_low"
            assert "ci_high" in forecast[period], f"Forecast {period} missing ci_high"
        
        # Validate change_points is a list
        assert isinstance(module_1["change_points"], list), "change_points must be a list"
        
        # If change points exist, validate structure
        if len(module_1["change_points"]) > 0:
            cp = module_1["change_points"][0]
            assert "date" in cp, "Change point missing date"
            assert "magnitude" in cp, "Change point missing magnitude"
            assert "direction" in cp, "Change point missing direction"
        
        print(f"\n✓ Module 1 output valid. Direction: {module_1['overall_direction']}, "
              f"Trend: {module_1['trend_slope_pct_per_month']:.2f}%/month")
    
    def test_06_verify_module_2_output(self):
        """Verify Module 2 (Page-Level Triage) output structure and content."""
        if not hasattr(self, 'report_data'):
            pytest.skip("Report data not available")
        
        module_2 = self.report_data.get("module_2")
        assert module_2 is not None, "Module 2 output missing"
        
        # Required top-level fields
        assert "pages" in module_2, "Module 2 missing pages list"
        assert "summary" in module_2, "Module 2 missing summary"
        
        pages = module_2["pages"]
        assert isinstance(pages, list), "pages must be a list"
        
        # If pages exist, validate structure
        if len(pages) > 0:
            page = pages[0]
            required_page_fields = [
                "url",
                "bucket",
                "current_monthly_clicks",
                "trend_slope",
                "priority_score"
            ]
            
            for field in required_page_fields:
                assert field in page, f"Page missing field: {field}"
            
            # Validate bucket is valid enum
            valid_buckets = ["growing", "stable", "decaying", "critical"]
            assert page["bucket"] in valid_buckets, f"Invalid bucket: {page['bucket']}"
        
        # Validate summary structure
        summary = module_2["summary"]
        required_summary_fields = [
            "total_pages_analyzed",
            "growing",
            "stable",
            "decaying",
            "critical"
        ]
        
        for field in required_summary_fields:
            assert field in summary, f"Summary missing field: {field}"
            assert isinstance(summary[field], (int, float)), \
                f"Summary {field} must be numeric"
        
        print(f"\n✓ Module 2 output valid. Analyzed {summary['total_pages_analyzed']} pages: "
              f"{summary['critical']} critical, {summary['decaying']} decaying, "
              f"{summary['stable']} stable, {summary['growing']} growing")
    
    def test_07_verify_module_5_output(self):
        """Verify Module 5 (The Gameplan) output structure and content."""
        if not hasattr(self, 'report_data'):
            pytest.skip("Report data not available")
        
        module_5 = self.report_data.get("module_5")
        assert module_5 is not None, "Module 5 output missing"
        
        # Required sections
        required_sections = ["critical", "quick_wins", "strategic", "structural"]
        
        for section in required_sections:
            assert section in module_5, f"Module 5 missing section: {section}"
            assert isinstance(module_5[section], list), f"{section} must be a list"
        
        # Validate impact estimates
        assert "total_estimated_monthly_click_recovery" in module_5, \
            "Missing total_estimated_monthly_click_recovery"
        assert "total_estimated_monthly_click_growth" in module_5, \
            "Missing total_estimated_monthly_click_growth"
        
        # Validate narrative exists
        assert "narrative" in module_5, "Missing narrative"
        assert isinstance(module_5["narrative"], str), "Narrative must be string"
        assert len(module_5["narrative"]) > 100, "Narrative too short"
        
        # If actions exist, validate structure
        all_actions = (
            module_5["critical"] + 
            module_5["quick_wins"] + 
            module_5["strategic"] + 
            module_5["structural"]
        )
        
        if len(all_actions) > 0:
            action = all_actions[0]
            assert "action" in action, "Action missing 'action' description"
            assert "impact" in action, "Action missing impact estimate"
            assert "effort" in action, "Action missing effort level"
            
            valid_efforts = ["low", "medium", "high"]
            assert action["effort"] in valid_efforts, \
                f"Invalid effort level: {action['effort']}"
        
        total_recovery = module_5["total_estimated_monthly_click_recovery"]
        total_growth = module_5["total_estimated_monthly_click_growth"]
        
        print(f"\n✓ Module 5 output valid. "
              f"Recovery opportunity: {total_recovery} clicks/month, "
              f"Growth opportunity: {total_growth} clicks/month")
        print(f"Total actions: {len(all_actions)} "
              f"({len(module_5['critical'])} critical, "
              f"{len(module_5['quick_wins'])} quick wins)")
    
    def test_08_verify_report_metadata(self, supabase_client: Client):
        """Verify report metadata in database is correct."""
        if not hasattr(self, 'report_id'):
            pytest.skip("Report not created")
        
        result = supabase_client.table("reports") \
            .select("*") \
            .eq("id", self.report_id) \
            .execute()
        
        assert len(result.data) > 0, "Report not found"
        report = result.data[0]
        
        # Verify status
        assert report["status"] == "complete", \
            f"Expected status 'complete', got '{report['status']}'"
        
        # Verify timestamps
        assert report["created_at"] is not None, "created_at is null"
        assert report["completed_at"] is not None, "completed_at is null"
        
        created_at = datetime.fromisoformat(report["created_at"].replace("Z", "+00:00"))
        completed_at = datetime.fromisoformat(report["completed_at"].replace("Z", "+00:00"))
        
        assert completed_at > created_at, "completed_at must be after created_at"
        
        duration = (completed_at - created_at).total_seconds()
        assert duration < INGESTION_TIMEOUT + ANALYSIS_TIMEOUT, \
            f"Report took too long: {duration}s"
        
        # Verify properties stored
        assert report["gsc_property"] is not None, "gsc_property is null"
        assert report["ga4_property"] is not None, "ga4_property is null"
        
        print(f"\n✓ Report metadata valid. Duration: {duration:.1f}s")
    
    def test_09_verify_api_cache_expiry(self, supabase_client: Client):
        """Verify API cache entries have proper expiry timestamps."""
        if not hasattr(self, 'report_id'):
            pytest.skip("Report not created")
        
        # Get report to find user_id
        report_result = supabase_client.table("reports") \
            .select("user_id") \
            .eq("id", self.report_id) \
            .execute()
        
        user_id = report_result.data[0]["user_id"]
        
        # Check cache entries
        cache_result = supabase_client.table("api_cache") \
            .select("*") \
            .eq("user_id", user_id) \
            .execute()
        
        assert len(cache_result.data) > 0, "No cache entries found"
        
        now = datetime.utcnow()
        
        for entry in cache_result.data:
            expires_at = datetime.fromisoformat(
                entry["expires_at"].replace("Z", "+00:00")
            )
            
            # Should expire in the future
            assert expires_at > now, f"Cache entry already expired: {entry['cache_key']}"
            
            # Should expire within 24 hours (our TTL policy)
            time_to_expiry = (expires_at - now).total_seconds()
            assert time_to_expiry <= 86400, \
                f"Cache entry expires too far in future: {time_to_expiry}s"
        
        print(f"\n✓ All {len(cache_result.data)} cache entries have valid expiry")
    
    def test_10_fetch_complete_report(self, api_client: httpx.Client):
        """Test fetching the complete generated report via API."""
        if not hasattr(self, 'report_id'):
            pytest.skip("Report not created")
        
        response = api_client.get(f"/api/reports/{self.report_id}")
        assert response.status_code == 200
        
        data = response.json()
        
        # Verify metadata
        assert data["id"] == self.report_id
        assert data["status"] == "complete"
        
        # Verify report_data structure
        assert "report_data" in data
        report_data = data["report_data"]
        
        # Should have all Phase 1 modules
        assert "module_1" in report_data
        assert "module_2" in report_data
        assert "module_5" in report_data
        
        # Verify metadata fields
        assert "gsc_property" in data
        assert "ga4_property" in data
        assert "created_at" in data
        assert "completed_at" in data
        
        print(f"\n✓ Complete report fetched successfully via API")
    
    def test_11_verify_report_json_structure(self):
        """Deep validation of complete report JSON structure."""
        if not hasattr(self, 'report_data'):
            pytest.skip("Report data not available")
        
        # Should be valid JSON (already parsed)
        assert isinstance(self.report_data, dict)
        
        # Validate top-level structure
        expected_modules = ["module_1", "module_2", "module_5"]
        for module in expected_modules:
            assert module in self.report_data, f"Missing {module}"
            assert isinstance(self.report_data[module], dict), \
                f"{module} must be a dict"
        
        # Verify no null