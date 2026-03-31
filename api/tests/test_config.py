"""
Test configuration for Search Intelligence Report E2E tests.

This file contains connection parameters, test data configurations,
and validation schemas for end-to-end testing of the Phase 1 pipeline.
"""

import os
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from pydantic import BaseModel, Field, validator


# =============================================================================
# Environment Configuration
# =============================================================================

class TestEnvironment:
    """Environment-specific configuration for tests."""
    
    # API endpoints
    API_BASE_URL = os.getenv("TEST_API_BASE_URL", "http://localhost:8000")
    
    # Supabase configuration
    SUPABASE_URL = os.getenv("SUPABASE_URL", "")
    SUPABASE_KEY = os.getenv("SUPABASE_ANON_KEY", "")
    
    # Test OAuth credentials (use test Google project)
    TEST_GSC_TOKEN = os.getenv("TEST_GSC_TOKEN", "")
    TEST_GA4_TOKEN = os.getenv("TEST_GA4_TOKEN", "")
    
    # Test properties
    TEST_GSC_PROPERTY = os.getenv("TEST_GSC_PROPERTY", "sc-domain:example.com")
    TEST_GA4_PROPERTY = os.getenv("TEST_GA4_PROPERTY", "properties/123456789")
    
    # Test execution parameters
    MAX_REPORT_WAIT_TIME = int(os.getenv("MAX_REPORT_WAIT_TIME", "600"))  # 10 minutes
    POLLING_INTERVAL = int(os.getenv("POLLING_INTERVAL", "5"))  # 5 seconds
    
    @classmethod
    def validate(cls) -> bool:
        """Validate that all required environment variables are set."""
        required = [
            cls.SUPABASE_URL,
            cls.SUPABASE_KEY,
            cls.TEST_GSC_TOKEN,
            cls.TEST_GA4_TOKEN,
            cls.TEST_GSC_PROPERTY,
            cls.TEST_GA4_PROPERTY,
        ]
        return all(required)
    
    @classmethod
    def get_missing_vars(cls) -> List[str]:
        """Return list of missing required environment variables."""
        missing = []
        if not cls.SUPABASE_URL:
            missing.append("SUPABASE_URL")
        if not cls.SUPABASE_KEY:
            missing.append("SUPABASE_ANON_KEY")
        if not cls.TEST_GSC_TOKEN:
            missing.append("TEST_GSC_TOKEN")
        if not cls.TEST_GA4_TOKEN:
            missing.append("TEST_GA4_TOKEN")
        if not cls.TEST_GSC_PROPERTY:
            missing.append("TEST_GSC_PROPERTY")
        if not cls.TEST_GA4_PROPERTY:
            missing.append("TEST_GA4_PROPERTY")
        return missing


# =============================================================================
# Test Data Configuration
# =============================================================================

class TestDataConfig:
    """Configuration for test data expectations."""
    
    # Date range for data pull (16 months as per spec)
    DATA_MONTHS = 16
    END_DATE = datetime.now().date()
    START_DATE = END_DATE - timedelta(days=DATA_MONTHS * 30)
    
    # Minimum data thresholds for validation
    MIN_GSC_ROWS = 100  # Minimum rows expected from GSC
    MIN_GA4_ROWS = 50   # Minimum rows expected from GA4
    MIN_QUERIES = 20    # Minimum unique queries
    MIN_PAGES = 10      # Minimum unique pages
    
    # Module execution expectations
    EXPECTED_MODULES = [
        "health_trajectory",
        "page_triage",
        "gameplan"
    ]
    
    # Performance thresholds
    MAX_INGESTION_TIME = 120  # 2 minutes
    MAX_ANALYSIS_TIME = 180   # 3 minutes per module
    MAX_TOTAL_TIME = 600      # 10 minutes total


# =============================================================================
# Validation Schemas
# =============================================================================

class ModuleStatus(BaseModel):
    """Schema for individual module status."""
    name: str
    status: str  # pending, running, complete, failed
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error: Optional[str] = None
    
    @validator('status')
    def validate_status(cls, v):
        valid_statuses = ['pending', 'running', 'complete', 'failed']
        if v not in valid_statuses:
            raise ValueError(f"Status must be one of {valid_statuses}")
        return v


class ReportProgress(BaseModel):
    """Schema for report generation progress."""
    report_id: str
    status: str
    stage: Optional[str] = None
    modules: Dict[str, str] = Field(default_factory=dict)
    progress_pct: float = 0.0
    current_module: Optional[str] = None
    error: Optional[str] = None
    
    @validator('status')
    def validate_status(cls, v):
        valid_statuses = ['pending', 'ingesting', 'analyzing', 'generating', 'complete', 'failed']
        if v not in valid_statuses:
            raise ValueError(f"Status must be one of {valid_statuses}")
        return v
    
    @validator('progress_pct')
    def validate_progress(cls, v):
        if not 0 <= v <= 100:
            raise ValueError("Progress must be between 0 and 100")
        return v


class HealthTrajectoryResult(BaseModel):
    """Validation schema for Module 1: Health & Trajectory output."""
    overall_direction: str
    trend_slope_pct_per_month: float
    change_points: List[Dict[str, Any]]
    seasonality: Dict[str, Any]
    anomalies: List[Dict[str, Any]]
    forecast: Dict[str, Dict[str, float]]
    
    @validator('overall_direction')
    def validate_direction(cls, v):
        valid_directions = ['strong_growth', 'growth', 'flat', 'decline', 'strong_decline']
        if v not in valid_directions:
            raise ValueError(f"Direction must be one of {valid_directions}")
        return v
    
    @validator('forecast')
    def validate_forecast(cls, v):
        required_periods = ['30d', '60d', '90d']
        for period in required_periods:
            if period not in v:
                raise ValueError(f"Forecast must include {period}")
            if not all(k in v[period] for k in ['clicks', 'ci_low', 'ci_high']):
                raise ValueError(f"Forecast {period} must include clicks, ci_low, ci_high")
        return v


class PageTriageResult(BaseModel):
    """Validation schema for Module 2: Page-Level Triage output."""
    pages: List[Dict[str, Any]]
    summary: Dict[str, Any]
    
    @validator('summary')
    def validate_summary(cls, v):
        required_keys = ['total_pages_analyzed', 'growing', 'stable', 'decaying', 'critical', 
                        'total_recoverable_clicks_monthly']
        for key in required_keys:
            if key not in v:
                raise ValueError(f"Summary must include {key}")
        return v
    
    @validator('pages')
    def validate_pages(cls, v):
        if not v:
            raise ValueError("Pages list cannot be empty")
        required_fields = ['url', 'bucket', 'current_monthly_clicks', 'priority_score']
        for page in v:
            for field in required_fields:
                if field not in page:
                    raise ValueError(f"Page must include {field}")
        return v


class GameplanResult(BaseModel):
    """Validation schema for Module 5: Gameplan output."""
    critical: List[Dict[str, Any]]
    quick_wins: List[Dict[str, Any]]
    strategic: List[Dict[str, Any]]
    structural: List[Dict[str, Any]]
    total_estimated_monthly_click_recovery: float
    total_estimated_monthly_click_growth: float
    narrative: str
    
    @validator('narrative')
    def validate_narrative(cls, v):
        if not v or len(v) < 50:
            raise ValueError("Narrative must be at least 50 characters")
        return v
    
    @validator('critical', 'quick_wins', 'strategic', 'structural')
    def validate_action_lists(cls, v):
        for action in v:
            required_fields = ['action', 'impact', 'effort']
            for field in required_fields:
                if field not in action:
                    raise ValueError(f"Action must include {field}")
        return v


class ReportData(BaseModel):
    """Complete report data validation schema."""
    report_id: str
    user_id: str
    gsc_property: str
    ga4_property: str
    generated_at: datetime
    data_range: Dict[str, str]
    
    # Module results
    health_trajectory: HealthTrajectoryResult
    page_triage: PageTriageResult
    gameplan: GameplanResult
    
    # Metadata
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    @validator('data_range')
    def validate_data_range(cls, v):
        if 'start_date' not in v or 'end_date' not in v:
            raise ValueError("Data range must include start_date and end_date")
        return v


# =============================================================================
# Test Scenarios
# =============================================================================

class TestScenario:
    """Base class for test scenarios."""
    
    name: str
    description: str
    expected_duration: int  # seconds
    
    def __init__(self, name: str, description: str, expected_duration: int):
        self.name = name
        self.description = description
        self.expected_duration = expected_duration


class E2ETestScenarios:
    """Collection of E2E test scenarios."""
    
    FULL_PIPELINE = TestScenario(
        name="full_pipeline",
        description="Complete Phase 1 pipeline: GSC+GA4 ingestion → 3 modules → report generation",
        expected_duration=300  # 5 minutes
    )
    
    FAST_FAIL = TestScenario(
        name="fast_fail",
        description="Test error handling with invalid credentials",
        expected_duration=10
    )
    
    POLLING = TestScenario(
        name="polling",
        description="Test job status polling mechanism",
        expected_duration=30
    )
    
    DATA_VALIDATION = TestScenario(
        name="data_validation",
        description="Validate all output schemas and data quality",
        expected_duration=60
    )


# =============================================================================
# Assertion Helpers
# =============================================================================

class ValidationHelpers:
    """Helper functions for test assertions."""
    
    @staticmethod
    def validate_report_structure(report_data: Dict[str, Any]) -> List[str]:
        """
        Validate complete report structure and return list of validation errors.
        
        Args:
            report_data: Report data dictionary from Supabase
            
        Returns:
            List of error messages (empty if valid)
        """
        errors = []
        
        # Validate top-level structure
        required_fields = ['report_id', 'user_id', 'gsc_property', 'generated_at']
        for field in required_fields:
            if field not in report_data:
                errors.append(f"Missing required field: {field}")
        
        # Validate module presence
        for module in TestDataConfig.EXPECTED_MODULES:
            if module not in report_data:
                errors.append(f"Missing module: {module}")
        
        # Validate using Pydantic schemas
        try:
            if 'health_trajectory' in report_data:
                HealthTrajectoryResult(**report_data['health_trajectory'])
        except Exception as e:
            errors.append(f"Health trajectory validation failed: {str(e)}")
        
        try:
            if 'page_triage' in report_data:
                PageTriageResult(**report_data['page_triage'])
        except Exception as e:
            errors.append(f"Page triage validation failed: {str(e)}")
        
        try:
            if 'gameplan' in report_data:
                GameplanResult(**report_data['gameplan'])
        except Exception as e:
            errors.append(f"Gameplan validation failed: {str(e)}")
        
        return errors
    
    @staticmethod
    def validate_progress_transition(old_status: str, new_status: str) -> bool:
        """
        Validate that status transition is valid.
        
        Valid transitions:
        pending → ingesting → analyzing → generating → complete
        any → failed (error case)
        """
        valid_transitions = {
            'pending': ['ingesting', 'failed'],
            'ingesting': ['analyzing', 'failed'],
            'analyzing': ['generating', 'failed'],
            'generating': ['complete', 'failed'],
            'complete': [],
            'failed': []
        }
        
        return new_status in valid_transitions.get(old_status, [])
    
    @staticmethod
    def validate_module_completion(modules: Dict[str, str]) -> bool:
        """
        Validate that all expected modules completed successfully.
        
        Args:
            modules: Dictionary of module_name: status
            
        Returns:
            True if all expected modules are complete
        """
        for module in TestDataConfig.EXPECTED_MODULES:
            if module not in modules or modules[module] != 'complete':
                return False
        return True
    
    @staticmethod
    def validate_data_quality(report_data: Dict[str, Any]) -> List[str]:
        """
        Validate data quality and completeness.
        
        Returns:
            List of data quality issues (empty if all checks pass)
        """
        issues = []
        
        # Check health trajectory
        if 'health_trajectory' in report_data:
            ht = report_data['health_trajectory']
            if not ht.get('change_points'):
                issues.append("Health trajectory has no change points detected")
            if not ht.get('forecast'):
                issues.append("Health trajectory missing forecast data")
        
        # Check page triage
        if 'page_triage' in report_data:
            pt = report_data['page_triage']
            if not pt.get('pages'):
                issues.append("Page triage has no pages analyzed")
            summary = pt.get('summary', {})
            if summary.get('total_pages_analyzed', 0) < TestDataConfig.MIN_PAGES:
                issues.append(f"Insufficient pages analyzed: {summary.get('total_pages_analyzed', 0)}")
        
        # Check gameplan
        if 'gameplan' in report_data:
            gp = report_data['gameplan']
            total_actions = len(gp.get('critical', [])) + len(gp.get('quick_wins', []))
            if total_actions == 0:
                issues.append("Gameplan has no actionable recommendations")
            if not gp.get('narrative'):
                issues.append("Gameplan missing narrative")
        
        return issues


# =============================================================================
# Mock Data for Unit Tests
# =============================================================================

class MockData:
    """Mock data generators for isolated unit tests."""
    
    @staticmethod
    def mock_gsc_response() -> Dict[str, Any]:
        """Generate mock GSC API response."""
        return {
            "rows": [
                {
                    "keys": ["query example 1"],
                    "clicks": 100,
                    "impressions": 1000,
                    "ctr": 0.1,
                    "position": 5.2
                },
                {
                    "keys": ["query example 2"],
                    "clicks": 50,
                    "impressions": 800,
                    "ctr": 0.0625,
                    "position": 8.1
                }
            ]
        }
    
    @staticmethod
    def mock_ga4_response() -> Dict[str, Any]:
        """Generate mock GA4 API response."""
        return {
            "rows": [
                {
                    "dimensionValues": [{"value": "/page1"}],
                    "metricValues": [
                        {"value": "500"},  # sessions
                        {"value": "400"},  # users
                        {"value": "0.65"}  # engagement_rate
                    ]
                }
            ]
        }
    
    @staticmethod
    def mock_report_progress(status: str = "analyzing") -> ReportProgress:
        """Generate mock report progress."""
        return ReportProgress(
            report_id="test-report-123",
            status=status,
            stage="Module 1",
            modules={
                "health_trajectory": "complete" if status != "ingesting" else "pending",
                "page_triage": "running" if status == "analyzing" else "pending",
                "gameplan": "pending"
            },
            progress_pct=33.3 if status == "analyzing" else 10.0,
            current_module="page_triage" if status == "analyzing" else None
        )


# =============================================================================
# Export Configuration
# =============================================================================

__all__ = [
    'TestEnvironment',
    'TestDataConfig',
    'ModuleStatus',
    'ReportProgress',
    'HealthTrajectoryResult',
    'PageTriageResult',
    'GameplanResult',
    'ReportData',
    'TestScenario',
    'E2ETestScenarios',
    'ValidationHelpers',
    'MockData'
]
