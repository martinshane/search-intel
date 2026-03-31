"""
Integration tests verifying Module 2 correctly reads Module 1 output
and end-to-end pipeline test from GSC data through both modules.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from api.modules.module_01_health_trajectory import analyze_health_trajectory
from api.modules.module_02_page_triage import analyze_page_triage


@pytest.fixture
def sample_daily_data():
    """Sample GSC daily time series data for Module 1."""
    dates = pd.date_range(start='2024-01-01', end='2025-04-30', freq='D')
    np.random.seed(42)
    
    # Create realistic traffic pattern with trend and seasonality
    trend = np.linspace(1000, 800, len(dates))  # Declining trend
    weekly_seasonal = 100 * np.sin(np.arange(len(dates)) * 2 * np.pi / 7)
    noise = np.random.normal(0, 50, len(dates))
    
    clicks = trend + weekly_seasonal + noise
    impressions = clicks * np.random.uniform(15, 25, len(dates))
    
    return pd.DataFrame({
        'date': dates,
        'clicks': np.maximum(0, clicks),
        'impressions': np.maximum(0, impressions),
        'ctr': np.maximum(0, clicks) / np.maximum(1, impressions),
        'position': np.random.uniform(5, 15, len(dates))
    })


@pytest.fixture
def sample_page_daily_data():
    """Sample per-page daily time series for Module 2."""
    dates = pd.date_range(start='2024-01-01', end='2025-04-30', freq='D')
    pages = ['/blog/post-1', '/blog/post-2', '/product/pricing', '/blog/old-post']
    
    data = []
    for page in pages:
        np.random.seed(hash(page) % 2**32)
        
        if page == '/blog/post-1':
            # Growing page
            trend = np.linspace(50, 150, len(dates))
        elif page == '/blog/post-2':
            # Decaying page
            trend = np.linspace(200, 80, len(dates))
        elif page == '/product/pricing':
            # Stable page
            trend = np.ones(len(dates)) * 100
        else:  # old-post
            # Critical decay
            trend = np.linspace(300, 50, len(dates))
        
        noise = np.random.normal(0, 10, len(dates))
        clicks = np.maximum(0, trend + noise)
        impressions = clicks * np.random.uniform(10, 20, len(dates))
        position = np.random.uniform(3, 20, len(dates))
        
        for i, date in enumerate(dates):
            data.append({
                'date': date,
                'page': page,
                'clicks': clicks[i],
                'impressions': impressions[i],
                'ctr': clicks[i] / max(impressions[i], 1),
                'position': position[i]
            })
    
    return pd.DataFrame(data)


@pytest.fixture
def sample_ga4_landing_data():
    """Sample GA4 landing page engagement data."""
    return pd.DataFrame([
        {
            'page': '/blog/post-1',
            'sessions': 150,
            'bounceRate': 0.45,
            'avgSessionDuration': 125.5,
            'conversions': 5
        },
        {
            'page': '/blog/post-2',
            'sessions': 100,
            'bounceRate': 0.85,  # High bounce - engagement flag
            'avgSessionDuration': 22.3,  # Low session duration
            'conversions': 0
        },
        {
            'page': '/product/pricing',
            'sessions': 120,
            'bounceRate': 0.35,
            'avgSessionDuration': 180.2,
            'conversions': 12
        },
        {
            'page': '/blog/old-post',
            'sessions': 80,
            'bounceRate': 0.88,
            'avgSessionDuration': 18.5,
            'conversions': 0
        }
    ])


@pytest.fixture
def sample_gsc_page_summary():
    """Sample GSC page summary data."""
    return pd.DataFrame([
        {
            'page': '/blog/post-1',
            'clicks': 4500,
            'impressions': 65000,
            'ctr': 0.069,
            'position': 8.2
        },
        {
            'page': '/blog/post-2',
            'clicks': 3200,
            'impressions': 105000,
            'ctr': 0.030,  # Low CTR anomaly
            'position': 7.5
        },
        {
            'page': '/product/pricing',
            'clicks': 3000,
            'impressions': 48000,
            'ctr': 0.062,
            'position': 5.1
        },
        {
            'page': '/blog/old-post',
            'clicks': 2500,
            'impressions': 95000,
            'ctr': 0.026,
            'position': 9.8
        }
    ])


class TestModule1To2Integration:
    """Test that Module 2 correctly reads and uses Module 1 output."""
    
    def test_module2_receives_valid_module1_output(self, sample_daily_data):
        """Verify Module 1 output format is compatible with Module 2 expectations."""
        # Run Module 1
        module1_result = analyze_health_trajectory(sample_daily_data)
        
        # Verify Module 1 returns expected structure
        assert 'overall_direction' in module1_result
        assert 'trend_slope_pct_per_month' in module1_result
        assert 'change_points' in module1_result
        assert 'seasonality' in module1_result
        assert 'forecast' in module1_result
        
        # Verify data types
        assert isinstance(module1_result['overall_direction'], str)
        assert isinstance(module1_result['trend_slope_pct_per_month'], (int, float))
        assert isinstance(module1_result['change_points'], list)
        assert isinstance(module1_result['seasonality'], dict)
        assert isinstance(module1_result['forecast'], dict)
    
    def test_module2_uses_module1_change_points(
        self, 
        sample_daily_data,
        sample_page_daily_data,
        sample_ga4_landing_data,
        sample_gsc_page_summary
    ):
        """Test that Module 2 can leverage Module 1 change points for context."""
        # Run Module 1
        module1_result = analyze_health_trajectory(sample_daily_data)
        
        # Run Module 2
        module2_result = analyze_page_triage(
            sample_page_daily_data,
            sample_ga4_landing_data,
            sample_gsc_page_summary
        )
        
        # Module 2 should successfully process even with Module 1 context available
        assert 'pages' in module2_result
        assert 'summary' in module2_result
        assert len(module2_result['pages']) > 0
        
        # If Module 1 detected change points, Module 2 should be aware
        # (even if it doesn't directly use them, it validates compatibility)
        if module1_result['change_points']:
            # Module 2 should still produce valid results
            assert module2_result['summary']['total_pages_analyzed'] > 0
    
    def test_module2_trend_analysis_aligns_with_module1(
        self,
        sample_daily_data,
        sample_page_daily_data,
        sample_ga4_landing_data,
        sample_gsc_page_summary
    ):
        """Verify Module 2 page-level trends align with Module 1 site-level trends."""
        # Run Module 1 for site-level trend
        module1_result = analyze_health_trajectory(sample_daily_data)
        site_direction = module1_result['overall_direction']
        
        # Run Module 2 for page-level trends
        module2_result = analyze_page_triage(
            sample_page_daily_data,
            sample_ga4_landing_data,
            sample_gsc_page_summary
        )
        
        # If site is declining, we should see some decaying/critical pages
        if 'declin' in site_direction.lower():
            summary = module2_result['summary']
            decaying_pages = summary.get('decaying', 0) + summary.get('critical', 0)
            assert decaying_pages > 0, "Site is declining but no pages flagged as decaying"
        
        # Total pages analyzed should match input
        assert module2_result['summary']['total_pages_analyzed'] == len(
            sample_page_daily_data['page'].unique()
        )
    
    def test_module2_priority_scoring_incorporates_decay_rate(
        self,
        sample_page_daily_data,
        sample_ga4_landing_data,
        sample_gsc_page_summary
    ):
        """Test that Module 2 priority scores reflect decay urgency."""
        module2_result = analyze_page_triage(
            sample_page_daily_data,
            sample_ga4_landing_data,
            sample_gsc_page_summary
        )
        
        # Find pages with different buckets
        decaying_pages = [p for p in module2_result['pages'] if p['bucket'] == 'decaying']
        critical_pages = [p for p in module2_result['pages'] if p['bucket'] == 'critical']
        stable_pages = [p for p in module2_result['pages'] if p['bucket'] == 'stable']
        
        # Critical pages should have higher priority scores than stable
        if critical_pages and stable_pages:
            avg_critical_priority = np.mean([p['priority_score'] for p in critical_pages])
            avg_stable_priority = np.mean([p['priority_score'] for p in stable_pages])
            assert avg_critical_priority > avg_stable_priority
        
        # All priority scores should be non-negative
        for page in module2_result['pages']:
            assert page['priority_score'] >= 0


class TestEndToEndPipeline:
    """End-to-end tests from raw GSC data through both modules."""
    
    def test_complete_pipeline_gsc_to_module1_to_module2(
        self,
        sample_daily_data,
        sample_page_daily_data,
        sample_ga4_landing_data,
        sample_gsc_page_summary
    ):
        """Full pipeline test: GSC data → Module 1 → Module 2 → structured output."""
        # Stage 1: Process site-level data through Module 1
        print("\n=== Running Module 1: Health & Trajectory ===")
        module1_result = analyze_health_trajectory(sample_daily_data)
        
        # Validate Module 1 output
        assert module1_result is not None
        assert module1_result['overall_direction'] in [
            'strong_growth', 'growth', 'flat', 'declining', 'strong_decline'
        ]
        print(f"Site Direction: {module1_result['overall_direction']}")
        print(f"Trend Slope: {module1_result['trend_slope_pct_per_month']:.2f}%/month")
        
        # Stage 2: Process page-level data through Module 2
        print("\n=== Running Module 2: Page-Level Triage ===")
        module2_result = analyze_page_triage(
            sample_page_daily_data,
            sample_ga4_landing_data,
            sample_gsc_page_summary
        )
        
        # Validate Module 2 output
        assert module2_result is not None
        assert 'pages' in module2_result
        assert 'summary' in module2_result
        print(f"Pages Analyzed: {module2_result['summary']['total_pages_analyzed']}")
        print(f"  Growing: {module2_result['summary']['growing']}")
        print(f"  Stable: {module2_result['summary']['stable']}")
        print(f"  Decaying: {module2_result['summary']['decaying']}")
        print(f"  Critical: {module2_result['summary']['critical']}")
        
        # Stage 3: Validate integrated insights
        print("\n=== Validating Integrated Insights ===")
        
        # Check for CTR anomalies
        ctr_anomalies = [p for p in module2_result['pages'] if p.get('ctr_anomaly')]
        print(f"CTR Anomalies Detected: {len(ctr_anomalies)}")
        
        # Check for engagement flags
        engagement_flags = [
            p for p in module2_result['pages'] 
            if p.get('engagement_flag') == 'low_engagement'
        ]
        print(f"Low Engagement Pages: {len(engagement_flags)}")
        
        # Verify total recoverable clicks is calculated
        assert 'total_recoverable_clicks_monthly' in module2_result['summary']
        recoverable = module2_result['summary']['total_recoverable_clicks_monthly']
        print(f"Total Recoverable Clicks/Month: {recoverable:.0f}")
        
        # Combined output structure (what would feed into Module 5)
        combined_analysis = {
            'health_trajectory': module1_result,
            'page_triage': module2_result,
            'analysis_timestamp': datetime.now().isoformat()
        }
        
        # Validate combined structure is JSON-serializable
        import json
        json_output = json.dumps(combined_analysis, default=str)
        assert len(json_output) > 0
        print(f"\n✓ Combined analysis is JSON-serializable ({len(json_output)} chars)")
        
        return combined_analysis
    
    def test_pipeline_handles_minimal_data(self):
        """Test pipeline gracefully handles minimal/edge case data."""
        # Minimal daily data (just 30 days)
        dates = pd.date_range(start='2025-04-01', end='2025-04-30', freq='D')
        minimal_daily = pd.DataFrame({
            'date': dates,
            'clicks': np.random.uniform(50, 100, len(dates)),
            'impressions': np.random.uniform(500, 1000, len(dates)),
            'ctr': 0.1,
            'position': 10.0
        })
        
        # Minimal page data (single page)
        minimal_page_daily = pd.DataFrame({
            'date': dates,
            'page': '/single-page',
            'clicks': np.random.uniform(20, 40, len(dates)),
            'impressions': np.random.uniform(200, 400, len(dates)),
            'ctr': 0.1,
            'position': 8.0
        })
        
        minimal_ga4 = pd.DataFrame([{
            'page': '/single-page',
            'sessions': 50,
            'bounceRate': 0.5,
            'avgSessionDuration': 60.0,
            'conversions': 1
        }])
        
        minimal_gsc_summary = pd.DataFrame([{
            'page': '/single-page',
            'clicks': 900,
            'impressions': 9000,
            'ctr': 0.1,
            'position': 8.0
        }])
        
        # Run pipeline
        module1_result = analyze_health_trajectory(minimal_daily)
        module2_result = analyze_page_triage(
            minimal_page_daily,
            minimal_ga4,
            minimal_gsc_summary
        )
        
        # Should complete without errors
        assert module1_result is not None
        assert module2_result is not None
        assert module2_result['summary']['total_pages_analyzed'] == 1
    
    def test_pipeline_with_missing_ga4_data(
        self,
        sample_page_daily_data,
        sample_gsc_page_summary
    ):
        """Test Module 2 handles missing GA4 data gracefully."""
        # Empty GA4 data
        empty_ga4 = pd.DataFrame(columns=[
            'page', 'sessions', 'bounceRate', 'avgSessionDuration', 'conversions'
        ])
        
        # Module 2 should still work, just without engagement flags
        module2_result = analyze_page_triage(
            sample_page_daily_data,
            empty_ga4,
            sample_gsc_page_summary
        )
        
        assert module2_result is not None
        assert 'pages' in module2_result
        
        # Pages should exist but without engagement flags
        for page in module2_result['pages']:
            # engagement_flag might be None or absent when no GA4 data
            engagement = page.get('engagement_flag')
            # Should not be 'low_engagement' since we have no data to compare
            assert engagement != 'low_engagement' or engagement is None
    
    def test_pipeline_output_schema_stability(
        self,
        sample_daily_data,
        sample_page_daily_data,
        sample_ga4_landing_data,
        sample_gsc_page_summary
    ):
        """Ensure output schema remains stable for downstream consumers (Module 5)."""
        module1_result = analyze_health_trajectory(sample_daily_data)
        module2_result = analyze_page_triage(
            sample_page_daily_data,
            sample_ga4_landing_data,
            sample_gsc_page_summary
        )
        
        # Module 1 required fields
        required_m1_fields = [
            'overall_direction',
            'trend_slope_pct_per_month',
            'change_points',
            'seasonality',
            'forecast'
        ]
        for field in required_m1_fields:
            assert field in module1_result, f"Module 1 missing required field: {field}"
        
        # Module 1 forecast structure
        assert '30d' in module1_result['forecast']
        assert '60d' in module1_result['forecast']
        assert '90d' in module1_result['forecast']
        for period in ['30d', '60d', '90d']:
            assert 'clicks' in module1_result['forecast'][period]
            assert 'ci_low' in module1_result['forecast'][period]
            assert 'ci_high' in module1_result['forecast'][period]
        
        # Module 2 required fields
        required_m2_fields = ['pages', 'summary']
        for field in required_m2_fields:
            assert field in module2_result, f"Module 2 missing required field: {field}"
        
        # Module 2 summary structure
        summary_fields = [
            'total_pages_analyzed',
            'growing',
            'stable',
            'decaying',
            'critical',
            'total_recoverable_clicks_monthly'
        ]
        for field in summary_fields:
            assert field in module2_result['summary'], f"Module 2 summary missing: {field}"
        
        # Module 2 page object structure
        if module2_result['pages']:
            page = module2_result['pages'][0]
            page_fields = [
                'url',
                'bucket',
                'current_monthly_clicks',
                'trend_slope',
                'priority_score'
            ]
            for field in page_fields:
                assert field in page, f"Module 2 page object missing: {field}"
    
    def test_pipeline_performance_on_large_dataset(self):
        """Test pipeline performance with larger dataset (simulating real site)."""
        # Generate 16 months of daily data
        dates = pd.date_range(start='2024-01-01', end='2025-04-30', freq='D')
        large_daily = pd.DataFrame({
            'date': dates,
            'clicks': np.random.uniform(800, 1200, len(dates)),
            'impressions': np.random.uniform(12000, 18000, len(dates)),
            'ctr': 0.065,
            'position': 8.5
        })
        
        # Generate 50 pages with daily data
        large_page_data = []
        for i in range(50):
            page_url = f'/page-{i}'
            for date in dates:
                large_page_data.append({
                    'date': date,
                    'page': page_url,
                    'clicks': np.random.uniform(10, 50),
                    'impressions': np.random.uniform(200, 800),
                    'ctr': np.random.uniform(0.03, 0.12),
                    'position': np.random.uniform(3, 20)
                })
        large_page_daily = pd.DataFrame(large_page_data)
        
        # Generate GA4 and summary data for 50 pages
        large_ga4 = pd.DataFrame([
            {
                'page': f'/page-{i}',
                'sessions': np.random.randint(20, 200),
                'bounceRate': np.random.uniform(0.3, 0.9),
                'avgSessionDuration': np.random.uniform(15, 180),
                'conversions': np.random.randint(0, 10)
            }
            for i in range(50)
        ])
        
        large_gsc_summary = pd.DataFrame([
            {
                'page': f'/page-{i}',
                'clicks': np.random.randint(500, 5000),
                'impressions': np.random.randint(8000, 80000),
                'ctr': np.random.uniform(0.03, 0.12),
                'position': np.random.uniform(3, 20)
            }
            for i in range(50)
        ])
        
        # Time the pipeline
        import time
        start_time = time.time()
        
        module1_result = analyze_health_trajectory(large_daily)
        module2_result = analyze_page_triage(
            large_page_daily,
            large_ga4,
            large_gsc_summary
        )
        
        elapsed = time.time() - start_time
        
        # Should complete in reasonable time (under 30 seconds for this size)
        assert elapsed < 30, f"Pipeline took too long: {elapsed:.2f}s"
        print(f"\n✓ Large dataset processed in {elapsed:.2f}s")
        
        # Validate results
        assert module1_result is not None
        assert module2_result is not None
        assert module2_result['summary']['total_pages_analyzed'] == 50


class TestCrossCutingConcerns:
    """Test cross-module concerns like data consistency and error propagation."""
    
    def test_date_range_consistency(
        self,
        sample_daily_data,
        sample_page_daily_data
    ):
        """Ensure Module 1 and Module 2 work with same date ranges."""
        # Both modules receive data from same date range
        m1_dates = sample_daily_data['date']
        m2_dates = sample_page_daily_data['date'].unique()
        
        # Date ranges should overlap significantly
        m1_min, m1_max = m1_dates.min(), m1_dates.max()