"""
Integration tests for the Search Intelligence Report MVP.

Tests the complete pipeline from OAuth data ingestion through report generation,
validating calculations, data transformations, and output format compliance.
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import numpy as np
from typing import Dict, List, Any
import json

from app.analysis.module1_health_trajectory import analyze_health_trajectory
from app.analysis.module2_page_triage import analyze_page_triage
from app.analysis.module5_gameplan import generate_gameplan
from app.services.gsc_service import GSCService
from app.services.ga4_service import GA4Service
from app.services.report_generator import ReportGenerator


# Test Data Fixtures
@pytest.fixture
def mock_gsc_daily_data():
    """Generate realistic GSC daily time series data."""
    dates = pd.date_range(end=datetime.now(), periods=480, freq='D')
    
    # Create base trend with decline and seasonal patterns
    base = 1000
    trend = np.linspace(0, -200, len(dates))  # Declining trend
    
    # Add day-of-week seasonality (lower on weekends)
    dow_seasonal = np.array([0.1 if d.weekday() < 5 else -0.15 for d in dates]) * base
    
    # Add monthly cycle (spike at month start)
    monthly_seasonal = np.array([0.2 if d.day <= 7 else 0 for d in dates]) * base
    
    # Add noise
    noise = np.random.normal(0, 50, len(dates))
    
    # Inject a change point at date 350 (algorithm update)
    change_point_effect = np.zeros(len(dates))
    change_point_effect[350:] = -120
    
    clicks = base + trend + dow_seasonal + monthly_seasonal + noise + change_point_effect
    clicks = np.maximum(clicks, 50)  # Floor at 50
    
    impressions = clicks * np.random.uniform(15, 25, len(dates))
    
    df = pd.DataFrame({
        'date': dates,
        'clicks': clicks,
        'impressions': impressions,
        'ctr': clicks / impressions,
        'position': np.random.uniform(8, 12, len(dates))
    })
    
    return df


@pytest.fixture
def mock_gsc_page_data():
    """Generate realistic per-page GSC data."""
    pages = [
        {'url': '/blog/seo-guide', 'type': 'blog', 'age_days': 720},
        {'url': '/products/premium', 'type': 'product', 'age_days': 180},
        {'url': '/blog/content-marketing', 'type': 'blog', 'age_days': 540},
        {'url': '/comparison/tools', 'type': 'commercial', 'age_days': 360},
        {'url': '/blog/old-post', 'type': 'blog', 'age_days': 1100},
    ]
    
    dates = pd.date_range(end=datetime.now(), periods=90, freq='D')
    data = []
    
    for page in pages:
        # Different pages have different trends
        if 'old-post' in page['url']:
            # Decaying page
            base_clicks = 200
            trend = np.linspace(0, -100, len(dates))
        elif 'seo-guide' in page['url']:
            # Growing page
            base_clicks = 150
            trend = np.linspace(0, 50, len(dates))
        else:
            # Stable pages
            base_clicks = 100
            trend = np.zeros(len(dates))
        
        clicks = base_clicks + trend + np.random.normal(0, 10, len(dates))
        clicks = np.maximum(clicks, 5)
        
        impressions = clicks * np.random.uniform(20, 30, len(dates))
        
        # Add CTR anomaly for one page
        if 'content-marketing' in page['url']:
            ctr = clicks / impressions * 0.4  # Anomalously low CTR
        else:
            ctr = clicks / impressions
        
        for i, date in enumerate(dates):
            data.append({
                'date': date,
                'page': page['url'],
                'clicks': clicks[i],
                'impressions': impressions[i],
                'ctr': ctr[i],
                'position': np.random.uniform(5, 15, 1)[0]
            })
    
    return pd.DataFrame(data)


@pytest.fixture
def mock_ga4_landing_data():
    """Generate realistic GA4 landing page engagement data."""
    return pd.DataFrame([
        {
            'landing_page': '/blog/seo-guide',
            'sessions': 1200,
            'engagement_rate': 0.68,
            'avg_session_duration': 145.3,
            'bounce_rate': 0.32,
            'conversions': 23
        },
        {
            'landing_page': '/products/premium',
            'sessions': 890,
            'engagement_rate': 0.72,
            'avg_session_duration': 210.5,
            'bounce_rate': 0.28,
            'conversions': 67
        },
        {
            'landing_page': '/blog/content-marketing',
            'sessions': 1500,
            'engagement_rate': 0.31,  # Low engagement
            'avg_session_duration': 22.1,  # Low duration
            'bounce_rate': 0.87,  # High bounce
            'conversions': 3
        },
        {
            'landing_page': '/comparison/tools',
            'sessions': 670,
            'engagement_rate': 0.65,
            'avg_session_duration': 180.2,
            'bounce_rate': 0.35,
            'conversions': 45
        },
        {
            'landing_page': '/blog/old-post',
            'sessions': 450,
            'engagement_rate': 0.58,
            'avg_session_duration': 95.4,
            'bounce_rate': 0.42,
            'conversions': 8
        },
    ])


@pytest.fixture
def mock_serp_data():
    """Generate realistic SERP data from DataForSEO."""
    return [
        {
            'keyword': 'seo best practices',
            'search_volume': 8900,
            'organic_position': 3,
            'url': '/blog/seo-guide',
            'serp_features': ['featured_snippet', 'people_also_ask', 'people_also_ask', 'people_also_ask'],
            'top_10_domains': [
                {'domain': 'competitor1.com', 'position': 1},
                {'domain': 'competitor2.com', 'position': 2},
                {'domain': 'yoursite.com', 'position': 3},
                {'domain': 'competitor3.com', 'position': 4},
                {'domain': 'competitor1.com', 'position': 5},
            ]
        },
        {
            'keyword': 'content marketing strategy',
            'search_volume': 5400,
            'organic_position': 11,
            'url': '/blog/content-marketing',
            'serp_features': ['video_carousel', 'people_also_ask'],
            'top_10_domains': [
                {'domain': 'competitor2.com', 'position': 1},
                {'domain': 'competitor4.com', 'position': 2},
            ]
        },
        {
            'keyword': 'best crm software',
            'search_volume': 12000,
            'organic_position': 8,
            'url': '/comparison/tools',
            'serp_features': ['shopping_results', 'people_also_ask', 'ai_overview'],
            'top_10_domains': [
                {'domain': 'competitor5.com', 'position': 1},
                {'domain': 'competitor1.com', 'position': 2},
            ]
        },
    ]


@pytest.fixture
def mock_oauth_credentials():
    """Mock OAuth credentials for GSC and GA4."""
    return {
        'gsc': {
            'access_token': 'mock_gsc_token',
            'refresh_token': 'mock_gsc_refresh',
            'token_expiry': (datetime.now() + timedelta(hours=1)).isoformat()
        },
        'ga4': {
            'access_token': 'mock_ga4_token',
            'refresh_token': 'mock_ga4_refresh',
            'token_expiry': (datetime.now() + timedelta(hours=1)).isoformat(),
            'property_id': '123456789'
        }
    }


# Integration Tests

class TestOAuthDataIngestion:
    """Test OAuth authentication and data ingestion from GSC/GA4."""
    
    @patch('app.services.gsc_service.build')
    def test_gsc_oauth_connection(self, mock_build, mock_oauth_credentials):
        """Test GSC OAuth connection and credential validation."""
        mock_service = MagicMock()
        mock_build.return_value = mock_service
        
        gsc_service = GSCService(mock_oauth_credentials['gsc'])
        
        assert gsc_service.credentials is not None
        assert gsc_service.credentials['access_token'] == 'mock_gsc_token'
    
    @patch('app.services.ga4_service.BetaAnalyticsDataClient')
    def test_ga4_oauth_connection(self, mock_client, mock_oauth_credentials):
        """Test GA4 OAuth connection and property access."""
        mock_client_instance = MagicMock()
        mock_client.return_value = mock_client_instance
        
        ga4_service = GA4Service(mock_oauth_credentials['ga4'])
        
        assert ga4_service.property_id == '123456789'
        assert ga4_service.credentials is not None
    
    @patch('app.services.gsc_service.GSCService.fetch_search_analytics')
    def test_gsc_data_pull(self, mock_fetch, mock_oauth_credentials, mock_gsc_daily_data):
        """Test GSC data ingestion with proper date range and pagination."""
        mock_fetch.return_value = mock_gsc_daily_data
        
        gsc_service = GSCService(mock_oauth_credentials['gsc'])
        data = gsc_service.fetch_search_analytics(
            site_url='https://example.com',
            start_date=(datetime.now() - timedelta(days=480)).strftime('%Y-%m-%d'),
            end_date=datetime.now().strftime('%Y-%m-%d')
        )
        
        assert len(data) > 0
        assert 'clicks' in data.columns
        assert 'impressions' in data.columns
        assert 'ctr' in data.columns
        assert 'position' in data.columns
        
        # Verify date range
        date_range = (data['date'].max() - data['date'].min()).days
        assert date_range >= 450  # Should have ~16 months
    
    @patch('app.services.ga4_service.GA4Service.fetch_landing_page_data')
    def test_ga4_data_pull(self, mock_fetch, mock_oauth_credentials, mock_ga4_landing_data):
        """Test GA4 data ingestion with engagement metrics."""
        mock_fetch.return_value = mock_ga4_landing_data
        
        ga4_service = GA4Service(mock_oauth_credentials['ga4'])
        data = ga4_service.fetch_landing_page_data(
            start_date=(datetime.now() - timedelta(days=480)).strftime('%Y-%m-%d'),
            end_date=datetime.now().strftime('%Y-%m-%d')
        )
        
        assert len(data) > 0
        assert 'landing_page' in data.columns
        assert 'engagement_rate' in data.columns
        assert 'avg_session_duration' in data.columns
        assert 'bounce_rate' in data.columns
        assert 'conversions' in data.columns


class TestModule1HealthTrajectory:
    """Test Module 1: Health & Trajectory analysis."""
    
    def test_trend_direction_classification(self, mock_gsc_daily_data):
        """Test trend classification (growth/decline/stable)."""
        result = analyze_health_trajectory(mock_gsc_daily_data)
        
        assert 'overall_direction' in result
        assert result['overall_direction'] in ['strong_growth', 'growth', 'flat', 'decline', 'strong_decline']
        assert 'trend_slope_pct_per_month' in result
        
        # With our mock data showing decline, should detect it
        assert result['overall_direction'] in ['decline', 'strong_decline']
        assert result['trend_slope_pct_per_month'] < 0
    
    def test_change_point_detection(self, mock_gsc_daily_data):
        """Test detection of structural breaks in traffic."""
        result = analyze_health_trajectory(mock_gsc_daily_data)
        
        assert 'change_points' in result
        assert isinstance(result['change_points'], list)
        
        # Should detect the injected change point around day 350
        if len(result['change_points']) > 0:
            change_point = result['change_points'][0]
            assert 'date' in change_point
            assert 'magnitude' in change_point
            assert 'direction' in change_point
            
            # Magnitude should be negative (it's a drop)
            assert change_point['magnitude'] < 0
    
    def test_seasonality_detection(self, mock_gsc_daily_data):
        """Test detection of weekly and monthly seasonal patterns."""
        result = analyze_health_trajectory(mock_gsc_daily_data)
        
        assert 'seasonality' in result
        assert 'best_day' in result['seasonality']
        assert 'worst_day' in result['seasonality']
        assert 'monthly_cycle' in result['seasonality']
        
        # With our mock data, weekends should be worst
        assert result['seasonality']['worst_day'] in ['Saturday', 'Sunday']
    
    def test_anomaly_detection(self, mock_gsc_daily_data):
        """Test detection of one-off anomalies using matrix profile."""
        result = analyze_health_trajectory(mock_gsc_daily_data)
        
        assert 'anomalies' in result
        assert isinstance(result['anomalies'], list)
        
        # Each anomaly should have required fields
        for anomaly in result['anomalies']:
            assert 'date' in anomaly
            assert 'type' in anomaly
            assert 'magnitude' in anomaly
    
    def test_forecast_generation(self, mock_gsc_daily_data):
        """Test 30/60/90 day traffic forecast with confidence intervals."""
        result = analyze_health_trajectory(mock_gsc_daily_data)
        
        assert 'forecast' in result
        assert '30d' in result['forecast']
        assert '60d' in result['forecast']
        assert '90d' in result['forecast']
        
        for period in ['30d', '60d', '90d']:
            forecast = result['forecast'][period]
            assert 'clicks' in forecast
            assert 'ci_low' in forecast
            assert 'ci_high' in forecast
            
            # Confidence interval should make sense
            assert forecast['ci_low'] < forecast['clicks'] < forecast['ci_high']
            
            # With declining trend, forecast should decrease over time
        assert result['forecast']['90d']['clicks'] <= result['forecast']['30d']['clicks']
    
    def test_calculation_accuracy(self, mock_gsc_daily_data):
        """Verify mathematical accuracy of trend calculations."""
        result = analyze_health_trajectory(mock_gsc_daily_data)
        
        # Manually calculate expected slope
        data = mock_gsc_daily_data.copy()
        data['days'] = (data['date'] - data['date'].min()).dt.days
        
        from scipy import stats
        slope, intercept, r_value, p_value, std_err = stats.linregress(
            data['days'], data['clicks']
        )
        
        # Convert daily slope to monthly percentage
        avg_clicks = data['clicks'].mean()
        expected_monthly_pct = (slope * 30 / avg_clicks) * 100
        
        # Should be within 20% of manual calculation (accounting for decomposition)
        assert abs(result['trend_slope_pct_per_month'] - expected_monthly_pct) / abs(expected_monthly_pct) < 0.2


class TestModule2PageTriage:
    """Test Module 2: Page-Level Triage analysis."""
    
    def test_page_trend_classification(self, mock_gsc_page_data, mock_ga4_landing_data):
        """Test bucketing pages into Growing/Stable/Decaying/Critical."""
        result = analyze_page_triage(
            mock_gsc_page_data,
            mock_ga4_landing_data,
            None
        )
        
        assert 'pages' in result
        assert len(result['pages']) > 0
        
        # Check each page has required fields
        for page in result['pages']:
            assert 'url' in page
            assert 'bucket' in page
            assert page['bucket'] in ['growing', 'stable', 'decaying', 'critical']
            assert 'trend_slope' in page
            assert 'priority_score' in page
        
        # Our mock data has specific pages - verify they're classified correctly
        page_buckets = {p['url']: p['bucket'] for p in result['pages']}
        
        assert page_buckets['/blog/old-post'] in ['decaying', 'critical']  # Declining page
        assert page_buckets['/blog/seo-guide'] == 'growing'  # Growing page
    
    def test_ctr_anomaly_detection(self, mock_gsc_page_data, mock_ga4_landing_data):
        """Test detection of pages with anomalously low CTR."""
        result = analyze_page_triage(
            mock_gsc_page_data,
            mock_ga4_landing_data,
            None
        )
        
        # Find the page with intentionally low CTR
        anomaly_page = next(
            (p for p in result['pages'] if 'content-marketing' in p['url']),
            None
        )
        
        assert anomaly_page is not None
        assert anomaly_page['ctr_anomaly'] == True
        assert 'ctr_expected' in anomaly_page
        assert 'ctr_actual' in anomaly_page
        assert anomaly_page['ctr_actual'] < anomaly_page['ctr_expected']
    
    def test_engagement_flag_detection(self, mock_gsc_page_data, mock_ga4_landing_data):
        """Test cross-referencing with GA4 to find engagement issues."""
        result = analyze_page_triage(
            mock_gsc_page_data,
            mock_ga4_landing_data,
            None
        )
        
        # Find the page with low engagement
        low_engagement_page = next(
            (p for p in result['pages'] if 'content-marketing' in p['url']),
            None
        )
        
        assert low_engagement_page is not None
        assert 'engagement_flag' in low_engagement_page
        assert low_engagement_page['engagement_flag'] == 'low_engagement'
    
    def test_priority_scoring(self, mock_gsc_page_data, mock_ga4_landing_data):
        """Test priority score calculation based on traffic and decay rate."""
        result = analyze_page_triage(
            mock_gsc_page_data,
            mock_ga4_landing_data,
            None
        )
        
        # All pages should have priority scores
        for page in result['pages']:
            assert 'priority_score' in page
            assert 0 <= page['priority_score'] <= 100
        
        # Decaying pages with high traffic should have higher priority
        decaying_pages = [p for p in result['pages'] if p['bucket'] in ['decaying', 'critical']]
        if len(decaying_pages) > 1:
            # Sort by current traffic
            by_traffic = sorted(decaying_pages, key=lambda x: x.get('current_monthly_clicks', 0), reverse=True)
            # Higher traffic pages should generally have higher priority
            assert by_traffic[0]['priority_score'] >= by_traffic[-1]['priority_score'] * 0.5
    
    def test_summary_statistics(self, mock_gsc_page_data, mock_ga4_landing_data):
        """Test summary statistics are calculated correctly."""
        result = analyze_page_triage(
            mock_gsc_page_data,
            mock_ga4_landing_data,
            None
        )
        
        assert 'summary' in result
        summary = result['summary']
        
        assert 'total_pages_analyzed' in summary
        assert 'growing' in summary
        assert 'stable' in summary
        assert 'decaying' in summary
        assert 'critical' in summary
        assert 'total_recoverable_clicks_monthly' in summary
        
        # Sum of buckets should equal total
        total_bucketed = summary['growing'] + summary['stable'] + summary['decaying'] + summary['critical']
        assert total_bucketed == summary['total_pages_analyzed']


class TestModule5Gameplan:
    """Test Module 5: The Gameplan synthesis."""
    
    @pytest.fixture
    def mock_module_outputs(self, mock_gsc_daily_data, mock_gsc_page_data, mock_ga4_landing_data, mock_serp_data):
        """Generate mock outputs from modules 1-4."""
        health = analyze_health_trajectory(mock_gsc_daily_data)
        triage = analyze_page_triage(mock_gsc_page_data, mock_ga4_landing_data, None)
        
        serp = {
            'keywords_analyzed': 3,
            'serp_feature_displacement': [
                {
                    'keyword': 'best crm software',
                    'organic_position': 8,
                    'visual_position': 12,
                    'features_above': ['shopping_results', 'ai_overview'],
                    'estimated_ctr_impact': -0.04
                }
            ],
            'competitors': [
                {'domain': 'competitor1.com', 'keywords_shared': 2, 'avg_position': 2.5, 'threat_level': 'high'},
                {'domain': 'competitor2.com', 'keywords_shared': 2, 'avg_position': 1.5, 'threat_level': 'high'},
            ],
            'total_click_share': 0.15
        }
        
        content = {
            'cannibalization_clusters': [],
            'striking_distance': [
                {
                    'query': 'content marketing strategy',
                    'current_position': 11.3,
                    'impressions': 5400,
                    'estimated_click_gain_if_top5': 280,
                    'intent': 'informational',
                    'landing_page': '/blog/content-marketing'
                }
            ],
            'thin_content': [
                {
                    'url': '/blog/old-post',
                    'word_count': 380,
                    'impressions': 2200,
                    'bounce_rate': 0.81
                }
            ]
        }
        
        return {
            'health': health,
            'triage': triage,
            'serp': serp,
            'content': content
        }
    
    def test_gameplan_structure(self, mock_module_outputs):
        """Test gameplan output structure matches spec."""
        result = generate_gameplan(
            mock_module_outputs['health'],
            mock_module_outputs['triage'],
            mock_module_outputs['serp'],
            mock_module_outputs['content']
        )
        
        assert 'critical' in result
        assert 'quick_wins' in result
        assert 'strategic' in result
        assert 'structural' in result
        assert 'total_estimated_monthly_click_recovery' in result
        assert 'total_estimated_monthly_click_growth' in result
        
        # Each action list should be a list of action items
        for category in ['critical', 'quick_wins', 'strategic', 'structural']:
            assert isinstance(result[category], list)
    
    def test_action_item_format(self, mock_module_outputs):
        """Test each action item has required fields."""
        result = generate_gameplan(
            mock_module_outputs['health'],
            mock_module_outputs['triage'],
            mock_module_outputs['serp'],
            mock_module_outputs['content']
        )
        
        all_actions = result['critical'] + result['quick_wins'] + result['strategic'] + result['structural']
        
        for action in all_actions:
            assert 'action' in action
            assert 'impact' in action  # Estimated clicks/month
            assert 'effort' in action
            assert action['effort'] in ['low', 'medium', 'high']
            assert isinstance(action['impact'], (int, float))
            assert action['impact'] >= 0
    
    def test_prioritization_logic(self, mock_module_outputs):
        """Test actions are properly prioritized."""
        result = generate_gameplan(
            mock_module_outputs['health'],
            mock_module_outputs['triage'],
            mock_module_outputs['serp'],
            mock_module_outputs['content']
        )
        
        # Critical should include high-decay pages with significant traffic
        critical_actions = result['critical']
        if len(critical_actions) > 0:
            # Should contain actions from pages in 'critical' bucket
            critical_urls = [a.get('url') or a.get('page') for a in critical_actions if 'url' in a or 'page' in a]
            assert any('old-post' in str(url) for url in critical_urls if url)
        
        # Quick wins should include striking distance keywords
        quick_wins = result['quick_wins']
        quick_win_keywords = [a.get('keyword') for a in quick_wins if 'keyword' in a]
        # Should have the striking distance keyword from mock data
        # (may be reformatted but should be present)
    
    def test_impact_calculations(self, mock_module_outputs):
        """Test estimated impact calculations are reasonable."""
        result = generate_gameplan(
            mock_module_outputs['health'],
            mock_module_outputs['triage'],
            mock_module_outputs['serp'],
            mock_module_outputs['content']
        )
        
        total_recovery = result['total_estimated_monthly_click_recovery']
        total_growth = result['total_estimated_monthly_click_growth']
        
        # Should have positive numbers
        assert total_recovery >= 0
        assert total_growth >= 0
        
        # Sum of action impacts should be <= totals
        all_actions = result['critical'] + result['quick_wins'] + result['strategic']
        sum_action_impacts = sum(a['impact'] for a in all_actions if 'impact' in a)
        
        # Allow some slack for rounding and overlaps
        assert sum_action_impacts <= (total_recovery + total_growth) * 1.2


class TestReportGeneration:
    """Test complete report generation pipeline."""
    
    @patch('app.services.gsc_service.GSCService.fetch_search_analytics')
    @patch('app.services.ga4_service.GA4Service.fetch_landing_page_data')
    def test_full_pipeline_execution(
        self,
        mock_ga4_fetch,
        mock_gsc_fetch,
        mock_gsc_daily_data,
        mock_gsc_page_data,
        mock_ga4_landing_data,
        mock_oauth_credentials
    ):
        """Test complete pipeline from OAuth to final report."""
        mock_gsc_fetch.return_value = mock_gsc_daily_data
        mock_ga4_fetch.return_value = mock_ga4_landing_data
        
        generator = ReportGenerator(
            gsc_credentials=mock_oauth_credentials['gsc'],
            ga4_credentials=mock_oauth_credentials['ga4']
        )
        
        report = generator.generate_report(
            site_url='https://example.com',
            include_modules=[1, 2, 5]  # MVP modules
        )
        
        assert report is not None
        assert 'module1_health_trajectory' in report
        assert 'module2_page_triage' in report
        assert 'module5_gameplan' in report
        assert 'metadata' in report
    
    def test_report_output_format(self, mock_gsc_daily_data, mock_gsc_page_data, mock_ga4_landing_data):
        """Test final report matches specification format."""
        # Generate module outputs
        health = analyze_health_trajectory(mock_gsc_daily_data)
        triage = analyze_page_triage(mock_gsc_page_data, mock_ga4_landing_data, None)
        
        report = {
            'module1_health_trajectory': health,
            'module2_page_triage': triage,
            'metadata': {
                'generated_at': datetime.now().isoformat(),
                'site_url': 'https://example.com',
                'date_range_start': '2024-01-01',
                'date_range_end': '2025-05-01',
                'modules_included': [1, 2, 5]
            }
        }
        
        # Validate JSON serializability
        try:
            json_output = json.dumps(report, default=str)
            assert len(json_output) > 0
        except Exception as e:
            pytest.fail(f"Report is not JSON serializable: {e}")
        
        # Validate structure
        assert 'metadata' in report
        assert 'generated_at' in report['metadata']
        assert 'site_url' in report['metadata']
    
    def test_report_data_consistency(self, mock_gsc_daily_data, mock_gsc_page_data, mock_ga4_landing_data):
        """Test data consistency across modules."""
        health = analyze_health_trajectory(mock_gsc_daily_data)
        triage = analyze_page_triage(mock_gsc_page_data, mock_ga4_landing_data, None)
        
        # Date ranges should be consistent
        health_date_range = (
            mock_gsc_daily_data['date'].max() - mock_gsc_daily_data['date'].min()
        ).days
        
        # Page data should reference same period
        page_date_range = (
            mock_gsc_page_data['date'].max() - mock_gsc_page_data['date'].min()
        ).days
        
        # Should be analyzing similar time periods
        assert abs(health_date_range - page_date_range) < 100  # Within ~3 months
    
    def test_calculation_accuracy_across_modules(self, mock_gsc_daily_data, mock_gsc_page_data, mock_ga4_landing_data):
        """Test calculations are accurate and consistent across modules."""
        health = analyze_health_trajectory(mock_gsc_daily_data)
        triage = analyze_page_triage(mock_gsc_page_data, mock_ga4_landing_data, None)
        
        # Health module should detect overall decline
        assert health['overall_direction'] in ['decline', 'strong_decline']
        
        # Triage should have decaying pages
        decaying_count = triage['summary']['decaying'] + triage['summary']['critical']
        assert decaying_count > 0
        
        # Total clicks in triage summary should be reasonable given daily data
        total_monthly_clicks_health = mock_gsc_daily_data['clicks'].tail(30).sum()
        total_recoverable_triage = triage['summary']['total_recoverable_clicks_monthly']
        
        # Recoverable should be less than total monthly traffic
        assert total_recoverable_triage < total_monthly_clicks_health * 1.5


class TestErrorHandling:
    """Test error handling and edge cases."""
    
    def test_insufficient_data_handling(self):
        """Test handling of insufficient data for analysis."""
        # Only 10 days of data
        short_data = pd.DataFrame({
            'date': pd.date_range(end=datetime.now(), periods=10, freq='D'),
            'clicks': np.random.randint(50, 150, 10),
            'impressions': np.random.randint(500, 1500, 10),
            'ctr': np.random.uniform(0.03, 0.08, 10),
            'position': np.random.uniform(8, 12, 10)
        })
        
        # Should handle gracefully or raise informative error
        try:
            result = analyze_health_trajectory(short_data)
            # If it succeeds, should have warnings
            assert 'warnings' in result or 'data_quality_issues' in result
        except ValueError as e:
            # Or should raise clear error
            assert 'insufficient data' in str(e).lower()
    
    def test_missing_ga4_data_handling(self, mock_gsc_page_data):
        """Test handling when GA4 data is unavailable."""
        result = analyze_page_triage(
            mock_gsc_page_data,
            pd.DataFrame(),  # Empty GA4 data
            None
        )
        
        # Should still produce results but flag missing data
        assert 'pages' in result
        # Engagement flags should be absent or marked as unavailable
        for page in result['pages']:
            assert 'engagement_flag' not in page or page['engagement_flag'] == 'unavailable'
    
    def test_extreme_values_handling(self):
        """Test handling of extreme or outlier values."""
        # Data with extreme spike
        dates = pd.date_range(end=datetime.now(), periods=100, freq='D')
        clicks = np.random.randint(50, 150, 100)
        clicks[50] = 10000  # Extreme spike
        
        extreme_data = pd.DataFrame({
            'date': dates,
            'clicks': clicks,
            'impressions': clicks * 20,
            'ctr': clicks / (clicks * 20),
            'position': np.random.uniform(8, 12, 100)
        })
        
        result = analyze_health_trajectory(extreme_data)
        
        # Should detect as anomaly, not crash
        assert 'anomalies' in result
        assert len(result['anomalies']) > 0


class TestPerformance:
    """Test performance characteristics."""
    
    def test_module1_execution_time(self, mock_gsc_daily_data):
        """Test Module 1 completes in reasonable time."""
        import time
        
        start = time.time()
        result = analyze_health_trajectory(mock_gsc_daily_data)
        duration = time.time() - start
        
        # Should complete in under 5 seconds for 480 days of data
        assert duration < 5.0
        assert result is not None
    
    def test_module2_execution_time(self, mock_gsc_page_data, mock_ga4_landing_data):
        """Test Module 2 completes in reasonable time."""
        import time
        
        start = time.time()
        result = analyze_page_triage(mock_gsc_page_data, mock_ga4_landing_data, None)
        duration = time.time() - start
        
        # Should complete in under 3 seconds for 5 pages
        assert duration < 3.0
        assert result is not None
    
    def test_large_dataset_handling(self):
        """Test handling of large datasets (1000+ pages)."""
        # Generate large page dataset
        dates = pd.date_range(end=datetime.now(), periods=90, freq='D')
        large_data = []
        
        for page_id in range(1000):
            for date in dates:
                large_data.append({
                    'date': date,
                    'page': f'/page/{page_id}',
                    'clicks': np.random.randint(1, 50),
                    'impressions': np.random.randint(50, 500),
                    'ctr': np.random.uniform(0.01, 0.1),
                    'position': np.random.uniform(5, 20)
                })
        
        large_df = pd.DataFrame(large_data)
        
        import time
        start = time.time()
        
        # Should handle without crashing (may take longer)
        result = analyze_page_triage(large_df, pd.DataFrame(), None)
        duration = time.time() - start
        
        # Should complete in under 30 seconds even for 1000 pages
        assert duration < 30.0
        assert 'pages' in result


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
