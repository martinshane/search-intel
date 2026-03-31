"""
Unit tests for Module 2 (Page-Level Triage) with synthetic data.

Tests cover:
- Per-page trend fitting (linear regression, slope calculation, bucket classification)
- CTR anomaly detection using Isolation Forest
- Engagement flag logic (GA4 cross-reference)
- Priority scoring algorithm
- Edge cases and data validation
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from api.modules.module_02_triage import (
    analyze_page_triage,
    fit_page_trends,
    detect_ctr_anomalies,
    calculate_priority_scores,
    classify_page_bucket,
    project_page1_loss_date
)


# ============================================================================
# Synthetic Data Generators
# ============================================================================

def generate_page_daily_data(
    n_pages: int = 50,
    n_days: int = 120,
    include_patterns: bool = True
) -> pd.DataFrame:
    """
    Generate synthetic per-page daily time series data.
    
    Args:
        n_pages: Number of unique pages to generate
        n_days: Number of days of history
        include_patterns: If True, inject known patterns (growing, decaying, stable)
    
    Returns:
        DataFrame with columns: page, date, clicks, impressions, position
    """
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=n_days - 1)
    dates = pd.date_range(start_date, end_date, freq='D')
    
    rows = []
    
    for page_idx in range(n_pages):
        page_url = f"/page-{page_idx}"
        
        # Determine page pattern
        if include_patterns and page_idx < 10:
            # Growing pages (10%)
            base_clicks = 50
            trend_slope = np.random.uniform(0.3, 1.0)  # clicks per day
            pattern = "growing"
        elif include_patterns and page_idx < 25:
            # Decaying pages (30%)
            base_clicks = 150
            trend_slope = np.random.uniform(-0.8, -0.2)
            pattern = "decaying"
        elif include_patterns and page_idx < 30:
            # Critical decay (10%)
            base_clicks = 200
            trend_slope = np.random.uniform(-1.5, -0.8)
            pattern = "critical"
        else:
            # Stable pages (50%)
            base_clicks = np.random.uniform(20, 200)
            trend_slope = np.random.uniform(-0.1, 0.1)
            pattern = "stable"
        
        # Base position (affects CTR)
        base_position = np.random.uniform(2, 15)
        
        for day_idx, date in enumerate(dates):
            # Trend component
            clicks = base_clicks + (trend_slope * day_idx)
            
            # Add noise
            clicks = max(0, clicks + np.random.normal(0, clicks * 0.15))
            
            # Position with slight variation
            position = max(1, base_position + np.random.normal(0, 0.5))
            
            # Calculate impressions based on position (rough inverse relationship)
            base_impressions = clicks / (0.3 * np.exp(-0.15 * position))
            impressions = max(clicks, base_impressions + np.random.normal(0, base_impressions * 0.1))
            
            rows.append({
                'page': page_url,
                'date': date.date(),
                'clicks': int(clicks),
                'impressions': int(impressions),
                'position': round(position, 1),
                '_pattern': pattern  # For testing validation
            })
    
    return pd.DataFrame(rows)


def generate_ga4_landing_data(page_daily_data: pd.DataFrame) -> pd.DataFrame:
    """
    Generate synthetic GA4 landing page engagement data matching the page set.
    
    Args:
        page_daily_data: Page daily data to derive page list from
    
    Returns:
        DataFrame with columns: page, sessions, bounce_rate, avg_session_duration
    """
    unique_pages = page_daily_data['page'].unique()
    
    rows = []
    for page in unique_pages:
        # Get average monthly clicks for this page
        page_data = page_daily_data[page_daily_data['page'] == page]
        avg_monthly_clicks = page_data.groupby(
            pd.to_datetime(page_data['date']).dt.to_period('M')
        )['clicks'].sum().mean()
        
        # Sessions roughly equal to clicks (with some variation)
        sessions = int(avg_monthly_clicks * np.random.uniform(0.9, 1.1))
        
        # Create engagement patterns
        # 30% of pages have low engagement (bounce > 80%, short session)
        if np.random.random() < 0.3:
            bounce_rate = np.random.uniform(0.8, 0.95)
            avg_session_duration = np.random.uniform(10, 30)
            engagement_flag = "low_engagement"
        else:
            bounce_rate = np.random.uniform(0.3, 0.7)
            avg_session_duration = np.random.uniform(45, 180)
            engagement_flag = None
        
        rows.append({
            'page': page,
            'sessions': sessions,
            'bounce_rate': round(bounce_rate, 3),
            'avg_session_duration': round(avg_session_duration, 1),
            '_engagement_flag': engagement_flag
        })
    
    return pd.DataFrame(rows)


def generate_gsc_page_summary(page_daily_data: pd.DataFrame) -> pd.DataFrame:
    """
    Generate GSC page-level summary statistics.
    
    Args:
        page_daily_data: Daily page data to aggregate
    
    Returns:
        DataFrame with columns: page, total_clicks, total_impressions, avg_position, avg_ctr
    """
    # Last 30 days summary
    last_30_days = page_daily_data['date'].max() - timedelta(days=30)
    recent_data = page_daily_data[page_daily_data['date'] >= last_30_days]
    
    summary = recent_data.groupby('page').agg({
        'clicks': 'sum',
        'impressions': 'sum',
        'position': 'mean'
    }).reset_index()
    
    summary.columns = ['page', 'total_clicks', 'total_impressions', 'avg_position']
    summary['avg_ctr'] = summary['total_clicks'] / summary['total_impressions']
    summary['avg_ctr'] = summary['avg_ctr'].round(4)
    summary['avg_position'] = summary['avg_position'].round(1)
    
    return summary


# ============================================================================
# Test: Trend Fitting
# ============================================================================

class TestTrendFitting:
    """Tests for per-page linear trend fitting."""
    
    def test_fit_page_trends_basic(self):
        """Test basic trend fitting on synthetic data."""
        page_daily_data = generate_page_daily_data(n_pages=20, n_days=90)
        
        trends = fit_page_trends(page_daily_data, min_days=30)
        
        assert len(trends) == 20
        assert all('page' in t for t in trends)
        assert all('slope' in t for t in trends)
        assert all('r_squared' in t for t in trends)
        assert all('days_of_data' in t for t in trends)
        
        # Verify slopes match injected patterns (roughly)
        for trend in trends:
            page = trend['page']
            pattern = page_daily_data[page_daily_data['page'] == page]['_pattern'].iloc[0]
            slope = trend['slope']
            
            if pattern == 'growing':
                assert slope > 0.15, f"Growing page {page} should have positive slope, got {slope}"
            elif pattern == 'critical':
                assert slope < -0.5, f"Critical page {page} should have strong negative slope, got {slope}"
            elif pattern == 'stable':
                assert -0.15 <= slope <= 0.15, f"Stable page {page} should have near-zero slope, got {slope}"
    
    def test_fit_page_trends_min_days_filter(self):
        """Test that pages with insufficient data are filtered out."""
        # Generate data with only 20 days
        page_daily_data = generate_page_daily_data(n_pages=10, n_days=20)
        
        # Request 30 days minimum
        trends = fit_page_trends(page_daily_data, min_days=30)
        
        # Should return empty or very few results
        assert len(trends) == 0, "Should filter out pages with < 30 days"
    
    def test_fit_page_trends_r_squared_quality(self):
        """Test that R² values are reasonable."""
        page_daily_data = generate_page_daily_data(n_pages=10, n_days=90)
        
        trends = fit_page_trends(page_daily_data, min_days=30)
        
        for trend in trends:
            r_squared = trend['r_squared']
            assert 0 <= r_squared <= 1, f"R² must be between 0 and 1, got {r_squared}"
            
            # With our synthetic data (clear trends + moderate noise), R² should be decent
            # We don't enforce high R² because real-world data can be noisy
            assert r_squared >= 0, "R² should be non-negative"


# ============================================================================
# Test: Bucket Classification
# ============================================================================

class TestBucketClassification:
    """Tests for classifying pages into buckets based on slope."""
    
    def test_classify_page_bucket_thresholds(self):
        """Test bucket classification matches defined thresholds."""
        test_cases = [
            (1.5, "growing"),
            (0.5, "growing"),
            (0.15, "growing"),
            (0.05, "stable"),
            (0.0, "stable"),
            (-0.05, "stable"),
            (-0.15, "decaying"),
            (-0.3, "decaying"),
            (-0.6, "critical"),
            (-1.2, "critical"),
        ]
        
        for slope, expected_bucket in test_cases:
            bucket = classify_page_bucket(slope)
            assert bucket == expected_bucket, \
                f"Slope {slope} should be classified as {expected_bucket}, got {bucket}"
    
    def test_classify_page_bucket_boundary_conditions(self):
        """Test exact boundary values."""
        # Thresholds: growing > 0.1, stable -0.1 to 0.1, decaying -0.5 to -0.1, critical < -0.5
        assert classify_page_bucket(0.1) == "stable"
        assert classify_page_bucket(0.100001) == "growing"
        assert classify_page_bucket(-0.1) == "stable"
        assert classify_page_bucket(-0.100001) == "decaying"
        assert classify_page_bucket(-0.5) == "decaying"
        assert classify_page_bucket(-0.500001) == "critical"


# ============================================================================
# Test: CTR Anomaly Detection
# ============================================================================

class TestCTRAnomaly:
    """Tests for CTR anomaly detection using Isolation Forest."""
    
    def test_detect_ctr_anomalies_basic(self):
        """Test basic CTR anomaly detection."""
        gsc_summary = pd.DataFrame([
            {'page': '/page-1', 'avg_position': 3.0, 'avg_ctr': 0.080, 'total_impressions': 10000},
            {'page': '/page-2', 'avg_position': 3.2, 'avg_ctr': 0.075, 'total_impressions': 9500},
            {'page': '/page-3', 'avg_position': 2.8, 'avg_ctr': 0.020, 'total_impressions': 12000},  # Anomaly
            {'page': '/page-4', 'avg_position': 5.0, 'avg_ctr': 0.045, 'total_impressions': 8000},
            {'page': '/page-5', 'avg_position': 5.2, 'avg_ctr': 0.042, 'total_impressions': 7500},
            {'page': '/page-6', 'avg_position': 5.1, 'avg_ctr': 0.015, 'total_impressions': 9000},  # Anomaly
        ])
        
        anomalies = detect_ctr_anomalies(gsc_summary, min_impressions=5000)
        
        assert len(anomalies) > 0, "Should detect at least one anomaly"
        
        # Check that page-3 and page-6 are flagged (low CTR for position)
        anomaly_pages = [a['page'] for a in anomalies]
        assert '/page-3' in anomaly_pages or '/page-6' in anomaly_pages, \
            "Should flag pages with anomalously low CTR"
        
        # Verify structure
        for anomaly in anomalies:
            assert 'page' in anomaly
            assert 'avg_position' in anomaly
            assert 'avg_ctr' in anomaly
            assert 'expected_ctr' in anomaly
            assert 'ctr_deficit' in anomaly
            assert anomaly['ctr_deficit'] < 0, "CTR deficit should be negative"
    
    def test_detect_ctr_anomalies_min_impressions_filter(self):
        """Test that low-impression pages are filtered out."""
        gsc_summary = pd.DataFrame([
            {'page': '/page-1', 'avg_position': 3.0, 'avg_ctr': 0.080, 'total_impressions': 100},  # Too low
            {'page': '/page-2', 'avg_position': 3.0, 'avg_ctr': 0.020, 'total_impressions': 200},  # Too low
        ])
        
        anomalies = detect_ctr_anomalies(gsc_summary, min_impressions=1000)
        
        assert len(anomalies) == 0, "Should filter out pages below impression threshold"
    
    def test_detect_ctr_anomalies_position_grouping(self):
        """Test that CTR anomalies are detected within position groups."""
        # Create pages at different positions with consistent CTRs except one outlier
        gsc_summary = pd.DataFrame([
            # Position ~3 group
            {'page': '/page-1', 'avg_position': 3.0, 'avg_ctr': 0.080, 'total_impressions': 10000},
            {'page': '/page-2', 'avg_position': 3.1, 'avg_ctr': 0.078, 'total_impressions': 10000},
            {'page': '/page-3', 'avg_position': 3.2, 'avg_ctr': 0.025, 'total_impressions': 10000},  # Outlier
            # Position ~8 group
            {'page': '/page-4', 'avg_position': 8.0, 'avg_ctr': 0.025, 'total_impressions': 10000},
            {'page': '/page-5', 'avg_position': 8.1, 'avg_ctr': 0.023, 'total_impressions': 10000},
            {'page': '/page-6', 'avg_position': 8.2, 'avg_ctr': 0.024, 'total_impressions': 10000},
        ])
        
        anomalies = detect_ctr_anomalies(gsc_summary, min_impressions=5000)
        
        # Should flag page-3 as anomaly within its position group
        anomaly_pages = [a['page'] for a in anomalies]
        assert '/page-3' in anomaly_pages, "Should detect CTR anomaly within position group"


# ============================================================================
# Test: Page Loss Projection
# ============================================================================

class TestPageLossProjection:
    """Tests for projecting when a page will fall below position threshold."""
    
    def test_project_page1_loss_date_decaying(self):
        """Test projection for a decaying page."""
        # Page currently at position 5, decaying at 0.02 positions/day
        # Will reach position 10 in (10-5)/0.02 = 250 days
        current_position = 5.0
        slope = 0.02  # positions per day (positive = getting worse)
        
        loss_date = project_page1_loss_date(
            current_position=current_position,
            slope=slope,
            threshold_position=10.0,
            current_date=datetime(2026, 1, 1).date()
        )
        
        assert loss_date is not None
        expected_days = (10.0 - 5.0) / 0.02
        expected_date = datetime(2026, 1, 1).date() + timedelta(days=expected_days)
        
        # Allow 1 day tolerance for rounding
        assert abs((loss_date - expected_date).days) <= 1
    
    def test_project_page1_loss_date_improving(self):
        """Test that improving pages return None."""
        current_position = 8.0
        slope = -0.01  # Improving (getting better position)
        
        loss_date = project_page1_loss_date(
            current_position=current_position,
            slope=slope,
            threshold_position=10.0
        )
        
        assert loss_date is None, "Improving pages should not have a loss date"
    
    def test_project_page1_loss_date_already_beyond(self):
        """Test pages already beyond threshold."""
        current_position = 12.0
        slope = 0.01
        
        loss_date = project_page1_loss_date(
            current_position=current_position,
            slope=slope,
            threshold_position=10.0
        )
        
        # Should return current date or None (already lost)
        assert loss_date is None or loss_date <= datetime.now().date()


# ============================================================================
# Test: Priority Scoring
# ============================================================================

class TestPriorityScoring:
    """Tests for page priority score calculation."""
    
    def test_calculate_priority_scores_basic(self):
        """Test basic priority scoring logic."""
        pages = [
            {
                'page': '/high-priority',
                'current_monthly_clicks': 500,
                'slope': -0.5,  # Strong decay
                'current_position': 6.0,
                'bucket': 'decaying',
                'ctr_anomaly': True
            },
            {
                'page': '/low-priority',
                'current_monthly_clicks': 50,
                'slope': -0.15,  # Mild decay
                'current_position': 15.0,
                'bucket': 'decaying',
                'ctr_anomaly': False
            },
            {
                'page': '/stable-page',
                'current_monthly_clicks': 300,
                'slope': 0.02,
                'current_position': 4.0,
                'bucket': 'stable',
                'ctr_anomaly': False
            }
        ]
        
        scored_pages = calculate_priority_scores(pages)
        
        assert len(scored_pages) == 3
        
        # All should have priority_score
        for page in scored_pages:
            assert 'priority_score' in page
            assert page['priority_score'] >= 0
        
        # High-priority page should score higher than low-priority
        high_score = next(p['priority_score'] for p in scored_pages if p['page'] == '/high-priority')
        low_score = next(p['priority_score'] for p in scored_pages if p['page'] == '/low-priority')
        
        assert high_score > low_score, \
            f"High-priority page should score higher: {high_score} vs {low_score}"
    
    def test_calculate_priority_scores_recoverability_factor(self):
        """Test that recoverability factor affects scoring correctly."""
        pages = [
            {
                'page': '/easy-fix',
                'current_monthly_clicks': 200,
                'slope': -0.3,
                'current_position': 8.0,  # Page 1, easier to recover
                'bucket': 'decaying',
                'ctr_anomaly': True  # CTR fix = easy
            },
            {
                'page': '/hard-fix',
                'current_monthly_clicks': 200,
                'slope': -0.3,
                'current_position': 25.0,  # Page 3, harder to recover
                'bucket': 'decaying',
                'ctr_anomaly': False  # Position fix = hard
            }
        ]
        
        scored_pages = calculate_priority_scores(pages)
        
        easy_score = next(p['priority_score'] for p in scored_pages if p['page'] == '/easy-fix')
        hard_score = next(p['priority_score'] for p in scored_pages if p['page'] == '/hard-fix')
        
        # Easy fix should score higher (more recoverable)
        assert easy_score > hard_score


# ============================================================================
# Test: Full Module Integration
# ============================================================================

class TestModule02Integration:
    """Integration tests for the full analyze_page_triage function."""
    
    def test_analyze_page_triage_full_pipeline(self):
        """Test the complete Module 2 pipeline with synthetic data."""
        # Generate synthetic datasets
        page_daily_data = generate_page_daily_data(n_pages=30, n_days=90, include_patterns=True)
        ga4_landing_data = generate_ga4_landing_data(page_daily_data)
        gsc_page_summary = generate_gsc_page_summary(page_daily_data)
        
        # Run analysis
        result = analyze_page_triage(
            page_daily_data=page_daily_data,
            ga4_landing_data=ga4_landing_data,
            gsc_page_summary=gsc_page_summary
        )
        
        # Verify output structure
        assert 'pages' in result
        assert 'summary' in result
        
        # Verify summary stats
        summary = result['summary']
        assert 'total_pages_analyzed' in summary
        assert 'growing' in summary
        assert 'stable' in summary
        assert 'decaying' in summary
        assert 'critical' in summary
        assert 'total_recoverable_clicks_monthly' in summary
        
        # Verify bucket counts sum correctly
        total = summary['growing'] + summary['stable'] + summary['decaying'] + summary['critical']
        assert total == summary['total_pages_analyzed']
        
        # Verify individual page results
        pages = result['pages']
        assert len(pages) > 0, "Should analyze at least some pages"
        
        for page in pages:
            # Required fields
            assert 'page' in page
            assert 'bucket' in page
            assert 'current_monthly_clicks' in page
            assert 'trend_slope' in page
            assert 'priority_score' in page
            assert 'recommended_action' in page
            
            # Bucket should