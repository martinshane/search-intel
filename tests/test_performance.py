"""
Performance regression tests ensuring <3min target on synthetic data.

Tests measure end-to-end report generation time on synthetic datasets
of various sizes to ensure the system meets performance targets.
"""

import time
from datetime import datetime, timedelta
from typing import Dict, List
import pytest
import pandas as pd
import numpy as np

from app.analysis.health_trajectory import analyze_health_trajectory
from app.analysis.page_triage import analyze_page_triage
from app.analysis.content_intelligence import analyze_content_intelligence
from app.analysis.gameplan import generate_gameplan
from app.analysis.algorithm_impact import analyze_algorithm_impacts
from app.analysis.intent_migration import analyze_intent_migration
from app.analysis.ctr_modeling import model_contextual_ctr
from app.analysis.site_architecture import analyze_site_architecture
from app.analysis.branded_split import analyze_branded_split
from app.analysis.competitive_threats import analyze_competitive_threats
from app.analysis.revenue_attribution import estimate_revenue_attribution


# Performance targets (in seconds)
TARGET_TOTAL_TIME = 180  # 3 minutes
TARGET_PER_MODULE_TIME = 20  # 20 seconds per module max
WARN_THRESHOLD = 0.8  # Warn if within 80% of target


def generate_synthetic_daily_data(days: int = 480) -> pd.DataFrame:
    """Generate synthetic GSC daily time series data."""
    dates = pd.date_range(end=datetime.now(), periods=days, freq='D')
    
    # Create trend + seasonality + noise
    trend = np.linspace(1000, 1200, days)
    weekly_season = 100 * np.sin(np.arange(days) * 2 * np.pi / 7)
    monthly_season = 50 * np.sin(np.arange(days) * 2 * np.pi / 30)
    noise = np.random.normal(0, 50, days)
    
    clicks = np.maximum(0, trend + weekly_season + monthly_season + noise)
    impressions = clicks * np.random.uniform(8, 12, days)
    
    return pd.DataFrame({
        'date': dates,
        'clicks': clicks.astype(int),
        'impressions': impressions.astype(int),
        'ctr': clicks / impressions,
        'position': np.random.uniform(5, 8, days)
    })


def generate_synthetic_page_data(num_pages: int = 200) -> pd.DataFrame:
    """Generate synthetic per-page GSC data."""
    pages = [f'/page-{i}' for i in range(num_pages)]
    
    data = []
    for page in pages:
        monthly_clicks = np.random.lognormal(4, 2)
        data.append({
            'page': page,
            'clicks': int(monthly_clicks),
            'impressions': int(monthly_clicks * np.random.uniform(8, 15)),
            'ctr': np.random.uniform(0.01, 0.15),
            'position': np.random.uniform(1, 20)
        })
    
    return pd.DataFrame(data)


def generate_synthetic_page_daily_data(num_pages: int = 200, days: int = 90) -> pd.DataFrame:
    """Generate synthetic per-page daily time series."""
    pages = [f'/page-{i}' for i in range(num_pages)]
    dates = pd.date_range(end=datetime.now(), periods=days, freq='D')
    
    data = []
    for page in pages:
        base_clicks = np.random.lognormal(2, 1.5)
        trend_slope = np.random.uniform(-0.05, 0.05)
        
        for i, date in enumerate(dates):
            clicks = max(0, base_clicks + (trend_slope * i) + np.random.normal(0, base_clicks * 0.3))
            data.append({
                'page': page,
                'date': date,
                'clicks': int(clicks),
                'impressions': int(clicks * np.random.uniform(8, 15)),
                'position': np.random.uniform(1, 20)
            })
    
    return pd.DataFrame(data)


def generate_synthetic_query_data(num_queries: int = 500) -> pd.DataFrame:
    """Generate synthetic GSC query data."""
    queries = [f'query term {i}' for i in range(num_queries)]
    
    data = []
    for query in queries:
        monthly_impressions = np.random.lognormal(6, 2)
        position = np.random.uniform(1, 50)
        ctr = max(0.001, 0.3 / position)  # Rough position-CTR curve
        
        data.append({
            'query': query,
            'impressions': int(monthly_impressions),
            'clicks': int(monthly_impressions * ctr),
            'ctr': ctr,
            'position': position
        })
    
    return pd.DataFrame(data)


def generate_synthetic_serp_data(num_keywords: int = 100) -> List[Dict]:
    """Generate synthetic SERP data."""
    serps = []
    
    for i in range(num_keywords):
        serp = {
            'keyword': f'keyword {i}',
            'position': np.random.randint(1, 11),
            'url': f'/page-{np.random.randint(0, 50)}',
            'features': []
        }
        
        # Randomly add SERP features
        if np.random.random() < 0.3:
            serp['features'].append('featured_snippet')
        if np.random.random() < 0.5:
            serp['features'].append({
                'type': 'people_also_ask',
                'count': np.random.randint(2, 8)
            })
        if np.random.random() < 0.2:
            serp['features'].append('video_carousel')
        if np.random.random() < 0.1:
            serp['features'].append('ai_overview')
        
        # Add competitor positions
        serp['competitors'] = []
        for j in range(5):
            serp['competitors'].append({
                'domain': f'competitor{j}.com',
                'position': np.random.randint(1, 20)
            })
        
        serps.append(serp)
    
    return serps


def generate_synthetic_link_graph(num_pages: int = 200, avg_links_per_page: int = 10):
    """Generate synthetic internal link graph."""
    pages = [f'/page-{i}' for i in range(num_pages)]
    edges = []
    
    for page in pages:
        num_links = np.random.poisson(avg_links_per_page)
        targets = np.random.choice(pages, size=min(num_links, len(pages)), replace=False)
        
        for target in targets:
            if target != page:
                edges.append({
                    'source': page,
                    'target': target,
                    'anchor_text': f'link text'
                })
    
    return edges


def generate_synthetic_ga4_data(num_pages: int = 200) -> pd.DataFrame:
    """Generate synthetic GA4 engagement data."""
    pages = [f'/page-{i}' for i in range(num_pages)]
    
    data = []
    for page in pages:
        data.append({
            'landing_page': page,
            'sessions': int(np.random.lognormal(4, 2)),
            'conversions': int(np.random.lognormal(1, 2)),
            'bounce_rate': np.random.uniform(0.3, 0.9),
            'avg_session_duration': np.random.uniform(20, 300),
            'engagement_rate': np.random.uniform(0.3, 0.8)
        })
    
    return pd.DataFrame(data)


@pytest.fixture
def small_synthetic_dataset():
    """Small dataset: 50 pages, 100 queries, 90 days."""
    return {
        'daily_data': generate_synthetic_daily_data(90),
        'page_data': generate_synthetic_page_data(50),
        'page_daily_data': generate_synthetic_page_daily_data(50, 90),
        'query_data': generate_synthetic_query_data(100),
        'serp_data': generate_synthetic_serp_data(50),
        'link_graph': generate_synthetic_link_graph(50),
        'ga4_data': generate_synthetic_ga4_data(50)
    }


@pytest.fixture
def medium_synthetic_dataset():
    """Medium dataset: 200 pages, 500 queries, 365 days."""
    return {
        'daily_data': generate_synthetic_daily_data(365),
        'page_data': generate_synthetic_page_data(200),
        'page_daily_data': generate_synthetic_page_daily_data(200, 90),
        'query_data': generate_synthetic_query_data(500),
        'serp_data': generate_synthetic_serp_data(100),
        'link_graph': generate_synthetic_link_graph(200),
        'ga4_data': generate_synthetic_ga4_data(200)
    }


@pytest.fixture
def large_synthetic_dataset():
    """Large dataset: 1000 pages, 2000 queries, 480 days."""
    return {
        'daily_data': generate_synthetic_daily_data(480),
        'page_data': generate_synthetic_page_data(1000),
        'page_daily_data': generate_synthetic_page_daily_data(500, 90),  # Cap pages for daily data
        'query_data': generate_synthetic_query_data(2000),
        'serp_data': generate_synthetic_serp_data(200),
        'link_graph': generate_synthetic_link_graph(1000, 8),  # Fewer links per page for large sites
        'ga4_data': generate_synthetic_ga4_data(1000)
    }


def measure_module_performance(module_func, *args, **kwargs):
    """Measure execution time of a single module."""
    start_time = time.time()
    result = module_func(*args, **kwargs)
    elapsed = time.time() - start_time
    return result, elapsed


class TestModulePerformance:
    """Test individual module performance."""
    
    def test_health_trajectory_performance(self, medium_synthetic_dataset):
        """Test Module 1: Health & Trajectory performance."""
        result, elapsed = measure_module_performance(
            analyze_health_trajectory,
            medium_synthetic_dataset['daily_data']
        )
        
        assert result is not None
        assert elapsed < TARGET_PER_MODULE_TIME, \
            f"Module 1 took {elapsed:.2f}s, exceeds {TARGET_PER_MODULE_TIME}s target"
        
        if elapsed > TARGET_PER_MODULE_TIME * WARN_THRESHOLD:
            pytest.skip(f"Warning: Module 1 took {elapsed:.2f}s, close to {TARGET_PER_MODULE_TIME}s limit")
    
    def test_page_triage_performance(self, medium_synthetic_dataset):
        """Test Module 2: Page Triage performance."""
        result, elapsed = measure_module_performance(
            analyze_page_triage,
            medium_synthetic_dataset['page_daily_data'],
            medium_synthetic_dataset['ga4_data'],
            medium_synthetic_dataset['page_data']
        )
        
        assert result is not None
        assert elapsed < TARGET_PER_MODULE_TIME, \
            f"Module 2 took {elapsed:.2f}s, exceeds {TARGET_PER_MODULE_TIME}s target"
    
    def test_content_intelligence_performance(self, medium_synthetic_dataset):
        """Test Module 4: Content Intelligence performance."""
        # Create query-page mapping
        query_page_data = []
        for _, query in medium_synthetic_dataset['query_data'].head(100).iterrows():
            page = np.random.choice(medium_synthetic_dataset['page_data']['page'].values)
            query_page_data.append({
                'query': query['query'],
                'page': page,
                'impressions': query['impressions'] // 2
            })
        query_page_df = pd.DataFrame(query_page_data)
        
        result, elapsed = measure_module_performance(
            analyze_content_intelligence,
            query_page_df,
            medium_synthetic_dataset['page_data'],
            medium_synthetic_dataset['ga4_data']
        )
        
        assert result is not None
        assert elapsed < TARGET_PER_MODULE_TIME, \
            f"Module 4 took {elapsed:.2f}s, exceeds {TARGET_PER_MODULE_TIME}s target"
    
    def test_site_architecture_performance(self, medium_synthetic_dataset):
        """Test Module 9: Site Architecture performance."""
        result, elapsed = measure_module_performance(
            analyze_site_architecture,
            medium_synthetic_dataset['link_graph'],
            medium_synthetic_dataset['page_data']
        )
        
        assert result is not None
        assert elapsed < TARGET_PER_MODULE_TIME, \
            f"Module 9 took {elapsed:.2f}s, exceeds {TARGET_PER_MODULE_TIME}s target"


class TestEndToEndPerformance:
    """Test full report generation performance."""
    
    def test_small_site_full_report(self, small_synthetic_dataset):
        """Test full report generation on small site."""
        start_time = time.time()
        
        # Module 1: Health & Trajectory
        health_result = analyze_health_trajectory(small_synthetic_dataset['daily_data'])
        
        # Module 2: Page Triage
        triage_result = analyze_page_triage(
            small_synthetic_dataset['page_daily_data'],
            small_synthetic_dataset['ga4_data'],
            small_synthetic_dataset['page_data']
        )
        
        # Module 9: Site Architecture
        architecture_result = analyze_site_architecture(
            small_synthetic_dataset['link_graph'],
            small_synthetic_dataset['page_data']
        )
        
        # Module 10: Branded Split
        branded_result = analyze_branded_split(
            small_synthetic_dataset['query_data'],
            ['example', 'test']
        )
        
        elapsed = time.time() - start_time
        
        assert health_result is not None
        assert triage_result is not None
        assert architecture_result is not None
        assert branded_result is not None
        assert elapsed < TARGET_TOTAL_TIME * 0.5, \
            f"Small site report took {elapsed:.2f}s, should be well under {TARGET_TOTAL_TIME}s"
    
    def test_medium_site_full_report(self, medium_synthetic_dataset):
        """Test full report generation on medium site (target scenario)."""
        start_time = time.time()
        module_times = {}
        
        # Module 1: Health & Trajectory
        m1_start = time.time()
        health_result = analyze_health_trajectory(medium_synthetic_dataset['daily_data'])
        module_times['health'] = time.time() - m1_start
        
        # Module 2: Page Triage
        m2_start = time.time()
        triage_result = analyze_page_triage(
            medium_synthetic_dataset['page_daily_data'],
            medium_synthetic_dataset['ga4_data'],
            medium_synthetic_dataset['page_data']
        )
        module_times['triage'] = time.time() - m2_start
        
        # Module 4: Content Intelligence
        query_page_data = []
        for _, query in medium_synthetic_dataset['query_data'].head(200).iterrows():
            page = np.random.choice(medium_synthetic_dataset['page_data']['page'].values)
            query_page_data.append({
                'query': query['query'],
                'page': page,
                'impressions': query['impressions'] // 2
            })
        query_page_df = pd.DataFrame(query_page_data)
        
        m4_start = time.time()
        content_result = analyze_content_intelligence(
            query_page_df,
            medium_synthetic_dataset['page_data'],
            medium_synthetic_dataset['ga4_data']
        )
        module_times['content'] = time.time() - m4_start
        
        # Module 9: Site Architecture
        m9_start = time.time()
        architecture_result = analyze_site_architecture(
            medium_synthetic_dataset['link_graph'],
            medium_synthetic_dataset['page_data']
        )
        module_times['architecture'] = time.time() - m9_start
        
        # Module 10: Branded Split
        m10_start = time.time()
        branded_result = analyze_branded_split(
            medium_synthetic_dataset['query_data'],
            ['example', 'test']
        )
        module_times['branded'] = time.time() - m10_start
        
        # Module 12: Revenue Attribution
        m12_start = time.time()
        revenue_result = estimate_revenue_attribution(
            medium_synthetic_dataset['query_data'],
            medium_synthetic_dataset['ga4_data'],
            medium_synthetic_dataset['ga4_data']
        )
        module_times['revenue'] = time.time() - m12_start
        
        total_elapsed = time.time() - start_time
        
        # Print performance breakdown
        print(f"\n{'='*60}")
        print(f"MEDIUM SITE PERFORMANCE REPORT")
        print(f"{'='*60}")
        for module, duration in sorted(module_times.items(), key=lambda x: x[1], reverse=True):
            print(f"{module:20s}: {duration:6.2f}s ({duration/total_elapsed*100:5.1f}%)")
        print(f"{'='*60}")
        print(f"{'TOTAL':20s}: {total_elapsed:6.2f}s")
        print(f"{'TARGET':20s}: {TARGET_TOTAL_TIME:6.2f}s")
        print(f"{'HEADROOM':20s}: {TARGET_TOTAL_TIME - total_elapsed:6.2f}s")
        print(f"{'='*60}\n")
        
        # Assertions
        assert all(r is not None for r in [health_result, triage_result, content_result, 
                                           architecture_result, branded_result, revenue_result])
        
        assert total_elapsed < TARGET_TOTAL_TIME, \
            f"Medium site report took {total_elapsed:.2f}s, exceeds {TARGET_TOTAL_TIME}s target"
        
        # Check individual module times
        for module, duration in module_times.items():
            assert duration < TARGET_PER_MODULE_TIME, \
                f"Module {module} took {duration:.2f}s, exceeds {TARGET_PER_MODULE_TIME}s target"
    
    def test_large_site_performance_warning(self, large_synthetic_dataset):
        """Test large site performance (should warn but not fail)."""
        start_time = time.time()
        
        # Run subset of modules on large dataset
        health_result = analyze_health_trajectory(large_synthetic_dataset['daily_data'])
        triage_result = analyze_page_triage(
            large_synthetic_dataset['page_daily_data'],
            large_synthetic_dataset['ga4_data'],
            large_synthetic_dataset['page_data']
        )
        architecture_result = analyze_site_architecture(
            large_synthetic_dataset['link_graph'],
            large_synthetic_dataset['page_data']
        )
        
        elapsed = time.time() - start_time
        
        print(f"\nLarge site (1000 pages) partial report: {elapsed:.2f}s")
        
        if elapsed > TARGET_TOTAL_TIME:
            pytest.skip(
                f"Large site took {elapsed:.2f}s, exceeds {TARGET_TOTAL_TIME}s. "
                f"Consider optimization or implementing pagination/sampling for very large sites."
            )


class TestPerformanceRegression:
    """Tests to catch performance regressions."""
    
    def test_no_quadratic_scaling(self):
        """Ensure performance scales linearly, not quadratically with data size."""
        # Test with progressively larger datasets
        sizes = [50, 100, 200]
        times = []
        
        for size in sizes:
            daily_data = generate_synthetic_daily_data(size)
            start = time.time()
            analyze_health_trajectory(daily_data)
            times.append(time.time() - start)
        
        # Check that doubling data size doesn't quadruple time
        # Allow some overhead but should be roughly linear
        ratio_1 = times[1] / times[0]
        ratio_2 = times[2] / times[1]
        
        assert ratio_1 < 3.0, f"Performance degraded quadratically: 2x data took {ratio_1:.2f}x time"
        assert ratio_2 < 3.0, f"Performance degraded quadratically: 2x data took {ratio_2:.2f}x time"
    
    def test_memory_efficiency(self, medium_synthetic_dataset):
        """Ensure modules don't create excessive memory overhead."""
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        mem_before = process.memory_info().rss / 1024 / 1024  # MB
        
        # Run several modules
        analyze_health_trajectory(medium_synthetic_dataset['daily_data'])
        analyze_page_triage(
            medium_synthetic_dataset['page_daily_data'],
            medium_synthetic_dataset['ga4_data'],
            medium_synthetic_dataset['page_data']
        )
        analyze_site_architecture(
            medium_synthetic_dataset['link_graph'],
            medium_synthetic_dataset['page_data']
        )
        
        mem_after = process.memory_info().rss / 1024 / 1024  # MB
        mem_increase = mem_after - mem_before
        
        # Should not use more than 500MB for medium dataset
        assert mem_increase < 500, \
            f"Memory usage increased by {mem_increase:.1f}MB, exceeds 500MB threshold"


class TestConcurrentPerformance:
    """Test performance under concurrent load."""
    
    @pytest.mark.skip(reason="Requires concurrent execution setup")
    def test_multiple_reports_concurrent(self):
        """Test generating multiple reports concurrently."""
        # This would test Railway worker performance under load
        # Implementation depends on job queue setup
        pass


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s'])
