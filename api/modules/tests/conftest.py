"""
Pytest fixtures for shared test data including synthetic GSC data generators and GA4 mock data.

This module provides:
- Synthetic GSC data generators for various scenarios (growing, declining, seasonal, etc.)
- Mock GA4 data that correlates with GSC data
- SERP feature data generators
- Internal link graph fixtures
- Algorithm update fixtures
- Reusable test configurations
"""

import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pytest


# =============================================================================
# GSC Data Generators
# =============================================================================


def generate_daily_gsc_data(
    start_date: str,
    days: int,
    base_clicks: int = 1000,
    base_impressions: int = 10000,
    trend_slope: float = 0.0,
    seasonal_amplitude: float = 0.15,
    noise_level: float = 0.05,
    change_points: Optional[List[Tuple[int, float]]] = None,
    seed: int = 42,
) -> pd.DataFrame:
    """
    Generate synthetic GSC daily time series data.
    
    Args:
        start_date: Starting date in YYYY-MM-DD format
        days: Number of days to generate
        base_clicks: Average daily clicks
        base_impressions: Average daily impressions
        trend_slope: Linear trend (clicks per day change rate, e.g., 0.01 = 1% daily growth)
        seasonal_amplitude: Amplitude of weekly seasonality (0-1)
        noise_level: Random noise level (0-1)
        change_points: List of (day_index, magnitude) tuples for abrupt changes
        seed: Random seed for reproducibility
    
    Returns:
        DataFrame with columns: date, clicks, impressions, ctr, position
    """
    np.random.seed(seed)
    
    dates = pd.date_range(start=start_date, periods=days, freq='D')
    
    # Base trend
    trend = np.arange(days) * trend_slope * base_clicks
    
    # Weekly seasonality (lower on weekends)
    day_of_week = np.array([d.weekday() for d in dates])
    seasonal = seasonal_amplitude * base_clicks * np.sin(2 * np.pi * day_of_week / 7)
    
    # Random noise
    noise = np.random.normal(0, noise_level * base_clicks, days)
    
    # Combine components
    clicks = base_clicks + trend + seasonal + noise
    
    # Apply change points
    if change_points:
        for day_idx, magnitude in change_points:
            if day_idx < days:
                clicks[day_idx:] += magnitude * base_clicks
    
    # Ensure non-negative
    clicks = np.maximum(clicks, 0)
    
    # Generate impressions (correlated with clicks but higher variance)
    ctr_base = base_clicks / base_impressions
    impressions = clicks / ctr_base * (1 + np.random.normal(0, 0.1, days))
    impressions = np.maximum(impressions, clicks)  # Impressions >= clicks
    
    # Calculate actual CTR
    ctr = clicks / impressions
    
    # Position (inversely correlated with clicks, some noise)
    position = 10 - (clicks - base_clicks) / (base_clicks * 0.1) + np.random.normal(0, 1, days)
    position = np.clip(position, 1, 100)
    
    return pd.DataFrame({
        'date': dates,
        'clicks': clicks.astype(int),
        'impressions': impressions.astype(int),
        'ctr': ctr,
        'position': position
    })


def generate_page_level_gsc_data(
    start_date: str,
    days: int,
    num_pages: int = 20,
    seed: int = 42,
) -> pd.DataFrame:
    """
    Generate per-page GSC data with varying trajectories.
    
    Creates a mix of:
    - Growing pages
    - Stable pages
    - Decaying pages
    - Critical declining pages
    
    Args:
        start_date: Starting date in YYYY-MM-DD format
        days: Number of days to generate
        num_pages: Number of pages to generate
        seed: Random seed
    
    Returns:
        DataFrame with columns: date, page, clicks, impressions, ctr, position
    """
    np.random.seed(seed)
    
    pages_data = []
    
    # Define page archetypes
    archetypes = [
        ('growing', 0.02, 100),  # Growing: 2% daily growth, 100 base clicks
        ('stable', 0.0, 200),    # Stable: no trend, 200 base clicks
        ('decaying', -0.01, 150),  # Decaying: -1% daily decline
        ('critical', -0.03, 80),  # Critical: -3% daily decline
    ]
    
    for i in range(num_pages):
        archetype, slope, base_clicks = archetypes[i % len(archetypes)]
        
        page_url = f"/page-{archetype}-{i}"
        
        # Generate data for this page
        page_df = generate_daily_gsc_data(
            start_date=start_date,
            days=days,
            base_clicks=base_clicks,
            base_impressions=base_clicks * 20,
            trend_slope=slope,
            seasonal_amplitude=0.1,
            noise_level=0.1,
            seed=seed + i,
        )
        
        page_df['page'] = page_url
        pages_data.append(page_df)
    
    return pd.concat(pages_data, ignore_index=True)


def generate_query_level_gsc_data(
    start_date: str,
    days: int,
    num_queries: int = 50,
    seed: int = 42,
) -> pd.DataFrame:
    """
    Generate per-query GSC data with realistic distributions.
    
    Args:
        start_date: Starting date in YYYY-MM-DD format
        days: Number of days to generate
        num_queries: Number of queries to generate
        seed: Random seed
    
    Returns:
        DataFrame with columns: date, query, clicks, impressions, ctr, position
    """
    np.random.seed(seed)
    
    queries_data = []
    
    # Generate queries with power-law distribution (few high-volume, many long-tail)
    for i in range(num_queries):
        # Power-law distribution for base impressions
        rank = i + 1
        base_impressions = int(10000 / (rank ** 0.8))
        
        # Position affects CTR
        position = np.random.uniform(1, 20)
        ctr_base = max(0.01, 0.3 - (position - 1) * 0.02)
        
        base_clicks = int(base_impressions * ctr_base)
        
        query_text = f"query {i+1} keyword phrase"
        
        # Generate data for this query
        query_df = generate_daily_gsc_data(
            start_date=start_date,
            days=days,
            base_clicks=base_clicks,
            base_impressions=base_impressions,
            trend_slope=np.random.uniform(-0.005, 0.005),
            seasonal_amplitude=0.05,
            noise_level=0.15,
            seed=seed + i,
        )
        
        query_df['query'] = query_text
        queries_data.append(query_df)
    
    return pd.concat(queries_data, ignore_index=True)


def generate_query_page_mapping(
    queries: List[str],
    pages: List[str],
    seed: int = 42,
) -> pd.DataFrame:
    """
    Generate query-to-page mapping data (for cannibalization analysis).
    
    Some queries will map to multiple pages to simulate cannibalization.
    
    Args:
        queries: List of query strings
        pages: List of page URLs
        seed: Random seed
    
    Returns:
        DataFrame with columns: query, page, clicks, impressions, ctr, position
    """
    np.random.seed(seed)
    
    mapping_data = []
    
    for query in queries:
        # Most queries map to 1 page, some to 2-3 (cannibalization)
        num_pages = np.random.choice([1, 1, 1, 2, 2, 3], p=[0.5, 0.3, 0.1, 0.05, 0.04, 0.01])
        
        selected_pages = np.random.choice(pages, size=min(num_pages, len(pages)), replace=False)
        
        # Base impressions for this query
        total_impressions = int(np.random.lognormal(6, 1.5))
        
        for page_idx, page in enumerate(selected_pages):
            # Split impressions across pages
            if num_pages > 1:
                # Cannibalization: split unevenly
                share = np.random.beta(2, 5) if page_idx > 0 else np.random.beta(5, 2)
            else:
                share = 1.0
            
            impressions = int(total_impressions * share)
            position = np.random.uniform(1, 20) if page_idx == 0 else np.random.uniform(5, 30)
            ctr = max(0.001, 0.3 - (position - 1) * 0.02)
            clicks = int(impressions * ctr)
            
            mapping_data.append({
                'query': query,
                'page': page,
                'clicks': clicks,
                'impressions': impressions,
                'ctr': ctr,
                'position': position,
            })
    
    return pd.DataFrame(mapping_data)


# =============================================================================
# GA4 Data Generators
# =============================================================================


def generate_ga4_landing_page_data(
    pages: List[str],
    seed: int = 42,
) -> pd.DataFrame:
    """
    Generate GA4 landing page engagement metrics.
    
    Args:
        pages: List of page URLs
        seed: Random seed
    
    Returns:
        DataFrame with columns: landing_page, sessions, engaged_sessions,
                               avg_session_duration, bounce_rate, conversions
    """
    np.random.seed(seed)
    
    ga4_data = []
    
    for page in pages:
        sessions = int(np.random.lognormal(5, 1))
        
        # Engagement metrics vary by page type
        if 'blog' in page or 'guide' in page:
            # Blog posts: higher engagement
            engaged_sessions = int(sessions * np.random.uniform(0.6, 0.9))
            avg_duration = np.random.uniform(60, 180)
            bounce_rate = np.random.uniform(0.3, 0.5)
            conversions = int(sessions * np.random.uniform(0.001, 0.01))
        elif 'product' in page or 'pricing' in page:
            # Product/pricing: high intent
            engaged_sessions = int(sessions * np.random.uniform(0.7, 0.95))
            avg_duration = np.random.uniform(90, 240)
            bounce_rate = np.random.uniform(0.2, 0.4)
            conversions = int(sessions * np.random.uniform(0.02, 0.08))
        else:
            # Generic pages: lower engagement
            engaged_sessions = int(sessions * np.random.uniform(0.4, 0.7))
            avg_duration = np.random.uniform(30, 90)
            bounce_rate = np.random.uniform(0.5, 0.8)
            conversions = int(sessions * np.random.uniform(0.0005, 0.005))
        
        ga4_data.append({
            'landing_page': page,
            'sessions': sessions,
            'engaged_sessions': engaged_sessions,
            'avg_session_duration': avg_duration,
            'bounce_rate': bounce_rate,
            'conversions': conversions,
        })
    
    return pd.DataFrame(ga4_data)


def generate_ga4_conversion_data(
    pages: List[str],
    seed: int = 42,
) -> pd.DataFrame:
    """
    Generate GA4 conversion data with revenue.
    
    Args:
        pages: List of page URLs
        seed: Random seed
    
    Returns:
        DataFrame with columns: landing_page, conversions, conversion_rate,
                               revenue, avg_order_value
    """
    np.random.seed(seed)
    
    conversion_data = []
    
    for page in pages:
        sessions = int(np.random.lognormal(5, 1))
        
        # Conversion rates vary by page type
        if 'product' in page or 'pricing' in page:
            cvr = np.random.uniform(0.02, 0.08)
            aov = np.random.uniform(100, 500)
        elif 'blog' in page:
            cvr = np.random.uniform(0.001, 0.01)
            aov = np.random.uniform(50, 200)
        else:
            cvr = np.random.uniform(0.005, 0.02)
            aov = np.random.uniform(75, 300)
        
        conversions = int(sessions * cvr)
        revenue = conversions * aov
        
        conversion_data.append({
            'landing_page': page,
            'sessions': sessions,
            'conversions': conversions,
            'conversion_rate': cvr,
            'revenue': revenue,
            'avg_order_value': aov if conversions > 0 else 0,
        })
    
    return pd.DataFrame(conversion_data)


# =============================================================================
# SERP Data Generators
# =============================================================================


def generate_serp_features(
    queries: List[str],
    user_domain: str = "example.com",
    seed: int = 42,
) -> List[Dict]:
    """
    Generate mock SERP feature data for queries.
    
    Args:
        queries: List of query strings
        user_domain: User's domain
        seed: Random seed
    
    Returns:
        List of SERP feature dictionaries
    """
    np.random.seed(seed)
    
    serp_data = []
    
    competitor_domains = [
        "competitor1.com",
        "competitor2.com",
        "competitor3.com",
        "bigbrand.com",
        "authority-site.org",
    ]
    
    for query in queries:
        # Determine which SERP features are present
        has_featured_snippet = np.random.random() < 0.15
        has_paa = np.random.random() < 0.6
        has_video = np.random.random() < 0.25
        has_shopping = np.random.random() < 0.1
        has_local_pack = np.random.random() < 0.05
        has_ai_overview = np.random.random() < 0.2
        
        # Generate organic results
        organic_results = []
        positions = list(range(1, 11))
        np.random.shuffle(positions)
        
        # User's position
        user_position = positions[0]
        
        for i, pos in enumerate(positions[:10]):
            if i == 0:
                domain = user_domain
            else:
                domain = np.random.choice(competitor_domains)
            
            organic_results.append({
                'position': pos,
                'domain': domain,
                'url': f"https://{domain}/page-{i}",
                'title': f"Title for {query} - {domain}",
            })
        
        # Sort by position
        organic_results = sorted(organic_results, key=lambda x: x['position'])
        
        serp_data.append({
            'query': query,
            'user_position': user_position,
            'features': {
                'featured_snippet': {
                    'present': has_featured_snippet,
                    'domain': np.random.choice(competitor_domains) if has_featured_snippet else None,
                },
                'people_also_ask': {
                    'present': has_paa,
                    'count': np.random.randint(3, 6) if has_paa else 0,
                },
                'video_carousel': {
                    'present': has_video,
                },
                'shopping_results': {
                    'present': has_shopping,
                },
                'local_pack': {
                    'present': has_local_pack,
                },
                'ai_overview': {
                    'present': has_ai_overview,
                },
            },
            'organic_results': organic_results,
        })
    
    return serp_data


# =============================================================================
# Internal Link Graph Generators
# =============================================================================


def generate_internal_link_graph(
    pages: List[str],
    seed: int = 42,
) -> pd.DataFrame:
    """
    Generate internal link graph (adjacency list).
    
    Args:
        pages: List of page URLs
        seed: Random seed
    
    Returns:
        DataFrame with columns: from_url, to_url, anchor_text
    """
    np.random.seed(seed)
    
    links = []
    
    # Homepage links to many pages
    homepage = pages[0] if pages else "/home"
    
    for page in pages[1:10]:  # Link to first 9 pages
        links.append({
            'from_url': homepage,
            'to_url': page,
            'anchor_text': f"Link to {page}",
        })
    
    # Random internal links
    for from_page in pages:
        # Each page links to 2-5 other pages
        num_links = np.random.randint(2, 6)
        to_pages = np.random.choice(
            [p for p in pages if p != from_page],
            size=min(num_links, len(pages) - 1),
            replace=False,
        )
        
        for to_page in to_pages:
            links.append({
                'from_url': from_page,
                'to_url': to_page,
                'anchor_text': f"Link text for {to_page}",
            })
    
    return pd.DataFrame(links)


# =============================================================================
# Algorithm Update Fixtures
# =============================================================================


def generate_algorithm_updates(
    start_date: str,
    end_date: str,
) -> List[Dict]:
    """
    Generate mock algorithm update data.
    
    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
    
    Returns:
        List of algorithm update dictionaries
    """
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    updates = [
        {
            'date': (start + timedelta(days=30)).strftime('%Y-%m-%d'),
            'name': 'Core Update January 2024',
            'type': 'core',
            'description': 'Broad core algorithm update',
        },
        {
            'date': (start + timedelta(days=120)).strftime('%Y-%m-%d'),
            'name': 'Spam Update April 2024',
            'type': 'spam',
            'description': 'Targeted spam reduction',
        },
        {
            'date': (start + timedelta(days=210)).strftime('%Y-%m-%d'),
            'name': 'Helpful Content Update July 2024',
            'type': 'helpful_content',
            'description': 'Reward genuinely helpful content',
        },
        {
            'date': (start + timedelta(days=330)).strftime('%Y-%m-%d'),
            'name': 'Core Update November 2024',
            'type': 'core',
            'description': 'Broad core algorithm update',
        },
    ]
    
    # Filter to date range
    return [u for u in updates if start_date <= u['date'] <= end_date]


# =============================================================================
# Pytest Fixtures
# =============================================================================


@pytest.fixture
def sample_date_range():
    """Standard date range for tests: 16 months of data."""
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=480)  # ~16 months
    return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')


@pytest.fixture
def sample_gsc_daily_data(sample_date_range):
    """Generate sample GSC daily time series data."""
    start_date, _ = sample_date_range
    return generate_daily_gsc_data(
        start_date=start_date,
        days=480,
        base_clicks=1000,
        base_impressions=10000,
        trend_slope=-0.001,  # Slight decline
        seasonal_amplitude=0.15,
        noise_level=0.05,
        change_points=[(180, -0.15)],  # Drop at day 180
    )


@pytest.fixture
def sample_gsc_page_data(sample_date_range):
    """Generate sample per-page GSC data."""
    start_date, _ = sample_date_range
    return generate_page_level_gsc_data(
        start_date=start_date,
        days=480,
        num_pages=20,
    )


@pytest.fixture
def sample_gsc_query_data(sample_date_range):
    """Generate sample per-query GSC data."""
    start_date, _ = sample_date_range
    return generate_query_level_gsc_data(
        start_date=start_date,
        days=480,
        num_queries=50,
    )


@pytest.fixture
def sample_pages():
    """Standard set of page URLs for testing."""
    return [
        "/",
        "/blog/post-1",
        "/blog/post-2",
        "/blog/post-3",
        "/product/feature-a",
        "/product/feature-b",
        "/pricing",
        "/about",
        "/contact",
        "/resources/guide-1",
        "/resources/guide-2",
    ]


@pytest.fixture
def sample_queries():
    """Standard set of queries for testing."""
    return [
        "best crm software",
        "crm pricing comparison",
        "crm for small business",
        "how to implement crm",
        "crm features",
        "top crm tools 2024",
        "crm vs erp",
        "free crm software",
        "cloud crm solutions",
        "crm integration guide",
    ]


@pytest.fixture
def sample_query_page_mapping(sample_queries, sample_pages):
    """Generate query-to-page mapping data."""
    return generate_query_page_mapping(
        queries=sample_queries,
        pages=sample_pages,
    )


@pytest.fixture
def sample_ga4_landing_data(sample_pages):
    """Generate GA4 landing page data."""
    return generate_ga4_landing_page_data(pages=sample_pages)


@pytest.fixture
def sample_ga4_conversion_data(sample_pages):
    """Generate GA4 conversion data."""
    return generate_ga4