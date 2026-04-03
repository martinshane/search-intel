"""
Integration test for full report generation flow.

Tests the complete pipeline:
1. Creates test project with mock GSC/GA4 data
2. Triggers module execution via /api/modules/run
3. Verifies all active modules (1, 2, 5) produce valid JSON output
4. Confirms report UI endpoint returns complete data
"""

import pytest
import asyncio
from datetime import datetime, timedelta
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
import json

from app.main import app
from app.models import Project, ModuleResult, User, OAuthConnection
from app.database import get_db, engine
from app.models import Base


@pytest.fixture
async def test_db():
    """Create test database tables."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    yield
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest.fixture
async def test_user(test_db):
    """Create a test user."""
    async with AsyncSession(engine) as session:
        user = User(
            email="test@example.com",
            google_id="test_google_id_123",
            name="Test User",
            picture=None
        )
        session.add(user)
        await session.commit()
        await session.refresh(user)
        return user


@pytest.fixture
async def test_oauth_connections(test_db, test_user):
    """Create test OAuth connections for GSC and GA4."""
    async with AsyncSession(engine) as session:
        gsc_connection = OAuthConnection(
            user_id=test_user.id,
            service="gsc",
            access_token="mock_gsc_token",
            refresh_token="mock_gsc_refresh",
            token_expiry=datetime.utcnow() + timedelta(hours=1),
            scope="https://www.googleapis.com/auth/webmasters.readonly"
        )
        ga4_connection = OAuthConnection(
            user_id=test_user.id,
            service="ga4",
            access_token="mock_ga4_token",
            refresh_token="mock_ga4_refresh",
            token_expiry=datetime.utcnow() + timedelta(hours=1),
            scope="https://www.googleapis.com/auth/analytics.readonly"
        )
        session.add(gsc_connection)
        session.add(ga4_connection)
        await session.commit()
        return gsc_connection, ga4_connection


@pytest.fixture
async def test_project(test_db, test_user):
    """Create a test project."""
    async with AsyncSession(engine) as session:
        project = Project(
            user_id=test_user.id,
            domain="testsite.com",
            gsc_property="sc-domain:testsite.com",
            ga4_property_id="123456789",
            status="ready",
            last_data_fetch=None,
            created_at=datetime.utcnow()
        )
        session.add(project)
        await session.commit()
        await session.refresh(project)
        return project


def generate_mock_gsc_data():
    """Generate comprehensive mock GSC data for 16 months."""
    data = {
        "daily_data": [],
        "page_data": [],
        "query_data": [],
        "query_page_data": []
    }
    
    # Generate 480 days of daily data (16 months)
    start_date = datetime.utcnow() - timedelta(days=480)
    base_clicks = 1000
    
    for i in range(480):
        date = start_date + timedelta(days=i)
        # Add weekly seasonality and slight downward trend
        weekly_cycle = 1.0 + 0.15 * ((i % 7) / 7.0 - 0.5)
        trend = 1.0 - (i / 480) * 0.15  # 15% decline over period
        noise = 1.0 + (hash(date.isoformat()) % 100 - 50) / 500.0
        
        clicks = int(base_clicks * weekly_cycle * trend * noise)
        impressions = clicks * 20
        
        data["daily_data"].append({
            "date": date.strftime("%Y-%m-%d"),
            "clicks": clicks,
            "impressions": impressions,
            "ctr": clicks / impressions if impressions > 0 else 0,
            "position": 8.5 + (i / 480) * 2.5  # Position declining from 8.5 to 11
        })
    
    # Generate page-level data
    pages = [
        {"url": "https://testsite.com/", "base_clicks": 300, "trend": "stable"},
        {"url": "https://testsite.com/blog/best-widgets", "base_clicks": 250, "trend": "declining"},
        {"url": "https://testsite.com/pricing", "base_clicks": 200, "trend": "growing"},
        {"url": "https://testsite.com/blog/widget-guide", "base_clicks": 150, "trend": "declining"},
        {"url": "https://testsite.com/features", "base_clicks": 100, "trend": "stable"},
    ]
    
    for page_info in pages:
        for i in range(480):
            date = start_date + timedelta(days=i)
            
            if page_info["trend"] == "declining":
                trend_factor = 1.0 - (i / 480) * 0.40  # 40% decline
            elif page_info["trend"] == "growing":
                trend_factor = 1.0 + (i / 480) * 0.30  # 30% growth
            else:
                trend_factor = 1.0
            
            clicks = int(page_info["base_clicks"] * trend_factor)
            impressions = clicks * 25
            
            data["page_data"].append({
                "date": date.strftime("%Y-%m-%d"),
                "page": page_info["url"],
                "clicks": clicks,
                "impressions": impressions,
                "ctr": clicks / impressions if impressions > 0 else 0,
                "position": 7.0 if page_info["trend"] == "growing" else (15.0 if page_info["trend"] == "declining" else 10.0)
            })
    
    # Generate query-level data
    queries = [
        {"query": "best widgets", "impressions": 50000, "position": 12},
        {"query": "widget comparison", "impressions": 30000, "position": 8},
        {"query": "widget pricing", "impressions": 25000, "position": 6},
        {"query": "how to use widgets", "impressions": 20000, "position": 15},
        {"query": "widget reviews", "impressions": 18000, "position": 9},
        {"query": "top widget brands", "impressions": 15000, "position": 11},
        {"query": "widget features", "impressions": 12000, "position": 7},
        {"query": "widget tutorial", "impressions": 10000, "position": 14},
    ]
    
    for query_info in queries:
        impressions = query_info["impressions"]
        position = query_info["position"]
        # CTR based on position (rough approximation)
        ctr = max(0.001, 0.3 * (20 - position) / 20)
        clicks = int(impressions * ctr)
        
        data["query_data"].append({
            "query": query_info["query"],
            "clicks": clicks,
            "impressions": impressions,
            "ctr": ctr,
            "position": position
        })
    
    # Generate query-page mapping
    query_page_pairs = [
        ("best widgets", "https://testsite.com/blog/best-widgets", 25000, 12),
        ("best widgets", "https://testsite.com/", 15000, 18),  # Cannibalization
        ("widget comparison", "https://testsite.com/blog/best-widgets", 30000, 8),
        ("widget pricing", "https://testsite.com/pricing", 25000, 6),
        ("how to use widgets", "https://testsite.com/blog/widget-guide", 20000, 15),
        ("widget reviews", "https://testsite.com/blog/best-widgets", 18000, 9),
        ("top widget brands", "https://testsite.com/blog/best-widgets", 15000, 11),
        ("widget features", "https://testsite.com/features", 12000, 7),
        ("widget tutorial", "https://testsite.com/blog/widget-guide", 10000, 14),
    ]
    
    for query, page, impressions, position in query_page_pairs:
        ctr = max(0.001, 0.3 * (20 - position) / 20)
        clicks = int(impressions * ctr)
        
        data["query_page_data"].append({
            "query": query,
            "page": page,
            "clicks": clicks,
            "impressions": impressions,
            "ctr": ctr,
            "position": position
        })
    
    return data


def generate_mock_ga4_data():
    """Generate mock GA4 data."""
    return {
        "landing_pages": [
            {
                "landingPage": "/",
                "sessions": 5000,
                "users": 4500,
                "bounceRate": 0.45,
                "avgSessionDuration": 120.5,
                "conversions": 250
            },
            {
                "landingPage": "/blog/best-widgets",
                "sessions": 4000,
                "users": 3800,
                "bounceRate": 0.72,  # High bounce rate
                "avgSessionDuration": 35.2,  # Low engagement
                "conversions": 80
            },
            {
                "landingPage": "/pricing",
                "sessions": 3500,
                "users": 3200,
                "bounceRate": 0.38,
                "avgSessionDuration": 180.3,
                "conversions": 420
            },
            {
                "landingPage": "/blog/widget-guide",
                "sessions": 2500,
                "users": 2300,
                "bounceRate": 0.68,
                "avgSessionDuration": 45.8,
                "conversions": 50
            },
            {
                "landingPage": "/features",
                "sessions": 2000,
                "users": 1850,
                "bounceRate": 0.42,
                "avgSessionDuration": 95.6,
                "conversions": 120
            },
        ],
        "traffic_by_source": [
            {"source": "google", "medium": "organic", "sessions": 12000, "conversions": 600},
            {"source": "(direct)", "medium": "(none)", "sessions": 3000, "conversions": 200},
            {"source": "newsletter", "medium": "email", "sessions": 1500, "conversions": 120},
            {"source": "facebook", "medium": "social", "sessions": 500, "conversions": 20},
        ]
    }


def generate_mock_serp_data():
    """Generate mock SERP data from DataForSEO."""
    return {
        "serp_results": [
            {
                "keyword": "best widgets",
                "location": "United States",
                "organic_results": [
                    {"position": 1, "url": "https://competitor1.com/best-widgets", "domain": "competitor1.com"},
                    {"position": 2, "url": "https://competitor2.com/widgets", "domain": "competitor2.com"},
                    {"position": 3, "url": "https://competitor3.com/top-widgets", "domain": "competitor3.com"},
                    {"position": 12, "url": "https://testsite.com/blog/best-widgets", "domain": "testsite.com"},
                ],
                "features": [
                    {"type": "featured_snippet", "position": 0, "domain": "competitor1.com"},
                    {"type": "people_also_ask", "position": 1, "questions": 4},
                    {"type": "video_carousel", "position": 5},
                ]
            },
            {
                "keyword": "widget comparison",
                "location": "United States",
                "organic_results": [
                    {"position": 1, "url": "https://competitor2.com/comparison", "domain": "competitor2.com"},
                    {"position": 8, "url": "https://testsite.com/blog/best-widgets", "domain": "testsite.com"},
                ],
                "features": [
                    {"type": "people_also_ask", "position": 2, "questions": 3},
                ]
            },
            {
                "keyword": "widget pricing",
                "location": "United States",
                "organic_results": [
                    {"position": 1, "url": "https://competitor1.com/pricing", "domain": "competitor1.com"},
                    {"position": 6, "url": "https://testsite.com/pricing", "domain": "testsite.com"},
                ],
                "features": [
                    {"type": "shopping_results", "position": 0},
                ]
            },
        ]
    }


def generate_mock_crawl_data():
    """Generate mock site crawl data."""
    return {
        "pages": [
            {
                "url": "https://testsite.com/",
                "title": "Best Widgets - TestSite",
                "meta_description": "Find the best widgets for your needs.",
                "h1": "Welcome to TestSite",
                "word_count": 1200,
                "internal_links_count": 25,
                "outgoing_links": [
                    {"url": "https://testsite.com/blog/best-widgets", "anchor": "Read our guide"},
                    {"url": "https://testsite.com/pricing", "anchor": "See pricing"},
                    {"url": "https://testsite.com/features", "anchor": "Features"},
                ],
                "schema_types": ["Organization", "WebSite"],
                "canonical": "https://testsite.com/"
            },
            {
                "url": "https://testsite.com/blog/best-widgets",
                "title": "Best Widgets 2025 - Complete Guide",
                "meta_description": "Our complete guide to the best widgets.",
                "h1": "Best Widgets 2025",
                "word_count": 450,  # Thin content
                "internal_links_count": 5,
                "outgoing_links": [
                    {"url": "https://testsite.com/", "anchor": "Home"},
                ],
                "schema_types": ["Article"],
                "canonical": "https://testsite.com/blog/best-widgets"
            },
            {
                "url": "https://testsite.com/pricing",
                "title": "Widget Pricing - TestSite",
                "meta_description": "Affordable widget pricing plans.",
                "h1": "Pricing Plans",
                "word_count": 800,
                "internal_links_count": 15,
                "outgoing_links": [
                    {"url": "https://testsite.com/", "anchor": "Home"},
                    {"url": "https://testsite.com/features", "anchor": "See features"},
                ],
                "schema_types": ["Product", "Offer"],
                "canonical": "https://testsite.com/pricing"
            },
        ],
        "internal_links": [
            {"from_url": "https://testsite.com/", "to_url": "https://testsite.com/blog/best-widgets", "anchor": "Read our guide"},
            {"from_url": "https://testsite.com/", "to_url": "https://testsite.com/pricing", "anchor": "See pricing"},
            {"from_url": "https://testsite.com/", "to_url": "https://testsite.com/features", "anchor": "Features"},
            {"from_url": "https://testsite.com/blog/best-widgets", "to_url": "https://testsite.com/", "anchor": "Home"},
            {"from_url": "https://testsite.com/pricing", "to_url": "https://testsite.com/", "anchor": "Home"},
            {"from_url": "https://testsite.com/pricing", "to_url": "https://testsite.com/features", "anchor": "See features"},
        ]
    }


@pytest.fixture
async def mock_data_cache(test_project):
    """Create mock cached data in database."""
    from app.services.data_cache import DataCacheService
    
    cache_service = DataCacheService()
    
    # Cache GSC data
    gsc_data = generate_mock_gsc_data()
    await cache_service.set_cache(
        project_id=test_project.id,
        data_type="gsc_daily",
        data=gsc_data["daily_data"]
    )
    await cache_service.set_cache(
        project_id=test_project.id,
        data_type="gsc_pages",
        data=gsc_data["page_data"]
    )
    await cache_service.set_cache(
        project_id=test_project.id,
        data_type="gsc_queries",
        data=gsc_data["query_data"]
    )
    await cache_service.set_cache(
        project_id=test_project.id,
        data_type="gsc_query_page",
        data=gsc_data["query_page_data"]
    )
    
    # Cache GA4 data
    ga4_data = generate_mock_ga4_data()
    await cache_service.set_cache(
        project_id=test_project.id,
        data_type="ga4_landing_pages",
        data=ga4_data["landing_pages"]
    )
    await cache_service.set_cache(
        project_id=test_project.id,
        data_type="ga4_traffic_sources",
        data=ga4_data["traffic_by_source"]
    )
    
    # Cache SERP data
    serp_data = generate_mock_serp_data()
    await cache_service.set_cache(
        project_id=test_project.id,
        data_type="serp_results",
        data=serp_data["serp_results"]
    )
    
    # Cache crawl data
    crawl_data = generate_mock_crawl_data()
    await cache_service.set_cache(
        project_id=test_project.id,
        data_type="site_crawl",
        data=crawl_data
    )
    
    return {
        "gsc": gsc_data,
        "ga4": ga4_data,
        "serp": serp_data,
        "crawl": crawl_data
    }


@pytest.mark.asyncio
async def test_module_1_execution(test_project, test_user, test_oauth_connections, mock_data_cache):
    """Test Module 1: Health & Trajectory analysis."""
    async with AsyncClient(app=app, base_url="http://test") as client:
        # Trigger module 1 execution
        response = await client.post(
            f"/api/modules/run",
            json={
                "project_id": test_project.id,
                "module_number": 1
            },
            headers={"X-User-ID": str(test_user.id)}
        )
        
        assert response.status_code == 200
        result = response.json()
        
        # Verify job was created
        assert "job_id" in result
        assert result["status"] == "processing"
        
        # Wait for processing (in real implementation, this would be async job queue)
        await asyncio.sleep(2)
        
        # Check module result was stored
        async with AsyncSession(engine) as session:
            stmt = select(ModuleResult).where(
                ModuleResult.project_id == test_project.id,
                ModuleResult.module_number == 1
            )
            db_result = await session.execute(stmt)
            module_result = db_result.scalar_one_or_none()
            
            assert module_result is not None
            assert module_result.status == "completed"
            assert module_result.result_data is not None
            
            # Validate schema
            data = module_result.result_data
            assert "overall_direction" in data
            assert data["overall_direction"] in ["strong_growth", "growth", "flat", "decline", "strong_decline"]
            assert "trend_slope_pct_per_month" in data
            assert isinstance(data["trend_slope_pct_per_month"], (int, float))
            assert "change_points" in data
            assert isinstance(data["change_points"], list)
            assert "seasonality" in data
            assert "best_day" in data["seasonality"]
            assert "worst_day" in data["seasonality"]
            assert "forecast" in data
            assert "30d" in data["forecast"]
            assert "60d" in data["forecast"]
            assert "90d" in data["forecast"]
            
            # Validate forecast structure
            for period in ["30d", "60d", "90d"]:
                assert "clicks" in data["forecast"][period]
                assert "ci_low" in data["forecast"][period]
                assert "ci_high" in data["forecast"][period]
            
            # Validate change points structure
            if len(data["change_points"]) > 0:
                cp = data["change_points"][0]
                assert "date" in cp
                assert "magnitude" in cp
                assert "direction" in cp


@pytest.mark.asyncio
async def test_module_2_execution(test_project, test_user, test_oauth_connections, mock_data_cache):
    """Test Module 2: Page-Level Triage analysis."""
    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.post(
            f"/api/modules/run",
            json={
                "project_id": test_project.id,
                "module_number": 2
            },
            headers={"X-User-ID": str(test_user.id)}
        )
        
        assert response.status_code == 200
        
        await asyncio.sleep(2)
        
        async with AsyncSession(engine) as session:
            stmt = select(ModuleResult).where(
                ModuleResult.project_id == test_project.id,
                ModuleResult.module_number == 2
            )
            db_result = await session.execute(stmt)
            module_result = db_result.scalar_one_or_none()
            
            assert module_result is not None
            assert module_result.status == "completed"
            
            data = module_result.result_data
            assert "pages" in data
            assert isinstance(data["pages"], list)
            assert "summary" in data
            
            # Validate summary
            summary = data["summary"]
            assert "total_pages_analyzed" in summary
            assert "growing" in summary
            assert "stable" in summary
            assert "decaying" in summary
            assert "critical" in summary
            assert "total_recoverable_clicks_monthly" in summary
            
            # Validate page structure
            if len(data["pages"]) > 0:
                page = data["pages"][0]
                assert "url" in page
                assert "bucket" in page
                assert page["bucket"] in ["growing", "stable", "decaying", "critical"]
                assert "current_monthly_clicks" in page
                assert "trend_slope" in page
                assert "priority_score" in page
                assert "recommended_action" in page
                
                # Should have CTR anomaly detection
                if "ctr_anomaly" in page and page["ctr_anomaly"]:
                    assert "ctr_expected" in page
                    assert "ctr_actual" in page


@pytest.mark.asyncio
async def test_module_5_execution(test_project, test_user, test_oauth_connections, mock_data_cache):
    """Test Module 5: The Gameplan synthesis."""
    # First run modules 1-4 to provide inputs
    async with AsyncClient(app=app, base_url="http://test") as client:
        for module_num in [1, 2, 3, 4]:
            response = await client.post(
                f"/api/modules/run",
                json={
                    "project_id": test_project.id,
                    "module_number": module_num
                },
                headers={"X-User-ID": str(test_user.id)}
            )
            assert response.status_code == 200
        
        await asyncio.sleep(5)  # Wait for all modules to complete
        
        # Now run module 5
        response = await client.post(
            f"/api/modules/run",
            json={
                "project_id": test_project.id,
                "module_number": 5
            },
            headers={"X-User-ID": str(test_user.id)}
        )
        
        assert response.status_code == 200
        
        await asyncio.sleep(2)
        
        async with AsyncSession(engine) as session:
            stmt = select(ModuleResult).where(
                ModuleResult.project_id == test_project.id,
                ModuleResult.module_number == 5
            )
            db_result = await session.execute(stmt)
            module_result = db_result.scalar_one_or_none()
            
            assert module_result is not None
            assert module_result.status == "completed"
            
            data = module_result.result_data
            assert "critical" in data
            assert "quick_wins" in data
            assert "strategic" in data
            assert "structural" in data
            assert "total_estimated_monthly_click_recovery" in data
            assert "total_estimated_monthly_click_growth" in data
            assert "narrative" in data
            
            # Validate action item structure
            for category in ["critical", "quick_wins", "strategic", "structural"]:
                assert isinstance(data[category], list)
                if len(data[category]) > 0:
                    action = data[category][0]
                    assert "action" in action
                    assert "impact" in action
                    assert "effort" in action
                    assert action["effort"] in ["low", "medium", "high"]


@pytest.mark.asyncio
async def test_complete_report_generation_flow(test_project, test_user, test_oauth_connections, mock_data_cache):
    """Test complete report generation from start to finish."""
    async with AsyncClient(app=app, base_url="http://test") as client:
        # Trigger full report generation (all active modules)
        response = await client.post(
            f"/api/projects/{test_project.id}/generate-report",
            headers={"X-User-ID": str(test_user.id)}
        )
        
        assert response.status_code == 200
        result = response.json()
        assert "job_id" in result
        
        # Wait for all modules to complete
        await asyncio.sleep(10)
        
        # Check all active modules completed
        async with AsyncSession(engine) as session:
            for module_num in [1, 2, 5]:  # Active modules
                stmt = select(ModuleResult).where(
                    ModuleResult.project_id == test_project.id,
                    ModuleResult.module_number == module_num
                )
                db_result = await session.execute(stmt)
                module_result = db_result.scalar_one_or_none()
                
                assert module_result is not None, f"Module {module_num} result not found"
                assert module_result.status == "completed", f"Module {module_num} status: {module_result.status}"
                assert module_result.result_data is not None
        
        # Verify report UI endpoint returns complete data
        response = await client.get(
            f"/api/projects/{test_project.id}/report",
            headers={"X-User-ID": str(test_user.id)}
        )
        
        assert response.status_code == 200
        report = response.json()
        
        # Verify report structure
        assert "project" in report
        assert "modules" in report
        assert report["project"]["id"] == test_project.id
        assert report["project"]["domain"] == test_project.domain
        
        # Verify all active modules are present
        module_numbers = [m["module_number"] for m in report["modules"]]
        assert 1 in module_numbers
        assert 2 in module_numbers
        assert 5 in module_numbers
        
        # Verify each module has valid data
        for module in report["modules"]:
            assert "module_number" in module
            assert "module_name" in module
            assert "status" in module
            assert module["status"] == "completed"
            assert "result_data" in module
            assert module["result_data"] is not None
            assert "generated_at" in module


@pytest.mark.asyncio
async def test_module_error_handling(test_project, test_user, test_oauth_connections):
    """Test error handling when module execution fails."""
    async with AsyncClient(app=app, base_url="http://test") as client:
        # Try to run module without cached data
        response = await client.post(
            f"/api/modules/run",
            json={
                "project_id": test_project.id,
                "module_number": 1
            },
            headers={"X-User-ID": str(test_user.id)}
        )
        
        # Should handle gracefully
        assert response.status_code in [200, 400, 422]
        
        if response.status_code == 200:
            await asyncio.sleep(2)
            
            async with AsyncSession(engine) as session:
                stmt = select(ModuleResult).where(
                    ModuleResult.project_id == test_project.id,
                    ModuleResult.module_number == 1
                )
                db_result = await session.execute(stmt)
                module_result = db_result.scalar_one_or_none()
                
                if module_result:
                    # If module ran, it should either complete or fail gracefully
                    assert module_result.status in ["completed", "failed"]
                    if module_result.status == "failed":
                        assert "error" in module_result.result_data


@pytest.mark.asyncio
async def test_concurrent_module_execution(test_project, test_user, test_oauth_connections, mock_data_cache):
    """Test that multiple modules can be triggered concurrently."""
    async with AsyncClient(app=app, base_url="http://test") as client:
        # Trigger multiple modules concurrently
        tasks = []
        for module_num in [1, 2]:
            task = client.post(
                f"/api/modules/run",
                json={
                    "project_id": test_project.id,
                    "module_number": module_num
                },
                headers={"X-User-ID": str(test_user.id)}
            )
            tasks.append(task)
        
        responses = await asyncio.gather(*tasks)
        
        for response in responses:
            assert response.status_code == 200
        
        await asyncio.sleep(5)
        
        # Both should complete
        async with AsyncSession(engine) as session:
            for module_num in [1, 2]:
                stmt = select(ModuleResult).where(
                    ModuleResult.project_id == test_project.id,
                    ModuleResult.module_number == module_num
                )
                db_result = await session.execute(stmt)
                module_result = db_result.scalar_one_or_none()
                
                assert module_result is not None
                assert module_result.status == "completed"


@pytest.mark.asyncio
async def test_report_caching_and_refresh(test_project, test_user, test_oauth_connections, mock_data_cache):
    """Test that reports are cached and can be refreshed."""
    async with AsyncClient(app=app, base_url="http://test") as client:
        # Generate initial report
        response = await client.post(
            f"/api/projects/{test_project.id}/generate-report",
            headers={"X-User-ID": str(test_user.id)}
        )
        assert response.status_code == 200
        
        await asyncio.sleep(10)
        
        # Get report
        response1 = await client.get(
            f"/api/projects/{test_project.id}/report",
            headers={"X-User-ID": str(test_user.id)}
        )
        assert response1.status_code == 200
        report1 = response1.json()
        first_generated_at = report1["modules"][0]["generated_at"]
        
        # Get report again (should be cached)
        response2 = await client.get(
            f"/api/projects/{test_project.id}/report",
            headers={"X-User-ID": str(test_user.id)}
        )
        assert response2.status_code == 200
        report2 = response2.json()
        second_generated_at = report2["modules"][0]["generated_at"]
        
        # Should be same result
        assert first_generated_at == second_generated_at
        
        # Trigger refresh
        response = await client.post(
            f"/api/projects/{test_project.id}/generate-report",
            json={"force_refresh": True},
            headers={"X-User-ID": str(test_user.id)}
        )
        assert response.status_code == 200
        
        await asyncio.sleep(10)
        
        # Get refreshed report
        response3 = await client.get(
            f"/api/projects/{test_project.id}/report",
            headers={"X-User-ID": str(test_user.id)}
        )
        assert response3.status_code == 200
        report3 = response3.json()
        third_generated_at = report3["modules"][0]["generated_at"]
        
        # Should be different timestamp
        assert third_generated_at != first_generated_at


@pytest.mark.asyncio
async def test_partial_report_with_failed_module(test_project, test_user, test_oauth_connections, mock_data_cache):
    """Test that report can still be generated if one module fails."""
    async with AsyncClient(app=app, base_url="http://test") as client:
        # Run module 1 successfully
        response = await client.post(
            f"/api/modules/run",
            json={
                "project_id": test_project.id,
                "module_number": 1
            },
            headers={"X-User-ID": str(test_user.id)}
        )
        assert response.status_code == 200
        await asyncio.sleep(2)
        
        # Manually create a failed module result
        async with AsyncSession(engine) as session:
            failed_result = ModuleResult(
                project_id=test_project.id,
                module_number=2,
                status="failed",
                result_data={"error": "Test failure"},
                generated_at=datetime.utcnow()
            )
            session.add(failed_result)
            await session.commit()
        
        # Get report (should return partial data)
        response = await client.get(
            f"/api/projects/{test_project.id}/report",
            headers={"X-User-ID": str(test_user.id)}
        )
        
        assert response.status_code == 200
        report = response.json()
        
        # Should have both modules but with different statuses
        assert len(report["modules"]) >= 2
        statuses = {m["module_number"]: m["status"] for m in report["modules"]}
        assert statuses[1] == "completed"
        assert statuses[2] == "failed"


@pytest.mark.asyncio
async def test_report_json_serialization(test_project, test_user, test_oauth_connections, mock_data_cache):
    """Test that all report data is properly JSON serializable."""
    async with AsyncClient(app=app, base_url="http://test") as client:
        # Generate report
        response = await client.post(
            f"/api/projects/{test_project.id}/generate-report",
            headers={"X-User-ID": str(test_user.id)}
        )
        assert response.status_code == 200
        
        await asyncio.sleep(10)
        
        # Get report
        response = await client.get(
            f"/api/projects/{test_project.id}/report",
            headers={"X-User-ID": str(test_user.id)}
        )
        
        assert response.status_code == 200
        report_json = response.text
        
        # Should be valid JSON
        try:
            report = json.loads(report_json)
            assert isinstance(report, dict)
        except json.JSONDecodeError as e:
            pytest.fail(f"Report is not valid JSON: {e}")
        
        # Should be able to re-serialize
        try:
            json.dumps(report)
        except TypeError as e:
            pytest.fail(f"Report contains non-serializable data: {e}")
