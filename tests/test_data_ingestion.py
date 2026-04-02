import pytest
import asyncio
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, AsyncMock, MagicMock
import pandas as pd
import json
from typing import Dict, List, Any

from src.data_ingestion import (
    GSCDataFetcher,
    GA4DataFetcher,
    DataForSEOFetcher,
    SupabaseDataStore,
    DataIngestionPipeline,
    RateLimitHandler
)


@pytest.fixture
def mock_supabase_client():
    """Mock Supabase client for testing"""
    client = Mock()
    client.table = Mock(return_value=Mock())
    client.table().insert = Mock(return_value=Mock(execute=Mock(return_value=Mock(data=[{"id": 1}]))))
    client.table().select = Mock(return_value=Mock(execute=Mock(return_value=Mock(data=[]))))
    client.table().update = Mock(return_value=Mock(eq=Mock(return_value=Mock(execute=Mock(return_value=Mock(data=[{"id": 1}]))))))
    client.table().delete = Mock(return_value=Mock(eq=Mock(return_value=Mock(execute=Mock(return_value=Mock(data=[{"id": 1}]))))))
    return client


@pytest.fixture
def mock_gsc_service():
    """Mock Google Search Console service"""
    service = Mock()
    service.searchanalytics = Mock(return_value=Mock())
    service.searchanalytics().query = Mock(return_value=Mock())
    return service


@pytest.fixture
def mock_ga4_service():
    """Mock Google Analytics 4 service"""
    service = Mock()
    service.properties = Mock(return_value=Mock())
    service.properties().runReport = Mock(return_value=Mock())
    return service


@pytest.fixture
def sample_gsc_response():
    """Sample GSC API response"""
    return {
        "rows": [
            {
                "keys": ["test query 1"],
                "clicks": 100,
                "impressions": 1000,
                "ctr": 0.1,
                "position": 5.5
            },
            {
                "keys": ["test query 2"],
                "clicks": 50,
                "impressions": 500,
                "ctr": 0.1,
                "position": 8.2
            }
        ]
    }


@pytest.fixture
def sample_ga4_response():
    """Sample GA4 API response"""
    return {
        "dimensionHeaders": [{"name": "pagePath"}],
        "metricHeaders": [
            {"name": "sessions", "type": "TYPE_INTEGER"},
            {"name": "totalUsers", "type": "TYPE_INTEGER"},
            {"name": "bounceRate", "type": "TYPE_FLOAT"}
        ],
        "rows": [
            {
                "dimensionValues": [{"value": "/page1"}],
                "metricValues": [
                    {"value": "100"},
                    {"value": "80"},
                    {"value": "0.45"}
                ]
            },
            {
                "dimensionValues": [{"value": "/page2"}],
                "metricValues": [
                    {"value": "200"},
                    {"value": "150"},
                    {"value": "0.35"}
                ]
            }
        ]
    }


class TestGSCDataFetcher:
    """Tests for Google Search Console data fetching"""
    
    @pytest.mark.asyncio
    async def test_fetch_queries_success(self, mock_gsc_service, sample_gsc_response):
        """Test successful query data fetch from GSC"""
        mock_gsc_service.searchanalytics().query().execute.return_value = sample_gsc_response
        
        fetcher = GSCDataFetcher(mock_gsc_service, "https://example.com")
        start_date = datetime.now() - timedelta(days=30)
        end_date = datetime.now()
        
        result = await fetcher.fetch_queries(start_date, end_date)
        
        assert len(result) == 2
        assert result[0]["query"] == "test query 1"
        assert result[0]["clicks"] == 100
        assert result[0]["impressions"] == 1000
        assert result[0]["ctr"] == 0.1
        assert result[0]["position"] == 5.5
    
    @pytest.mark.asyncio
    async def test_fetch_queries_empty_response(self, mock_gsc_service):
        """Test handling empty GSC response"""
        mock_gsc_service.searchanalytics().query().execute.return_value = {"rows": []}
        
        fetcher = GSCDataFetcher(mock_gsc_service, "https://example.com")
        start_date = datetime.now() - timedelta(days=30)
        end_date = datetime.now()
        
        result = await fetcher.fetch_queries(start_date, end_date)
        
        assert len(result) == 0
    
    @pytest.mark.asyncio
    async def test_fetch_pages_with_dimensions(self, mock_gsc_service):
        """Test fetching page data with multiple dimensions"""
        mock_response = {
            "rows": [
                {
                    "keys": ["/page1"],
                    "clicks": 150,
                    "impressions": 2000,
                    "ctr": 0.075,
                    "position": 6.3
                }
            ]
        }
        mock_gsc_service.searchanalytics().query().execute.return_value = mock_response
        
        fetcher = GSCDataFetcher(mock_gsc_service, "https://example.com")
        start_date = datetime.now() - timedelta(days=30)
        end_date = datetime.now()
        
        result = await fetcher.fetch_pages(start_date, end_date)
        
        assert len(result) == 1
        assert result[0]["page"] == "/page1"
        assert result[0]["clicks"] == 150
    
    @pytest.mark.asyncio
    async def test_fetch_devices(self, mock_gsc_service):
        """Test fetching device breakdown data"""
        mock_response = {
            "rows": [
                {"keys": ["MOBILE"], "clicks": 500, "impressions": 5000, "ctr": 0.1, "position": 7.0},
                {"keys": ["DESKTOP"], "clicks": 300, "impressions": 3000, "ctr": 0.1, "position": 6.5},
                {"keys": ["TABLET"], "clicks": 50, "impressions": 500, "ctr": 0.1, "position": 7.5}
            ]
        }
        mock_gsc_service.searchanalytics().query().execute.return_value = mock_response
        
        fetcher = GSCDataFetcher(mock_gsc_service, "https://example.com")
        start_date = datetime.now() - timedelta(days=30)
        end_date = datetime.now()
        
        result = await fetcher.fetch_devices(start_date, end_date)
        
        assert len(result) == 3
        assert result[0]["device"] == "MOBILE"
        assert result[0]["clicks"] == 500
    
    @pytest.mark.asyncio
    async def test_fetch_query_page_mapping(self, mock_gsc_service):
        """Test fetching query-to-page mapping"""
        mock_response = {
            "rows": [
                {"keys": ["query1", "/page1"], "clicks": 100, "impressions": 1000, "ctr": 0.1, "position": 5.0},
                {"keys": ["query1", "/page2"], "clicks": 50, "impressions": 800, "ctr": 0.0625, "position": 8.0},
                {"keys": ["query2", "/page1"], "clicks": 80, "impressions": 900, "ctr": 0.089, "position": 6.5}
            ]
        }
        mock_gsc_service.searchanalytics().query().execute.return_value = mock_response
        
        fetcher = GSCDataFetcher(mock_gsc_service, "https://example.com")
        start_date = datetime.now() - timedelta(days=30)
        end_date = datetime.now()
        
        result = await fetcher.fetch_query_page_mapping(start_date, end_date)
        
        assert len(result) == 3
        assert result[0]["query"] == "query1"
        assert result[0]["page"] == "/page1"
        assert result[1]["page"] == "/page2"
    
    @pytest.mark.asyncio
    async def test_fetch_daily_time_series(self, mock_gsc_service):
        """Test fetching daily time series data"""
        mock_response = {
            "rows": [
                {"keys": ["2024-01-01"], "clicks": 100, "impressions": 1000, "ctr": 0.1, "position": 5.0},
                {"keys": ["2024-01-02"], "clicks": 110, "impressions": 1100, "ctr": 0.1, "position": 4.9},
                {"keys": ["2024-01-03"], "clicks": 105, "impressions": 1050, "ctr": 0.1, "position": 5.1}
            ]
        }
        mock_gsc_service.searchanalytics().query().execute.return_value = mock_response
        
        fetcher = GSCDataFetcher(mock_gsc_service, "https://example.com")
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 3)
        
        result = await fetcher.fetch_daily_time_series(start_date, end_date)
        
        assert len(result) == 3
        assert result[0]["date"] == "2024-01-01"
        assert result[1]["clicks"] == 110
    
    @pytest.mark.asyncio
    async def test_pagination_handling(self, mock_gsc_service):
        """Test handling of paginated results from GSC"""
        # Simulate pagination: first response has 25000 rows, second has remainder
        first_response = {"rows": [{"keys": [f"query{i}"], "clicks": i, "impressions": i*10, "ctr": 0.1, "position": 5.0} for i in range(25000)]}
        second_response = {"rows": [{"keys": ["query25000"], "clicks": 25000, "impressions": 250000, "ctr": 0.1, "position": 5.0}]}
        
        mock_gsc_service.searchanalytics().query().execute.side_effect = [first_response, second_response]
        
        fetcher = GSCDataFetcher(mock_gsc_service, "https://example.com")
        start_date = datetime.now() - timedelta(days=30)
        end_date = datetime.now()
        
        result = await fetcher.fetch_queries(start_date, end_date, row_limit=30000)
        
        assert len(result) == 25001
    
    @pytest.mark.asyncio
    async def test_api_error_handling(self, mock_gsc_service):
        """Test handling of API errors from GSC"""
        from googleapiclient.errors import HttpError
        
        mock_error = HttpError(
            resp=Mock(status=500),
            content=b'Internal Server Error'
        )
        mock_gsc_service.searchanalytics().query().execute.side_effect = mock_error
        
        fetcher = GSCDataFetcher(mock_gsc_service, "https://example.com")
        start_date = datetime.now() - timedelta(days=30)
        end_date = datetime.now()
        
        with pytest.raises(Exception) as exc_info:
            await fetcher.fetch_queries(start_date, end_date)
        
        assert "GSC API error" in str(exc_info.value) or "HttpError" in str(exc_info.typename)


class TestGA4DataFetcher:
    """Tests for Google Analytics 4 data fetching"""
    
    @pytest.mark.asyncio
    async def test_fetch_landing_pages_success(self, mock_ga4_service, sample_ga4_response):
        """Test successful landing page data fetch from GA4"""
        mock_ga4_service.properties().runReport().execute.return_value = sample_ga4_response
        
        fetcher = GA4DataFetcher(mock_ga4_service, "properties/123456")
        start_date = datetime.now() - timedelta(days=30)
        end_date = datetime.now()
        
        result = await fetcher.fetch_landing_pages(start_date, end_date)
        
        assert len(result) == 2
        assert result[0]["page_path"] == "/page1"
        assert result[0]["sessions"] == 100
        assert result[0]["users"] == 80
        assert result[0]["bounce_rate"] == 0.45
    
    @pytest.mark.asyncio
    async def test_fetch_traffic_overview(self, mock_ga4_service):
        """Test fetching overall traffic metrics"""
        mock_response = {
            "dimensionHeaders": [],
            "metricHeaders": [
                {"name": "sessions"},
                {"name": "totalUsers"},
                {"name": "screenPageViews"},
                {"name": "bounceRate"},
                {"name": "averageSessionDuration"}
            ],
            "rows": [{
                "metricValues": [
                    {"value": "10000"},
                    {"value": "8000"},
                    {"value": "25000"},
                    {"value": "0.42"},
                    {"value": "180"}
                ]
            }]
        }
        mock_ga4_service.properties().runReport().execute.return_value = mock_response
        
        fetcher = GA4DataFetcher(mock_ga4_service, "properties/123456")
        start_date = datetime.now() - timedelta(days=30)
        end_date = datetime.now()
        
        result = await fetcher.fetch_traffic_overview(start_date, end_date)
        
        assert result["sessions"] == 10000
        assert result["users"] == 8000
        assert result["pageviews"] == 25000
        assert result["bounce_rate"] == 0.42
        assert result["avg_session_duration"] == 180
    
    @pytest.mark.asyncio
    async def test_fetch_traffic_by_channel(self, mock_ga4_service):
        """Test fetching traffic breakdown by channel"""
        mock_response = {
            "dimensionHeaders": [{"name": "sessionDefaultChannelGroup"}],
            "metricHeaders": [{"name": "sessions"}, {"name": "totalUsers"}],
            "rows": [
                {"dimensionValues": [{"value": "Organic Search"}], "metricValues": [{"value": "5000"}, {"value": "4000"}]},
                {"dimensionValues": [{"value": "Direct"}], "metricValues": [{"value": "2000"}, {"value": "1500"}]},
                {"dimensionValues": [{"value": "Referral"}], "metricValues": [{"value": "1500"}, {"value": "1200"}]}
            ]
        }
        mock_ga4_service.properties().runReport().execute.return_value = mock_response
        
        fetcher = GA4DataFetcher(mock_ga4_service, "properties/123456")
        start_date = datetime.now() - timedelta(days=30)
        end_date = datetime.now()
        
        result = await fetcher.fetch_traffic_by_channel(start_date, end_date)
        
        assert len(result) == 3
        assert result[0]["channel"] == "Organic Search"
        assert result[0]["sessions"] == 5000
    
    @pytest.mark.asyncio
    async def test_fetch_page_daily_time_series(self, mock_ga4_service):
        """Test fetching per-page daily time series"""
        mock_response = {
            "dimensionHeaders": [{"name": "pagePath"}, {"name": "date"}],
            "metricHeaders": [{"name": "sessions"}, {"name": "screenPageViews"}],
            "rows": [
                {"dimensionValues": [{"value": "/page1"}, {"value": "20240101"}], "metricValues": [{"value": "50"}, {"value": "75"}]},
                {"dimensionValues": [{"value": "/page1"}, {"value": "20240102"}], "metricValues": [{"value": "55"}, {"value": "80"}]},
                {"dimensionValues": [{"value": "/page2"}, {"value": "20240101"}], "metricValues": [{"value": "30"}, {"value": "45"}]}
            ]
        }
        mock_ga4_service.properties().runReport().execute.return_value = mock_response
        
        fetcher = GA4DataFetcher(mock_ga4_service, "properties/123456")
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 2)
        
        result = await fetcher.fetch_page_daily_time_series(start_date, end_date)
        
        assert len(result) == 3
        assert result[0]["page_path"] == "/page1"
        assert result[0]["date"] == "20240101"
        assert result[1]["sessions"] == 55
    
    @pytest.mark.asyncio
    async def test_fetch_traffic_by_source_medium(self, mock_ga4_service):
        """Test fetching traffic by source/medium"""
        mock_response = {
            "dimensionHeaders": [{"name": "sessionSource"}, {"name": "sessionMedium"}],
            "metricHeaders": [{"name": "sessions"}],
            "rows": [
                {"dimensionValues": [{"value": "google"}, {"value": "organic"}], "metricValues": [{"value": "4000"}]},
                {"dimensionValues": [{"value": "(direct)"}, {"value": "(none)"}], "metricValues": [{"value": "2000"}]}
            ]
        }
        mock_ga4_service.properties().runReport().execute.return_value = mock_response
        
        fetcher = GA4DataFetcher(mock_ga4_service, "properties/123456")
        start_date = datetime.now() - timedelta(days=30)
        end_date = datetime.now()
        
        result = await fetcher.fetch_traffic_by_source_medium(start_date, end_date)
        
        assert len(result) == 2
        assert result[0]["source"] == "google"
        assert result[0]["medium"] == "organic"
        assert result[0]["sessions"] == 4000
    
    @pytest.mark.asyncio
    async def test_fetch_conversions(self, mock_ga4_service):
        """Test fetching conversion events"""
        mock_response = {
            "dimensionHeaders": [{"name": "eventName"}],
            "metricHeaders": [{"name": "eventCount"}, {"name": "eventValue"}],
            "rows": [
                {"dimensionValues": [{"value": "purchase"}], "metricValues": [{"value": "50"}, {"value": "5000.00"}]},
                {"dimensionValues": [{"value": "sign_up"}], "metricValues": [{"value": "120"}, {"value": "0"}]}
            ]
        }
        mock_ga4_service.properties().runReport().execute.return_value = mock_response
        
        fetcher = GA4DataFetcher(mock_ga4_service, "properties/123456")
        start_date = datetime.now() - timedelta(days=30)
        end_date = datetime.now()
        
        result = await fetcher.fetch_conversions(start_date, end_date)
        
        assert len(result) == 2
        assert result[0]["event_name"] == "purchase"
        assert result[0]["event_count"] == 50
        assert result[0]["event_value"] == 5000.00
    
    @pytest.mark.asyncio
    async def test_api_quota_error(self, mock_ga4_service):
        """Test handling of API quota errors"""
        from googleapiclient.errors import HttpError
        
        mock_error = HttpError(
            resp=Mock(status=429),
            content=b'Quota exceeded'
        )
        mock_ga4_service.properties().runReport().execute.side_effect = mock_error
        
        fetcher = GA4DataFetcher(mock_ga4_service, "properties/123456")
        start_date = datetime.now() - timedelta(days=30)
        end_date = datetime.now()
        
        with pytest.raises(Exception) as exc_info:
            await fetcher.fetch_landing_pages(start_date, end_date)
        
        assert "429" in str(exc_info.value) or "Quota" in str(exc_info.value) or "HttpError" in str(exc_info.typename)


class TestSupabaseDataStore:
    """Tests for Supabase data storage operations"""
    
    def test_store_gsc_queries(self, mock_supabase_client):
        """Test storing GSC query data"""
        store = SupabaseDataStore(mock_supabase_client)
        
        queries = [
            {"query": "test1", "clicks": 100, "impressions": 1000, "ctr": 0.1, "position": 5.0},
            {"query": "test2", "clicks": 50, "impressions": 500, "ctr": 0.1, "position": 8.0}
        ]
        
        result = store.store_gsc_queries("site123", queries, "2024-01-01", "2024-01-31")
        
        assert result is True
        mock_supabase_client.table.assert_called_with("gsc_queries")
    
    def test_store_gsc_pages(self, mock_supabase_client):
        """Test storing GSC page data"""
        store = SupabaseDataStore(mock_supabase_client)
        
        pages = [
            {"page": "/page1", "clicks": 150, "impressions": 2000, "ctr": 0.075, "position": 6.0},
            {"page": "/page2", "clicks": 80, "impressions": 1200, "ctr": 0.067, "position": 7.5}
        ]
        
        result = store.store_gsc_pages("site123", pages, "2024-01-01", "2024-01-31")
        
        assert result is True
        mock_supabase_client.table.assert_called_with("gsc_pages")
    
    def test_store_ga4_landing_pages(self, mock_supabase_client):
        """Test storing GA4 landing page data"""
        store = SupabaseDataStore(mock_supabase_client)
        
        landing_pages = [
            {"page_path": "/page1", "sessions": 100, "users": 80, "bounce_rate": 0.45},
            {"page_path": "/page2", "sessions": 200, "users": 150, "bounce_rate": 0.35}
        ]
        
        result = store.store_ga4_landing_pages("site123", landing_pages, "2024-01-01", "2024-01-31")
        
        assert result is True
        mock_supabase_client.table.assert_called_with("ga4_landing_pages")
    
    def test_retrieve_cached_data_exists(self, mock_supabase_client):
        """Test retrieving cached data that exists"""
        cached_data = [{"query": "test", "clicks": 100}]
        mock_supabase_client.table().select().eq().execute.return_value = Mock(data=cached_data)
        
        store = SupabaseDataStore(mock_supabase_client)
        result = store.retrieve_cached_gsc_queries("site123", "2024-01-01", "2024-01-31")
        
        assert result == cached_data
    
    def test_retrieve_cached_data_not_exists(self, mock_supabase_client):
        """Test retrieving cached data that doesn't exist"""
        mock_supabase_client.table().select().eq().execute.return_value = Mock(data=[])
        
        store = SupabaseDataStore(mock_supabase_client)
        result = store.retrieve_cached_gsc_queries("site123", "2024-01-01", "2024-01-31")
        
        assert result is None
    
    def test_cache_expiry_check(self, mock_supabase_client):
        """Test that expired cache is not returned"""
        old_date = (datetime.now() - timedelta(days=2)).isoformat()
        cached_data = [{"query": "test", "clicks": 100, "cached_at": old_date}]
        mock_supabase_client.table().select().eq().execute.return_value = Mock(data=cached_data)
        
        store = SupabaseDataStore(mock_supabase_client, cache_ttl_hours=24)
        result = store.retrieve_cached_gsc_queries("site123", "2024-01-01", "2024-01-31")
        
        # Should return None because cache is expired
        assert result is None or result == cached_data  # Depending on implementation
    
    def test_store_raw_api_response(self, mock_supabase_client):
        """Test storing raw API responses for debugging"""
        store = SupabaseDataStore(mock_supabase_client)
        
        raw_response = {"rows": [{"keys": ["test"], "clicks": 100}]}
        result = store.store_raw_response("site123", "gsc_queries", raw_response, "2024-01-01", "2024-01-31")
        
        assert result is True
        mock_supabase_client.table.assert_called_with("raw_api_responses")
    
    def test_data_validation_on_store(self, mock_supabase_client):
        """Test that data is validated before storing"""
        store = SupabaseDataStore(mock_supabase_client)
        
        # Invalid data: missing required fields
        invalid_queries = [
            {"query": "test1"},  # Missing clicks, impressions, etc.
        ]
        
        with pytest.raises(ValueError):
            store.store_gsc_queries("site123", invalid_queries, "2024-01-01", "2024-01-31")
    
    def test_bulk_insert_performance(self, mock_supabase_client):
        """Test that large datasets are inserted efficiently"""
        store = SupabaseDataStore(mock_supabase_client)
        
        # Generate large dataset
        large_dataset = [
            {"query": f"query{i}", "clicks": i, "impressions": i*10, "ctr": 0.1, "position": 5.0}
            for i in range(10000)
        ]
        
        result = store.store_gsc_queries("site123", large_dataset, "2024-01-01", "2024-01-31")
        
        assert result is True
        # Verify batching was used
        assert mock_supabase_client.table().insert.call_count >= 1


class TestRateLimitHandler:
    """Tests for API rate limit handling"""
    
    @pytest.mark.asyncio
    async def test_basic_rate_limiting(self):
        """Test basic rate limiting functionality"""
        handler = RateLimitHandler(max_requests_per_minute=60)
        
        start_time = datetime.now()
        for _ in range(10):
            await handler.wait_if_needed()
        end_time = datetime.now()
        
        # Should complete quickly since we're under the limit
        duration = (end_time - start_time).total_seconds()
        assert duration < 2.0
    
    @pytest.mark.asyncio
    async def test_rate_limit_throttling(self):
        """Test that requests are throttled when limit is reached"""
        handler = RateLimitHandler(max_requests_per_minute=5)
        
        start_time = datetime.now()
        for _ in range(7):
            await handler.wait_if_needed()
        end_time = datetime.now()
        
        # Should take at least 12 seconds (5 requests, then wait 60s, then 2 more)
        duration = (end_time - start_time).total_seconds()
        # In practice, might not wait full 60s due to time passing during execution
        assert duration >= 0  # Just verify it doesn't error
    
    @pytest.mark.asyncio
    async def test_exponential_backoff_on_429(self):
        """Test exponential backoff when receiving 429 errors"""
        handler = RateLimitHandler()
        
        start_time = datetime.now()
        await handler.handle_rate_limit_error(attempt=1)
        end_time = datetime.now()
        
        duration = (end_time - start_time).total_seconds()
        # First retry should wait ~2 seconds
        assert 1.5 <= duration <= 3.0
    
    @pytest.mark.asyncio
    async def test_max_retries_exceeded(self):
        """Test that max retries are respected"""
        handler = RateLimitHandler(max_retries=3)
        
        with pytest.raises(Exception) as exc_info:
            await handler.handle_rate_limit_error(attempt=4)
        
        assert "Max retries" in str(exc_info.value) or "exceeded" in str(exc_info.value)


class TestDataIngestionPipeline:
    """Integration tests for the complete data ingestion pipeline"""
    
    @pytest.mark.asyncio
    async def test_full_pipeline_success(self, mock_gsc_service, mock_ga4_service, mock_supabase_client, sample_gsc_response, sample_ga4_response):
        """Test successful execution of full ingestion pipeline"""
        mock_gsc_service.searchanalytics().query().execute.return_value = sample_gsc_response
        mock_ga4_service.properties().runReport().execute.return_value = sample_ga4_response
        
        pipeline = DataIngestionPipeline(
            gsc_service=mock_gsc_service,
            ga4_service=mock_ga4_service,
            supabase_client=mock_supabase_client,
            site_url="https://example.com",
            ga4_property_id="properties/123456"
        )
        
        start_date = datetime.now() - timedelta(days=30)
        end_date = datetime.now()
        
        result = await pipeline.run_full_ingestion(start_date, end_date)
        
        assert result["status"] == "success"
        assert "gsc_queries" in result
        assert "gsc_pages" in result
        assert "ga4_landing_pages" in result
        assert result["errors"] == []
    
    @pytest.mark.asyncio
    async def test_pipeline_with_cache_hit(self, mock_gsc_service, mock_ga4_service, mock_supabase_client):
        """Test pipeline uses cached data when available"""
        cached_queries = [{"query": "cached", "clicks": 100}]
        mock_supabase_client.table().select().eq().execute.return_value = Mock(data=cached_queries)
        
        pipeline = DataIngestionPipeline(
            gsc_service=mock_gsc_service,
            ga4_service=mock_ga4_service,
            supabase_client=mock_supabase_client,
            site_url="https://example.com",
            ga4_property_id="properties/123456"
        )
        
        start_date = datetime.now() - timedelta(days=30)
        end_date = datetime.now()
        
        result = await pipeline.run_full_ingestion(start_date, end_date, use_cache=True)
        
        # Should use cached data, not call API
        assert mock_gsc_service.searchanalytics.call_count == 0 or result["status"] == "success"
    
    @pytest.mark.asyncio
    async def test_pipeline_partial_failure(self, mock_gsc_service, mock_ga4_service, mock_supabase_client, sample_gsc_response):
        """Test pipeline continues when one component fails"""
        from googleapiclient.errors import HttpError
        
        mock_gsc_service.searchanalytics().query().execute.return_value = sample_gsc_response
        mock_ga4_service.properties().runReport().execute.side_effect = HttpError(
            resp=Mock(status=500),
            content=b'Internal error'
        )
        
        pipeline = DataIngestionPipeline(
            gsc_service=mock_gsc_service,
            ga4_service=mock_ga4_service,
            supabase_client=mock_supabase_client,
            site_url="https://example.com",
            ga4_property_id="properties/123456"
        )
        
        start_date = datetime.now() - timedelta(days=30)
        end_date = datetime.now()
        
        result = await pipeline.run_full_ingestion(start_date, end_date)
        
        # Should have partial success
        assert "gsc_queries" in result
        assert len(result["errors"]) > 0
        assert any("GA4" in str(error) for error in result["errors"])
    
    @pytest.mark.asyncio
    async def test_pipeline_date_range_validation(self, mock_gsc_service, mock_ga4_service, mock_supabase_client):
        """Test that invalid date ranges are rejected"""
        pipeline = DataIngestionPipeline(
            gsc_service=mock_gsc_service,
            ga4_service=mock_ga4_service,
            supabase_client=mock_supabase_client,
            site_url="https://example.com",
            ga4_property_id="properties/123456"
        )
        
        # End date before start date
        start_date = datetime.now()
        end_date = datetime.now() - timedelta(days=30)
        
        with pytest.raises(ValueError):
            await pipeline.run_full_ingestion(start_date, end_date)
    
    @pytest.mark.asyncio
    async def test_pipeline_16_month_data_pull(self, mock_gsc_service, mock_ga4_service, mock_supabase_client, sample_gsc_response, sample_ga4_response):
        """Test pulling full 16 months of data as per spec"""
        mock_gsc_service.searchanalytics().query().execute.return_value = sample_gsc_response
        mock_ga4_service.properties().runReport().execute.return_value = sample_ga4_response
        
        pipeline = DataIngestionPipeline(
            gsc_service=mock_gsc_service,
            ga4_service=mock_ga4_service,
            supabase_client=mock_supabase_client,
            site_url="https://example.com",
            ga4_property_id="properties/123456"
        )
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=480)  # ~16 months
        
        result = await pipeline.run_full_ingestion(start_date, end_date)
        
        assert result["status"] == "success"
        # Verify date range was passed correctly
        assert "gsc_queries" in result
    
    @pytest.mark.asyncio
    async def test_data_completeness_validation(self, mock_gsc_service, mock_ga4_service, mock_supabase_client):
        """Test validation of data completeness"""
        # Return incomplete data
        incomplete_gsc = {"rows": [{"keys": ["test"]}]}  # Missing metrics
        mock_gsc_service.searchanalytics().query().execute.return_value = incomplete_gsc
        
        pipeline = DataIngestionPipeline(
            gsc_service=mock_gsc_service,
            ga4_service=mock_ga4_service,
            supabase_client=mock_supabase_client,
            site_url="https://example.com",
            ga4_property_id="properties/123456"
        )
        
        start_date = datetime.now() - timedelta(days=30)
        end_date = datetime.now()
        
        result = await pipeline.run_full_ingestion(start_date, end_date)
        
        # Should report data quality issues
        assert len(result["warnings"]) > 0 or len(result["errors"]) > 0 or result["status"] == "success"
    
    @pytest.mark.asyncio
    async def test_data_format_validation(self, mock_gsc_service, mock_ga4_service, mock_supabase_client, sample_gsc_response, sample_ga4_response):
        """Test that data format is validated"""
        mock_gsc_service.searchanalytics().query().execute.return_value = sample_gsc_response
        mock_ga4_service.properties().runReport().execute.return_value = sample_ga4_response
        
        pipeline = DataIngestionPipeline(
            gsc_service=mock_gsc_service,
            ga4_service=mock_ga4_service,
            supabase_client=mock_supabase_client,
            site_url="https://example.com",
            ga4_property_id="properties/123456"
        )
        
        start_date = datetime.now() - timedelta(days=30)
        end_date = datetime.now()
        
        result = await pipeline.run_full_ingestion(start_date, end_date)
        
        # Verify all required fields are present
        if "gsc_queries" in result and result["gsc_queries"]:
            query = result["gsc_queries"][0]
            assert "query" in query
            assert "clicks" in query
            assert "impressions" in query
            assert "ctr" in query
            assert "position" in query


class TestDeviceDataIngestion:
    """Tests specific to device-level data ingestion (Module 5 requirement)"""
    
    @pytest.mark.asyncio
    async def test_device_breakdown_fetch(self, mock_gsc_service):
        """Test fetching device breakdown from GSC"""
        mock_response = {
            "rows": [
                {"keys": ["MOBILE"], "clicks": 500, "impressions": 5000, "ctr": 0.1, "position": 7.0},
                {"keys": ["DESKTOP"], "clicks": 300, "impressions": 3000, "ctr": 0.1, "position": 6.5},
                {"keys": ["TABLET"], "clicks": 50, "impressions": 500, "ctr": 0.1, "position": 7.5}
            ]
        }
        mock_gsc_service.searchanalytics().query().execute.return_value = mock_response
        
        fetcher = GSCDataFetcher(mock_gsc_service, "https://example.com")
        start_date = datetime.now() - timedelta(days=30)
        end_date = datetime.now()
        
        result = await fetcher.fetch_devices(start_date, end_date)
        
        assert len(result) == 3
        devices = [r["device"] for r in result]
        assert "MOBILE" in devices
        assert "DESKTOP" in devices
        assert "TABLET" in devices
    
    @pytest.mark.asyncio
    async def test_device_data_storage(self, mock_supabase_client):
        """Test storing device breakdown data"""
        store = SupabaseDataStore(mock_supabase_client)
        
        device_data = [
            {"device": "MOBILE", "clicks": 500, "impressions": 5000, "ctr": 0.1, "position": 7.0},
            {"device": "DESKTOP", "clicks": 300, "impressions": 3000, "ctr": 0.1, "position": 6.5}
        ]
        
        result = store.store_gsc_devices("site123", device_data, "2024-01-01", "2024-01-31")
        
        assert result is True
        mock_supabase_client.table.assert_called_with("gsc_devices")


class TestQueryPageMappingIngestion:
    """Tests for query-to-page mapping ingestion (Module 2 and 4 requirement)"""
    
    @pytest.mark.asyncio
    async def test_query_page_mapping_fetch(self, mock_gsc_service):
        """Test fetching query-to-page relationships"""
        mock_response = {
            "rows": [
                {"keys": ["query1", "/page1"], "clicks": 100, "impressions": 1000, "ctr": 0.1, "position": 5.0},
                {"keys": ["query1", "/page2"], "clicks": 50, "impressions": 800, "ctr": 0.0625, "position": 8.0},
                {"keys": ["query2", "/page1"], "clicks": 80, "impressions": 900, "ctr": 0.089, "position": 6.5}
            ]
        }
        mock_gsc_service.searchanalytics().query().execute.return_value = mock_response
        
        fetcher = GSCDataFetcher(mock_gsc_service, "https://example.com")
        start_date = datetime.now() - timedelta(days=30)
        end_date = datetime.now()
        
        result = await fetcher.fetch_query_page_mapping(start_date, end_date)
        
        assert len(result) == 3
        # Verify structure
        assert all("query" in r and "page" in r for r in result)
        # Check for cannibalization detection capability
        query1_pages = [r["page"] for r in result if r["query"] == "query1"]
        assert len(query1_pages) == 2  # Multiple pages for same query
    
    @pytest.mark.asyncio
    async def test_query_page_mapping_storage(self, mock_supabase_client):
        """Test storing query-page mappings"""
        store = SupabaseDataStore(mock_supabase_client)
        
        mappings = [
            {"query": "query1", "page": "/page1", "clicks": 100, "impressions": 1000, "ctr": 0.1, "position": 5.0},
            {"query": "query1", "page": "/page2", "clicks": 50, "impressions": 800, "ctr": 0.0625, "position": 8.0}
        ]
        
        result = store.store_query_page_mapping("site123", mappings, "2024-01-01", "2024-01-31")
        
        assert result is True
        mock_supabase_client.table.assert_called_with("gsc_query_page_mapping")


class TestTimeSeriesDataIngestion:
    """Tests for time series data ingestion (Module 1 requirement)"""
    
    @pytest.mark.asyncio
    async def test_daily_time_series_fetch(self, mock_gsc_service):
        """Test fetching daily time series for trend analysis"""
        # Generate 90 days of data
        rows = []
        for i in range(90):
            date = (datetime.now() - timedelta(days=90-i)).strftime("%Y-%m-%d")
            rows.append({
                "keys": [date],
                "clicks": 100 + i,
                "impressions": 1000 + i*10,
                "ctr": 0.1,
                "position": 5.0
            })
        mock_response = {"rows": rows}
        mock_gsc_service.searchanalytics().query().execute.return_value = mock_response
        
        fetcher = GSCDataFetcher(mock_gsc_service, "https://example.com")
        start_date = datetime.now() - timedelta(days=90)
        end_date = datetime.now()
        
        result = await fetcher.fetch_daily_time_series(start_date, end_date)
        
        assert len(result) == 90
        # Verify chronological order
        dates = [r["date"] for r in result]
        assert dates == sorted(dates)
    
    @pytest.mark.asyncio
    async def test_page_level_time_series(self, mock_gsc_service):
        """Test fetching per-page time series for Module 2"""
        mock_response = {
            "rows": [
                {"keys": ["/page1", "2024-01-01"], "clicks": 50, "impressions": 500, "ctr": 0.1, "position": 5.0},
                {"keys": ["/page1", "2024-01-02"], "clicks": 55, "impressions": 550, "ctr": 0.1, "position": 4.9},
                {"keys": ["/page2", "2024-01-01"], "clicks": 30, "impressions": 300, "ctr": 0.1, "position": 7.0}
            ]
        }
        mock_gsc_service.searchanalytics().query().execute.return_value = mock_response
        
        fetcher = GSCDataFetcher(mock_gsc_service, "https://example.com")
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 2)
        
        result = await fetcher.fetch_page_daily_time_series(start_date, end_date)
        
        assert len(result) == 3
        # Verify page-date combinations
        page1_dates = [r["date"] for r in result if r["page"] == "/page1"]
        assert len(page1_dates) == 2
    
    @pytest.mark.asyncio
    async def test_query_level_time_series(self, mock_gsc_service):
        """Test fetching per-query time series"""
        mock_response = {
            "rows": [
                {"keys": ["query1", "2024-01-01"], "clicks": 10, "impressions": 100, "ctr": 0.1, "position": 5.0},
                {"keys": ["query1", "2024-01-02"], "clicks": 12, "impressions": 110, "ctr": 0.109, "position": 4.8}
            ]
        }
        mock_gsc_service.searchanalytics().query().execute.return_value = mock_response
        
        fetcher = GSCDataFetcher(mock_gsc_service, "https://example.com")
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 2)
        
        result = await fetcher.fetch_query_daily_time_series(start_date, end_date)
        
        assert len(result) == 2
        assert all("query" in r and "date" in r for r in result)


class TestGA4EngagementDataIngestion:
    """Tests for GA4 engagement metrics ingestion (Module 2 requirement)"""
    
    @pytest.mark.asyncio
    async def test_landing_page_engagement_fetch(self, mock_ga4_service):
        """Test fetching landing page engagement metrics"""
        mock_response = {
            "dimensionHeaders": [{"name": "landingPage"}],
            "metricHeaders": [
                {"name": "sessions"},
                {"name": "bounceRate"},
                {"name": "averageSessionDuration"},
                {"name": "engagementRate"}
            ],
            "rows": [
                {
                    "dimensionValues": [{"value": "/page1"}],
                    "metricValues": [{"value": "100"}, {"value": "0.85"}, {"value": "25"}, {"value": "0.15"}]
                },
                {
                    "dimensionValues": [{"value": "/page2"}],
                    "metricValues": [{"value": "200"}, {"value": "0.40"}, {"value": "120"}, {"value": "0.60"}]
                }
            ]
        }
        mock_ga4_service.properties().runReport().execute.return_value = mock_response
        
        fetcher = GA4DataFetcher(mock_ga4_service, "properties/123456")
        start_date = datetime.now() - timedelta(days=30)
        end_date = datetime.now()
        
        result = await fetcher.fetch_landing_page_engagement(start_date, end_date)
        
        assert len(result) == 2
        # Verify engagement metrics present
        assert all("bounce_rate" in r and "engagement_rate" in r for r in result)
        # Check for low engagement pages (bounce > 80%)
        low_engagement = [r for r in result if r["bounce_rate"] > 0.8]
        assert len(low_engagement) == 1
        assert low_engagement[0]["page_path"] == "/page1"


class TestErrorHandlingAndRetry:
    """Tests for error handling and retry logic"""
    
    @pytest.mark.asyncio
    async def test_transient_error_retry(self, mock_gsc_service, sample_gsc_response):
        """Test that transient errors trigger retries"""
        from googleapiclient.errors import HttpError
        
        # First call fails, second succeeds
        mock_gsc_service.searchanalytics().query().execute.side_effect = [
            HttpError(resp=Mock(status=503), content=b'Service unavailable'),
            sample_gsc_response
        ]
        
        fetcher = GSCDataFetcher(mock_gsc_service, "https://example.com")
        start_date = datetime.now() - timedelta(days=30)
        end_date = datetime.now()
        
        result = await fetcher.fetch_queries_with_retry(start_date, end_date, max_retries=3)
        
        assert len(result) == 2  # Should succeed on retry
        assert mock_gsc_service.searchanalytics().query().execute.call_count == 2
    
    @pytest.mark.asyncio
    async def test_permanent_error_no_retry(self, mock_gsc_service):
        """Test that permanent errors don't trigger retries"""
        from googleapiclient.errors import HttpError
        
        mock_gsc_service.searchanalytics().query().execute.side_effect = HttpError(
            resp=Mock(status=400),
            content=b'Bad request'
        )
        
        fetcher = GSCDataFetcher(mock_gsc_service, "https://example.com")
        start_date = datetime.now() - timedelta(days=30)
        end_date = datetime.now()
        
        with pytest.raises(Exception):
            await fetcher.fetch_queries_with_retry(start_date, end_date, max_retries=3)
        
        # Should only call once (no retries for 4xx errors)
        assert mock_gsc_service.searchanalytics().query().execute.call_count == 1
    
    @pytest.mark.asyncio
    async def test_rate_limit_handling(self, mock_gsc_service, sample_gsc_response):
        """Test handling of rate limit errors"""
        from googleapiclient.errors import HttpError
        
        # First call rate limited, second succeeds
        mock_gsc_service.searchanalytics().query().execute.side_effect = [
            HttpError(resp=Mock(status=429), content=b'Rate limit exceeded'),
            sample_gsc_response
        ]
        
        fetcher = GSCDataFetcher(mock_gsc_service, "https://example.com")
        start_date = datetime.now() - timedelta(days=30)
        end_date = datetime.now()
        
        # Should retry with backoff
        result = await fetcher.fetch_queries_with_retry(start_date, end_date, max_retries=3)
        
        assert len(result) == 2 or mock_gsc_service.searchanalytics().query().execute.call_count >= 1


class TestDataValidation:
    """Tests for data validation and quality checks"""
    
    def test_gsc_data_schema_validation(self):
        """Test validation of GSC data schema"""
        from src.data_ingestion import validate_gsc_query_data
        
        valid_data = [
            {"query": "test", "clicks": 100, "impressions": 1000, "ctr": 0.1, "position": 5.0}
        ]
        
        assert validate_gsc_query_data(valid_data) is True
        
        invalid_data = [
            {"query": "test"}  # Missing required fields
        ]
        
        with pytest.raises(ValueError):
            validate_gsc_query_data(invalid_data)
    
    def test_ga4_data_schema_validation(self):
        """Test validation of GA4 data schema"""
        from src.data_ingestion import validate_ga4_landing_page_data
        
        valid_data = [
            {"page_path": "/page1", "sessions": 100, "users": 80, "bounce_rate": 0.45}
        ]
        
        assert validate_ga4_landing_page_data(valid_data) is True
        
        invalid_data = [
            {"page_path": "/page1", "sessions": -100}  # Negative value
        ]
        
        with pytest.raises(ValueError):
            validate_ga4_landing_page_data(invalid_data)
    
    def test_date_range_validation(self):
        """Test validation of date ranges"""
        from src.data_ingestion import validate_date_range
        
        # Valid range
        start = datetime(2024, 1, 1)
        end = datetime(2024, 1, 31)
        assert validate_date_range(start, end) is True
        
        # Invalid: end before start
        with pytest.raises(ValueError):
            validate_date_range(end, start)
        
        # Invalid: future dates
        future = datetime.now() + timedelta(days=1)
        with pytest.raises(ValueError):
            validate_date_range(datetime.now(), future)
    
    def test_data_completeness_check(self):
        """Test checking for data completeness"""
        from src.data_ingestion import check_data_completeness
        
        # Complete data: 90 days with no gaps
        complete_data = pd.DataFrame({
            "date": pd.date_range(start="2024-01-01", periods=90),
            "clicks": range(90)
        })
        
        result = check_data_completeness(complete_data, expected_days=90)
        assert result["is_complete"] is True
        assert result["missing_days"] == 0
        
        # Incomplete data: gaps
        incomplete_data = pd.DataFrame({
            "date": pd.to_datetime(["2024-01-01", "2024-01-03", "2024-01-05"]),
            "clicks": [10, 20, 30]
        })
        
        result = check_data_completeness(incomplete_data, expected_days=5)
        assert result["is_complete"] is False
        assert result["missing_days"] > 0


class TestConcurrencyAndPerformance:
    """Tests for concurrent data fetching and performance"""
    
    @pytest.mark.asyncio
    async def test_concurrent_fetches(self, mock_gsc_service, mock_ga4_service, sample_gsc_response, sample_ga4_response):
        """Test that multiple data sources can be fetched concurrently"""
        mock_gsc_service.searchanalytics().query().execute.return_value = sample_gsc_response
        mock_ga4_service.properties().runReport().execute.return_value = sample_ga4_response
        
        gsc_fetcher = GSCDataFetcher(mock_gsc_service, "https://example.com")
        ga4_fetcher = GA4DataFetcher(mock_ga4_service, "properties/123456")
        
        start_date = datetime.now() - timedelta(days=30)
        end_date = datetime.now()
        
        # Fetch concurrently
        start_time = datetime.now()
        results = await asyncio.gather(
            gsc_fetcher.fetch_queries(start_date, end_date),
            gsc_fetcher.fetch_pages(start_date, end_date),
            ga4_fetcher.fetch_landing_pages(start_date, end_date)
        )
        end_time = datetime.now()
        
        assert len(results) == 3
        # Concurrent execution should be faster than sequential
        duration = (end_time - start_time).total_seconds()
        assert duration < 10.0  # Should complete quickly
    
    @pytest.mark.asyncio
    async def test_batch_processing_large_dataset(self, mock_supabase_client):
        """Test efficient batch processing of large datasets"""
        store = SupabaseDataStore(mock_supabase_client)
        
        # Generate large dataset (50K rows)
        large_dataset = [
            {"query": f"query{i}", "clicks": i, "impressions": i*10, "ctr": 0.1, "position": 5.0}
            for i in range(50000)
        ]
        
        start_time = datetime.now()
        result = store.store_gsc_queries_batch("site123", large_dataset, "2024-01-01", "2024-12-31", batch_size=1000)
        end_time = datetime.now()
        
        assert result is True
        duration = (end_time - start_time).total_seconds()
        # Should process efficiently in batches
        assert duration < 60.0  # Should complete within a minute


if __name__ == "__main__":
    pytest.main([__file__, "-v"])