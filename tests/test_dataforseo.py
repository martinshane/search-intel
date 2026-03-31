"""
Unit tests for DataForSEO integration with mocked API responses.
Tests the implementation in ingestion/dataforseo.py.
"""

import pytest
from unittest.mock import Mock, patch, AsyncMock
from datetime import datetime, date
import json
from ingestion.dataforseo import (
    DataForSEOClient,
    pull_live_serps,
    select_keywords_for_serp_pull,
    RateLimitError,
    DataForSEOError
)


@pytest.fixture
def mock_dataforseo_client():
    """Create a mock DataForSEO client with test credentials."""
    with patch('ingestion.dataforseo.httpx.AsyncClient') as mock_async_client:
        client = DataForSEOClient(
            login="test_user",
            password="test_password",
            test_mode=True
        )
        client._client = mock_async_client.return_value
        yield client


@pytest.fixture
def sample_serp_response():
    """Sample successful SERP API response from DataForSEO."""
    return {
        "version": "0.1.20220216",
        "status_code": 20000,
        "status_message": "Ok.",
        "time": "1.23 sec.",
        "cost": 0.002,
        "tasks_count": 1,
        "tasks_error": 0,
        "tasks": [{
            "id": "12071946-1535-0216-0000-7d3d5a6d8e9f",
            "status_code": 20000,
            "status_message": "Ok.",
            "time": "1.12 sec.",
            "cost": 0.002,
            "result_count": 1,
            "path": ["v3", "serp", "google", "organic", "live", "advanced"],
            "data": {
                "api": "serp",
                "function": "live",
                "se": "google",
                "se_type": "organic",
                "keyword": "best crm software",
                "location_code": 2840,
                "language_code": "en",
                "device": "desktop",
                "os": "windows"
            },
            "result": [{
                "keyword": "best crm software",
                "type": "organic",
                "se_domain": "google.com",
                "location_code": 2840,
                "language_code": "en",
                "check_url": "https://www.google.com/search?q=best+crm+software",
                "datetime": "2024-03-15 10:30:00 +00:00",
                "spell": None,
                "item_types": ["organic", "people_also_ask", "featured_snippet"],
                "se_results_count": 1250000,
                "items_count": 47,
                "items": [
                    {
                        "type": "featured_snippet",
                        "rank_group": 1,
                        "rank_absolute": 1,
                        "position": "left",
                        "xpath": "/html/body/div[1]/div[2]",
                        "domain": "example.com",
                        "title": "Best CRM Software 2024",
                        "description": "Top CRM solutions include Salesforce, HubSpot...",
                        "url": "https://example.com/best-crm",
                        "table": None
                    },
                    {
                        "type": "people_also_ask",
                        "rank_group": 2,
                        "rank_absolute": 2,
                        "position": "left",
                        "xpath": "/html/body/div[1]/div[3]",
                        "items": [
                            {
                                "type": "people_also_ask_element",
                                "title": "What is the #1 CRM software?",
                                "xpath": "/html/body/div[1]/div[3]/div[1]"
                            },
                            {
                                "type": "people_also_ask_element", 
                                "title": "Is CRM software worth it?",
                                "xpath": "/html/body/div[1]/div[3]/div[2]"
                            }
                        ]
                    },
                    {
                        "type": "organic",
                        "rank_group": 3,
                        "rank_absolute": 3,
                        "position": "left",
                        "xpath": "/html/body/div[1]/div[4]",
                        "domain": "competitor.com",
                        "title": "Top 10 CRM Systems Compared",
                        "description": "Compare features, pricing, and reviews...",
                        "url": "https://competitor.com/crm-comparison",
                        "breadcrumb": "https://competitor.com › business › crm"
                    },
                    {
                        "type": "organic",
                        "rank_group": 4,
                        "rank_absolute": 4,
                        "position": "left",
                        "xpath": "/html/body/div[1]/div[5]",
                        "domain": "testsite.com",
                        "title": "CRM Software Guide 2024",
                        "description": "Everything you need to know about CRM systems",
                        "url": "https://testsite.com/crm-guide",
                        "breadcrumb": "https://testsite.com › guides"
                    }
                ]
            }]
        }]
    }


@pytest.fixture
def sample_rate_limit_response():
    """Sample rate limit error response."""
    return {
        "status_code": 50100,
        "status_message": "Rate limit exceeded",
        "time": "0.01 sec.",
        "cost": 0,
        "tasks_count": 0,
        "tasks_error": 1
    }


@pytest.fixture
def sample_gsc_keywords():
    """Sample GSC keyword data for keyword selection."""
    return [
        {
            "query": "testsite crm software",  # Branded
            "impressions": 5000,
            "clicks": 400,
            "position": 1.2,
            "ctr": 0.08
        },
        {
            "query": "best crm software",  # Non-branded, high impressions
            "impressions": 8900,
            "clicks": 234,
            "position": 11.3,
            "ctr": 0.026
        },
        {
            "query": "crm implementation guide",  # Non-branded, good position
            "impressions": 1200,
            "clicks": 89,
            "position": 6.8,
            "ctr": 0.074
        },
        {
            "query": "testsite pricing",  # Branded
            "impressions": 2300,
            "clicks": 198,
            "position": 1.0,
            "ctr": 0.086
        },
        {
            "query": "crm for small business",  # Non-branded
            "impressions": 3400,
            "clicks": 145,
            "position": 8.2,
            "ctr": 0.043
        },
        {
            "query": "free crm software",  # Non-branded, low impressions
            "impressions": 450,
            "clicks": 12,
            "position": 15.6,
            "ctr": 0.027
        }
    ]


@pytest.fixture
def sample_gsc_query_date_data():
    """Sample GSC query-date data for position change detection."""
    return [
        # Stable keyword
        {"query": "crm implementation guide", "date": "2024-02-15", "position": 6.8, "clicks": 3},
        {"query": "crm implementation guide", "date": "2024-03-15", "position": 6.9, "clicks": 3},
        
        # Position drop keyword
        {"query": "best crm software", "date": "2024-02-15", "position": 7.2, "clicks": 8},
        {"query": "best crm software", "date": "2024-03-15", "position": 11.3, "clicks": 6},
        
        # Position gain keyword
        {"query": "crm for small business", "date": "2024-02-15", "position": 12.5, "clicks": 4},
        {"query": "crm for small business", "date": "2024-03-15", "position": 8.2, "clicks": 5}
    ]


class TestDataForSEOClient:
    """Tests for DataForSEOClient class."""
    
    def test_client_initialization(self):
        """Test client initializes with correct credentials."""
        client = DataForSEOClient(login="user123", password="pass456")
        assert client.login == "user123"
        assert client.password == "pass456"
        assert client.base_url == "https://api.dataforseo.com"
        assert client.test_mode is False
    
    def test_client_test_mode(self):
        """Test client respects test_mode flag."""
        client = DataForSEOClient(login="test", password="test", test_mode=True)
        assert client.test_mode is True
    
    @pytest.mark.asyncio
    async def test_get_account_balance_success(self, mock_dataforseo_client):
        """Test successful account balance retrieval."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "status_code": 20000,
            "tasks": [{
                "result": [{
                    "money": 15.50,
                    "currency": "USD"
                }]
            }]
        }
        
        mock_dataforseo_client._client.get = AsyncMock(return_value=mock_response)
        
        balance = await mock_dataforseo_client.get_account_balance()
        assert balance == 15.50
    
    @pytest.mark.asyncio
    async def test_get_account_balance_insufficient_funds(self, mock_dataforseo_client):
        """Test error when balance is too low."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "status_code": 20000,
            "tasks": [{
                "result": [{
                    "money": 0.05,
                    "currency": "USD"
                }]
            }]
        }
        
        mock_dataforseo_client._client.get = AsyncMock(return_value=mock_response)
        
        with pytest.raises(DataForSEOError, match="Insufficient DataForSEO balance"):
            await mock_dataforseo_client.get_account_balance()
    
    @pytest.mark.asyncio
    async def test_pull_serp_success(self, mock_dataforseo_client, sample_serp_response):
        """Test successful SERP pull for a keyword."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_serp_response
        
        mock_dataforseo_client._client.post = AsyncMock(return_value=mock_response)
        
        result = await mock_dataforseo_client.pull_serp(
            keyword="best crm software",
            location_code=2840,
            language_code="en"
        )
        
        assert result["keyword"] == "best crm software"
        assert result["location_code"] == 2840
        assert len(result["items"]) == 4
        assert result["items"][0]["type"] == "featured_snippet"
        assert result["items"][1]["type"] == "people_also_ask"
        assert result["cost"] == 0.002
    
    @pytest.mark.asyncio
    async def test_pull_serp_rate_limit(self, mock_dataforseo_client, sample_rate_limit_response):
        """Test rate limit error handling."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_rate_limit_response
        
        mock_dataforseo_client._client.post = AsyncMock(return_value=mock_response)
        
        with pytest.raises(RateLimitError, match="Rate limit exceeded"):
            await mock_dataforseo_client.pull_serp("test keyword")
    
    @pytest.mark.asyncio
    async def test_pull_serp_api_error(self, mock_dataforseo_client):
        """Test API error handling."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "status_code": 40000,
            "status_message": "Invalid API key",
            "tasks_count": 0
        }
        
        mock_dataforseo_client._client.post = AsyncMock(return_value=mock_response)
        
        with pytest.raises(DataForSEOError, match="DataForSEO API error"):
            await mock_dataforseo_client.pull_serp("test keyword")
    
    @pytest.mark.asyncio
    async def test_pull_serp_network_error(self, mock_dataforseo_client):
        """Test network error handling."""
        mock_dataforseo_client._client.post = AsyncMock(
            side_effect=Exception("Connection timeout")
        )
        
        with pytest.raises(DataForSEOError, match="Failed to pull SERP"):
            await mock_dataforseo_client.pull_serp("test keyword")
    
    @pytest.mark.asyncio
    async def test_pull_serp_test_mode(self, mock_dataforseo_client):
        """Test that test_mode returns mock data without API call."""
        mock_dataforseo_client.test_mode = True
        
        result = await mock_dataforseo_client.pull_serp("test keyword")
        
        assert result["keyword"] == "test keyword"
        assert result["test_mode"] is True
        assert "items" in result
        assert result["cost"] == 0.0
        
        # Verify no actual API call was made
        mock_dataforseo_client._client.post.assert_not_called()


class TestKeywordSelection:
    """Tests for keyword selection logic."""
    
    def test_select_keywords_basic(self, sample_gsc_keywords):
        """Test basic keyword selection filters branded and sorts by impressions."""
        selected = select_keywords_for_serp_pull(
            gsc_keywords=sample_gsc_keywords,
            brand_terms=["testsite"],
            max_keywords=3
        )
        
        assert len(selected) == 3
        assert selected[0]["query"] == "best crm software"  # Highest impressions
        assert selected[1]["query"] == "crm for small business"
        assert selected[2]["query"] == "crm implementation guide"
        
        # Verify branded keywords are excluded
        queries = [kw["query"] for kw in selected]
        assert "testsite crm software" not in queries
        assert "testsite pricing" not in queries
    
    def test_select_keywords_respects_max_limit(self, sample_gsc_keywords):
        """Test that max_keywords limit is respected."""
        selected = select_keywords_for_serp_pull(
            gsc_keywords=sample_gsc_keywords,
            brand_terms=["testsite"],
            max_keywords=2
        )
        
        assert len(selected) == 2
    
    def test_select_keywords_minimum_impressions(self, sample_gsc_keywords):
        """Test filtering by minimum impressions."""
        selected = select_keywords_for_serp_pull(
            gsc_keywords=sample_gsc_keywords,
            brand_terms=["testsite"],
            min_impressions=1000,
            max_keywords=10
        )
        
        # Should exclude "free crm software" (450 impressions)
        queries = [kw["query"] for kw in selected]
        assert "free crm software" not in queries
        assert len(selected) == 3
    
    def test_select_keywords_includes_position_changes(self, sample_gsc_keywords, sample_gsc_query_date_data):
        """Test that keywords with significant position changes are included."""
        selected = select_keywords_for_serp_pull(
            gsc_keywords=sample_gsc_keywords,
            brand_terms=["testsite"],
            max_keywords=5,
            query_date_data=sample_gsc_query_date_data,
            position_change_threshold=3.0
        )
        
        queries = [kw["query"] for kw in selected]
        
        # "best crm software" dropped from 7.2 to 11.3 (4.1 positions)
        assert "best crm software" in queries
        
        # "crm for small business" improved from 12.5 to 8.2 (4.3 positions)
        assert "crm for small business" in queries
    
    def test_select_keywords_no_duplicates(self, sample_gsc_keywords, sample_gsc_query_date_data):
        """Test that keywords aren't duplicated when included for multiple reasons."""
        selected = select_keywords_for_serp_pull(
            gsc_keywords=sample_gsc_keywords,
            brand_terms=["testsite"],
            max_keywords=10,
            query_date_data=sample_gsc_query_date_data,
            position_change_threshold=3.0
        )
        
        queries = [kw["query"] for kw in selected]
        # Check no duplicates
        assert len(queries) == len(set(queries))
    
    def test_select_keywords_empty_input(self):
        """Test handling of empty keyword list."""
        selected = select_keywords_for_serp_pull(
            gsc_keywords=[],
            brand_terms=["test"],
            max_keywords=50
        )
        
        assert len(selected) == 0
    
    def test_select_keywords_all_branded(self):
        """Test when all keywords are branded."""
        branded_only = [
            {"query": "testsite features", "impressions": 1000, "position": 1.0},
            {"query": "testsite pricing", "impressions": 2000, "position": 1.0}
        ]
        
        selected = select_keywords_for_serp_pull(
            gsc_keywords=branded_only,
            brand_terms=["testsite"],
            max_keywords=50
        )
        
        assert len(selected) == 0


class TestPullLiveSerps:
    """Tests for the main pull_live_serps orchestration function."""
    
    @pytest.mark.asyncio
    async def test_pull_live_serps_success(self, sample_gsc_keywords, sample_serp_response):
        """Test successful end-to-end SERP pulling."""
        with patch('ingestion.dataforseo.DataForSEOClient') as MockClient:
            mock_client_instance = Mock()
            mock_client_instance.get_account_balance = AsyncMock(return_value=10.0)
            mock_client_instance.pull_serp = AsyncMock(return_value=sample_serp_response["tasks"][0]["result"][0])
            mock_client_instance.close = AsyncMock()
            MockClient.return_value = mock_client_instance
            
            with patch('ingestion.dataforseo.get_supabase_client') as mock_supabase:
                mock_db = Mock()
                mock_supabase.return_value = mock_db
                
                results = await pull_live_serps(
                    gsc_keywords=sample_gsc_keywords,
                    brand_terms=["testsite"],
                    max_keywords=3,
                    dataforseo_login="test",
                    dataforseo_password="test"
                )
        
        assert results["status"] == "success"
        assert results["keywords_pulled"] == 3
        assert results["total_cost"] > 0
        assert len(results["serp_data"]) == 3
        assert results["balance_remaining"] < 10.0
    
    @pytest.mark.asyncio
    async def test_pull_live_serps_insufficient_balance(self, sample_gsc_keywords):
        """Test handling of insufficient account balance."""
        with patch('ingestion.dataforseo.DataForSEOClient') as MockClient:
            mock_client_instance = Mock()
            mock_client_instance.get_account_balance = AsyncMock(
                side_effect=DataForSEOError("Insufficient DataForSEO balance: $0.05")
            )
            mock_client_instance.close = AsyncMock()
            MockClient.return_value = mock_client_instance
            
            results = await pull_live_serps(
                gsc_keywords=sample_gsc_keywords,
                brand_terms=["testsite"],
                max_keywords=50,
                dataforseo_login="test",
                dataforseo_password="test"
            )
        
        assert results["status"] == "error"
        assert "Insufficient DataForSEO balance" in results["error"]
    
    @pytest.mark.asyncio
    async def test_pull_live_serps_rate_limit_retry(self, sample_gsc_keywords, sample_serp_response):
        """Test retry logic for rate limit errors."""
        with patch('ingestion.dataforseo.DataForSEOClient') as MockClient:
            mock_client_instance = Mock()
            mock_client_instance.get_account_balance = AsyncMock(return_value=10.0)
            
            # First call raises rate limit, second succeeds
            mock_client_instance.pull_serp = AsyncMock(
                side_effect=[
                    RateLimitError("Rate limit exceeded"),
                    sample_serp_response["tasks"][0]["result"][0]
                ]
            )
            mock_client_instance.close = AsyncMock()
            MockClient.return_value = mock_client_instance
            
            with patch('ingestion.dataforseo.get_supabase_client') as mock_supabase:
                mock_db = Mock()
                mock_supabase.return_value = mock_db
                
                with patch('ingestion.dataforseo.asyncio.sleep', new_callable=AsyncMock):
                    results = await pull_live_serps(
                        gsc_keywords=sample_gsc_keywords[:1],
                        brand_terms=["testsite"],
                        max_keywords=1,
                        dataforseo_login="test",
                        dataforseo_password="test",
                        max_retries=2
                    )
        
        # Should eventually succeed after retry
        assert results["status"] == "success"
        assert mock_client_instance.pull_serp.call_count == 2
    
    @pytest.mark.asyncio
    async def test_pull_live_serps_max_retries_exceeded(self, sample_gsc_keywords):
        """Test failure after max retries exceeded."""
        with patch('ingestion.dataforseo.DataForSEOClient') as MockClient:
            mock_client_instance = Mock()
            mock_client_instance.get_account_balance = AsyncMock(return_value=10.0)
            mock_client_instance.pull_serp = AsyncMock(
                side_effect=RateLimitError("Rate limit exceeded")
            )
            mock_client_instance.close = AsyncMock()
            MockClient.return_value = mock_client_instance
            
            with patch('ing