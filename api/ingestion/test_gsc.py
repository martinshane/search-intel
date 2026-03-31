"""
Unit tests for GSC ingestion module.
Tests pagination, caching, date range splitting, and error handling using mocks.
"""

import pytest
from datetime import date, datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
import json

from ingestion.gsc import (
    GSCClient,
    fetch_gsc_performance,
    fetch_gsc_query_page_mapping,
    fetch_gsc_all_data,
    GSCIngestionError,
    GSCAuthenticationError,
    GSCRateLimitError,
)


# --- Fixtures ---

@pytest.fixture
def mock_supabase():
    """Mock Supabase client."""
    mock = MagicMock()
    mock.table.return_value = mock
    mock.select.return_value = mock
    mock.eq.return_value = mock
    mock.single.return_value = mock
    mock.insert.return_value = mock
    mock.upsert.return_value = mock
    mock.execute.return_value = MagicMock(data=None)
    return mock


@pytest.fixture
def mock_gsc_credentials():
    """Mock Google OAuth credentials."""
    mock_creds = Mock()
    mock_creds.valid = True
    mock_creds.expired = False
    mock_creds.token = "mock_access_token"
    return mock_creds


@pytest.fixture
def gsc_client(mock_supabase, mock_gsc_credentials):
    """Initialize GSC client with mocks."""
    with patch("ingestion.gsc.build") as mock_build:
        mock_service = MagicMock()
        mock_build.return_value = mock_service
        
        client = GSCClient(
            credentials=mock_gsc_credentials,
            site_url="https://tradeify.co/",
            supabase_client=mock_supabase,
            user_id="test_user_123"
        )
        client.service = mock_service
        return client


@pytest.fixture
def sample_gsc_response():
    """Sample GSC API response with 3 rows."""
    return {
        "rows": [
            {
                "keys": ["best crm software"],
                "clicks": 120,
                "impressions": 5000,
                "ctr": 0.024,
                "position": 4.5
            },
            {
                "keys": ["crm pricing"],
                "clicks": 85,
                "impressions": 3200,
                "ctr": 0.0265625,
                "position": 6.2
            },
            {
                "keys": ["crm comparison"],
                "clicks": 45,
                "impressions": 1800,
                "ctr": 0.025,
                "position": 8.1
            }
        ]
    }


@pytest.fixture
def large_gsc_response():
    """GSC response with 25K rows (pagination boundary)."""
    rows = []
    for i in range(25000):
        rows.append({
            "keys": [f"query_{i}"],
            "clicks": 10 + (i % 100),
            "impressions": 500 + (i % 1000),
            "ctr": 0.02 + (i % 50) * 0.001,
            "position": 5.0 + (i % 20)
        })
    return {"rows": rows}


@pytest.fixture
def empty_gsc_response():
    """Empty GSC response."""
    return {"rows": []}


# --- Test GSCClient initialization ---

def test_gsc_client_initialization(gsc_client):
    """Test GSC client initializes correctly."""
    assert gsc_client.site_url == "https://tradeify.co/"
    assert gsc_client.user_id == "test_user_123"
    assert gsc_client.service is not None


def test_gsc_client_invalid_credentials():
    """Test GSC client raises error with invalid credentials."""
    mock_creds = Mock()
    mock_creds.valid = False
    
    with pytest.raises(GSCAuthenticationError):
        GSCClient(
            credentials=mock_creds,
            site_url="https://tradeify.co/",
            supabase_client=MagicMock(),
            user_id="test_user"
        )


# --- Test caching mechanism ---

def test_cache_hit_returns_cached_data(gsc_client, sample_gsc_response):
    """Test that cache hit returns data without API call."""
    # Set up cache hit
    cache_data = {
        "response": sample_gsc_response,
        "expires_at": (datetime.utcnow() + timedelta(hours=12)).isoformat()
    }
    gsc_client.supabase.execute.return_value = MagicMock(data=cache_data)
    
    result = gsc_client._get_from_cache("test_cache_key")
    
    assert result == sample_gsc_response
    gsc_client.supabase.table.assert_called_with("api_cache")


def test_cache_miss_returns_none(gsc_client):
    """Test that cache miss returns None."""
    gsc_client.supabase.execute.return_value = MagicMock(data=None)
    
    result = gsc_client._get_from_cache("nonexistent_key")
    
    assert result is None


def test_cache_expired_returns_none(gsc_client, sample_gsc_response):
    """Test that expired cache returns None."""
    # Set up expired cache entry
    cache_data = {
        "response": sample_gsc_response,
        "expires_at": (datetime.utcnow() - timedelta(hours=1)).isoformat()
    }
    gsc_client.supabase.execute.return_value = MagicMock(data=cache_data)
    
    result = gsc_client._get_from_cache("expired_key")
    
    assert result is None


def test_set_cache_stores_data(gsc_client, sample_gsc_response):
    """Test that data is stored in cache with TTL."""
    gsc_client._set_cache("test_key", sample_gsc_response, ttl_hours=24)
    
    # Verify upsert was called
    gsc_client.supabase.table.assert_called_with("api_cache")
    call_args = gsc_client.supabase.upsert.call_args
    assert call_args is not None


# --- Test date range splitting ---

def test_split_date_range_single_month(gsc_client):
    """Test splitting date range that fits in single month."""
    start = date(2025, 1, 1)
    end = date(2025, 1, 31)
    
    ranges = gsc_client._split_date_range(start, end)
    
    assert len(ranges) == 1
    assert ranges[0] == (start, end)


def test_split_date_range_multiple_months(gsc_client):
    """Test splitting date range across multiple months."""
    start = date(2025, 1, 1)
    end = date(2025, 3, 31)
    
    ranges = gsc_client._split_date_range(start, end)
    
    assert len(ranges) == 3
    assert ranges[0] == (date(2025, 1, 1), date(2025, 1, 31))
    assert ranges[1] == (date(2025, 2, 1), date(2025, 2, 28))
    assert ranges[2] == (date(2025, 3, 1), date(2025, 3, 31))


def test_split_date_range_partial_months(gsc_client):
    """Test splitting date range with partial start/end months."""
    start = date(2025, 1, 15)
    end = date(2025, 3, 10)
    
    ranges = gsc_client._split_date_range(start, end)
    
    assert len(ranges) == 3
    assert ranges[0] == (date(2025, 1, 15), date(2025, 1, 31))
    assert ranges[1] == (date(2025, 2, 1), date(2025, 2, 28))
    assert ranges[2] == (date(2025, 3, 1), date(2025, 3, 10))


def test_split_date_range_16_months(gsc_client):
    """Test splitting 16-month date range."""
    start = date(2024, 1, 1)
    end = date(2025, 4, 30)
    
    ranges = gsc_client._split_date_range(start, end)
    
    assert len(ranges) == 16


# --- Test pagination handling ---

def test_fetch_with_pagination_single_page(gsc_client, sample_gsc_response):
    """Test fetching data that fits in single page."""
    mock_request = MagicMock()
    mock_request.execute.return_value = sample_gsc_response
    gsc_client.service.searchanalytics.return_value.query.return_value = mock_request
    
    result = gsc_client._fetch_with_pagination(
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 31),
        dimensions=["query"],
        row_limit=25000
    )
    
    assert len(result) == 3
    assert result[0]["keys"] == ["best crm software"]


def test_fetch_with_pagination_multiple_pages(gsc_client, large_gsc_response):
    """Test fetching data requiring pagination."""
    # First page: 25K rows
    # Second page: 100 rows
    second_page = {"rows": [{"keys": [f"query_{i}"], "clicks": 1, "impressions": 10, "ctr": 0.1, "position": 5.0} for i in range(100)]}
    
    mock_request = MagicMock()
    mock_request.execute.side_effect = [large_gsc_response, second_page, empty_gsc_response]
    gsc_client.service.searchanalytics.return_value.query.return_value = mock_request
    
    result = gsc_client._fetch_with_pagination(
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 31),
        dimensions=["query"],
        row_limit=25000
    )
    
    assert len(result) == 25100  # 25K + 100
    assert mock_request.execute.call_count == 3


def test_fetch_with_pagination_stops_at_empty(gsc_client, sample_gsc_response, empty_gsc_response):
    """Test pagination stops when empty response received."""
    mock_request = MagicMock()
    mock_request.execute.side_effect = [sample_gsc_response, empty_gsc_response]
    gsc_client.service.searchanalytics.return_value.query.return_value = mock_request
    
    result = gsc_client._fetch_with_pagination(
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 31),
        dimensions=["query"],
        row_limit=25000
    )
    
    assert len(result) == 3
    assert mock_request.execute.call_count == 2


# --- Test main fetch functions ---

def test_fetch_gsc_performance_by_query(gsc_client, sample_gsc_response):
    """Test fetching performance data grouped by query."""
    mock_request = MagicMock()
    mock_request.execute.return_value = sample_gsc_response
    gsc_client.service.searchanalytics.return_value.query.return_value = mock_request
    
    result = fetch_gsc_performance(
        gsc_client=gsc_client,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 31),
        dimensions=["query"]
    )
    
    assert len(result) == 3
    assert result[0]["query"] == "best crm software"
    assert result[0]["clicks"] == 120


def test_fetch_gsc_performance_by_page(gsc_client, sample_gsc_response):
    """Test fetching performance data grouped by page."""
    page_response = {
        "rows": [
            {"keys": ["/blog/crm-guide"], "clicks": 450, "impressions": 12000, "ctr": 0.0375, "position": 5.2},
            {"keys": ["/pricing"], "clicks": 320, "impressions": 8000, "ctr": 0.04, "position": 3.1},
        ]
    }
    
    mock_request = MagicMock()
    mock_request.execute.return_value = page_response
    gsc_client.service.searchanalytics.return_value.query.return_value = mock_request
    
    result = fetch_gsc_performance(
        gsc_client=gsc_client,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 31),
        dimensions=["page"]
    )
    
    assert len(result) == 2
    assert result[0]["page"] == "/blog/crm-guide"


def test_fetch_gsc_performance_by_date(gsc_client):
    """Test fetching performance data grouped by date."""
    date_response = {
        "rows": [
            {"keys": ["2025-01-01"], "clicks": 450, "impressions": 12000, "ctr": 0.0375, "position": 5.2},
            {"keys": ["2025-01-02"], "clicks": 480, "impressions": 12500, "ctr": 0.0384, "position": 5.1},
        ]
    }
    
    mock_request = MagicMock()
    mock_request.execute.return_value = date_response
    gsc_client.service.searchanalytics.return_value.query.return_value = mock_request
    
    result = fetch_gsc_performance(
        gsc_client=gsc_client,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 31),
        dimensions=["date"]
    )
    
    assert len(result) == 2
    assert result[0]["date"] == "2025-01-01"


def test_fetch_gsc_query_page_mapping(gsc_client):
    """Test fetching query-page mapping."""
    mapping_response = {
        "rows": [
            {"keys": ["best crm", "/blog/crm-guide"], "clicks": 120, "impressions": 5000, "ctr": 0.024, "position": 4.5},
            {"keys": ["crm pricing", "/pricing"], "clicks": 85, "impressions": 3200, "ctr": 0.0265625, "position": 6.2},
        ]
    }
    
    mock_request = MagicMock()
    mock_request.execute.return_value = mapping_response
    gsc_client.service.searchanalytics.return_value.query.return_value = mock_request
    
    result = fetch_gsc_query_page_mapping(
        gsc_client=gsc_client,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 31)
    )
    
    assert len(result) == 2
    assert result[0]["query"] == "best crm"
    assert result[0]["page"] == "/blog/crm-guide"


def test_fetch_gsc_all_data(gsc_client, sample_gsc_response):
    """Test fetching all GSC data types."""
    mock_request = MagicMock()
    mock_request.execute.return_value = sample_gsc_response
    gsc_client.service.searchanalytics.return_value.query.return_value = mock_request
    
    result = fetch_gsc_all_data(
        gsc_client=gsc_client,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 31)
    )
    
    assert "by_query" in result
    assert "by_page" in result
    assert "by_date" in result
    assert "query_page_mapping" in result
    assert len(result["by_query"]) == 3


# --- Test error handling ---

def test_fetch_handles_authentication_error(gsc_client):
    """Test that authentication errors are properly raised."""
    mock_request = MagicMock()
    mock_request.execute.side_effect = Exception("401: Invalid Credentials")
    gsc_client.service.searchanalytics.return_value.query.return_value = mock_request
    
    with pytest.raises(GSCAuthenticationError):
        fetch_gsc_performance(
            gsc_client=gsc_client,
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 31),
            dimensions=["query"]
        )


def test_fetch_handles_rate_limit_error(gsc_client):
    """Test that rate limit errors are properly raised."""
    mock_request = MagicMock()
    mock_request.execute.side_effect = Exception("429: Rate Limit Exceeded")
    gsc_client.service.searchanalytics.return_value.query.return_value = mock_request
    
    with pytest.raises(GSCRateLimitError):
        fetch_gsc_performance(
            gsc_client=gsc_client,
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 31),
            dimensions=["query"]
        )


def test_fetch_handles_generic_error(gsc_client):
    """Test that generic errors are wrapped in GSCIngestionError."""
    mock_request = MagicMock()
    mock_request.execute.side_effect = Exception("500: Internal Server Error")
    gsc_client.service.searchanalytics.return_value.query.return_value = mock_request
    
    with pytest.raises(GSCIngestionError):
        fetch_gsc_performance(
            gsc_client=gsc_client,
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 31),
            dimensions=["query"]
        )


def test_fetch_with_retry_on_transient_error(gsc_client, sample_gsc_response):
    """Test that transient errors are retried."""
    mock_request = MagicMock()
    # Fail twice, then succeed
    mock_request.execute.side_effect = [
        Exception("503: Service Unavailable"),
        Exception("503: Service Unavailable"),
        sample_gsc_response
    ]
    gsc_client.service.searchanalytics.return_value.query.return_value = mock_request
    
    result = gsc_client._fetch_with_pagination(
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 31),
        dimensions=["query"],
        row_limit=25000,
        max_retries=3
    )
    
    assert len(result) == 3
    assert mock_request.execute.call_count == 3


# --- Test cache key generation ---

def test_cache_key_generation_is_deterministic(gsc_client):
    """Test that same parameters generate same cache key."""
    key1 = gsc_client._generate_cache_key(
        dimensions=["query"],
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 31)
    )
    
    key2 = gsc_client._generate_cache_key(
        dimensions=["query"],
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 31)
    )
    
    assert key1 == key2


def test_cache_key_generation_varies_with_params(gsc_client):
    """Test that different parameters generate different cache keys."""
    key1 = gsc_client._generate_cache_key(
        dimensions=["query"],
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 31)
    )
    
    key2 = gsc_client._generate_cache_key(
        dimensions=["page"],
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 31)
    )
    
    assert key1 != key2


# --- Test data deduplication ---

def test_merge_deduplicate_by_query(gsc_client):
    """Test merging and deduplicating data by query."""
    chunk1 = [
        {"query": "test query", "clicks": 10, "impressions": 100, "ctr": 0.1, "position": 5.0},
        {"query": "another query", "clicks": 20, "impressions": 200, "ctr": 0.1, "position": 6.0},
    ]
    
    chunk2 = [
        {"query": "test query", "clicks": 15, "impressions": 150, "ctr": 0.1, "position": 4.5},  # Duplicate
        {"query": "third query", "clicks": 30, "impressions": 300, "ctr": 0.1, "position": 7.0},
    ]
    
    result = gsc_client._merge_and_deduplicate([chunk1, chunk2], key_field="query")
    
    # Should sum metrics for duplicates
    assert len(result) == 3
    test_query = next(r for r in result if r["query"] == "test query")
    assert test_query["clicks"] == 25  # 10 + 15
    assert test_query["impressions"] == 250  # 100 + 150


def test_merge_deduplicate_by_page_date(gsc_client):
    """Test merging and deduplicating data by page and date."""
    chunk1 = [
        {"page": "/page1", "date": "2025-01-01", "clicks": 10, "impressions": 100, "ctr": 0.1, "position": 5.0},
    ]
    
    chunk2 = [
        {"page": "/page1", "date": "2025-01-01", "clicks": 5, "impressions": 50, "ctr": 0.1, "position": 5.2},
        {"page": "/page1", "date": "2025-01-02", "clicks": 8, "impressions": 80, "ctr": 0.1, "position": 5.1},
    ]
    
    result = gsc_client._merge_and_deduplicate([chunk1, chunk2], key_field=("page", "date"))
    
    assert len(result) == 2
    jan1 = next(r for r in result if r["date"] == "2025-01