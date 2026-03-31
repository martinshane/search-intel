import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
import pandas as pd
from ingestion.ga4 import (
    GA4Client,
    GA4ReportType,
    get_ga4_client,
    fetch_all_ga4_reports,
    GA4IngestionError
)


@pytest.fixture
def mock_credentials():
    """Mock Google OAuth credentials."""
    mock_creds = Mock()
    mock_creds.valid = True
    mock_creds.expired = False
    mock_creds.token = "mock_access_token"
    return mock_creds


@pytest.fixture
def ga4_client(mock_credentials):
    """Create GA4Client instance with mocked credentials."""
    with patch('ingestion.ga4.BetaAnalyticsDataClient'):
        client = GA4Client(
            credentials=mock_credentials,
            property_id="properties/123456789"
        )
        return client


@pytest.fixture
def date_range():
    """Standard 16-month date range for testing."""
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=480)  # ~16 months
    return start_date, end_date


@pytest.fixture
def mock_ga4_response():
    """Mock GA4 API response structure."""
    def create_response(rows_data):
        mock_response = Mock()
        mock_rows = []
        for row_data in rows_data:
            mock_row = Mock()
            mock_row.dimension_values = [Mock(value=str(v)) for v in row_data['dimensions']]
            mock_row.metric_values = [Mock(value=str(v)) for v in row_data['metrics']]
            mock_rows.append(mock_row)
        mock_response.rows = mock_rows
        mock_response.row_count = len(mock_rows)
        return mock_response
    return create_response


class TestGA4Client:
    """Test GA4Client initialization and configuration."""

    def test_client_initialization(self, mock_credentials):
        """Test client initializes correctly with credentials."""
        with patch('ingestion.ga4.BetaAnalyticsDataClient') as mock_client:
            client = GA4Client(
                credentials=mock_credentials,
                property_id="properties/123456789"
            )
            assert client.property_id == "properties/123456789"
            assert client.credentials == mock_credentials
            mock_client.assert_called_once_with(credentials=mock_credentials)

    def test_client_initialization_without_property_id(self, mock_credentials):
        """Test client can be initialized without property_id."""
        with patch('ingestion.ga4.BetaAnalyticsDataClient'):
            client = GA4Client(credentials=mock_credentials)
            assert client.property_id is None

    def test_get_ga4_client_factory(self, mock_credentials):
        """Test factory function creates client correctly."""
        with patch('ingestion.ga4.BetaAnalyticsDataClient'):
            client = get_ga4_client(
                credentials=mock_credentials,
                property_id="properties/123456789"
            )
            assert isinstance(client, GA4Client)
            assert client.property_id == "properties/123456789"


class TestTrafficOverview:
    """Test traffic overview report generation."""

    def test_fetch_traffic_overview_success(self, ga4_client, date_range, mock_ga4_response):
        """Test successful traffic overview fetch."""
        start_date, end_date = date_range
        
        mock_response = mock_ga4_response([
            {
                'dimensions': ['20250101'],
                'metrics': [100, 80, 250, 0.65, 120.5, 1.8]
            },
            {
                'dimensions': ['20250102'],
                'metrics': [110, 85, 270, 0.68, 125.0, 1.9]
            }
        ])
        
        ga4_client.client.run_report = Mock(return_value=mock_response)
        
        result = ga4_client.fetch_traffic_overview(start_date, end_date)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert 'date' in result.columns
        assert 'sessions' in result.columns
        assert 'users' in result.columns
        assert 'pageviews' in result.columns
        assert 'bounce_rate' in result.columns
        assert 'avg_session_duration' in result.columns
        assert 'pages_per_session' in result.columns
        
        assert result.iloc[0]['sessions'] == 100
        assert result.iloc[1]['users'] == 85

    def test_fetch_traffic_overview_empty_response(self, ga4_client, date_range, mock_ga4_response):
        """Test traffic overview with empty response."""
        start_date, end_date = date_range
        
        mock_response = mock_ga4_response([])
        ga4_client.client.run_report = Mock(return_value=mock_response)
        
        result = ga4_client.fetch_traffic_overview(start_date, end_date)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_fetch_traffic_overview_api_error(self, ga4_client, date_range):
        """Test traffic overview handles API errors."""
        start_date, end_date = date_range
        
        ga4_client.client.run_report = Mock(side_effect=Exception("API Error"))
        
        with pytest.raises(GA4IngestionError) as exc_info:
            ga4_client.fetch_traffic_overview(start_date, end_date)
        
        assert "Failed to fetch traffic overview" in str(exc_info.value)


class TestLandingPages:
    """Test landing pages report generation."""

    def test_fetch_landing_pages_success(self, ga4_client, date_range, mock_ga4_response):
        """Test successful landing pages fetch."""
        start_date, end_date = date_range
        
        mock_response = mock_ga4_response([
            {
                'dimensions': ['/blog/post-1'],
                'metrics': [150, 120, 0.45, 85.5, 2.1]
            },
            {
                'dimensions': ['/products/item-a'],
                'metrics': [200, 180, 0.35, 120.0, 2.5]
            }
        ])
        
        ga4_client.client.run_report = Mock(return_value=mock_response)
        
        result = ga4_client.fetch_landing_pages(start_date, end_date)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert 'landing_page' in result.columns
        assert 'sessions' in result.columns
        assert 'users' in result.columns
        assert 'bounce_rate' in result.columns
        assert 'avg_session_duration' in result.columns
        assert 'pages_per_session' in result.columns
        
        assert result.iloc[0]['landing_page'] == '/blog/post-1'
        assert result.iloc[1]['sessions'] == 200


class TestChannelGrouping:
    """Test channel grouping report generation."""

    def test_fetch_traffic_by_channel_success(self, ga4_client, date_range, mock_ga4_response):
        """Test successful channel grouping fetch."""
        start_date, end_date = date_range
        
        mock_response = mock_ga4_response([
            {
                'dimensions': ['Organic Search'],
                'metrics': [500, 400, 1200]
            },
            {
                'dimensions': ['Direct'],
                'metrics': [300, 280, 750]
            },
            {
                'dimensions': ['Referral'],
                'metrics': [150, 140, 380]
            }
        ])
        
        ga4_client.client.run_report = Mock(return_value=mock_response)
        
        result = ga4_client.fetch_traffic_by_channel(start_date, end_date)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        assert 'channel_group' in result.columns
        assert 'sessions' in result.columns
        assert 'users' in result.columns
        assert 'pageviews' in result.columns
        
        assert result.iloc[0]['channel_group'] == 'Organic Search'
        assert result.iloc[0]['sessions'] == 500


class TestSourceMedium:
    """Test source/medium report generation."""

    def test_fetch_traffic_by_source_medium_success(self, ga4_client, date_range, mock_ga4_response):
        """Test successful source/medium fetch."""
        start_date, end_date = date_range
        
        mock_response = mock_ga4_response([
            {
                'dimensions': ['google', 'organic'],
                'metrics': [450, 380, 1100]
            },
            {
                'dimensions': ['(direct)', '(none)'],
                'metrics': [300, 280, 750]
            }
        ])
        
        ga4_client.client.run_report = Mock(return_value=mock_response)
        
        result = ga4_client.fetch_traffic_by_source_medium(start_date, end_date)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert 'source' in result.columns
        assert 'medium' in result.columns
        assert 'sessions' in result.columns
        
        assert result.iloc[0]['source'] == 'google'
        assert result.iloc[0]['medium'] == 'organic'


class TestConversions:
    """Test conversions report generation."""

    def test_fetch_conversions_success(self, ga4_client, date_range, mock_ga4_response):
        """Test successful conversions fetch."""
        start_date, end_date = date_range
        
        mock_response = mock_ga4_response([
            {
                'dimensions': ['purchase'],
                'metrics': [45, 6750.50]
            },
            {
                'dimensions': ['sign_up'],
                'metrics': [120, 0]
            }
        ])
        
        ga4_client.client.run_report = Mock(return_value=mock_response)
        
        result = ga4_client.fetch_conversions(start_date, end_date)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert 'event_name' in result.columns
        assert 'conversions' in result.columns
        assert 'total_revenue' in result.columns
        
        assert result.iloc[0]['event_name'] == 'purchase'
        assert result.iloc[0]['conversions'] == 45
        assert result.iloc[0]['total_revenue'] == 6750.50


class TestPagePathByDate:
    """Test page path by date report generation."""

    def test_fetch_page_path_by_date_success(self, ga4_client, date_range, mock_ga4_response):
        """Test successful page path by date fetch."""
        start_date, end_date = date_range
        
        mock_response = mock_ga4_response([
            {
                'dimensions': ['/blog/post-1', '20250101'],
                'metrics': [50, 40, 125]
            },
            {
                'dimensions': ['/blog/post-1', '20250102'],
                'metrics': [55, 42, 130]
            }
        ])
        
        ga4_client.client.run_report = Mock(return_value=mock_response)
        
        result = ga4_client.fetch_page_path_by_date(start_date, end_date)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert 'page_path' in result.columns
        assert 'date' in result.columns
        assert 'sessions' in result.columns
        assert 'pageviews' in result.columns
        
        assert result.iloc[0]['page_path'] == '/blog/post-1'


class TestPagePathBySource:
    """Test page path by source report generation."""

    def test_fetch_page_path_by_source_success(self, ga4_client, date_range, mock_ga4_response):
        """Test successful page path by source fetch."""
        start_date, end_date = date_range
        
        mock_response = mock_ga4_response([
            {
                'dimensions': ['/products/item-a', 'google'],
                'metrics': [120, 95, 280]
            },
            {
                'dimensions': ['/products/item-a', '(direct)'],
                'metrics': [80, 75, 190]
            }
        ])
        
        ga4_client.client.run_report = Mock(return_value=mock_response)
        
        result = ga4_client.fetch_page_path_by_source(start_date, end_date)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert 'page_path' in result.columns
        assert 'source' in result.columns
        assert 'sessions' in result.columns
        
        assert result.iloc[0]['page_path'] == '/products/item-a'
        assert result.iloc[0]['source'] == 'google'


class TestDeviceBreakdown:
    """Test device breakdown report generation."""

    def test_fetch_device_breakdown_success(self, ga4_client, date_range, mock_ga4_response):
        """Test successful device breakdown fetch."""
        start_date, end_date = date_range
        
        mock_response = mock_ga4_response([
            {
                'dimensions': ['desktop'],
                'metrics': [400, 350, 980]
            },
            {
                'dimensions': ['mobile'],
                'metrics': [350, 320, 820]
            },
            {
                'dimensions': ['tablet'],
                'metrics': [50, 45, 120]
            }
        ])
        
        ga4_client.client.run_report = Mock(return_value=mock_response)
        
        result = ga4_client.fetch_device_breakdown(start_date, end_date)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        assert 'device_category' in result.columns
        assert 'sessions' in result.columns
        assert 'users' in result.columns
        assert 'pageviews' in result.columns
        
        assert result.iloc[0]['device_category'] == 'desktop'
        assert result.iloc[1]['device_category'] == 'mobile'


class TestCaching:
    """Test response caching functionality."""

    @patch('ingestion.ga4.get_cached_response')
    @patch('ingestion.ga4.cache_response')
    def test_cache_hit(self, mock_cache_set, mock_cache_get, ga4_client, date_range):
        """Test that cached responses are used when available."""
        start_date, end_date = date_range
        
        cached_df = pd.DataFrame({
            'date': ['2025-01-01', '2025-01-02'],
            'sessions': [100, 110],
            'users': [80, 85]
        })
        mock_cache_get.return_value = cached_df
        
        result = ga4_client.fetch_traffic_overview(start_date, end_date, use_cache=True)
        
        mock_cache_get.assert_called_once()
        mock_cache_set.assert_not_called()
        assert result.equals(cached_df)

    @patch('ingestion.ga4.get_cached_response')
    @patch('ingestion.ga4.cache_response')
    def test_cache_miss(self, mock_cache_set, mock_cache_get, ga4_client, date_range, mock_ga4_response):
        """Test that API is called and result cached on cache miss."""
        start_date, end_date = date_range
        
        mock_cache_get.return_value = None
        
        mock_response = mock_ga4_response([
            {
                'dimensions': ['20250101'],
                'metrics': [100, 80, 250, 0.65, 120.5, 1.8]
            }
        ])
        ga4_client.client.run_report = Mock(return_value=mock_response)
        
        result = ga4_client.fetch_traffic_overview(start_date, end_date, use_cache=True)
        
        mock_cache_get.assert_called_once()
        mock_cache_set.assert_called_once()
        assert isinstance(result, pd.DataFrame)

    @patch('ingestion.ga4.get_cached_response')
    def test_cache_bypass(self, mock_cache_get, ga4_client, date_range, mock_ga4_response):
        """Test that cache can be bypassed with use_cache=False."""
        start_date, end_date = date_range
        
        mock_response = mock_ga4_response([
            {
                'dimensions': ['20250101'],
                'metrics': [100, 80, 250, 0.65, 120.5, 1.8]
            }
        ])
        ga4_client.client.run_report = Mock(return_value=mock_response)
        
        result = ga4_client.fetch_traffic_overview(start_date, end_date, use_cache=False)
        
        mock_cache_get.assert_not_called()
        assert isinstance(result, pd.DataFrame)


class TestFetchAllReports:
    """Test batch fetching of all report types."""

    @patch('ingestion.ga4.GA4Client')
    def test_fetch_all_reports_success(self, mock_client_class, mock_credentials, date_range):
        """Test successful batch fetch of all reports."""
        start_date, end_date = date_range
        
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        
        # Mock all report methods
        mock_client.fetch_traffic_overview.return_value = pd.DataFrame({'sessions': [100]})
        mock_client.fetch_landing_pages.return_value = pd.DataFrame({'landing_page': ['/']})
        mock_client.fetch_traffic_by_channel.return_value = pd.DataFrame({'channel_group': ['Organic']})
        mock_client.fetch_traffic_by_source_medium.return_value = pd.DataFrame({'source': ['google']})
        mock_client.fetch_conversions.return_value = pd.DataFrame({'event_name': ['purchase']})
        mock_client.fetch_page_path_by_date.return_value = pd.DataFrame({'page_path': ['/']})
        mock_client.fetch_page_path_by_source.return_value = pd.DataFrame({'page_path': ['/']})
        mock_client.fetch_device_breakdown.return_value = pd.DataFrame({'device_category': ['desktop']})
        
        results = fetch_all_ga4_reports(
            credentials=mock_credentials,
            property_id="properties/123456789",
            start_date=start_date,
            end_date=end_date
        )
        
        assert len(results) == 8
        assert GA4ReportType.TRAFFIC_OVERVIEW in results
        assert GA4ReportType.LANDING_PAGES in results
        assert GA4ReportType.CHANNEL_GROUPING in results
        assert GA4ReportType.SOURCE_MEDIUM in results
        assert GA4ReportType.CONVERSIONS in results
        assert GA4ReportType.PAGE_PATH_BY_DATE in results
        assert GA4ReportType.PAGE_PATH_BY_SOURCE in results
        assert GA4ReportType.DEVICE_BREAKDOWN in results

    @patch('ingestion.ga4.GA4Client')
    def test_fetch_all_reports_partial_failure(self, mock_client_class, mock_credentials, date_range):
        """Test batch fetch with some reports failing."""
        start_date, end_date = date_range
        
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        
        # Some reports succeed
        mock_client.fetch_traffic_overview.return_value = pd.DataFrame({'sessions': [100]})
        mock_client.fetch_landing_pages.return_value = pd.DataFrame({'landing_page': ['/']})
        
        # Some reports fail
        mock_client.fetch_traffic_by_channel.side_effect = GA4IngestionError("API Error")
        mock_client.fetch_traffic_by_source_medium.side_effect = GA4IngestionError("API Error")
        mock_client.fetch_conversions.return_value = pd.DataFrame({'event_name': ['purchase']})
        mock_client.fetch_page_path_by_date.side_effect = GA4IngestionError("API Error")
        mock_client.fetch_page_path_by_source.return_value = pd.DataFrame({'page_path': ['/']})
        mock_client.fetch_device_breakdown.return_value = pd.DataFrame({'device_category': ['desktop']})
        
        results = fetch_all_ga4_reports(
            credentials=mock_credentials,
            property_id="properties/123456789",
            start_date=start_date,
            end_date=end_date,
            raise_on_error=False
        )
        
        # Should return successful reports only
        assert len(results) == 5
        assert GA4ReportType.TRAFFIC_OVERVIEW in results
        assert GA4ReportType.CHANNEL_GROUPING not in results


class TestErrorHandling:
    """Test error handling scenarios."""

    def test_invalid_property_id_format(self, mock_credentials):
        """Test error with invalid property ID format."""
        with patch('ingestion.ga4.BetaAnalyticsDataClient'):
            with pytest.raises(ValueError) as exc_info:
                GA4Client(
                    credentials=mock_credentials,
                    property_id="invalid_format"
                )
            assert "Property ID must start with 'properties/'" in str(exc_info.value)

    def test_missing_credentials(self):
        """Test error when credentials are missing."""
        with pytest.raises(ValueError) as exc_info:
            GA4Client(credentials=None, property_id="properties/123456789")
        assert "Credentials are required" in str(exc_info.value)

    def test_invalid_date_range(self, ga4_client):
        """Test error with invalid date range."""
        start_date = datetime.now().date()
        end_date = start_date - timedelta(days=30)
        
        with pytest.raises(ValueError) as exc_info:
            ga4_client.fetch_traffic_overview(start_date, end_date)
        assert "Start date must be before end date" in str(exc_info.value)

    def test_api_quota_exceeded(self, ga4_client, date_range):
        """Test handling of API quota exceeded error."""
        start_date, end_date = date_range
        
        quota_error = Exception("Quota exceeded")
        ga4_client.client.run_report = Mock(side_effect=quota_error)
        
        with pytest.raises(GA4IngestionError) as exc_info:
            ga4_client.fetch_traffic_overview(start_date, end_date)
        assert "Failed to fetch traffic overview" in str(exc_info.value)


    def test_network_timeout(self, ga4_client, date_range):
        """Test handling of network timeout error."""
        start_date, end_date = date_range
        
        timeout_error = Exception("Connection timed out")
        ga4_client.client.run_report = Mock(side_effect=timeout_error)
        
        with pytest.raises(GA4IngestionError) as exc_info:
            ga4_client.fetch_traffic_overview(start_date, end_date)
        assert "Failed to fetch traffic overview" in str(exc_info.value)
