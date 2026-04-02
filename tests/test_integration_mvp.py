import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from fastapi.testclient import TestClient
import json

from app.main import app
from app.oauth.oauth_handler import OAuthHandler
from app.data_ingestion.gsc_client import GSCClient
from app.data_ingestion.ga4_client import GA4Client
from app.analysis.module1_health_trajectory import analyze_health_trajectory
from app.analysis.module2_page_triage import analyze_page_triage
from app.analysis.module5_technical_seo import analyze_technical_seo


@pytest.fixture
def test_client():
    """FastAPI test client"""
    return TestClient(app)


@pytest.fixture
def mock_oauth_tokens():
    """Mock OAuth tokens for testing"""
    return {
        "access_token": "mock_access_token_12345",
        "refresh_token": "mock_refresh_token_67890",
        "token_type": "Bearer",
        "expires_in": 3600,
        "scope": "https://www.googleapis.com/auth/webmasters.readonly https://www.googleapis.com/auth/analytics.readonly"
    }


@pytest.fixture
def mock_gsc_data():
    """Mock GSC data for 16 months"""
    dates = pd.date_range(end=datetime.now(), periods=480, freq='D')
    
    # Daily time series with trend, seasonality, and noise
    np.random.seed(42)
    trend = np.linspace(1000, 800, 480)  # Declining trend
    seasonal = 100 * np.sin(np.arange(480) * 2 * np.pi / 7)  # Weekly seasonality
    noise = np.random.normal(0, 50, 480)
    clicks = trend + seasonal + noise
    
    daily_data = pd.DataFrame({
        'date': dates,
        'clicks': np.maximum(clicks, 0),
        'impressions': np.maximum(clicks * 10, 0),
        'ctr': np.random.uniform(0.08, 0.12, 480),
        'position': np.random.uniform(8, 12, 480)
    })
    
    # Per-page data
    pages = [f'/page-{i}' for i in range(20)]
    page_data = []
    for page in pages:
        for date in dates[-90:]:  # Last 90 days for page data
            page_data.append({
                'page': page,
                'date': date,
                'clicks': np.random.randint(10, 100),
                'impressions': np.random.randint(100, 1000),
                'ctr': np.random.uniform(0.05, 0.15),
                'position': np.random.uniform(5, 20)
            })
    
    page_daily_data = pd.DataFrame(page_data)
    
    # Per-query data
    queries = [f'keyword {i}' for i in range(50)]
    query_data = []
    for query in queries:
        query_data.append({
            'query': query,
            'clicks': np.random.randint(50, 500),
            'impressions': np.random.randint(500, 5000),
            'ctr': np.random.uniform(0.05, 0.15),
            'position': np.random.uniform(3, 15)
        })
    
    query_summary = pd.DataFrame(query_data)
    
    # Query-page mapping
    query_page_data = []
    for query in queries[:20]:
        for page in pages[:5]:
            query_page_data.append({
                'query': query,
                'page': page,
                'clicks': np.random.randint(5, 50),
                'impressions': np.random.randint(50, 500),
                'ctr': np.random.uniform(0.05, 0.15),
                'position': np.random.uniform(3, 20)
            })
    
    query_page_mapping = pd.DataFrame(query_page_data)
    
    return {
        'daily_data': daily_data,
        'page_daily_data': page_daily_data,
        'page_summary': page_daily_data.groupby('page').agg({
            'clicks': 'sum',
            'impressions': 'sum',
            'ctr': 'mean',
            'position': 'mean'
        }).reset_index(),
        'query_summary': query_summary,
        'query_page_mapping': query_page_mapping
    }


@pytest.fixture
def mock_ga4_data():
    """Mock GA4 landing page engagement data"""
    pages = [f'/page-{i}' for i in range(20)]
    
    landing_page_data = []
    for page in pages:
        landing_page_data.append({
            'landing_page': page,
            'sessions': np.random.randint(100, 1000),
            'users': np.random.randint(80, 900),
            'engagement_rate': np.random.uniform(0.3, 0.8),
            'avg_session_duration': np.random.uniform(30, 300),
            'bounce_rate': np.random.uniform(0.2, 0.9),
            'conversions': np.random.randint(0, 50)
        })
    
    return {
        'landing_pages': pd.DataFrame(landing_page_data),
        'traffic_sources': pd.DataFrame({
            'source': ['google', 'direct', 'bing', 'social'],
            'sessions': [5000, 2000, 500, 300],
            'users': [4500, 1800, 450, 280]
        }),
        'device_breakdown': pd.DataFrame({
            'device': ['mobile', 'desktop', 'tablet'],
            'sessions': [4000, 3500, 300],
            'engagement_rate': [0.55, 0.65, 0.60]
        })
    }


@pytest.fixture
def mock_crawl_data():
    """Mock site crawl data for technical SEO"""
    pages = [f'/page-{i}' for i in range(20)]
    
    crawl_results = []
    for i, page in enumerate(pages):
        crawl_results.append({
            'url': page,
            'title': f'Page {i} Title',
            'meta_description': f'Meta description for page {i}' if i % 2 == 0 else None,
            'h1': f'H1 for page {i}' if i % 3 != 0 else None,
            'word_count': np.random.randint(200, 2000),
            'canonical': page if i % 4 == 0 else None,
            'status_code': 200 if i < 18 else (404 if i == 18 else 301),
            'load_time': np.random.uniform(0.5, 3.0),
            'mobile_friendly': i % 5 != 0,
            'has_schema': i % 3 == 0,
            'schema_types': ['Article', 'BreadcrumbList'] if i % 3 == 0 else [],
            'internal_links_count': np.random.randint(5, 50),
            'external_links_count': np.random.randint(0, 10),
            'images_without_alt': np.random.randint(0, 5)
        })
    
    internal_links = []
    for from_page in pages[:15]:
        num_links = np.random.randint(3, 10)
        to_pages = np.random.choice(pages, size=num_links, replace=False)
        for to_page in to_pages:
            internal_links.append({
                'from_url': from_page,
                'to_url': to_page,
                'anchor_text': f'Link to {to_page}'
            })
    
    return {
        'pages': pd.DataFrame(crawl_results),
        'internal_links': pd.DataFrame(internal_links)
    }


class TestOAuthFlow:
    """Test OAuth token exchange and refresh"""
    
    @patch('app.oauth.oauth_handler.OAuth2Session')
    def test_oauth_authorization_url_generation(self, mock_oauth_session, test_client):
        """Test OAuth authorization URL generation"""
        mock_session = Mock()
        mock_session.authorization_url.return_value = (
            'https://accounts.google.com/o/oauth2/auth?client_id=test',
            'mock_state_123'
        )
        mock_oauth_session.return_value = mock_session
        
        response = test_client.get('/api/auth/login')
        
        assert response.status_code == 200
        data = response.json()
        assert 'authorization_url' in data
        assert 'state' in data
        assert 'accounts.google.com' in data['authorization_url']
    
    @patch('app.oauth.oauth_handler.OAuth2Session')
    @patch('app.database.db.upsert_user_tokens')
    def test_oauth_token_exchange(self, mock_db_upsert, mock_oauth_session, 
                                  test_client, mock_oauth_tokens):
        """Test OAuth callback token exchange"""
        mock_session = Mock()
        mock_session.fetch_token.return_value = mock_oauth_tokens
        mock_oauth_session.return_value = mock_session
        
        mock_db_upsert.return_value = True
        
        response = test_client.get(
            '/api/auth/callback',
            params={
                'code': 'mock_auth_code',
                'state': 'mock_state_123'
            }
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data['success'] is True
        assert 'user_id' in data
        mock_db_upsert.assert_called_once()
    
    @patch('app.oauth.oauth_handler.OAuth2Session')
    @patch('app.database.db.get_user_tokens')
    @patch('app.database.db.upsert_user_tokens')
    def test_oauth_token_refresh(self, mock_db_upsert, mock_db_get, 
                                 mock_oauth_session, mock_oauth_tokens):
        """Test OAuth token refresh when expired"""
        expired_tokens = mock_oauth_tokens.copy()
        expired_tokens['expires_at'] = (datetime.now() - timedelta(hours=1)).timestamp()
        
        refreshed_tokens = mock_oauth_tokens.copy()
        refreshed_tokens['expires_at'] = (datetime.now() + timedelta(hours=1)).timestamp()
        
        mock_db_get.return_value = expired_tokens
        
        mock_session = Mock()
        mock_session.refresh_token.return_value = refreshed_tokens
        mock_oauth_session.return_value = mock_session
        
        oauth_handler = OAuthHandler()
        result = oauth_handler.get_valid_tokens('test_user_123')
        
        assert result['access_token'] == refreshed_tokens['access_token']
        mock_session.refresh_token.assert_called_once()
        mock_db_upsert.assert_called_once()


class TestDataIngestion:
    """Test GSC and GA4 data ingestion"""
    
    @patch('app.data_ingestion.gsc_client.build')
    @patch('app.database.db.cache_api_response')
    @patch('app.database.db.get_cached_api_response')
    def test_gsc_daily_data_pull(self, mock_get_cache, mock_set_cache, 
                                 mock_gsc_build, mock_gsc_data, mock_oauth_tokens):
        """Test GSC daily performance data pull with caching"""
        mock_get_cache.return_value = None  # No cache hit
        
        mock_service = Mock()
        mock_response = {
            'rows': [
                {
                    'keys': [date.strftime('%Y-%m-%d')],
                    'clicks': int(row['clicks']),
                    'impressions': int(row['impressions']),
                    'ctr': row['ctr'],
                    'position': row['position']
                }
                for date, row in mock_gsc_data['daily_data'].iterrows()
            ]
        }
        mock_service.searchanalytics().query().execute.return_value = mock_response
        mock_gsc_build.return_value = mock_service
        
        gsc_client = GSCClient(mock_oauth_tokens)
        result = gsc_client.get_daily_performance(
            site_url='https://example.com',
            start_date=(datetime.now() - timedelta(days=480)).strftime('%Y-%m-%d'),
            end_date=datetime.now().strftime('%Y-%m-%d')
        )
        
        assert len(result) == 480
        assert 'date' in result.columns
        assert 'clicks' in result.columns
        assert 'impressions' in result.columns
        mock_set_cache.assert_called_once()
    
    @patch('app.data_ingestion.gsc_client.build')
    def test_gsc_query_page_mapping(self, mock_gsc_build, mock_gsc_data, 
                                    mock_oauth_tokens):
        """Test GSC query-page mapping extraction"""
        mock_service = Mock()
        mock_response = {
            'rows': [
                {
                    'keys': [row['query'], row['page']],
                    'clicks': int(row['clicks']),
                    'impressions': int(row['impressions']),
                    'ctr': row['ctr'],
                    'position': row['position']
                }
                for _, row in mock_gsc_data['query_page_mapping'].iterrows()
            ]
        }
        mock_service.searchanalytics().query().execute.return_value = mock_response
        mock_gsc_build.return_value = mock_service
        
        gsc_client = GSCClient(mock_oauth_tokens)
        result = gsc_client.get_query_page_mapping(
            site_url='https://example.com',
            start_date=(datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d'),
            end_date=datetime.now().strftime('%Y-%m-%d')
        )
        
        assert len(result) > 0
        assert 'query' in result.columns
        assert 'page' in result.columns
        assert 'clicks' in result.columns
    
    @patch('google.analytics.data_v1beta.BetaAnalyticsDataClient')
    @patch('app.database.db.cache_api_response')
    @patch('app.database.db.get_cached_api_response')
    def test_ga4_landing_page_data(self, mock_get_cache, mock_set_cache,
                                   mock_ga4_client, mock_ga4_data, mock_oauth_tokens):
        """Test GA4 landing page engagement data pull"""
        mock_get_cache.return_value = None
        
        mock_client_instance = Mock()
        mock_response = Mock()
        
        # Mock dimension headers
        mock_response.dimension_headers = [Mock(name='landingPage')]
        
        # Mock metric headers
        mock_response.metric_headers = [
            Mock(name='sessions'),
            Mock(name='activeUsers'),
            Mock(name='engagementRate'),
            Mock(name='averageSessionDuration'),
            Mock(name='bounceRate'),
            Mock(name='conversions')
        ]
        
        # Mock rows
        mock_rows = []
        for _, row in mock_ga4_data['landing_pages'].iterrows():
            mock_row = Mock()
            mock_row.dimension_values = [Mock(value=row['landing_page'])]
            mock_row.metric_values = [
                Mock(value=str(row['sessions'])),
                Mock(value=str(row['users'])),
                Mock(value=str(row['engagement_rate'])),
                Mock(value=str(row['avg_session_duration'])),
                Mock(value=str(row['bounce_rate'])),
                Mock(value=str(row['conversions']))
            ]
            mock_rows.append(mock_row)
        
        mock_response.rows = mock_rows
        mock_client_instance.run_report.return_value = mock_response
        mock_ga4_client.return_value = mock_client_instance
        
        ga4_client = GA4Client(mock_oauth_tokens)
        result = ga4_client.get_landing_page_engagement(
            property_id='123456789',
            start_date='2024-01-01',
            end_date='2025-01-01'
        )
        
        assert len(result) == 20
        assert 'landing_page' in result.columns
        assert 'sessions' in result.columns
        assert 'engagement_rate' in result.columns
        mock_set_cache.assert_called_once()


class TestModule1HealthTrajectory:
    """Test Module 1: Health & Trajectory Analysis"""
    
    def test_trend_classification(self, mock_gsc_data):
        """Test traffic trend classification"""
        result = analyze_health_trajectory(mock_gsc_data['daily_data'])
        
        assert 'overall_direction' in result
        assert result['overall_direction'] in ['strong_growth', 'growth', 'flat', 
                                                'decline', 'strong_decline']
        assert 'trend_slope_pct_per_month' in result
        assert isinstance(result['trend_slope_pct_per_month'], (int, float))
    
    def test_seasonality_detection(self, mock_gsc_data):
        """Test seasonality pattern detection"""
        result = analyze_health_trajectory(mock_gsc_data['daily_data'])
        
        assert 'seasonality' in result
        seasonality = result['seasonality']
        assert 'best_day' in seasonality
        assert 'worst_day' in seasonality
        assert seasonality['best_day'] in ['Monday', 'Tuesday', 'Wednesday', 
                                            'Thursday', 'Friday', 'Saturday', 'Sunday']
    
    def test_change_point_detection(self, mock_gsc_data):
        """Test structural change point detection"""
        result = analyze_health_trajectory(mock_gsc_data['daily_data'])
        
        assert 'change_points' in result
        if len(result['change_points']) > 0:
            cp = result['change_points'][0]
            assert 'date' in cp
            assert 'magnitude' in cp
            assert 'direction' in cp
            assert cp['direction'] in ['drop', 'spike', 'shift']
    
    def test_forecast_generation(self, mock_gsc_data):
        """Test 30/60/90 day traffic forecast"""
        result = analyze_health_trajectory(mock_gsc_data['daily_data'])
        
        assert 'forecast' in result
        forecast = result['forecast']
        
        for period in ['30d', '60d', '90d']:
            assert period in forecast
            assert 'clicks' in forecast[period]
            assert 'ci_low' in forecast[period]
            assert 'ci_high' in forecast[period]
            assert forecast[period]['ci_low'] <= forecast[period]['clicks']
            assert forecast[period]['clicks'] <= forecast[period]['ci_high']
    
    def test_anomaly_detection(self, mock_gsc_data):
        """Test anomaly detection in traffic data"""
        # Add a clear anomaly
        daily_data = mock_gsc_data['daily_data'].copy()
        daily_data.loc[100, 'clicks'] = daily_data['clicks'].mean() * 3
        
        result = analyze_health_trajectory(daily_data)
        
        assert 'anomalies' in result
        # Should detect the injected anomaly
        assert len(result['anomalies']) > 0


class TestModule2PageTriage:
    """Test Module 2: Page-Level Triage"""
    
    def test_page_trend_bucketing(self, mock_gsc_data, mock_ga4_data):
        """Test page classification into growing/stable/decaying/critical"""
        result = analyze_page_triage(
            mock_gsc_data['page_daily_data'],
            mock_ga4_data['landing_pages'],
            mock_gsc_data['page_summary']
        )
        
        assert 'pages' in result
        assert 'summary' in result
        
        summary = result['summary']
        assert 'growing' in summary
        assert 'stable' in summary
        assert 'decaying' in summary
        assert 'critical' in summary
        assert summary['total_pages_analyzed'] == 20
    
    def test_ctr_anomaly_detection(self, mock_gsc_data, mock_ga4_data):
        """Test CTR anomaly detection with PyOD"""
        # Create a page with anomalously low CTR
        page_summary = mock_gsc_data['page_summary'].copy()
        page_summary.loc[0, 'ctr'] = 0.01  # Very low CTR for its position
        
        result = analyze_page_triage(
            mock_gsc_data['page_daily_data'],
            mock_ga4_data['landing_pages'],
            page_summary
        )
        
        # Find pages with CTR anomalies
        ctr_anomaly_pages = [p for p in result['pages'] if p.get('ctr_anomaly', False)]
        assert len(ctr_anomaly_pages) > 0
        
        anomaly_page = ctr_anomaly_pages[0]
        assert 'ctr_expected' in anomaly_page
        assert 'ctr_actual' in anomaly_page
        assert anomaly_page['ctr_actual'] < anomaly_page['ctr_expected']
    
    def test_engagement_flags(self, mock_gsc_data, mock_ga4_data):
        """Test engagement flag detection (low engagement, high bounce)"""
        # Create a page with high search traffic but terrible engagement
        ga4_landing = mock_ga4_data['landing_pages'].copy()
        ga4_landing.loc[0, 'bounce_rate'] = 0.95
        ga4_landing.loc[0, 'avg_session_duration'] = 15
        
        result = analyze_page_triage(
            mock_gsc_data['page_daily_data'],
            ga4_landing,
            mock_gsc_data['page_summary']
        )
        
        flagged_pages = [p for p in result['pages'] if p.get('engagement_flag')]
        assert len(flagged_pages) > 0
    
    def test_priority_scoring(self, mock_gsc_data, mock_ga4_data):
        """Test priority score calculation"""
        result = analyze_page_triage(
            mock_gsc_data['page_daily_data'],
            mock_ga4_data['landing_pages'],
            mock_gsc_data['page_summary']
        )
        
        for page in result['pages']:
            assert 'priority_score' in page
            assert 0 <= page['priority_score'] <= 100
            
            # High-traffic decaying pages should have higher priority
            if page['bucket'] == 'decaying' and page['current_monthly_clicks'] > 500:
                assert page['priority_score'] > 50
    
    def test_recommended_actions(self, mock_gsc_data, mock_ga4_data):
        """Test recommended action generation"""
        result = analyze_page_triage(
            mock_gsc_data['page_daily_data'],
            mock_ga4_data['landing_pages'],
            mock_gsc_data['page_summary']
        )
        
        valid_actions = ['title_rewrite', 'content_expansion', 'redirect',
                        'consolidate', 'investigate', 'monitor']
        
        for page in result['pages']:
            if 'recommended_action' in page:
                assert page['recommended_action'] in valid_actions


class TestModule5TechnicalSEO:
    """Test Module 5: Technical SEO Analysis"""
    
    def test_missing_metadata_detection(self, mock_crawl_data, mock_gsc_data):
        """Test detection of missing titles, descriptions, H1s"""
        result = analyze_technical_seo(
            mock_crawl_data['pages'],
            mock_crawl_data['internal_links'],
            mock_gsc_data['page_summary']
        )
        
        assert 'metadata_issues' in result
        metadata = result['metadata_issues']
        
        assert 'missing_titles' in metadata
        assert 'missing_descriptions' in metadata
        assert 'missing_h1' in metadata
        
        # Based on our mock data patterns
        assert len(metadata['missing_descriptions']) > 0
        assert len(metadata['missing_h1']) > 0
    
    def test_duplicate_content_detection(self, mock_crawl_data, mock_gsc_data):
        """Test duplicate title/description detection"""
        # Add duplicate titles
        pages = mock_crawl_data['pages'].copy()
        pages.loc[1, 'title'] = pages.loc[0, 'title']
        pages.loc[2, 'title'] = pages.loc[0, 'title']
        
        result = analyze_technical_seo(
            pages,
            mock_crawl_data['internal_links'],
            mock_gsc_data['page_summary']
        )
        
        assert 'duplicate_content' in result
        duplicates = result['duplicate_content']
        
        assert 'duplicate_titles' in duplicates
        assert len(duplicates['duplicate_titles']) > 0
    
    def test_canonical_issues(self, mock_crawl_data, mock_gsc_data):
        """Test canonical tag issue detection"""
        result = analyze_technical_seo(
            mock_crawl_data['pages'],
            mock_crawl_data['internal_links'],
            mock_gsc_data['page_summary']
        )
        
        assert 'canonical_issues' in result
        canonical = result['canonical_issues']
        
        assert 'missing_canonical' in canonical
        assert 'self_referencing_canonical' in canonical
        
        # Some pages should be missing canonicals based on mock data
        assert len(canonical['missing_canonical']) > 0
    
    def test_mobile_friendliness(self, mock_crawl_data, mock_gsc_data):
        """Test mobile-friendly issue detection"""
        result = analyze_technical_seo(
            mock_crawl_data['pages'],
            mock_crawl_data['internal_links'],
            mock_gsc_data['page_summary']
        )
        
        assert 'mobile_issues' in result
        mobile = result['mobile_issues']
        
        assert 'not_mobile_friendly' in mobile
        assert isinstance(mobile['not_mobile_friendly'], list)
    
    def test_page_speed_analysis(self, mock_crawl_data, mock_gsc_data):
        """Test page speed issue flagging"""
        result = analyze_technical_seo(
            mock_crawl_data['pages'],
            mock_crawl_data['internal_links'],
            mock_gsc_data['page_summary']
        )
        
        assert 'performance_issues' in result
        perf = result['performance_issues']
        
        assert 'slow_pages' in perf
        for page in perf['slow_pages']:
            assert page['load_time'] > 2.0  # Threshold for "slow"
    
    def test_schema_markup_analysis(self, mock_crawl_data, mock_gsc_data):
        """Test schema markup presence and recommendations"""
        result = analyze_technical_seo(
            mock_crawl_data['pages'],
            mock_crawl_data['internal_links'],
            mock_gsc_data['page_summary']
        )
        
        assert 'schema_analysis' in result
        schema = result['schema_analysis']
        
        assert 'pages_with_schema' in schema
        assert 'pages_without_schema' in schema
        assert 'schema_coverage_pct' in schema
        
        assert 0 <= schema['schema_coverage_pct'] <= 100
    
    def test_internal_link_structure(self, mock_crawl_data, mock_gsc_data):
        """Test internal link graph analysis"""
        result = analyze_technical_seo(
            mock_crawl_data['pages'],
            mock_crawl_data['internal_links'],
            mock_gsc_data['page_summary']
        )
        
        assert 'internal_linking' in result
        linking = result['internal_linking']
        
        assert 'orphan_pages' in linking
        assert 'pages_with_few_internal_links' in linking
        assert 'avg_internal_links_per_page' in linking
        
        assert linking['avg_internal_links_per_page'] > 0
    
    def test_http_status_issues(self, mock_crawl_data, mock_gsc_data):
        """Test detection of 404s, redirects, etc."""
        result = analyze_technical_seo(
            mock_crawl_data['pages'],
            mock_crawl_data['internal_links'],
            mock_gsc_data['page_summary']
        )
        
        assert 'http_status_issues' in result
        status = result['http_status_issues']
        
        assert '404_errors' in status
        assert '301_redirects' in status
        
        # Based on mock data, we have 1 404 and 1 301
        assert len(status['404_errors']) == 1
        assert len(status['301_redirects']) == 1
    
    def test_image_optimization(self, mock_crawl_data, mock_gsc_data):
        """Test image optimization issues (missing alt text)"""
        result = analyze_technical_seo(
            mock_crawl_data['pages'],
            mock_crawl_data['internal_links'],
            mock_gsc_data['page_summary']
        )
        
        assert 'image_issues' in result
        images = result['image_issues']
        
        assert 'pages_with_missing_alt' in images
        assert 'total_images_without_alt' in images
        
        assert images['total_images_without_alt'] >= 0


class TestFullPipelineIntegration:
    """Integration tests for the complete MVP pipeline"""
    
    @patch('app.oauth.oauth_handler.OAuth2Session')
    @patch('app.data_ingestion.gsc_client.build')
    @patch('google.analytics.data_v1beta.BetaAnalyticsDataClient')
    @patch('app.database.db.cache_api_response')
    @patch('app.database.db.get_cached_api_response')
    @patch('app.database.db.upsert_user_tokens')
    @patch('app.database.db.get_user_tokens')
    @patch('app.database.db.save_report')
    def test_end_to_end_report_generation(
        self, mock_save_report, mock_get_tokens, mock_upsert_tokens,
        mock_get_cache, mock_set_cache, mock_ga4_client, mock_gsc_build,
        mock_oauth_session, test_client, mock_oauth_tokens, mock_gsc_data,
        mock_ga4_data, mock_crawl_data
    ):
        """Test complete pipeline from OAuth to report generation"""
        
        # Setup OAuth mocks
        mock_get_tokens.return_value = mock_oauth_tokens
        
        # Setup GSC mocks
        mock_gsc_service = Mock()
        mock_gsc_service.searchanalytics().query().execute.side_effect = [
            # Daily data
            {
                'rows': [
                    {
                        'keys': [date.strftime('%Y-%m-%d')],
                        'clicks': int(row['clicks']),
                        'impressions': int(row['impressions']),
                        'ctr': row['ctr'],
                        'position': row['position']
                    }
                    for date, row in mock_gsc_data['daily_data'].iterrows()
                ]
            },
            # Page data
            {
                'rows': [
                    {
                        'keys': [row['page'], row['date'].strftime('%Y-%m-%d')],
                        'clicks': int(row['clicks']),
                        'impressions': int(row['impressions']),
                        'ctr': row['ctr'],
                        'position': row['position']
                    }
                    for _, row in mock_gsc_data['page_daily_data'].iterrows()
                ]
            },
            # Query data
            {
                'rows': [
                    {
                        'keys': [row['query']],
                        'clicks': int(row['clicks']),
                        'impressions': int(row['impressions']),
                        'ctr': row['ctr'],
                        'position': row['position']
                    }
                    for _, row in mock_gsc_data['query_summary'].iterrows()
                ]
            }
        ]
        mock_gsc_build.return_value = mock_gsc_service
        
        # Setup GA4 mocks
        mock_ga4_instance = Mock()
        mock_ga4_response = Mock()
        mock_ga4_response.dimension_headers = [Mock(name='landingPage')]
        mock_ga4_response.metric_headers = [
            Mock(name='sessions'),
            Mock(name='activeUsers'),
            Mock(name='engagementRate'),
            Mock(name='averageSessionDuration'),
            Mock(name='bounceRate'),
            Mock(name='conversions')
        ]
        mock_ga4_rows = []
        for _, row in mock_ga4_data['landing_pages'].iterrows():
            mock_row = Mock()
            mock_row.dimension_values = [Mock(value=row['landing_page'])]
            mock_row.metric_values = [
                Mock(value=str(row['sessions'])),
                Mock(value=str(row['users'])),
                Mock(value=str(row['engagement_rate'])),
                Mock(value=str(row['avg_session_duration'])),
                Mock(value=str(row['bounce_rate'])),
                Mock(value=str(row['conversions']))
            ]
            mock_ga4_rows.append(mock_row)
        mock_ga4_response.rows = mock_ga4_rows
        mock_ga4_instance.run_report.return_value = mock_ga4_response
        mock_ga4_client.return_value = mock_ga4_instance
        
        # Setup cache mocks (no cache hits for fresh run)
        mock_get_cache.return_value = None
        
        # Setup save report mock
        mock_save_report.return_value = 'report_123456'
        
        # Initiate report generation
        response = test_client.post(
            '/api/reports/generate',
            json={
                'user_id': 'test_user_123',
                'site_url': 'https://example.com',
                'ga4_property_id': '123456789'
            }
        )
        
        assert response.status_code == 202  # Accepted for async processing
        data = response.json()
        assert 'job_id' in data
        job_id = data['job_id']
        
        # Check job status
        status_response = test_client.get(f'/api/reports/status/{job_id}')
        assert status_response.status_code == 200
        
        # Note: In real async processing, we'd wait for completion
        # For this test, we verify the mocks were called correctly
        assert mock_gsc_service.searchanalytics().query().execute.call_count >= 3
        assert mock_ga4_instance.run_report.called
        mock_save_report.assert_called_once()
    
    @patch('app.data_ingestion.gsc_client.build')
    @patch('google.analytics.data_v1beta.BetaAnalyticsDataClient')
    def test_pipeline_with_cached_data(
        self, mock_ga4_client, mock_gsc_build, test_client,
        mock_oauth_tokens, mock_gsc_data, mock_ga4_data
    ):
        """Test pipeline reuses cached API responses within TTL"""
        
        with patch('app.database.db.get_cached_api_response') as mock_get_cache:
            # Simulate cache hits
            mock_get_cache.side_effect = [
                mock_gsc_data['daily_data'].to_json(),  # GSC daily cached
                mock_gsc_data['page_daily_data'].to_json(),  # GSC page cached
                mock_ga4_data['landing_pages'].to_json()  # GA4 cached
            ]
            
            # GSC and GA4 clients should not be called if cache hits
            gsc_client = GSCClient(mock_oauth_tokens)
            ga4_client = GA4Client(mock_oauth_tokens)
            
            # Verify cache was checked
            assert mock_get_cache.call_count >= 3
            
            # GSC build should not be called due to cache hits
            assert mock_gsc_build.call_count == 0
    
    def test_error_handling_invalid_oauth_tokens(self, test_client):
        """Test error handling when OAuth tokens are invalid"""
        response = test_client.post(
            '/api/reports/generate',
            json={
                'user_id': 'nonexistent_user',
                'site_url': 'https://example.com',
                'ga4_property_id': '123456789'
            }
        )
        
        assert response.status_code in [401, 403]
        data = response.json()
        assert 'error' in data
    
    def test_error_handling_missing_gsc_property(self, test_client, mock_oauth_tokens):
        """Test error handling when GSC property doesn't exist"""
        with patch('app.database.db.get_user_tokens', return_value=mock_oauth_tokens):
            with patch('app.data_ingestion.gsc_client.build') as mock_gsc_build:
                mock_service = Mock()
                mock_service.searchanalytics().query().execute.side_effect = Exception(
                    "User does not have access to site"
                )
                mock_gsc_build.return_value = mock_service
                
                response = test_client.post(
                    '/api/reports/generate',
                    json={
                        'user_id': 'test_user_123',
                        'site_url': 'https://nonexistent.com',
                        'ga4_property_id': '123456789'
                    }
                )
                
                assert response.status_code in [400, 403]
                data = response.json()
                assert 'error' in data
    
    def test_data_flow_consistency(self, mock_gsc_data, mock_ga4_data, mock_crawl_data):
        """Test data consistency across modules"""
        
        # Run Module 1
        module1_result = analyze_health_trajectory(mock_gsc_data['daily_data'])
        
        # Run Module 2
        module2_result = analyze_page_triage(
            mock_gsc_data['page_daily_data'],
            mock_ga4_data['landing_pages'],
            mock_gsc_data['page_summary']
        )
        
        # Run Module 5
        module5_result = analyze_technical_seo(
            mock_crawl_data['pages'],
            mock_crawl_data['internal_links'],
            mock_gsc_data['page_summary']
        )
        
        # Verify data consistency
        # Pages analyzed in Module 2 should match pages in crawl data
        module2_pages = {p['url'] for p in module2_result['pages']}
        crawl_pages = set(mock_crawl_data['pages']['url'].values)
        
        # Should have significant overlap
        overlap = module2_pages.intersection(crawl_pages)
        assert len(overlap) > 0
        
        # All modules should produce valid output
        assert 'overall_direction' in module1_result
        assert 'pages' in module2_result
        assert 'metadata_issues' in module5_result


class TestReportOutputStructure:
    """Test the final report JSON structure"""
    
    def test_report_json_schema(self, mock_gsc_data, mock_ga4_data, mock_crawl_data):
        """Test that report output matches expected schema"""
        
        # Generate all module outputs
        module1 = analyze_health_trajectory(mock_gsc_data['daily_data'])
        module2 = analyze_page_triage(
            mock_gsc_data['page_daily_data'],
            mock_ga4_data['landing_pages'],
            mock_gsc_data['page_summary']
        )
        module5 = analyze_technical_seo(
            mock_crawl_data['pages'],
            mock_crawl_data['internal_links'],
            mock_gsc_data['page_summary']
        )
        
        # Construct report
        report = {
            'report_id': 'test_report_123',
            'site_url': 'https://example.com',
            'generated_at': datetime.now().isoformat(),
            'modules': {
                'health_trajectory': module1,
                'page_triage': module2,
                'technical_seo': module5
            }
        }
        
        # Validate structure
        assert 'report_id' in report
        assert 'site_url' in report
        assert 'generated_at' in report
        assert 'modules' in report
        
        modules = report['modules']
        assert 'health_trajectory' in modules
        assert 'page_triage' in modules
        assert 'technical_seo' in modules
        
        # Validate JSON serializability
        json_str = json.dumps(report, default=str)
        assert len(json_str) > 0
        
        # Validate deserialization
        parsed = json.loads(json_str)
        assert parsed['report_id'] == report['report_id']
    
    def test_report_has_actionable_recommendations(self, mock_gsc_data, 
                                                   mock_ga4_data, mock_crawl_data):
        """Test that report contains actionable recommendations"""
        
        module2 = analyze_page_triage(
            mock_gsc_data['page_daily_data'],
            mock_ga4_data['landing_pages'],
            mock_gsc_data['page_summary']
        )
        
        module5 = analyze_technical_seo(
            mock_crawl_data['pages'],
            mock_crawl_data['internal_links'],
            mock_gsc_data['page_summary']
        )
        
        # Module 2 should have recommended actions
        assert 'pages' in module2
        pages_with_actions = [p for p in module2['pages'] if 'recommended_action' in p]
        assert len(pages_with_actions) > 0
        
        # Module 5 should have fixable issues
        assert 'metadata_issues' in module5
        metadata = module5['metadata_issues']
        total_issues = (
            len(metadata.get('missing_titles', [])) +
            len(metadata.get('missing_descriptions', [])) +
            len(metadata.get('missing_h1', []))
        )
        assert total_issues > 0  # Should find some issues in mock data


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
