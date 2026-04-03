"""
Integration tests for OAuth flows, token storage, and data ingestion endpoints.

Tests the complete flow:
1. Mock OAuth callbacks for GSC and GA4
2. Verify token storage in Supabase
3. Test data ingestion endpoints with mock data
4. Validate that modules 1, 2, and 5 execute successfully
"""

import pytest
import json
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
from fastapi.testclient import TestClient

from app.main import app
from app.services.oauth_service import OAuthService
from app.services.gsc_service import GSCService
from app.services.ga4_service import GA4Service
from app.modules.health_trajectory import analyze_health_trajectory
from app.modules.page_triage import analyze_page_triage
from app.modules.gameplan import generate_gameplan


@pytest.fixture
def client():
    """Create test client."""
    return TestClient(app)


@pytest.fixture
def mock_supabase():
    """Mock Supabase client."""
    with patch('app.database.supabase_client') as mock:
        mock_table = MagicMock()
        mock.table.return_value = mock_table
        
        # Mock insert
        mock_table.insert.return_value.execute.return_value = MagicMock(
            data=[{"id": "test-user-123"}]
        )
        
        # Mock select
        mock_table.select.return_value.eq.return_value.execute.return_value = MagicMock(
            data=[{
                "id": "test-user-123",
                "gsc_tokens": json.dumps({
                    "access_token": "gsc-access-token",
                    "refresh_token": "gsc-refresh-token",
                    "expires_at": (datetime.now() + timedelta(hours=1)).timestamp()
                }),
                "ga4_tokens": json.dumps({
                    "access_token": "ga4-access-token",
                    "refresh_token": "ga4-refresh-token",
                    "expires_at": (datetime.now() + timedelta(hours=1)).timestamp()
                }),
                "gsc_property": "https://example.com",
                "ga4_property_id": "123456789"
            }]
        )
        
        # Mock update
        mock_table.update.return_value.eq.return_value.execute.return_value = MagicMock(
            data=[{"id": "test-user-123"}]
        )
        
        yield mock


@pytest.fixture
def mock_gsc_daily_data():
    """Generate mock GSC daily time series data for 16 months."""
    dates = pd.date_range(
        end=datetime.now().date(),
        periods=480,
        freq='D'
    )
    
    # Create realistic traffic pattern with seasonality and trend
    base_clicks = 1000
    trend = 0.001  # Slight growth
    noise = 0.1
    
    clicks = []
    impressions = []
    for i, date in enumerate(dates):
        # Day of week seasonality (lower on weekends)
        dow_factor = 0.7 if date.dayofweek >= 5 else 1.0
        
        # Monthly seasonality
        month_factor = 1 + 0.15 * (date.day % 7 / 7)
        
        # Trend
        trend_factor = 1 + (trend * i)
        
        # Random noise
        noise_factor = 1 + (pd.np.random.random() - 0.5) * noise
        
        daily_clicks = base_clicks * dow_factor * month_factor * trend_factor * noise_factor
        daily_impressions = daily_clicks * (10 + pd.np.random.random() * 5)
        
        clicks.append(int(daily_clicks))
        impressions.append(int(daily_impressions))
    
    return pd.DataFrame({
        'date': dates,
        'clicks': clicks,
        'impressions': impressions,
        'ctr': [c / i if i > 0 else 0 for c, i in zip(clicks, impressions)],
        'position': [5 + pd.np.random.random() * 3 for _ in dates]
    })


@pytest.fixture
def mock_gsc_page_data():
    """Generate mock GSC page-level data."""
    pages = [
        '/blog/best-widgets',
        '/products/widget-pro',
        '/blog/widget-guide',
        '/about',
        '/blog/widget-comparison'
    ]
    
    data = []
    for page in pages:
        # Generate 120 days of data per page
        for i in range(120):
            date = datetime.now().date() - timedelta(days=120 - i)
            
            # Different patterns for different pages
            if 'decaying' in page or page == '/blog/best-widgets':
                base_clicks = 50 - (i * 0.3)  # Decaying
            elif 'growing' in page or page == '/products/widget-pro':
                base_clicks = 30 + (i * 0.2)  # Growing
            else:
                base_clicks = 40  # Stable
            
            clicks = max(1, int(base_clicks + pd.np.random.random() * 10))
            impressions = clicks * (8 + pd.np.random.random() * 4)
            position = 5 + pd.np.random.random() * 5
            
            data.append({
                'page': page,
                'date': date,
                'clicks': clicks,
                'impressions': int(impressions),
                'ctr': clicks / impressions if impressions > 0 else 0,
                'position': position
            })
    
    return pd.DataFrame(data)


@pytest.fixture
def mock_ga4_landing_data():
    """Generate mock GA4 landing page engagement data."""
    pages = [
        '/blog/best-widgets',
        '/products/widget-pro',
        '/blog/widget-guide',
        '/about',
        '/blog/widget-comparison'
    ]
    
    data = []
    for page in pages:
        # Different engagement patterns
        if 'products' in page:
            bounce_rate = 0.35
            avg_session = 180
        elif page == '/blog/best-widgets':
            bounce_rate = 0.85  # High bounce
            avg_session = 25  # Low session
        else:
            bounce_rate = 0.50
            avg_session = 90
        
        data.append({
            'landing_page': page,
            'sessions': int(1000 + pd.np.random.random() * 500),
            'bounce_rate': bounce_rate + (pd.np.random.random() - 0.5) * 0.1,
            'avg_session_duration': avg_session + pd.np.random.random() * 30,
            'conversions': int(pd.np.random.random() * 50)
        })
    
    return pd.DataFrame(data)


@pytest.fixture
def mock_gsc_page_summary():
    """Generate mock GSC page summary data."""
    pages = [
        '/blog/best-widgets',
        '/products/widget-pro',
        '/blog/widget-guide',
        '/about',
        '/blog/widget-comparison'
    ]
    
    data = []
    for page in pages:
        data.append({
            'page': page,
            'total_clicks': int(1000 + pd.np.random.random() * 2000),
            'total_impressions': int(10000 + pd.np.random.random() * 20000),
            'avg_ctr': 0.05 + pd.np.random.random() * 0.05,
            'avg_position': 5 + pd.np.random.random() * 5
        })
    
    return pd.DataFrame(data)


class TestOAuthFlow:
    """Test OAuth authentication and token storage flows."""
    
    @patch('app.services.oauth_service.google_oauth')
    def test_gsc_oauth_initiation(self, mock_google_oauth, client):
        """Test GSC OAuth flow initiation."""
        mock_google_oauth.create_authorization_url.return_value = (
            'https://accounts.google.com/o/oauth2/auth?...',
            'test-state'
        )
        
        response = client.get('/api/auth/gsc/login')
        
        assert response.status_code == 200
        data = response.json()
        assert 'authorization_url' in data
        assert 'state' in data
        assert data['state'] == 'test-state'
    
    @patch('app.services.oauth_service.google_oauth')
    def test_gsc_oauth_callback(self, mock_google_oauth, client, mock_supabase):
        """Test GSC OAuth callback and token storage."""
        # Mock token exchange
        mock_google_oauth.fetch_token.return_value = {
            'access_token': 'gsc-access-token',
            'refresh_token': 'gsc-refresh-token',
            'expires_in': 3600,
            'token_type': 'Bearer'
        }
        
        # Mock userinfo request
        mock_google_oauth.get.return_value.json.return_value = {
            'email': 'test@example.com',
            'sub': 'google-user-123'
        }
        
        response = client.get(
            '/api/auth/gsc/callback',
            params={
                'code': 'test-auth-code',
                'state': 'test-state'
            }
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data['status'] == 'success'
        assert 'user_id' in data
        
        # Verify token storage was called
        mock_supabase.table.assert_called()
    
    @patch('app.services.oauth_service.google_oauth')
    def test_ga4_oauth_initiation(self, mock_google_oauth, client):
        """Test GA4 OAuth flow initiation."""
        mock_google_oauth.create_authorization_url.return_value = (
            'https://accounts.google.com/o/oauth2/auth?...',
            'test-state-ga4'
        )
        
        response = client.get('/api/auth/ga4/login')
        
        assert response.status_code == 200
        data = response.json()
        assert 'authorization_url' in data
        assert 'state' in data
        assert data['state'] == 'test-state-ga4'
    
    @patch('app.services.oauth_service.google_oauth')
    def test_ga4_oauth_callback(self, mock_google_oauth, client, mock_supabase):
        """Test GA4 OAuth callback and token storage."""
        # Mock token exchange
        mock_google_oauth.fetch_token.return_value = {
            'access_token': 'ga4-access-token',
            'refresh_token': 'ga4-refresh-token',
            'expires_in': 3600,
            'token_type': 'Bearer'
        }
        
        # Mock userinfo request
        mock_google_oauth.get.return_value.json.return_value = {
            'email': 'test@example.com',
            'sub': 'google-user-123'
        }
        
        response = client.get(
            '/api/auth/ga4/callback',
            params={
                'code': 'test-auth-code',
                'state': 'test-state-ga4',
                'user_id': 'test-user-123'
            }
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data['status'] == 'success'
        
        # Verify token update was called
        mock_supabase.table.assert_called()
    
    def test_token_retrieval(self, mock_supabase):
        """Test token retrieval from Supabase."""
        oauth_service = OAuthService()
        
        tokens = oauth_service.get_user_tokens('test-user-123', 'gsc')
        
        assert tokens is not None
        assert tokens['access_token'] == 'gsc-access-token'
        assert tokens['refresh_token'] == 'gsc-refresh-token'
        assert 'expires_at' in tokens
    
    @patch('app.services.oauth_service.google_oauth')
    def test_token_refresh(self, mock_google_oauth, mock_supabase):
        """Test automatic token refresh when expired."""
        oauth_service = OAuthService()
        
        # Mock expired token
        expired_tokens = {
            'access_token': 'old-access-token',
            'refresh_token': 'refresh-token',
            'expires_at': (datetime.now() - timedelta(hours=1)).timestamp()
        }
        
        # Mock token refresh
        mock_google_oauth.refresh_token.return_value = {
            'access_token': 'new-access-token',
            'refresh_token': 'refresh-token',
            'expires_in': 3600
        }
        
        new_tokens = oauth_service.refresh_access_token(
            'test-user-123',
            'gsc',
            expired_tokens
        )
        
        assert new_tokens['access_token'] == 'new-access-token'
        assert new_tokens['expires_at'] > datetime.now().timestamp()


class TestDataIngestionEndpoints:
    """Test data ingestion endpoints with mock data."""
    
    @patch('app.services.gsc_service.GSCService.fetch_performance_data')
    def test_gsc_ingestion_endpoint(
        self,
        mock_fetch,
        client,
        mock_supabase,
        mock_gsc_daily_data
    ):
        """Test /api/ingest/gsc endpoint."""
        # Mock GSC API response
        mock_fetch.return_value = mock_gsc_daily_data.to_dict('records')
        
        response = client.post(
            '/api/ingest/gsc',
            json={
                'user_id': 'test-user-123',
                'property': 'https://example.com',
                'start_date': '2024-01-01',
                'end_date': '2025-04-01'
            }
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data['status'] == 'success'
        assert 'rows_ingested' in data
        assert data['rows_ingested'] > 0
        assert 'cache_key' in data
    
    @patch('app.services.gsc_service.GSCService.fetch_page_performance')
    def test_gsc_page_ingestion(
        self,
        mock_fetch,
        client,
        mock_supabase,
        mock_gsc_page_data
    ):
        """Test GSC page-level data ingestion."""
        mock_fetch.return_value = mock_gsc_page_data.to_dict('records')
        
        response = client.post(
            '/api/ingest/gsc/pages',
            json={
                'user_id': 'test-user-123',
                'property': 'https://example.com',
                'start_date': '2024-01-01',
                'end_date': '2025-04-01'
            }
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data['status'] == 'success'
        assert data['pages_ingested'] > 0
    
    @patch('app.services.ga4_service.GA4Service.fetch_landing_pages')
    def test_ga4_ingestion_endpoint(
        self,
        mock_fetch,
        client,
        mock_supabase,
        mock_ga4_landing_data
    ):
        """Test /api/ingest/ga4 endpoint."""
        mock_fetch.return_value = mock_ga4_landing_data.to_dict('records')
        
        response = client.post(
            '/api/ingest/ga4',
            json={
                'user_id': 'test-user-123',
                'property_id': '123456789',
                'start_date': '2024-01-01',
                'end_date': '2025-04-01'
            }
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data['status'] == 'success'
        assert 'rows_ingested' in data
        assert data['rows_ingested'] > 0
    
    @patch('app.services.gsc_service.GSCService.fetch_performance_data')
    def test_data_caching(self, mock_fetch, client, mock_supabase, mock_gsc_daily_data):
        """Test that subsequent requests use cached data."""
        mock_fetch.return_value = mock_gsc_daily_data.to_dict('records')
        
        # First request
        response1 = client.post(
            '/api/ingest/gsc',
            json={
                'user_id': 'test-user-123',
                'property': 'https://example.com',
                'start_date': '2024-01-01',
                'end_date': '2025-04-01'
            }
        )
        
        assert response1.status_code == 200
        cache_key1 = response1.json()['cache_key']
        
        # Second request (should use cache)
        response2 = client.post(
            '/api/ingest/gsc',
            json={
                'user_id': 'test-user-123',
                'property': 'https://example.com',
                'start_date': '2024-01-01',
                'end_date': '2025-04-01'
            }
        )
        
        assert response2.status_code == 200
        cache_key2 = response2.json()['cache_key']
        
        # Cache keys should match
        assert cache_key1 == cache_key2
        
        # API should only be called once
        assert mock_fetch.call_count == 1
    
    def test_pagination_handling(self, client, mock_supabase):
        """Test pagination for large datasets (>25K rows)."""
        with patch('app.services.gsc_service.GSCService.fetch_performance_data') as mock_fetch:
            # Simulate multiple pages of data
            page1 = [{'date': f'2024-01-{i:02d}', 'clicks': 100} for i in range(1, 26)]
            page2 = [{'date': f'2024-02-{i:02d}', 'clicks': 100} for i in range(1, 26)]
            
            mock_fetch.side_effect = [page1, page2, []]  # Empty list indicates end
            
            response = client.post(
                '/api/ingest/gsc',
                json={
                    'user_id': 'test-user-123',
                    'property': 'https://example.com',
                    'start_date': '2024-01-01',
                    'end_date': '2024-02-29'
                }
            )
            
            assert response.status_code == 200
            data = response.json()
            assert data['rows_ingested'] == 50  # Combined from both pages


class TestModuleExecution:
    """Test that analysis modules execute successfully with mock data."""
    
    def test_module1_health_trajectory(self, mock_gsc_daily_data):
        """Test Module 1: Health & Trajectory analysis."""
        result = analyze_health_trajectory(mock_gsc_daily_data)
        
        # Verify structure
        assert 'overall_direction' in result
        assert result['overall_direction'] in [
            'strong_growth', 'growth', 'flat', 'decline', 'strong_decline'
        ]
        
        assert 'trend_slope_pct_per_month' in result
        assert isinstance(result['trend_slope_pct_per_month'], (int, float))
        
        assert 'change_points' in result
        assert isinstance(result['change_points'], list)
        
        assert 'seasonality' in result
        assert 'best_day' in result['seasonality']
        assert 'worst_day' in result['seasonality']
        
        assert 'forecast' in result
        assert '30d' in result['forecast']
        assert '60d' in result['forecast']
        assert '90d' in result['forecast']
        
        # Verify forecast structure
        for period in ['30d', '60d', '90d']:
            forecast = result['forecast'][period]
            assert 'clicks' in forecast
            assert 'ci_low' in forecast
            assert 'ci_high' in forecast
            assert forecast['ci_low'] <= forecast['clicks'] <= forecast['ci_high']
    
    def test_module2_page_triage(
        self,
        mock_gsc_page_data,
        mock_ga4_landing_data,
        mock_gsc_page_summary
    ):
        """Test Module 2: Page-Level Triage analysis."""
        result = analyze_page_triage(
            mock_gsc_page_data,
            mock_ga4_landing_data,
            mock_gsc_page_summary
        )
        
        # Verify structure
        assert 'pages' in result
        assert isinstance(result['pages'], list)
        assert len(result['pages']) > 0
        
        # Check first page structure
        page = result['pages'][0]
        assert 'url' in page
        assert 'bucket' in page
        assert page['bucket'] in ['growing', 'stable', 'decaying', 'critical']
        assert 'current_monthly_clicks' in page
        assert 'trend_slope' in page
        assert 'priority_score' in page
        
        # Verify summary
        assert 'summary' in result
        summary = result['summary']
        assert 'total_pages_analyzed' in summary
        assert 'growing' in summary
        assert 'stable' in summary
        assert 'decaying' in summary
        assert 'critical' in summary
        
        # Total should match sum of buckets
        total = summary['growing'] + summary['stable'] + summary['decaying'] + summary['critical']
        assert total == summary['total_pages_analyzed']
    
    def test_module5_gameplan_generation(
        self,
        mock_gsc_daily_data,
        mock_gsc_page_data,
        mock_ga4_landing_data,
        mock_gsc_page_summary
    ):
        """Test Module 5: Gameplan generation."""
        # Generate inputs from modules 1 and 2
        health = analyze_health_trajectory(mock_gsc_daily_data)
        triage = analyze_page_triage(
            mock_gsc_page_data,
            mock_ga4_landing_data,
            mock_gsc_page_summary
        )
        
        # Mock SERP and content data (simplified for testing)
        serp = {
            'keywords_analyzed': 50,
            'serp_feature_displacement': [],
            'competitors': [],
            'total_click_share': 0.12
        }
        
        content = {
            'cannibalization_clusters': [],
            'striking_distance': [],
            'thin_content': []
        }
        
        result = generate_gameplan(health, triage, serp, content)
        
        # Verify structure
        assert 'critical' in result
        assert 'quick_wins' in result
        assert 'strategic' in result
        assert 'structural' in result
        
        # Each section should be a list
        for section in ['critical', 'quick_wins', 'strategic', 'structural']:
            assert isinstance(result[section], list)
        
        # Verify action item structure if any exist
        all_actions = (
            result['critical'] +
            result['quick_wins'] +
            result['strategic'] +
            result['structural']
        )
        
        if all_actions:
            action = all_actions[0]
            assert 'action' in action
            assert 'impact' in action
            assert 'effort' in action
            assert action['effort'] in ['low', 'medium', 'high']
        
        # Verify estimates
        assert 'total_estimated_monthly_click_recovery' in result
        assert 'total_estimated_monthly_click_growth' in result
        assert isinstance(result['total_estimated_monthly_click_recovery'], (int, float))
        assert isinstance(result['total_estimated_monthly_click_growth'], (int, float))
    
    def test_module_error_handling(self):
        """Test that modules handle invalid data gracefully."""
        # Empty dataframe
        empty_df = pd.DataFrame()
        
        with pytest.raises(ValueError) as exc_info:
            analyze_health_trajectory(empty_df)
        
        assert 'insufficient data' in str(exc_info.value).lower()
    
    def test_module_with_minimal_data(self):
        """Test modules with minimal valid data."""
        # Create minimal dataset (31 days)
        dates = pd.date_range(end=datetime.now().date(), periods=31, freq='D')
        minimal_data = pd.DataFrame({
            'date': dates,
            'clicks': [100 + i for i in range(31)],
            'impressions': [1000 + i * 10 for i in range(31)],
            'ctr': [0.1] * 31,
            'position': [5.0] * 31
        })
        
        result = analyze_health_trajectory(minimal_data)
        
        # Should still produce valid results
        assert 'overall_direction' in result
        assert 'forecast' in result


class TestEndToEndFlow:
    """Test complete end-to-end workflow."""
    
    @patch('app.services.gsc_service.GSCService.fetch_performance_data')
    @patch('app.services.gsc_service.GSCService.fetch_page_performance')
    @patch('app.services.ga4_service.GA4Service.fetch_landing_pages')
    def test_full_report_generation(
        self,
        mock_ga4_fetch,
        mock_gsc_page_fetch,
        mock_gsc_daily_fetch,
        client,
        mock_supabase,
        mock_gsc_daily_data,
        mock_gsc_page_data,
        mock_ga4_landing_data
    ):
        """Test full report generation from OAuth to final output."""
        # Setup mocks
        mock_gsc_daily_fetch.return_value = mock_gsc_daily_data.to_dict('records')
        mock_gsc_page_fetch.return_value = mock_gsc_page_data.to_dict('records')
        mock_ga4_fetch.return_value = mock_ga4_landing_data.to_dict('records')
        
        # Step 1: Ingest GSC data
        gsc_response = client.post(
            '/api/ingest/gsc',
            json={
                'user_id': 'test-user-123',
                'property': 'https://example.com',
                'start_date': '2024-01-01',
                'end_date': '2025-04-01'
            }
        )
        assert gsc_response.status_code == 200
        
        # Step 2: Ingest GSC page data
        gsc_page_response = client.post(
            '/api/ingest/gsc/pages',
            json={
                'user_id': 'test-user-123',
                'property': 'https://example.com',
                'start_date': '2024-01-01',
                'end_date': '2025-04-01'
            }
        )
        assert gsc_page_response.status_code == 200
        
        # Step 3: Ingest GA4 data
        ga4_response = client.post(
            '/api/ingest/ga4',
            json={
                'user_id': 'test-user-123',
                'property_id': '123456789',
                'start_date': '2024-01-01',
                'end_date': '2025-04-01'
            }
        )
        assert ga4_response.status_code == 200
        
        # Step 4: Generate report
        report_response = client.post(
            '/api/reports/generate',
            json={
                'user_id': 'test-user-123',
                'modules': [1, 2, 5]  # Test modules 1, 2, and 5
            }
        )
        
        assert report_response.status_code == 200
        report_data = report_response.json()
        
        # Verify report structure
        assert 'report_id' in report_data
        assert 'status' in report_data
        assert report_data['status'] == 'completed'
        
        assert 'modules' in report_data
        modules = report_data['modules']
        
        # Verify Module 1 results
        assert 'module_1' in modules
        assert 'overall_direction' in modules['module_1']
        assert 'forecast' in modules['module_1']
        
        # Verify Module 2 results
        assert 'module_2' in modules
        assert 'pages' in modules['module_2']
        assert 'summary' in modules['module_2']
        
        # Verify Module 5 results
        assert 'module_5' in modules
        assert 'critical' in modules['module_5']
        assert 'quick_wins' in modules['module_5']
    
    def test_async_report_generation(self, client, mock_supabase):
        """Test asynchronous report generation with job queue."""
        with patch('app.services.report_service.enqueue_report_job') as mock_enqueue:
            mock_enqueue.return_value = 'job-123'
            
            response = client.post(
                '/api/reports/generate',
                json={
                    'user_id': 'test-user-123',
                    'modules': [1, 2, 5],
                    'async': True
                }
            )
            
            assert response.status_code == 202  # Accepted
            data = response.json()
            assert data['status'] == 'queued'
            assert 'job_id' in data
            assert data['job_id'] == 'job-123'
    
    def test_report_status_polling(self, client, mock_supabase):
        """Test polling for report generation status."""
        response = client.get('/api/reports/status/job-123')
        
        assert response.status_code == 200
        data = response.json()
        assert 'status' in data
        assert data['status'] in ['queued', 'processing', 'completed', 'failed']


class TestErrorHandling:
    """Test error handling across the integration."""
    
    def test_invalid_user_id(self, client):
        """Test handling of invalid user ID."""
        response = client.post(
            '/api/ingest/gsc',
            json={
                'user_id': 'nonexistent-user',
                'property': 'https://example.com',
                'start_date': '2024-01-01',
                'end_date': '2025-04-01'
            }
        )
        
        assert response.status_code == 404
        data = response.json()
        assert 'error' in data
        assert 'user not found' in data['error'].lower()
    
    def test_expired_tokens(self, client, mock_supabase):
        """Test handling of expired OAuth tokens."""
        # Mock expired token
        with patch('app.services.oauth_service.OAuthService.get_user_tokens') as mock_get:
            mock_get.return_value = {
                'access_token': 'expired-token',
                'refresh_token': 'refresh-token',
                'expires_at': (datetime.now() - timedelta(hours=1)).timestamp()
            }
            
            with patch('app.services.oauth_service.OAuthService.refresh_access_token') as mock_refresh:
                mock_refresh.return_value = {
                    'access_token': 'new-token',
                    'refresh_token': 'refresh-token',
                    'expires_at': (datetime.now() + timedelta(hours=1)).timestamp()
                }
                
                response = client.post(
                    '/api/ingest/gsc',
                    json={
                        'user_id': 'test-user-123',
                        'property': 'https://example.com',
                        'start_date': '2024-01-01',
                        'end_date': '2025-04-01'
                    }
                )
                
                # Should succeed after token refresh
                assert response.status_code in [200, 202]
                mock_refresh.assert_called_once()
    
    def test_api_rate_limiting(self, client):
        """Test handling of API rate limits."""
        with patch('app.services.gsc_service.GSCService.fetch_performance_data') as mock_fetch:
            from googleapiclient.errors import HttpError
            
            # Simulate rate limit error
            mock_fetch.side_effect = HttpError(
                resp=MagicMock(status=429),
                content=b'Rate limit exceeded'
            )
            
            response = client.post(
                '/api/ingest/gsc',
                json={
                    'user_id': 'test-user-123',
                    'property': 'https://example.com',
                    'start_date': '2024-01-01',
                    'end_date': '2025-04-01'
                }
            )
            
            assert response.status_code == 429
            data = response.json()
            assert 'error' in data
            assert 'rate limit' in data['error'].lower()
    
    def test_missing_required_data(self, client):
        """Test handling of missing required data sources."""
        response = client.post(
            '/api/reports/generate',
            json={
                'user_id': 'test-user-123',
                'modules': [1, 2, 5]
            }
        )
        
        # Should fail if no data has been ingested
        assert response.status_code == 400
        data = response.json()
        assert 'error' in data


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
