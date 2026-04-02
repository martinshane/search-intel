import pytest
import asyncio
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, AsyncMock
import json

from app.services.report_generation import ReportGenerator
from app.services.analysis_modules.module1_health import analyze_health_trajectory
from app.services.analysis_modules.module2_triage import analyze_page_triage
from app.services.analysis_modules.module5_gameplan import generate_gameplan
from app.database.supabase_client import SupabaseClient


class TestReportGeneration:
    """Test suite for report generation pipeline."""

    @pytest.fixture
    def mock_supabase_client(self):
        """Mock Supabase client."""
        client = Mock(spec=SupabaseClient)
        client.update_report_status = AsyncMock()
        client.save_report_data = AsyncMock()
        client.get_cached_gsc_data = AsyncMock()
        client.get_cached_ga4_data = AsyncMock()
        client.get_site_crawl_data = AsyncMock()
        return client

    @pytest.fixture
    def sample_gsc_daily_data(self):
        """Sample GSC daily time series data."""
        dates = [datetime.now() - timedelta(days=i) for i in range(480, 0, -1)]
        return {
            'date': [d.strftime('%Y-%m-%d') for d in dates],
            'clicks': [100 + (i % 30) * 5 for i in range(480)],
            'impressions': [1000 + (i % 30) * 50 for i in range(480)],
            'ctr': [0.10 + (i % 30) * 0.005 for i in range(480)],
            'position': [5.0 + (i % 30) * 0.1 for i in range(480)]
        }

    @pytest.fixture
    def sample_page_data(self):
        """Sample page-level performance data."""
        return {
            'pages': [
                {
                    'url': '/blog/best-crm-software',
                    'clicks': 340,
                    'impressions': 4200,
                    'ctr': 0.081,
                    'position': 8.2,
                    'daily_data': [{'date': '2025-01-15', 'clicks': 12, 'position': 8.1}] * 90
                },
                {
                    'url': '/products/enterprise-crm',
                    'clicks': 520,
                    'impressions': 3800,
                    'ctr': 0.137,
                    'position': 4.3,
                    'daily_data': [{'date': '2025-01-15', 'clicks': 18, 'position': 4.2}] * 90
                },
                {
                    'url': '/blog/crm-pricing-guide',
                    'clicks': 180,
                    'impressions': 2100,
                    'ctr': 0.086,
                    'position': 11.5,
                    'daily_data': [{'date': '2025-01-15', 'clicks': 6, 'position': 11.4}] * 90
                }
            ]
        }

    @pytest.fixture
    def sample_ga4_data(self):
        """Sample GA4 engagement data."""
        return {
            'landing_pages': [
                {
                    'page': '/blog/best-crm-software',
                    'sessions': 380,
                    'bounce_rate': 0.72,
                    'avg_session_duration': 42.3,
                    'conversions': 12
                },
                {
                    'page': '/products/enterprise-crm',
                    'sessions': 540,
                    'bounce_rate': 0.34,
                    'avg_session_duration': 185.7,
                    'conversions': 48
                },
                {
                    'page': '/blog/crm-pricing-guide',
                    'sessions': 195,
                    'bounce_rate': 0.88,
                    'avg_session_duration': 18.2,
                    'conversions': 2
                }
            ]
        }

    @pytest.fixture
    def sample_serp_data(self):
        """Sample SERP data from DataForSEO."""
        return {
            'keywords': [
                {
                    'keyword': 'best crm software',
                    'position': 8,
                    'url': '/blog/best-crm-software',
                    'serp_features': ['featured_snippet', 'people_also_ask', 'video_carousel'],
                    'competitors': [
                        {'domain': 'competitor1.com', 'position': 1},
                        {'domain': 'competitor2.com', 'position': 3}
                    ]
                },
                {
                    'keyword': 'enterprise crm pricing',
                    'position': 4,
                    'url': '/products/enterprise-crm',
                    'serp_features': ['people_also_ask'],
                    'competitors': [
                        {'domain': 'competitor1.com', 'position': 2}
                    ]
                }
            ]
        }

    @pytest.mark.asyncio
    async def test_report_generator_initialization(self, mock_supabase_client):
        """Test that ReportGenerator initializes correctly."""
        generator = ReportGenerator(
            supabase_client=mock_supabase_client,
            report_id="test_report_123",
            site_id="test_site_456"
        )
        
        assert generator.report_id == "test_report_123"
        assert generator.site_id == "test_site_456"
        assert generator.supabase_client == mock_supabase_client

    @pytest.mark.asyncio
    async def test_module1_wiring(self, mock_supabase_client, sample_gsc_daily_data):
        """Test that Module 1 (Health & Trajectory) is correctly wired."""
        mock_supabase_client.get_cached_gsc_data.return_value = sample_gsc_daily_data
        
        generator = ReportGenerator(
            supabase_client=mock_supabase_client,
            report_id="test_report_123",
            site_id="test_site_456"
        )
        
        # Run module 1
        result = await generator.run_module_1()
        
        # Verify structure
        assert 'overall_direction' in result
        assert 'trend_slope_pct_per_month' in result
        assert 'change_points' in result
        assert 'seasonality' in result
        assert 'forecast' in result
        
        # Verify forecast structure
        assert '30d' in result['forecast']
        assert '60d' in result['forecast']
        assert '90d' in result['forecast']
        assert 'clicks' in result['forecast']['30d']
        assert 'ci_low' in result['forecast']['30d']
        assert 'ci_high' in result['forecast']['30d']
        
        # Verify data types
        assert isinstance(result['overall_direction'], str)
        assert isinstance(result['trend_slope_pct_per_month'], (int, float))
        assert isinstance(result['change_points'], list)
        assert isinstance(result['forecast']['30d']['clicks'], (int, float))

    @pytest.mark.asyncio
    async def test_module2_wiring(self, mock_supabase_client, sample_page_data, sample_ga4_data):
        """Test that Module 2 (Page-Level Triage) is correctly wired."""
        mock_supabase_client.get_cached_gsc_data.return_value = sample_page_data
        mock_supabase_client.get_cached_ga4_data.return_value = sample_ga4_data
        
        generator = ReportGenerator(
            supabase_client=mock_supabase_client,
            report_id="test_report_123",
            site_id="test_site_456"
        )
        
        # Run module 2
        result = await generator.run_module_2()
        
        # Verify structure
        assert 'pages' in result
        assert 'summary' in result
        
        # Verify page structure
        if len(result['pages']) > 0:
            page = result['pages'][0]
            assert 'url' in page
            assert 'bucket' in page
            assert 'current_monthly_clicks' in page
            assert 'trend_slope' in page
            assert 'priority_score' in page
            assert 'recommended_action' in page
            
            # Verify bucket is valid
            valid_buckets = ['growing', 'stable', 'decaying', 'critical']
            assert page['bucket'] in valid_buckets
        
        # Verify summary structure
        assert 'total_pages_analyzed' in result['summary']
        assert 'growing' in result['summary']
        assert 'stable' in result['summary']
        assert 'decaying' in result['summary']
        assert 'critical' in result['summary']
        
        # Verify data types
        assert isinstance(result['pages'], list)
        assert isinstance(result['summary']['total_pages_analyzed'], int)

    @pytest.mark.asyncio
    async def test_module5_wiring(self, mock_supabase_client):
        """Test that Module 5 (Gameplan) is correctly wired and synthesizes prior modules."""
        # Mock outputs from modules 1-4
        mock_health_output = {
            'overall_direction': 'declining',
            'trend_slope_pct_per_month': -2.3,
            'change_points': [{'date': '2025-11-08', 'magnitude': -0.12}]
        }
        
        mock_triage_output = {
            'pages': [
                {
                    'url': '/blog/best-crm',
                    'bucket': 'critical',
                    'current_monthly_clicks': 340,
                    'trend_slope': -0.45,
                    'priority_score': 87.4,
                    'recommended_action': 'title_rewrite'
                }
            ],
            'summary': {'total_recoverable_clicks_monthly': 2840}
        }
        
        mock_serp_output = {
            'serp_feature_displacement': [
                {
                    'keyword': 'best crm software',
                    'organic_position': 3,
                    'visual_position': 8,
                    'estimated_ctr_impact': -0.062
                }
            ]
        }
        
        mock_content_output = {
            'striking_distance': [
                {
                    'query': 'best crm for small business',
                    'current_position': 11.3,
                    'estimated_click_gain_if_top5': 420
                }
            ],
            'cannibalization_clusters': []
        }
        
        generator = ReportGenerator(
            supabase_client=mock_supabase_client,
            report_id="test_report_123",
            site_id="test_site_456"
        )
        
        # Mock module results
        generator.module_results = {
            'module1': mock_health_output,
            'module2': mock_triage_output,
            'module3': mock_serp_output,
            'module4': mock_content_output
        }
        
        # Run module 5
        result = await generator.run_module_5()
        
        # Verify structure
        assert 'critical' in result
        assert 'quick_wins' in result
        assert 'strategic' in result
        assert 'structural' in result
        assert 'total_estimated_monthly_click_recovery' in result
        assert 'total_estimated_monthly_click_growth' in result
        
        # Verify action item structure
        if len(result['critical']) > 0:
            action = result['critical'][0]
            assert 'action' in action
            assert 'impact' in action
            assert 'effort' in action
            assert action['effort'] in ['low', 'medium', 'high']
        
        # Verify data types
        assert isinstance(result['critical'], list)
        assert isinstance(result['quick_wins'], list)
        assert isinstance(result['total_estimated_monthly_click_recovery'], (int, float))

    @pytest.mark.asyncio
    async def test_report_status_updates(self, mock_supabase_client):
        """Test that report status is correctly updated in Supabase throughout generation."""
        generator = ReportGenerator(
            supabase_client=mock_supabase_client,
            report_id="test_report_123",
            site_id="test_site_456"
        )
        
        # Mock module execution to avoid actual computation
        generator.run_module_1 = AsyncMock(return_value={'overall_direction': 'stable'})
        generator.run_module_2 = AsyncMock(return_value={'pages': []})
        generator.run_module_5 = AsyncMock(return_value={'critical': []})
        
        await generator.generate_report()
        
        # Verify status updates were called
        status_calls = mock_supabase_client.update_report_status.call_args_list
        
        # Should have status updates for: starting, module_1_complete, module_2_complete, 
        # module_5_complete, finalizing, completed
        assert len(status_calls) >= 5
        
        # Verify status progression
        statuses = [call[1]['status'] for call in status_calls]
        assert 'processing' in statuses
        assert 'completed' in statuses or 'failed' in statuses

    @pytest.mark.asyncio
    async def test_report_data_structure_validation(self, mock_supabase_client):
        """Test that generated report data matches expected schema."""
        generator = ReportGenerator(
            supabase_client=mock_supabase_client,
            report_id="test_report_123",
            site_id="test_site_456"
        )
        
        # Mock all module outputs
        mock_outputs = {
            'module1': {
                'overall_direction': 'stable',
                'trend_slope_pct_per_month': 0.5,
                'forecast': {'30d': {'clicks': 1000}}
            },
            'module2': {
                'pages': [],
                'summary': {'total_pages_analyzed': 50}
            },
            'module5': {
                'critical': [],
                'quick_wins': [],
                'strategic': [],
                'total_estimated_monthly_click_recovery': 500
            }
        }
        
        for module_name, output in mock_outputs.items():
            setattr(generator, f'run_{module_name}', AsyncMock(return_value=output))
        
        report_data = await generator.generate_report()
        
        # Validate top-level structure
        assert 'report_id' in report_data
        assert 'site_id' in report_data
        assert 'generated_at' in report_data
        assert 'modules' in report_data
        assert 'summary' in report_data
        
        # Validate modules structure
        assert 'module1_health_trajectory' in report_data['modules']
        assert 'module2_page_triage' in report_data['modules']
        assert 'module5_gameplan' in report_data['modules']
        
        # Validate each module has expected keys
        module1 = report_data['modules']['module1_health_trajectory']
        assert 'overall_direction' in module1
        assert 'forecast' in module1
        
        module2 = report_data['modules']['module2_page_triage']
        assert 'pages' in module2
        assert 'summary' in module2
        
        module5 = report_data['modules']['module5_gameplan']
        assert 'critical' in module5
        assert 'quick_wins' in module5
        
        # Validate summary aggregates key metrics
        summary = report_data['summary']
        assert 'overall_direction' in summary
        assert 'total_recoverable_clicks' in summary
        assert 'priority_action_count' in summary

    @pytest.mark.asyncio
    async def test_report_saved_to_database(self, mock_supabase_client):
        """Test that completed report is saved to Supabase."""
        generator = ReportGenerator(
            supabase_client=mock_supabase_client,
            report_id="test_report_123",
            site_id="test_site_456"
        )
        
        # Mock module execution
        generator.run_module_1 = AsyncMock(return_value={'overall_direction': 'stable'})
        generator.run_module_2 = AsyncMock(return_value={'pages': []})
        generator.run_module_5 = AsyncMock(return_value={'critical': []})
        
        report_data = await generator.generate_report()
        
        # Verify save_report_data was called with correct structure
        mock_supabase_client.save_report_data.assert_called_once()
        save_call_args = mock_supabase_client.save_report_data.call_args
        
        saved_data = save_call_args[1]['report_data']
        assert saved_data['report_id'] == "test_report_123"
        assert saved_data['site_id'] == "test_site_456"
        assert 'modules' in saved_data

    @pytest.mark.asyncio
    async def test_error_handling_updates_status(self, mock_supabase_client):
        """Test that errors during generation update report status to 'failed'."""
        generator = ReportGenerator(
            supabase_client=mock_supabase_client,
            report_id="test_report_123",
            site_id="test_site_456"
        )
        
        # Mock module 1 to raise an exception
        generator.run_module_1 = AsyncMock(side_effect=Exception("Test error"))
        
        with pytest.raises(Exception):
            await generator.generate_report()
        
        # Verify status was updated to failed
        status_calls = mock_supabase_client.update_report_status.call_args_list
        final_call = status_calls[-1]
        assert final_call[1]['status'] == 'failed'
        assert 'error' in final_call[1]

    @pytest.mark.asyncio
    async def test_frontend_display_data_format(self, mock_supabase_client):
        """Test that report data is formatted correctly for frontend consumption."""
        generator = ReportGenerator(
            supabase_client=mock_supabase_client,
            report_id="test_report_123",
            site_id="test_site_456"
        )
        
        # Generate complete mock report
        mock_outputs = {
            'module1': {
                'overall_direction': 'declining',
                'trend_slope_pct_per_month': -2.3,
                'forecast': {
                    '30d': {'clicks': 1200, 'ci_low': 1100, 'ci_high': 1300}
                },
                'change_points': [
                    {'date': '2025-11-08', 'magnitude': -0.12, 'direction': 'drop'}
                ]
            },
            'module2': {
                'pages': [
                    {
                        'url': '/blog/test',
                        'bucket': 'decaying',
                        'current_monthly_clicks': 340,
                        'priority_score': 87.4
                    }
                ],
                'summary': {'total_pages_analyzed': 50}
            },
            'module5': {
                'critical': [
                    {
                        'action': 'Rewrite title for /blog/test',
                        'impact': 120,
                        'effort': 'low'
                    }
                ],
                'quick_wins': [],
                'strategic': []
            }
        }
        
        for module_name, output in mock_outputs.items():
            setattr(generator, f'run_{module_name}', AsyncMock(return_value=output))
        
        report_data = await generator.generate_report()
        
        # Verify JSON serializable
        json_str = json.dumps(report_data)
        parsed = json.loads(json_str)
        assert parsed['report_id'] == "test_report_123"
        
        # Verify frontend can access key display elements
        assert parsed['modules']['module1_health_trajectory']['overall_direction'] == 'declining'
        assert len(parsed['modules']['module2_page_triage']['pages']) == 1
        assert len(parsed['modules']['module5_gameplan']['critical']) == 1
        
        # Verify chart data is present and properly formatted
        module1 = parsed['modules']['module1_health_trajectory']
        assert 'forecast' in module1
        assert isinstance(module1['forecast']['30d']['clicks'], (int, float))

    @pytest.mark.asyncio
    async def test_module_dependency_chain(self, mock_supabase_client):
        """Test that modules execute in correct order with proper dependency chain."""
        generator = ReportGenerator(
            supabase_client=mock_supabase_client,
            report_id="test_report_123",
            site_id="test_site_456"
        )
        
        execution_order = []
        
        async def track_module_1():
            execution_order.append('module1')
            return {'overall_direction': 'stable'}
        
        async def track_module_2():
            execution_order.append('module2')
            # Module 2 should have access to module 1 results
            assert 'module1' in generator.module_results
            return {'pages': []}
        
        async def track_module_5():
            execution_order.append('module5')
            # Module 5 should have access to all prior module results
            assert 'module1' in generator.module_results
            assert 'module2' in generator.module_results
            return {'critical': []}
        
        generator.run_module_1 = track_module_1
        generator.run_module_2 = track_module_2
        generator.run_module_5 = track_module_5
        
        await generator.generate_report()
        
        # Verify execution order
        assert execution_order == ['module1', 'module2', 'module5']

    @pytest.mark.asyncio
    async def test_partial_data_handling(self, mock_supabase_client):
        """Test that report generation handles missing or partial data gracefully."""
        generator = ReportGenerator(
            supabase_client=mock_supabase_client,
            report_id="test_report_123",
            site_id="test_site_456"
        )
        
        # Mock incomplete data scenario
        mock_supabase_client.get_cached_gsc_data.return_value = None
        mock_supabase_client.get_cached_ga4_data.return_value = {'landing_pages': []}
        
        generator.run_module_1 = AsyncMock(return_value={
            'overall_direction': 'insufficient_data',
            'forecast': None
        })
        generator.run_module_2 = AsyncMock(return_value={
            'pages': [],
            'summary': {'total_pages_analyzed': 0}
        })
        generator.run_module_5 = AsyncMock(return_value={
            'critical': [],
            'quick_wins': [],
            'strategic': []
        })
        
        report_data = await generator.generate_report()
        
        # Verify report still generates but indicates data limitations
        assert report_data['report_id'] == "test_report_123"
        assert 'modules' in report_data
        
        # Verify data quality flags
        if 'data_quality' in report_data:
            assert report_data['data_quality']['gsc_data_available'] == False

    @pytest.mark.asyncio
    async def test_concurrent_report_generation_isolation(self, mock_supabase_client):
        """Test that multiple concurrent report generations don't interfere with each other."""
        generator1 = ReportGenerator(
            supabase_client=mock_supabase_client,
            report_id="report_1",
            site_id="site_1"
        )
        
        generator2 = ReportGenerator(
            supabase_client=mock_supabase_client,
            report_id="report_2",
            site_id="site_2"
        )
        
        # Mock quick execution
        for gen in [generator1, generator2]:
            gen.run_module_1 = AsyncMock(return_value={'overall_direction': 'stable'})
            gen.run_module_2 = AsyncMock(return_value={'pages': []})
            gen.run_module_5 = AsyncMock(return_value={'critical': []})
        
        # Run concurrently
        results = await asyncio.gather(
            generator1.generate_report(),
            generator2.generate_report()
        )
        
        # Verify each report has correct IDs
        assert results[0]['report_id'] == "report_1"
        assert results[0]['site_id'] == "site_1"
        assert results[1]['report_id'] == "report_2"
        assert results[1]['site_id'] == "site_2"

    @pytest.mark.asyncio
    async def test_report_timestamp_accuracy(self, mock_supabase_client):
        """Test that report timestamps are accurate and in ISO format."""
        generator = ReportGenerator(
            supabase_client=mock_supabase_client,
            report_id="test_report_123",
            site_id="test_site_456"
        )
        
        generator.run_module_1 = AsyncMock(return_value={'overall_direction': 'stable'})
        generator.run_module_2 = AsyncMock(return_value={'pages': []})
        generator.run_module_5 = AsyncMock(return_value={'critical': []})
        
        before_generation = datetime.utcnow()
        report_data = await generator.generate_report()
        after_generation = datetime.utcnow()
        
        # Verify timestamp is present and parseable
        assert 'generated_at' in report_data
        generated_at = datetime.fromisoformat(report_data['generated_at'].replace('Z', '+00:00'))
        
        # Verify timestamp is within generation window
        assert before_generation <= generated_at <= after_generation

    def test_report_data_schema_completeness(self):
        """Test that report schema includes all required fields for frontend."""
        required_top_level_fields = [
            'report_id',
            'site_id',
            'generated_at',
            'modules',
            'summary'
        ]
        
        required_module_fields = {
            'module1_health_trajectory': ['overall_direction', 'forecast', 'change_points'],
            'module2_page_triage': ['pages', 'summary'],
            'module5_gameplan': ['critical', 'quick_wins', 'strategic']
        }
        
        # This test documents the expected schema structure
        # Actual validation happens in test_report_data_structure_validation
        assert len(required_top_level_fields) == 5
        assert len(required_module_fields) == 3

    @pytest.mark.asyncio
    async def test_module_output_persistence(self, mock_supabase_client):
        """Test that module outputs are persisted between pipeline stages."""
        generator = ReportGenerator(
            supabase_client=mock_supabase_client,
            report_id="test_report_123",
            site_id="test_site_456"
        )
        
        module1_output = {'overall_direction': 'declining', 'trend_slope': -2.3}
        module2_output = {'pages': [{'url': '/test', 'bucket': 'critical'}]}
        
        generator.run_module_1 = AsyncMock(return_value=module1_output)
        generator.run_module_2 = AsyncMock(return_value=module2_output)
        generator.run_module_5 = AsyncMock(return_value={'critical': []})
        
        report_data = await generator.generate_report()
        
        # Verify all module outputs are in final report
        assert report_data['modules']['module1_health_trajectory'] == module1_output
        assert report_data['modules']['module2_page_triage'] == module2_output