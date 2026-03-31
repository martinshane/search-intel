import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
from modules.module_1_health_trajectory import (
    analyze_health_trajectory,
    calculate_core_web_vitals,
    score_mobile_usability,
    check_https_security,
    validate_structured_data,
    generate_technical_health_report
)


@pytest.fixture
def mock_gsc_daily_data():
    """Generate mock GSC daily time series data for 16 months."""
    dates = pd.date_range(end=datetime.now(), periods=480, freq='D')
    np.random.seed(42)
    
    # Create realistic traffic pattern with trend and seasonality
    base_clicks = 1000
    trend = np.linspace(0, -200, len(dates))  # Declining trend
    weekly_seasonality = 100 * np.sin(2 * np.pi * np.arange(len(dates)) / 7)
    monthly_seasonality = 50 * np.sin(2 * np.pi * np.arange(len(dates)) / 30)
    noise = np.random.normal(0, 30, len(dates))
    
    clicks = base_clicks + trend + weekly_seasonality + monthly_seasonality + noise
    clicks = np.maximum(clicks, 100)  # Ensure no negative values
    
    impressions = clicks * np.random.uniform(15, 25, len(dates))
    ctr = clicks / impressions
    position = np.random.uniform(5, 15, len(dates))
    
    return pd.DataFrame({
        'date': dates,
        'clicks': clicks,
        'impressions': impressions,
        'ctr': ctr,
        'position': position
    })


@pytest.fixture
def mock_ga4_core_web_vitals():
    """Generate mock GA4 Core Web Vitals data."""
    return {
        'lcp_samples': [1800, 2200, 2500, 1900, 2100, 2400, 1700, 2300, 2000, 2200],
        'fid_samples': [50, 80, 100, 60, 70, 90, 55, 85, 65, 75],
        'cls_samples': [0.05, 0.15, 0.20, 0.10, 0.12, 0.18, 0.08, 0.16, 0.11, 0.14],
        'ttfb_samples': [400, 600, 800, 500, 550, 700, 450, 650, 520, 580],
        'fcp_samples': [1200, 1500, 1800, 1300, 1400, 1600, 1250, 1550, 1350, 1450]
    }


@pytest.fixture
def mock_page_data():
    """Generate mock page crawl data."""
    return pd.DataFrame({
        'url': [
            'https://example.com/',
            'https://example.com/blog/article-1',
            'https://example.com/products/widget',
            'http://example.com/old-page',
            'https://example.com/about'
        ],
        'https_enabled': [True, True, True, False, True],
        'has_ssl_cert': [True, True, True, False, True],
        'cert_expiry_days': [89, 89, 89, None, 89],
        'mixed_content': [False, False, True, False, False],
        'mobile_viewport': [True, True, True, True, False],
        'mobile_friendly': [True, True, False, True, True],
        'touch_elements_sized': [True, True, False, True, True],
        'font_size_legible': [True, False, True, True, True],
        'schema_types': [
            ['Organization', 'WebSite'],
            ['Article', 'BreadcrumbList'],
            ['Product'],
            [],
            ['Organization']
        ],
        'schema_valid': [True, True, False, False, True],
        'schema_errors': [
            [],
            [],
            ['Missing required field: price'],
            [],
            []
        ]
    })


class TestCoreWebVitalsCalculation:
    """Test Core Web Vitals calculation with mock GA4 data."""
    
    def test_lcp_calculation(self, mock_ga4_core_web_vitals):
        """Test Largest Contentful Paint calculation and scoring."""
        result = calculate_core_web_vitals(mock_ga4_core_web_vitals)
        
        assert 'lcp' in result
        assert 'p75' in result['lcp']
        assert 'score' in result['lcp']
        assert 'rating' in result['lcp']
        
        # P75 should be around 2200-2400ms based on mock data
        assert 2000 <= result['lcp']['p75'] <= 2600
        
        # Rating should be 'needs_improvement' for values between 2500-4000ms
        # or 'good' for values <= 2500ms
        assert result['lcp']['rating'] in ['good', 'needs_improvement', 'poor']
        
        # Score should be between 0 and 100
        assert 0 <= result['lcp']['score'] <= 100
    
    def test_fid_calculation(self, mock_ga4_core_web_vitals):
        """Test First Input Delay calculation and scoring."""
        result = calculate_core_web_vitals(mock_ga4_core_web_vitals)
        
        assert 'fid' in result
        assert 'p75' in result['fid']
        assert 'score' in result['fid']
        assert 'rating' in result['fid']
        
        # P75 should be around 75-90ms based on mock data
        assert 60 <= result['fid']['p75'] <= 100
        
        # Rating should be 'good' for values <= 100ms
        assert result['fid']['rating'] in ['good', 'needs_improvement', 'poor']
        
        assert 0 <= result['fid']['score'] <= 100
    
    def test_cls_calculation(self, mock_ga4_core_web_vitals):
        """Test Cumulative Layout Shift calculation and scoring."""
        result = calculate_core_web_vitals(mock_ga4_core_web_vitals)
        
        assert 'cls' in result
        assert 'p75' in result['cls']
        assert 'score' in result['cls']
        assert 'rating' in result['cls']
        
        # P75 should be around 0.14-0.18 based on mock data
        assert 0.10 <= result['cls']['p75'] <= 0.25
        
        # Rating should be 'needs_improvement' for values between 0.1-0.25
        assert result['cls']['rating'] in ['good', 'needs_improvement', 'poor']
        
        assert 0 <= result['cls']['score'] <= 100
    
    def test_overall_cwv_score(self, mock_ga4_core_web_vitals):
        """Test overall Core Web Vitals score calculation."""
        result = calculate_core_web_vitals(mock_ga4_core_web_vitals)
        
        assert 'overall_score' in result
        assert 'overall_rating' in result
        
        # Overall score should be weighted average of individual metrics
        expected_score = (
            result['lcp']['score'] * 0.25 +
            result['fid']['score'] * 0.25 +
            result['cls']['score'] * 0.25 +
            result.get('ttfb', {}).get('score', 0) * 0.125 +
            result.get('fcp', {}).get('score', 0) * 0.125
        )
        
        assert abs(result['overall_score'] - expected_score) < 1
        assert result['overall_rating'] in ['good', 'needs_improvement', 'poor']
    
    def test_empty_data_handling(self):
        """Test handling of empty or missing data."""
        empty_data = {
            'lcp_samples': [],
            'fid_samples': [],
            'cls_samples': []
        }
        
        result = calculate_core_web_vitals(empty_data)
        
        assert result['lcp']['rating'] == 'no_data'
        assert result['fid']['rating'] == 'no_data'
        assert result['cls']['rating'] == 'no_data'
        assert result['overall_rating'] == 'no_data'


class TestMobileUsabilityScoring:
    """Test mobile usability scoring."""
    
    def test_mobile_friendly_scoring(self, mock_page_data):
        """Test mobile-friendly page scoring."""
        result = score_mobile_usability(mock_page_data)
        
        assert 'mobile_friendly_pct' in result
        assert 'viewport_meta_pct' in result
        assert 'touch_target_pct' in result
        assert 'font_legibility_pct' in result
        assert 'overall_score' in result
        assert 'issues' in result
        
        # 3 out of 5 pages are fully mobile friendly
        assert result['mobile_friendly_pct'] == 60.0
        
        # 4 out of 5 pages have viewport meta
        assert result['viewport_meta_pct'] == 80.0
        
        # Overall score should be between 0-100
        assert 0 <= result['overall_score'] <= 100
    
    def test_mobile_issues_detection(self, mock_page_data):
        """Test detection of specific mobile usability issues."""
        result = score_mobile_usability(mock_page_data)
        
        issues = result['issues']
        
        # Should detect missing viewport on /about
        viewport_issues = [i for i in issues if i['type'] == 'missing_viewport']
        assert len(viewport_issues) == 1
        assert '/about' in viewport_issues[0]['url']
        
        # Should detect touch target issues on /products/widget
        touch_issues = [i for i in issues if i['type'] == 'touch_targets_too_small']
        assert len(touch_issues) == 1
        assert '/products/widget' in touch_issues[0]['url']
        
        # Should detect font size issues on /blog/article-1
        font_issues = [i for i in issues if i['type'] == 'font_size_too_small']
        assert len(font_issues) == 1
        assert '/blog/article-1' in font_issues[0]['url']
    
    def test_mobile_score_breakdown(self, mock_page_data):
        """Test detailed breakdown of mobile usability score."""
        result = score_mobile_usability(mock_page_data)
        
        assert 'score_breakdown' in result
        breakdown = result['score_breakdown']
        
        assert 'viewport' in breakdown
        assert 'touch_targets' in breakdown
        assert 'font_size' in breakdown
        assert 'mobile_friendly_test' in breakdown
        
        # Each component should have weight and score
        for component in breakdown.values():
            assert 'weight' in component
            assert 'score' in component
            assert 0 <= component['score'] <= 100
            assert 0 <= component['weight'] <= 1
    
    def test_all_mobile_friendly(self):
        """Test scoring when all pages are mobile-friendly."""
        perfect_data = pd.DataFrame({
            'url': ['https://example.com/page1', 'https://example.com/page2'],
            'mobile_viewport': [True, True],
            'mobile_friendly': [True, True],
            'touch_elements_sized': [True, True],
            'font_size_legible': [True, True]
        })
        
        result = score_mobile_usability(perfect_data)
        
        assert result['mobile_friendly_pct'] == 100.0
        assert result['viewport_meta_pct'] == 100.0
        assert result['touch_target_pct'] == 100.0
        assert result['font_legibility_pct'] == 100.0
        assert result['overall_score'] == 100.0
        assert len(result['issues']) == 0


class TestHTTPSSecurityChecks:
    """Test HTTPS and security validation."""
    
    def test_https_coverage(self, mock_page_data):
        """Test HTTPS coverage calculation."""
        result = check_https_security(mock_page_data)
        
        assert 'https_coverage_pct' in result
        assert 'ssl_cert_valid_pct' in result
        assert 'mixed_content_issues' in result
        assert 'security_score' in result
        
        # 4 out of 5 pages have HTTPS
        assert result['https_coverage_pct'] == 80.0
        
        # 4 out of 5 pages have valid SSL cert
        assert result['ssl_cert_valid_pct'] == 80.0
    
    def test_http_page_detection(self, mock_page_data):
        """Test detection of HTTP (non-secure) pages."""
        result = check_https_security(mock_page_data)
        
        http_pages = result['http_pages']
        
        assert len(http_pages) == 1
        assert http_pages[0]['url'] == 'http://example.com/old-page'
        assert http_pages[0]['issue'] == 'not_https'
    
    def test_mixed_content_detection(self, mock_page_data):
        """Test detection of mixed content issues."""
        result = check_https_security(mock_page_data)
        
        mixed_content = result['mixed_content_issues']
        
        assert len(mixed_content) == 1
        assert '/products/widget' in mixed_content[0]['url']
    
    def test_ssl_certificate_expiry(self, mock_page_data):
        """Test SSL certificate expiry warnings."""
        result = check_https_security(mock_page_data)
        
        assert 'ssl_expiry_warnings' in result
        
        # Mock data has certs expiring in 89 days - should not trigger warning
        # (warning threshold is typically < 30 days)
        expiry_warnings = result['ssl_expiry_warnings']
        assert len(expiry_warnings) == 0
    
    def test_ssl_expiry_warning_threshold(self):
        """Test SSL expiry warning when certificate expires soon."""
        data_with_expiring_cert = pd.DataFrame({
            'url': ['https://example.com/'],
            'https_enabled': [True],
            'has_ssl_cert': [True],
            'cert_expiry_days': [15],  # Expires in 15 days
            'mixed_content': [False]
        })
        
        result = check_https_security(data_with_expiring_cert)
        
        assert len(result['ssl_expiry_warnings']) == 1
        assert result['ssl_expiry_warnings'][0]['days_until_expiry'] == 15
    
    def test_overall_security_score(self, mock_page_data):
        """Test overall security score calculation."""
        result = check_https_security(mock_page_data)
        
        score = result['security_score']
        
        # Score should be penalized for HTTP page and mixed content
        assert 0 <= score <= 100
        assert score < 100  # Not perfect due to issues
        
        # Should have recommendations
        assert 'recommendations' in result
        assert len(result['recommendations']) > 0


class TestStructuredDataValidation:
    """Test structured data (schema.org) validation."""
    
    def test_schema_coverage(self, mock_page_data):
        """Test schema markup coverage calculation."""
        result = validate_structured_data(mock_page_data)
        
        assert 'schema_coverage_pct' in result
        assert 'schema_valid_pct' in result
        assert 'total_pages' in result
        assert 'pages_with_schema' in result
        
        # 4 out of 5 pages have schema
        assert result['schema_coverage_pct'] == 80.0
        
        # 3 out of 4 schema-enabled pages are valid
        assert result['schema_valid_pct'] == 75.0
    
    def test_schema_type_distribution(self, mock_page_data):
        """Test distribution of schema types across pages."""
        result = validate_structured_data(mock_page_data)
        
        assert 'schema_types_distribution' in result
        distribution = result['schema_types_distribution']
        
        # Check common schema types are detected
        assert 'Organization' in distribution
        assert 'Article' in distribution
        assert 'Product' in distribution
        
        # Organization appears on 2 pages
        assert distribution['Organization'] == 2
        
        # Article appears on 1 page
        assert distribution['Article'] == 1
    
    def test_schema_validation_errors(self, mock_page_data):
        """Test detection of schema validation errors."""
        result = validate_structured_data(mock_page_data)
        
        assert 'validation_errors' in result
        errors = result['validation_errors']
        
        # Should detect error on /products/widget
        product_errors = [e for e in errors if '/products/widget' in e['url']]
        assert len(product_errors) == 1
        assert 'Missing required field: price' in product_errors[0]['errors']
    
    def test_missing_schema_recommendations(self, mock_page_data):
        """Test recommendations for pages missing schema."""
        result = validate_structured_data(mock_page_data)
        
        assert 'missing_schema_pages' in result
        missing = result['missing_schema_pages']
        
        # /old-page has no schema
        assert any('/old-page' in page['url'] for page in missing)
    
    def test_schema_score_calculation(self, mock_page_data):
        """Test overall structured data score."""
        result = validate_structured_data(mock_page_data)
        
        assert 'overall_score' in result
        score = result['overall_score']
        
        # Score should consider both coverage and validity
        assert 0 <= score <= 100
        
        # Should be penalized for missing schema and validation errors
        assert score < 100
    
    def test_recommended_schema_types(self, mock_page_data):
        """Test recommendations for appropriate schema types."""
        result = validate_structured_data(mock_page_data)
        
        assert 'recommendations' in result
        recommendations = result['recommendations']
        
        # Should recommend schema for pages without it
        assert any('Add schema markup' in rec['action'] for rec in recommendations)


class TestGenerateTechnicalHealthIntegration:
    """Test complete module output structure and integration."""
    
    def test_complete_report_structure(self, mock_gsc_daily_data, mock_ga4_core_web_vitals, mock_page_data):
        """Test that complete report has all required sections."""
        result = generate_technical_health_report(
            gsc_daily_data=mock_gsc_daily_data,
            ga4_cwv_data=mock_ga4_core_web_vitals,
            page_data=mock_page_data
        )
        
        # Top-level structure
        assert 'health_trajectory' in result
        assert 'core_web_vitals' in result
        assert 'mobile_usability' in result
        assert 'https_security' in result
        assert 'structured_data' in result
        assert 'overall_technical_score' in result
        assert 'critical_issues' in result
        assert 'recommendations' in result
        assert 'metadata' in result
    
    def test_health_trajectory_section(self, mock_gsc_daily_data, mock_ga4_core_web_vitals, mock_page_data):
        """Test health trajectory section structure."""
        result = generate_technical_health_report(
            gsc_daily_data=mock_gsc_daily_data,
            ga4_cwv_data=mock_ga4_core_web_vitals,
            page_data=mock_page_data
        )
        
        health = result['health_trajectory']
        
        assert 'overall_direction' in health
        assert 'trend_slope_pct_per_month' in health
        assert 'change_points' in health
        assert 'seasonality' in health
        assert 'anomalies' in health
        assert 'forecast' in health
        
        # Validate forecast structure
        forecast = health['forecast']
        assert '30d' in forecast
        assert '60d' in forecast
        assert '90d' in forecast
        
        for period in ['30d', '60d', '90d']:
            assert 'clicks' in forecast[period]
            assert 'ci_low' in forecast[period]
            assert 'ci_high' in forecast[period]
    
    def test_core_web_vitals_section(self, mock_gsc_daily_data, mock_ga4_core_web_vitals, mock_page_data):
        """Test Core Web Vitals section structure."""
        result = generate_technical_health_report(
            gsc_daily_data=mock_gsc_daily_data,
            ga4_cwv_data=mock_ga4_core_web_vitals,
            page_data=mock_page_data
        )
        
        cwv = result['core_web_vitals']
        
        assert 'lcp' in cwv
        assert 'fid' in cwv
        assert 'cls' in cwv
        assert 'overall_score' in cwv
        assert 'overall_rating' in cwv
        
        # Validate metric structure
        for metric in ['lcp', 'fid', 'cls']:
            assert 'p75' in cwv[metric]
            assert 'score' in cwv[metric]
            assert 'rating' in cwv[metric]
    
    def test_mobile_usability_section(self, mock_gsc_daily_data, mock_ga4_core_web_vitals, mock_page_data):
        """Test mobile usability section structure."""
        result = generate_technical_health_report(
            gsc_daily_data=mock_gsc_daily_data,
            ga4_cwv_data=mock_ga4_core_web_vitals,
            page_data=mock_page_data
        )
        
        mobile = result['mobile_usability']
        
        assert 'mobile_friendly_pct' in mobile
        assert 'viewport_meta_pct' in mobile
        assert 'touch_target_pct' in mobile
        assert 'font_legibility_pct' in mobile
        assert 'overall_score' in mobile
        assert 'issues' in mobile
        assert 'score_breakdown' in mobile
    
    def test_https_security_section(self, mock_gsc_daily_data, mock_ga4_core_web_vitals, mock_page_data):
        """Test HTTPS security section structure."""
        result = generate_technical_health_report(
            gsc_daily_data=mock_gsc_daily_data,
            ga4_cwv_data=mock_ga4_core_web_vitals,
            page_data=mock_page_data
        )
        
        security = result['https_security']
        
        assert 'https_coverage_pct' in security
        assert 'ssl_cert_valid_pct' in security
        assert 'mixed_content_issues' in security
        assert 'http_pages' in security
        assert 'security_score' in security
        assert 'recommendations' in security
    
    def test_structured_data_section(self, mock_gsc_daily_data, mock_ga4_core_web_vitals, mock_page_data):
        """Test structured data section structure."""
        result = generate_technical_health_report(
            gsc_daily_data=mock_gsc_daily_data,
            ga4_cwv_data=mock_ga4_core_web_vitals,
            page_data=mock_page_data
        )
        
        schema = result['structured_data']
        
        assert 'schema_coverage_pct' in schema
        assert 'schema_valid_pct' in schema
        assert 'total_pages' in schema
        assert 'pages_with_schema' in schema
        assert 'schema_types_distribution' in schema
        assert 'validation_errors' in schema
        assert 'overall_score' in schema
    
    def test_overall_technical_score(self, mock_gsc_daily_data, mock_ga4_core_web_vitals, mock_page_data):
        """Test overall technical score calculation."""
        result = generate_technical_health_report(
            gsc_daily_data=mock_gsc_daily_data,
            ga4_cwv_data=mock_ga4_core_web_vitals,
            page_data=mock_page_data
        )
        
        overall_score = result['overall_technical_score']
        
        assert 'score' in overall_score
        assert 'rating' in overall_score
        assert 'components' in overall_score
        
        # Score should be 0-100
        assert 0 <= overall_score['score'] <= 100
        
        # Rating should be one of the expected values
        assert overall_score['rating'] in ['excellent', 'good', 'needs_improvement', 'poor']
        
        # Components should show weighted contribution
        components = overall_score['components']
        assert 'core_web_vitals' in components
        assert 'mobile_usability' in components
        assert 'https_security' in components
        assert 'structured_data' in components
        
        # Each component should have weight and score
        for component in components.values():
            assert 'weight' in component
            assert 'score' in component
            assert 'contribution' in component
    
    def test_critical_issues_flagging(self, mock_gsc_daily_data, mock_ga4_core_web_vitals, mock_page_data):
        """Test that critical issues are properly flagged."""
        result = generate_technical_health_report(
            gsc_daily_data=mock_gsc_daily_data,
            ga4_cwv_data=mock_ga4_core_web_vitals,
            page_data=mock_page_data
        )
        
        critical = result['critical_issues']
        
        assert isinstance(critical, list)
        
        # Each critical issue should have required fields
        for issue in critical:
            assert 'type' in issue
            assert 'severity' in issue
            assert 'description' in issue
            assert 'affected_pages' in issue or 'affected_metrics' in issue
            assert 'recommended_action' in issue
            assert 'priority' in issue
    
    def test_recommendations_structure(self, mock_gsc_daily_data, mock_ga4_core_web_vitals, mock_page_data):
        """Test recommendations structure."""
        result = generate_technical_health_report(
            gsc_daily_data=mock_gsc_daily_data,
            ga4_cwv_data=mock_ga4_core_web_vitals,
            page_data=mock_page_data
        )
        
        recommendations = result['recommendations']
        
        assert 'immediate' in recommendations
        assert 'short_term' in recommendations
        assert 'long_term' in recommendations
        
        # Each priority level should have list of actions
        for priority in ['immediate', 'short_term', 'long_term']:
            assert isinstance(recommendations[priority], list)
            
            for rec in recommendations[priority]:
                assert 'action' in rec
                assert 'category' in rec
                assert 'impact' in rec
                assert 'effort' in rec
    
    def test_metadata_section(self, mock_gsc_daily_data, mock_ga4_core_web_vitals, mock_page_data):
        """Test metadata section structure."""
        result = generate_technical_health_report(
            gsc_daily_data=mock_gsc_daily_data,
            ga4_cwv_data=mock_ga4_core_web_vitals,
            page_data=mock_page_data
        )
        
        metadata = result['metadata']
        
        assert 'generated_at' in metadata
        assert 'data_period_start' in metadata
        assert 'data_period_end' in metadata
        assert 'total_pages_analyzed' in metadata
        assert 'module_version' in metadata
        
        # Validate timestamp format
        generated_at = datetime.fromisoformat(metadata['generated_at'].replace('Z', '+00:00'))
        assert isinstance(generated_at, datetime)
    
    def test_report_json_serializable(self, mock_gsc_daily_data, mock_ga4_core_web_vitals, mock_page_data):
        """Test that the complete report is JSON serializable."""
        import json
        
        result = generate_technical_health_report(
            gsc_daily_data=mock_gsc_daily_data,
            ga4_cwv_data=mock_ga4_core_web_vitals,
            page_data=mock_page_data
        )
        
        # This should not raise an exception
        json_str = json.dumps(result)
        assert isinstance(json_str, str)
        
        # Should be able to deserialize back
        deserialized = json.loads(json_str)
        assert deserialized == result
    
    def test_report_consistency(self, mock_gsc_daily_data, mock_ga4_core_web_vitals, mock_page_data):
        """Test that report components are internally consistent."""
        result = generate_technical_health_report(
            gsc_daily_data=mock_gsc_daily_data,
            ga4_cwv_data=mock_ga4_core_web_vitals,
            page_data=mock_page_data
        )
        
        # Overall score should reflect component scores
        components = result['overall_technical_score']['components']
        
        cwv_score = result['core_web_vitals']['overall_score']
        mobile_score = result['mobile_usability']['overall_score']
        security_score = result['https_security']['security_score']
        schema_score = result['structured_data']['overall_score']
        
        assert components['core_web_vitals']['score'] == cwv_score
        assert components['mobile_usability']['score'] == mobile_score
        assert components['https_security']['score'] == security_score
        assert components['structured_data']['score'] == schema_score
        
        # Weighted sum should match overall score
        expected_overall = sum(
            comp['weight'] * comp['score'] 
            for comp in components.values()
        )
        
        assert abs(result['overall_technical_score']['score'] - expected_overall) < 0.1
    
    def test_error_handling_missing_data(self):
        """Test graceful handling of missing or incomplete data."""
        # Test with minimal data
        minimal_gsc = pd.DataFrame({
            'date': pd.date_range(end=datetime.now(), periods=30, freq='D'),
            'clicks': [100] * 30,
            'impressions': [1000] * 30,
            'ctr': [0.1] * 30,
            'position': [10.0] * 30
        })
        
        minimal_cwv = {
            'lcp_samples': [2000],
            'fid_samples': [50],
            'cls_samples': [0.1]
        }
        
        minimal_pages = pd.DataFrame({
            'url': ['https://example.com/'],
            'https_enabled': [True],
            'has_ssl_cert': [True],
            'cert_expiry_days': [90],
            'mixed_content': [False],
            'mobile_viewport': [True],
            'mobile_friendly': [True],
            'touch_elements_sized': [True],
            'font_size_legible': [True],
            'schema_types': [['Organization']],
            'schema_valid': [True],
            'schema_errors': [[]]
        })
        
        # Should not raise exception
        result = generate_technical_health_report(
            gsc_daily_data=minimal_gsc,
            ga4_cwv_data=minimal_cwv,
            page_data=minimal_pages
        )
        
        assert result is not None
        assert 'overall_technical_score' in result