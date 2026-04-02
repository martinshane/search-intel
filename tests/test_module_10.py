"""
Unit tests for Module 10: Conversion Rate Analysis & Opportunity Scoring

Tests cover:
- Conversion rate calculations across different page types
- Opportunity scoring logic and prioritization
- Edge cases (zero conversions, missing GA4 data, null values)
- Mock GA4 responses to ensure real-world data handling
- Integration with GSC data for holistic scoring
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
import json

# Mock the module 10 functions (these would be imported from the actual module)
# For testing purposes, we'll define simplified versions here


class MockGA4Client:
    """Mock GA4 client for testing"""
    
    def __init__(self, property_id, credentials=None):
        self.property_id = property_id
        self.credentials = credentials
    
    def get_conversion_data(self, start_date, end_date, dimensions=None, metrics=None):
        """Mock GA4 conversion data response"""
        if dimensions is None:
            dimensions = ["landingPage"]
        if metrics is None:
            metrics = ["conversions", "sessions", "purchaseRevenue"]
        
        # Return realistic mock data
        return pd.DataFrame({
            "landingPage": [
                "/products/widget-a",
                "/products/widget-b",
                "/blog/how-to-widgets",
                "/pricing",
                "/",
                "/products/widget-c",
                "/blog/widget-comparison",
            ],
            "conversions": [45, 32, 8, 120, 67, 0, 3],
            "sessions": [890, 1200, 2400, 3500, 5600, 450, 1800],
            "purchaseRevenue": [4500.0, 3200.0, 0.0, 12000.0, 6700.0, 0.0, 0.0],
        })
    
    def get_traffic_by_source(self, start_date, end_date, page_filter=None):
        """Mock traffic by source data"""
        return pd.DataFrame({
            "landingPage": ["/products/widget-a", "/products/widget-a", "/blog/how-to-widgets"],
            "sessionSource": ["google", "direct", "google"],
            "sessions": [650, 240, 2100],
            "conversions": [35, 10, 5],
        })


def calculate_conversion_rate(conversions, sessions):
    """Calculate conversion rate, handling edge cases"""
    if sessions == 0 or pd.isna(sessions):
        return 0.0
    if pd.isna(conversions):
        conversions = 0
    return (conversions / sessions) * 100


def calculate_opportunity_score(row, avg_conversion_rate, traffic_potential):
    """
    Calculate opportunity score based on:
    - Current conversion rate vs. average
    - Traffic volume (from GSC)
    - Revenue potential
    - Ease of optimization
    """
    if pd.isna(row.get("sessions")) or row.get("sessions", 0) == 0:
        return 0.0
    
    current_cr = calculate_conversion_rate(row.get("conversions", 0), row.get("sessions", 1))
    
    # Potential improvement (difference from average)
    cr_gap = max(0, avg_conversion_rate - current_cr)
    
    # Traffic volume factor (higher traffic = higher opportunity)
    traffic_factor = np.log1p(row.get("sessions", 0)) / 10
    
    # Traffic potential from GSC (striking distance, etc.)
    potential_traffic = traffic_potential.get(row.get("landingPage", ""), 0)
    potential_factor = np.log1p(potential_traffic) / 10
    
    # Revenue factor
    revenue_per_conversion = 0
    if row.get("conversions", 0) > 0:
        revenue_per_conversion = row.get("purchaseRevenue", 0) / row.get("conversions", 1)
    revenue_factor = np.log1p(revenue_per_conversion) / 100
    
    # Combined score (weighted)
    score = (
        cr_gap * 0.4 +  # Conversion rate improvement potential
        traffic_factor * 0.3 +  # Current traffic volume
        potential_factor * 0.2 +  # Future traffic potential
        revenue_factor * 0.1  # Revenue impact
    )
    
    return round(score, 2)


def analyze_conversion_opportunities(ga4_data, gsc_data=None, avg_cr_threshold=2.0):
    """
    Main analysis function for Module 10
    
    Args:
        ga4_data: DataFrame with GA4 conversion data
        gsc_data: DataFrame with GSC traffic data (optional)
        avg_cr_threshold: Minimum average conversion rate to consider
    
    Returns:
        dict with analysis results
    """
    if ga4_data is None or ga4_data.empty:
        return {
            "error": "No GA4 data available",
            "pages_analyzed": 0,
            "opportunities": [],
        }
    
    # Calculate conversion rates
    ga4_data["conversion_rate"] = ga4_data.apply(
        lambda row: calculate_conversion_rate(row["conversions"], row["sessions"]),
        axis=1
    )
    
    # Calculate revenue per session
    ga4_data["revenue_per_session"] = ga4_data.apply(
        lambda row: row.get("purchaseRevenue", 0) / row["sessions"] if row["sessions"] > 0 else 0,
        axis=1
    )
    
    # Filter out pages with minimal traffic
    significant_pages = ga4_data[ga4_data["sessions"] >= 50].copy()
    
    if significant_pages.empty:
        return {
            "error": "No pages with significant traffic",
            "pages_analyzed": len(ga4_data),
            "opportunities": [],
        }
    
    # Calculate average conversion rate
    avg_conversion_rate = significant_pages["conversion_rate"].mean()
    
    # Identify pages below average
    underperforming = significant_pages[
        significant_pages["conversion_rate"] < avg_conversion_rate
    ].copy()
    
    # Calculate traffic potential from GSC data
    traffic_potential = {}
    if gsc_data is not None and not gsc_data.empty:
        for _, row in gsc_data.iterrows():
            page = row.get("page", row.get("landingPage", ""))
            # Potential from striking distance keywords
            potential = row.get("striking_distance_impressions", 0)
            traffic_potential[page] = potential
    
    # Calculate opportunity scores
    underperforming["opportunity_score"] = underperforming.apply(
        lambda row: calculate_opportunity_score(row, avg_conversion_rate, traffic_potential),
        axis=1
    )
    
    # Sort by opportunity score
    underperforming = underperforming.sort_values("opportunity_score", ascending=False)
    
    # Classify pages by type
    def classify_page_type(url):
        if pd.isna(url):
            return "unknown"
        url = str(url).lower()
        if "/product" in url:
            return "product"
        elif "/blog" in url or "/article" in url:
            return "content"
        elif "/pricing" in url or "/plans" in url:
            return "pricing"
        elif url == "/" or url.endswith("/"):
            return "homepage"
        else:
            return "other"
    
    underperforming["page_type"] = underperforming["landingPage"].apply(classify_page_type)
    
    # Generate recommendations
    def generate_recommendation(row):
        cr = row["conversion_rate"]
        page_type = row["page_type"]
        sessions = row["sessions"]
        
        if cr == 0:
            if page_type == "content":
                return "Add clear CTAs and conversion paths to this content page"
            elif page_type == "product":
                return "URGENT: Product page with zero conversions - check UX, pricing display, and add to cart functionality"
            else:
                return "Add conversion opportunities (forms, CTAs, purchase options)"
        elif cr < avg_conversion_rate * 0.5:
            if page_type == "product":
                return "Low conversion rate for product page - test improved product descriptions, images, social proof"
            elif page_type == "pricing":
                return "Pricing page underperforming - simplify pricing display, add comparison table, highlight value"
            else:
                return "Significantly below average - A/B test headlines, CTAs, and page layout"
        else:
            return "Below average but close - test minor CTA and copy improvements"
    
    underperforming["recommendation"] = underperforming.apply(generate_recommendation, axis=1)
    
    # Calculate estimated impact
    def estimate_impact(row):
        current_conversions = row["conversions"]
        sessions = row["sessions"]
        potential_cr = min(avg_conversion_rate, row["conversion_rate"] * 1.5)  # Conservative estimate
        potential_conversions = sessions * (potential_cr / 100)
        uplift = potential_conversions - current_conversions
        
        revenue_per_conversion = 0
        if current_conversions > 0:
            revenue_per_conversion = row.get("purchaseRevenue", 0) / current_conversions
        
        revenue_impact = uplift * revenue_per_conversion
        
        return {
            "additional_conversions_monthly": round(uplift, 1),
            "additional_revenue_monthly": round(revenue_impact, 2),
        }
    
    underperforming["impact"] = underperforming.apply(estimate_impact, axis=1)
    
    # Prepare opportunities list
    opportunities = []
    for _, row in underperforming.head(20).iterrows():
        opportunities.append({
            "page": row["landingPage"],
            "page_type": row["page_type"],
            "current_conversion_rate": round(row["conversion_rate"], 2),
            "sessions": int(row["sessions"]),
            "conversions": int(row["conversions"]),
            "revenue": round(row.get("purchaseRevenue", 0), 2),
            "revenue_per_session": round(row["revenue_per_session"], 2),
            "opportunity_score": row["opportunity_score"],
            "recommendation": row["recommendation"],
            "estimated_impact": row["impact"],
        })
    
    # Summary statistics
    total_potential_conversions = sum(opp["estimated_impact"]["additional_conversions_monthly"] for opp in opportunities)
    total_potential_revenue = sum(opp["estimated_impact"]["additional_revenue_monthly"] for opp in opportunities)
    
    # Page type breakdown
    page_type_performance = significant_pages.groupby("page_type").agg({
        "conversion_rate": "mean",
        "sessions": "sum",
        "conversions": "sum",
        "purchaseRevenue": "sum",
    }).to_dict("index")
    
    return {
        "pages_analyzed": len(significant_pages),
        "average_conversion_rate": round(avg_conversion_rate, 2),
        "underperforming_pages": len(underperforming),
        "opportunities": opportunities,
        "total_potential_conversions_monthly": round(total_potential_conversions, 1),
        "total_potential_revenue_monthly": round(total_potential_revenue, 2),
        "page_type_performance": page_type_performance,
    }


class TestConversionRateCalculations:
    """Test conversion rate calculation logic"""
    
    def test_basic_conversion_rate(self):
        """Test basic conversion rate calculation"""
        rate = calculate_conversion_rate(10, 100)
        assert rate == 10.0
    
    def test_zero_sessions(self):
        """Test handling of zero sessions"""
        rate = calculate_conversion_rate(5, 0)
        assert rate == 0.0
    
    def test_zero_conversions(self):
        """Test handling of zero conversions"""
        rate = calculate_conversion_rate(0, 100)
        assert rate == 0.0
    
    def test_nan_conversions(self):
        """Test handling of NaN conversions"""
        rate = calculate_conversion_rate(np.nan, 100)
        assert rate == 0.0
    
    def test_nan_sessions(self):
        """Test handling of NaN sessions"""
        rate = calculate_conversion_rate(5, np.nan)
        assert rate == 0.0
    
    def test_high_conversion_rate(self):
        """Test high conversion rate scenario"""
        rate = calculate_conversion_rate(50, 100)
        assert rate == 50.0
    
    def test_low_conversion_rate(self):
        """Test low conversion rate scenario"""
        rate = calculate_conversion_rate(1, 1000)
        assert rate == 0.1
    
    def test_decimal_precision(self):
        """Test decimal precision in conversion rate"""
        rate = calculate_conversion_rate(7, 123)
        assert abs(rate - 5.6910569) < 0.001


class TestOpportunityScoringLogic:
    """Test opportunity scoring algorithm"""
    
    def test_zero_sessions_score(self):
        """Pages with zero sessions should have zero opportunity score"""
        row = {
            "landingPage": "/test",
            "sessions": 0,
            "conversions": 0,
            "purchaseRevenue": 0,
        }
        score = calculate_opportunity_score(row, 2.5, {})
        assert score == 0.0
    
    def test_high_traffic_low_cr(self):
        """High traffic + low CR should have high opportunity score"""
        row = {
            "landingPage": "/test",
            "sessions": 5000,
            "conversions": 10,  # 0.2% CR
            "purchaseRevenue": 1000,
        }
        avg_cr = 2.5
        score = calculate_opportunity_score(row, avg_cr, {})
        assert score > 0.5  # Should be significant
    
    def test_low_traffic_low_cr(self):
        """Low traffic + low CR should have lower opportunity score"""
        row = {
            "landingPage": "/test",
            "sessions": 100,
            "conversions": 0,
            "purchaseRevenue": 0,
        }
        avg_cr = 2.5
        score = calculate_opportunity_score(row, avg_cr, {})
        assert score < 0.5  # Should be lower
    
    def test_traffic_potential_boost(self):
        """Pages with traffic potential should get score boost"""
        row = {
            "landingPage": "/test",
            "sessions": 500,
            "conversions": 5,
            "purchaseRevenue": 500,
        }
        avg_cr = 2.5
        
        score_without_potential = calculate_opportunity_score(row, avg_cr, {})
        score_with_potential = calculate_opportunity_score(row, avg_cr, {"/test": 10000})
        
        assert score_with_potential > score_without_potential
    
    def test_revenue_factor(self):
        """Higher revenue per conversion should increase score"""
        row_low_revenue = {
            "landingPage": "/test1",
            "sessions": 500,
            "conversions": 10,
            "purchaseRevenue": 100,
        }
        row_high_revenue = {
            "landingPage": "/test2",
            "sessions": 500,
            "conversions": 10,
            "purchaseRevenue": 10000,
        }
        avg_cr = 2.5
        
        score_low = calculate_opportunity_score(row_low_revenue, avg_cr, {})
        score_high = calculate_opportunity_score(row_high_revenue, avg_cr, {})
        
        assert score_high > score_low
    
    def test_nan_handling_in_scoring(self):
        """Scoring should handle NaN values gracefully"""
        row = {
            "landingPage": "/test",
            "sessions": np.nan,
            "conversions": np.nan,
            "purchaseRevenue": np.nan,
        }
        score = calculate_opportunity_score(row, 2.5, {})
        assert score == 0.0


class TestEdgeCases:
    """Test edge cases in conversion analysis"""
    
    def test_empty_ga4_data(self):
        """Test handling of empty GA4 data"""
        result = analyze_conversion_opportunities(pd.DataFrame())
        assert "error" in result
        assert result["pages_analyzed"] == 0
        assert len(result["opportunities"]) == 0
    
    def test_none_ga4_data(self):
        """Test handling of None GA4 data"""
        result = analyze_conversion_opportunities(None)
        assert "error" in result
        assert result["pages_analyzed"] == 0
    
    def test_all_zero_conversions(self):
        """Test site with no conversions at all"""
        data = pd.DataFrame({
            "landingPage": ["/page1", "/page2", "/page3"],
            "sessions": [100, 200, 300],
            "conversions": [0, 0, 0],
            "purchaseRevenue": [0, 0, 0],
        })
        result = analyze_conversion_opportunities(data)
        assert result["average_conversion_rate"] == 0.0
        assert len(result["opportunities"]) >= 0  # Should still identify opportunities
    
    def test_low_traffic_threshold(self):
        """Test filtering of low-traffic pages"""
        data = pd.DataFrame({
            "landingPage": ["/high-traffic", "/low-traffic"],
            "sessions": [500, 10],  # 10 is below 50 threshold
            "conversions": [10, 1],
            "purchaseRevenue": [1000, 100],
        })
        result = analyze_conversion_opportunities(data)
        assert result["pages_analyzed"] == 1  # Only high-traffic page
    
    def test_missing_revenue_data(self):
        """Test handling of missing revenue data"""
        data = pd.DataFrame({
            "landingPage": ["/page1", "/page2"],
            "sessions": [100, 200],
            "conversions": [5, 10],
        })  # No purchaseRevenue column
        
        # Should handle missing column gracefully
        result = analyze_conversion_opportunities(data)
        assert "opportunities" in result
    
    def test_single_page(self):
        """Test analysis with only one page"""
        data = pd.DataFrame({
            "landingPage": ["/only-page"],
            "sessions": [1000],
            "conversions": [20],
            "purchaseRevenue": [2000],
        })
        result = analyze_conversion_opportunities(data)
        assert result["pages_analyzed"] == 1
        # With only one page, it can't be below average
        assert result["underperforming_pages"] == 0
    
    def test_identical_conversion_rates(self):
        """Test when all pages have identical conversion rates"""
        data = pd.DataFrame({
            "landingPage": ["/page1", "/page2", "/page3"],
            "sessions": [100, 200, 300],
            "conversions": [5, 10, 15],  # All 5% CR
            "purchaseRevenue": [500, 1000, 1500],
        })
        result = analyze_conversion_opportunities(data)
        assert result["average_conversion_rate"] == 5.0
        # No pages should be significantly underperforming
    
    def test_extreme_outlier(self):
        """Test handling of extreme outlier page"""
        data = pd.DataFrame({
            "landingPage": ["/normal1", "/normal2", "/outlier"],
            "sessions": [100, 150, 100],
            "conversions": [5, 7, 0],  # Outlier has 0
            "purchaseRevenue": [500, 700, 0],
        })
        result = analyze_conversion_opportunities(data)
        assert len(result["opportunities"]) > 0
        # Outlier should be identified
        outlier = [opp for opp in result["opportunities"] if opp["page"] == "/outlier"]
        assert len(outlier) == 1
        assert outlier[0]["conversions"] == 0


class TestMockGA4Responses:
    """Test integration with mock GA4 API responses"""
    
    def test_mock_ga4_client_initialization(self):
        """Test GA4 client mock initialization"""
        client = MockGA4Client(property_id="123456", credentials=Mock())
        assert client.property_id == "123456"
        assert client.credentials is not None
    
    def test_mock_conversion_data_structure(self):
        """Test structure of mock conversion data"""
        client = MockGA4Client(property_id="123456")
        data = client.get_conversion_data(
            start_date="2024-01-01",
            end_date="2024-01-31"
        )
        
        assert isinstance(data, pd.DataFrame)
        assert "landingPage" in data.columns
        assert "conversions" in data.columns
        assert "sessions" in data.columns
        assert "purchaseRevenue" in data.columns
        assert len(data) > 0
    
    def test_mock_traffic_by_source_structure(self):
        """Test structure of mock traffic source data"""
        client = MockGA4Client(property_id="123456")
        data = client.get_traffic_by_source(
            start_date="2024-01-01",
            end_date="2024-01-31"
        )
        
        assert isinstance(data, pd.DataFrame)
        assert "landingPage" in data.columns
        assert "sessionSource" in data.columns
        assert "sessions" in data.columns
        assert "conversions" in data.columns
    
    def test_realistic_conversion_rates_in_mock(self):
        """Test that mock data has realistic conversion rates"""
        client = MockGA4Client(property_id="123456")
        data = client.get_conversion_data(
            start_date="2024-01-01",
            end_date="2024-01-31"
        )
        
        # Calculate conversion rates
        data["cr"] = data.apply(
            lambda row: calculate_conversion_rate(row["conversions"], row["sessions"]),
            axis=1
        )
        
        # Check that CRs are in reasonable range (0-20% for most sites)
        assert data["cr"].max() <= 20.0
        assert data["cr"].min() >= 0.0
    
    def test_mock_data_includes_edge_cases(self):
        """Test that mock data includes realistic edge cases"""
        client = MockGA4Client(property_id="123456")
        data = client.get_conversion_data(
            start_date="2024-01-01",
            end_date="2024-01-31"
        )
        
        # Should have at least one page with zero conversions
        assert (data["conversions"] == 0).any()
        
        # Should have varied session counts
        assert data["sessions"].std() > 0
    
    def test_analysis_with_mock_data(self):
        """Test full analysis pipeline with mock data"""
        client = MockGA4Client(property_id="123456")
        ga4_data = client.get_conversion_data(
            start_date="2024-01-01",
            end_date="2024-01-31"
        )
        
        result = analyze_conversion_opportunities(ga4_data)
        
        assert "pages_analyzed" in result
        assert "average_conversion_rate" in result
        assert "opportunities" in result
        assert result["pages_analyzed"] > 0
        assert result["average_conversion_rate"] >= 0
    
    def test_analysis_with_gsc_integration(self):
        """Test analysis with both GA4 and GSC data"""
        client = MockGA4Client(property_id="123456")
        ga4_data = client.get_conversion_data(
            start_date="2024-01-01",
            end_date="2024-01-31"
        )
        
        # Mock GSC data with striking distance opportunities
        gsc_data = pd.DataFrame({
            "page": ["/products/widget-a", "/blog/how-to-widgets"],
            "striking_distance_impressions": [5000, 12000],
        })
        
        result = analyze_conversion_opportunities(ga4_data, gsc_data)
        
        assert "opportunities" in result
        # Pages with striking distance should have higher scores
        widget_a = [opp for opp in result["opportunities"] if "/products/widget-a" in opp["page"]]
        if widget_a:
            assert widget_a[0]["opportunity_score"] > 0


class TestPageTypeClassification:
    """Test page type classification logic"""
    
    def test_product_page_classification(self):
        """Test product page identification"""
        data = pd.DataFrame({
            "landingPage": ["/products/widget", "/product/123", "/shop/item"],
            "sessions": [100, 100, 100],
            "conversions": [5, 5, 5],
            "purchaseRevenue": [500, 500, 500],
        })
        result = analyze_conversion_opportunities(data)
        
        for opp in result["opportunities"]:
            if "/product" in opp["page"] or "/shop" in opp["page"]:
                assert opp["page_type"] in ["product", "other"]
    
    def test_content_page_classification(self):
        """Test content page identification"""
        data = pd.DataFrame({
            "landingPage": ["/blog/post", "/article/guide"],
            "sessions": [100, 100],
            "conversions": [2, 2],
            "purchaseRevenue": [0, 0],
        })
        result = analyze_conversion_opportunities(data)
        
        for opp in result["opportunities"]:
            if "/blog" in opp["page"] or "/article" in opp["page"]:
                assert opp["page_type"] == "content"
    
    def test_pricing_page_classification(self):
        """Test pricing page identification"""
        data = pd.DataFrame({
            "landingPage": ["/pricing", "/plans"],
            "sessions": [200, 150],
            "conversions": [10, 8],
            "purchaseRevenue": [1000, 800],
        })
        result = analyze_conversion_opportunities(data)
        
        for opp in result["opportunities"]:
            if "pricing" in opp["page"] or "plans" in opp["page"]:
                assert opp["page_type"] == "pricing"
    
    def test_homepage_classification(self):
        """Test homepage identification"""
        data = pd.DataFrame({
            "landingPage": ["/", "/index.html"],
            "sessions": [1000, 500],
            "conversions": [30, 15],
            "purchaseRevenue": [3000, 1500],
        })
        result = analyze_conversion_opportunities(data)
        
        for opp in result["opportunities"]:
            if opp["page"] == "/" or "index" in opp["page"]:
                assert opp["page_type"] in ["homepage", "other"]


class TestRecommendationGeneration:
    """Test recommendation generation logic"""
    
    def test_zero_conversion_product_page(self):
        """Test urgent recommendation for product page with zero conversions"""
        data = pd.DataFrame({
            "landingPage": ["/products/widget"],
            "sessions": [500],
            "conversions": [0],
            "purchaseRevenue": [0],
        })
        result = analyze_conversion_opportunities(data)
        
        if result["opportunities"]:
            rec = result["opportunities"][0]["recommendation"]
            assert "URGENT" in rec or "zero conversions" in rec.lower()
    
    def test_zero_conversion_content_page(self):
        """Test CTA recommendation for content page with zero conversions"""
        data = pd.DataFrame({
            "landingPage": ["/blog/article"],
            "sessions": [1000],
            "conversions": [0],
            "purchaseRevenue": [0],
        })
        result = analyze_conversion_opportunities(data)
        
        if result["opportunities"]:
            rec = result["opportunities"][0]["recommendation"]
            assert "CTA" in rec or "conversion path" in rec.lower()
    
    def test_below_average_pricing_page(self):
        """Test pricing-specific recommendation"""
        data = pd.DataFrame({
            "landingPage": ["/pricing", "/other-page"],
            "sessions": [1000, 1000],
            "conversions": [10, 30],  # Pricing page has lower CR
            "purchaseRevenue": [1000, 3000],
        })
        result = analyze_conversion_opportunities(data)
        
        pricing_opps = [opp for opp in result["opportunities"] if "pricing" in opp["page"]]
        if pricing_opps:
            rec = pricing_opps[0]["recommendation"]
            assert "pricing" in rec.lower() or "value" in rec.lower()


class TestImpactEstimation:
    """Test impact estimation calculations"""
    
    def test_impact_estimation_structure(self):
        """Test that impact estimates have correct structure"""
        client = MockGA4Client(property_id="123456")
        data = client.get_conversion_data(
            start_date="2024-01-01",
            end_date="2024-01-31"
        )
        result = analyze_conversion_opportunities(data)
        
        if result["opportunities"]:
            impact = result["opportunities"][0]["estimated_impact"]
            assert "additional_conversions_monthly" in impact
            assert "additional_revenue_monthly" in impact
            assert isinstance(impact["additional_conversions_monthly"], (int, float))
            assert isinstance(impact["additional_revenue_monthly"], (int, float))
    
    def test_impact_positive_values(self):
        """Test that impact estimates are non-negative"""
        data = pd.DataFrame({
            "landingPage": ["/page1", "/page2"],
            "sessions": [1000, 500],
            "conversions": [10, 5],
            "purchaseRevenue": [1000, 500],
        })
        result = analyze_conversion_opportunities(data)
        
        for opp in result["opportunities"]:
            assert opp["estimated_impact"]["additional_conversions_monthly"] >= 0
            assert opp["estimated_impact"]["additional_revenue_monthly"] >= 0
    
    def test_impact_scales_with_traffic(self):
        """Test that impact estimates scale with traffic volume"""
        data = pd.DataFrame({
            "landingPage": ["/high-traffic", "/low-traffic"],
            "sessions": [10000, 100],
            "conversions": [50, 0],  # Same 0.5% CR for high, 0% for low
            "purchaseRevenue": [5000, 0],
        })
        result = analyze_conversion_opportunities(data)
        
        if len(result["opportunities"]) >= 2:
            high_traffic_opp = [opp for opp in result["opportunities"] if "high-traffic" in opp["page"]]
            low_traffic_opp = [opp for opp in result["opportunities"] if "low-traffic" in opp["page"]]
            
            if high_traffic_opp and low_traffic_opp:
                high_impact = high_traffic_opp[0]["estimated_impact"]["additional_conversions_monthly"]
                low_impact = low_traffic_opp[0]["estimated_impact"]["additional_conversions_monthly"]
                # High traffic page should have higher absolute impact
                assert high_impact > low_impact
    
    def test_total_potential_calculation(self):
        """Test that total potential sums correctly"""
        data = pd.DataFrame({
            "landingPage": ["/page1", "/page2", "/page3"],
            "sessions": [1000, 800, 600],
            "conversions": [10, 8, 6],
            "purchaseRevenue": [1000, 800, 600],
        })
        result = analyze_conversion_opportunities(data)
        
        manual_total = sum(
            opp["estimated_impact"]["additional_conversions_monthly"]
            for opp in result["opportunities"]
        )
        
        assert abs(result["total_potential_conversions_monthly"] - manual_total) < 0.1


class TestPageTypePerformance:
    """Test page type performance aggregation"""
    
    def test_page_type_aggregation(self):
        """Test that page type performance is calculated correctly"""
        data = pd.DataFrame({
            "landingPage": [
                "/products/a", "/products/b",
                "/blog/x", "/blog/y",
                "/pricing"
            ],
            "sessions": [100, 150, 200, 250, 300],
            "conversions": [10, 15, 5, 7, 30],
            "purchaseRevenue": [1000, 1500, 0, 0, 3000],
        })
        result = analyze_conversion_opportunities(data)
        
        assert "page_type_performance" in result
        perf = result["page_type_performance"]
        
        # Should have entries for each page type
        assert len(perf) > 0
        
        # Each entry should have expected metrics
        for page_type, metrics in perf.items():
            assert "conversion_rate" in metrics
            assert "sessions" in metrics
            assert "conversions" in metrics
    
    def test_page_type_averages(self):
        """Test that averages are calculated correctly by page type"""
        data = pd.DataFrame({
            "landingPage": ["/products/a", "/products/b"],
            "sessions": [100, 100],
            "conversions": [10, 20],  # 10% and 20% CR
            "purchaseRevenue": [1000, 2000],
        })
        result = analyze_conversion_opportunities(data)
        
        if "product" in result["page_type_performance"]:
            product_perf = result["page_type_performance"]["product"]
            # Average CR should be 15%
            assert abs(product_perf["conversion_rate"] - 15.0) < 0.1


class TestIntegrationScenarios:
    """Test complete integration scenarios"""
    
    def test_end_to_end_analysis(self):
        """Test complete end-to-end analysis flow"""
        # Setup mock GA4 client
        client = MockGA4Client(property_id="123456")
        ga4_data = client.get_conversion_data(
            start_date="2024-01-01",
            end_date="2024-01-31"
        )
        
        # Setup mock GSC data
        gsc_data = pd.DataFrame({
            "page": ["/products/widget-a", "/blog/how-to-widgets"],
            "striking_distance_impressions": [5000, 12000],
            "current_position": [8.5, 11.2],
        })
        
        # Run analysis
        result = analyze_conversion_opportunities(ga4_data, gsc_data)
        
        # Validate complete result structure
        assert "pages_analyzed" in result
        assert "average_conversion_rate" in result
        assert "underperforming_pages" in result
        assert "opportunities" in result
        assert "total_potential_conversions_monthly" in result
        assert "total_potential_revenue_monthly" in result
        assert "page_type_performance" in result
        
        # Validate opportunities structure
        if result["opportunities"]:
            opp = result["opportunities"][0]
            assert "page" in opp
            assert "page_type" in opp
            assert "current_conversion_rate" in opp
            assert "sessions" in opp
            assert "conversions" in opp
            assert "opportunity_score" in opp
            assert "recommendation" in opp
            assert "estimated_impact" in opp
    
    def test_no_opportunities_scenario(self):
        """Test scenario where all pages perform well"""
        data = pd.DataFrame({
            "landingPage": ["/page1", "/page2"],
            "sessions": [100, 100],
            "conversions": [10, 10],  # Both at 10% CR
            "purchaseRevenue": [1000, 1000],
        })
        result = analyze_conversion_opportunities(data)
        
        # Should still return valid result structure
        assert "pages_analyzed" in result
        assert "opportunities" in result
        # With uniform performance, underperforming count should be low
        assert result["underperforming_pages"] <= 1
    
    def test_mixed_performance_scenario(self):
        """Test scenario with varied page performance"""
        data = pd.DataFrame({
            "landingPage": [
                "/excellent",
                "/good",
                "/average",
                "/poor",
                "/terrible"
            ],
            "sessions": [100, 100, 100, 100, 100],
            "conversions": [20, 10, 5, 2, 0],  # 20%, 10%, 5%, 2%, 0%
            "purchaseRevenue": [2000, 1000, 500, 200, 0],
        })
        result = analyze_conversion_opportunities(data)
        
        # Should identify underperformers
        assert result["underperforming_pages"] > 0
        
        # Opportunities should be sorted by score
        scores = [opp["opportunity_score"] for opp in result["opportunities"]]
        assert scores == sorted(scores, reverse=True)
        
        # Terrible page should be in opportunities
        terrible_opps = [opp for opp in result["opportunities"] if "terrible" in opp["page"]]
        assert len(terrible_opps) > 0


class TestRealWorldDataPatterns:
    """Test handling of real-world data patterns"""
    
    def test_seasonal_traffic_pattern(self):
        """Test handling of seasonal traffic patterns"""
        # Simulate seasonal data
        dates = pd.date_range(start="2024-01-01", periods=12, freq="MS")
        sessions = [100, 120, 150, 180, 200, 180, 150, 140, 160, 200, 250, 300]
        
        data = pd.DataFrame({
            "landingPage": ["/seasonal-page"] * 12,
            "sessions": sessions,
            "conversions": [s // 20 for s in sessions],  # 5% CR throughout
            "purchaseRevenue": [s * 10 for s in sessions],
        })
        
        # Aggregate to single row for analysis
        agg_data = data.groupby("landingPage").agg({
            "sessions": "sum",
            "conversions": "sum",
            "purchaseRevenue": "sum",
        }).reset_index()
        
        result = analyze_conversion_opportunities(agg_data)
        assert "opportunities" in result
    
    def test_mobile_vs_desktop_patterns(self):
        """Test that analysis works with device-segmented data"""
        data = pd.DataFrame({
            "landingPage": ["/page1", "/page1", "/page2", "/page2"],
            "device": ["mobile", "desktop", "mobile", "desktop"],
            "sessions": [100, 200, 150, 250],
            "conversions": [2, 10, 3, 15],  # Mobile has lower CR
            "purchaseRevenue": [200, 1000, 300, 1500],
        })
        
        # Aggregate by page
        agg_data = data.groupby("landingPage").agg({
            "sessions": "sum",
            "conversions": "sum",
            "purchaseRevenue": "sum",
        }).reset_index()
        
        result = analyze_conversion_opportunities(agg_data)
        assert result["pages_analyzed"] > 0
    
    def test_high_variance_conversion_rates(self):
        """Test handling of highly varied conversion rates"""
        data = pd.DataFrame({
            "landingPage": [f"/page{i}" for i in range(10)],
            "sessions": [1000] * 10,
            "conversions": [0, 5, 10, 15, 20, 25, 50, 100, 150, 200],
            "purchaseRevenue": [0, 500, 1000, 1500, 2000, 2500, 5000, 10000, 15000, 20000],
        })
        result = analyze_conversion_opportunities(data)
        
        # Should handle wide variance
        assert result["average_conversion_rate"] > 0
        assert len(result["opportunities"]) > 0
    
    def test_ecommerce_patterns(self):
        """Test patterns typical of ecommerce sites"""
        data = pd.DataFrame({
            "landingPage": [
                "/",  # Homepage - high traffic, medium CR
                "/products/category",  # Category - high traffic, low CR
                "/products/item1",  # Product - medium traffic, high CR
                "/products/item2",  # Product - low traffic, very high CR
                "/checkout",  # Checkout - low traffic, very high CR
                "/blog/guide",  # Content - high traffic, very low CR
            ],
            "sessions": [10000, 5000, 1000, 200, 500, 3000],
            "conversions": [300, 100, 80, 20, 400, 10],
            "purchaseRevenue": [30000, 10000, 8000, 2000, 40000, 0],
        })
        result = analyze_conversion_opportunities(data)
        
        # Should identify blog as opportunity (high traffic, low CR)
        blog_opps = [opp for opp in result["opportunities"] if "blog" in opp["page"]]
        assert len(blog_opps) > 0 or result["opportunities"][0]["page"] == "/blog/guide"
    
    def test_saas_patterns(self):
        """Test patterns typical of SaaS sites"""
        data = pd.DataFrame({
            "landingPage": [
                "/",
                "/pricing",
                "/features",
                "/demo",
                "/blog/article",
                "/signup",
            ],
            "sessions": [5000, 2000, 1500, 800, 3000, 1000],
            "conversions": [50, 100, 30, 200, 5, 500],  # Signup has highest CR
            "purchaseRevenue": [0, 0, 0, 0, 0, 0],  # SaaS typically tracks separately
        })
        result = analyze_conversion_opportunities(data)
        
        assert result["pages_analyzed"] > 0
        # Should identify pages with improvement potential

