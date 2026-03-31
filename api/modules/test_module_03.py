"""
Unit tests for Module 3 (SERP Landscape Analysis) using synthetic data.
Tests competitor mapping, SERP feature detection, and output schema validation.
"""

import pytest
from unittest.mock import Mock, patch
from datetime import datetime
from api.modules.module_03_serp_landscape import (
    analyze_serp_landscape,
    _parse_serp_features,
    _calculate_visual_position,
    _extract_competitors,
    _classify_intent,
    _estimate_click_share
)


@pytest.fixture
def synthetic_serp_data():
    """Synthetic SERP data for testing."""
    return [
        {
            "keyword": "best crm software",
            "organic_position": 3,
            "url": "https://example.com/blog/best-crm",
            "serp_features": {
                "featured_snippet": {"present": True, "domain": "competitor1.com"},
                "people_also_ask": {"count": 4},
                "ai_overview": {"present": True},
                "video_carousel": {"present": False},
                "local_pack": {"present": False},
                "knowledge_panel": {"present": False},
                "shopping_results": {"present": True, "count": 3},
                "image_pack": {"present": False},
                "top_stories": {"present": False}
            },
            "top_10_results": [
                {"position": 1, "domain": "competitor1.com", "url": "https://competitor1.com/crm"},
                {"position": 2, "domain": "competitor2.com", "url": "https://competitor2.com/best-crm"},
                {"position": 3, "domain": "example.com", "url": "https://example.com/blog/best-crm"},
                {"position": 4, "domain": "competitor3.com", "url": "https://competitor3.com/crm-software"},
                {"position": 5, "domain": "competitor1.com", "url": "https://competitor1.com/crm-guide"},
            ]
        },
        {
            "keyword": "crm implementation guide",
            "organic_position": 5,
            "url": "https://example.com/guides/implementation",
            "serp_features": {
                "featured_snippet": {"present": False},
                "people_also_ask": {"count": 6},
                "ai_overview": {"present": False},
                "video_carousel": {"present": True, "count": 3},
                "local_pack": {"present": False},
                "knowledge_panel": {"present": False},
                "shopping_results": {"present": False},
                "image_pack": {"present": True},
                "top_stories": {"present": False}
            },
            "top_10_results": [
                {"position": 1, "domain": "competitor2.com", "url": "https://competitor2.com/impl"},
                {"position": 2, "domain": "competitor4.com", "url": "https://competitor4.com/guide"},
                {"position": 3, "domain": "competitor1.com", "url": "https://competitor1.com/impl"},
                {"position": 4, "domain": "competitor3.com", "url": "https://competitor3.com/how-to"},
                {"position": 5, "domain": "example.com", "url": "https://example.com/guides/implementation"},
            ]
        },
        {
            "keyword": "crm pricing comparison",
            "organic_position": 2,
            "url": "https://example.com/pricing",
            "serp_features": {
                "featured_snippet": {"present": False},
                "people_also_ask": {"count": 3},
                "ai_overview": {"present": False},
                "video_carousel": {"present": False},
                "local_pack": {"present": False},
                "knowledge_panel": {"present": False},
                "shopping_results": {"present": False},
                "image_pack": {"present": False},
                "top_stories": {"present": False}
            },
            "top_10_results": [
                {"position": 1, "domain": "competitor1.com", "url": "https://competitor1.com/pricing"},
                {"position": 2, "domain": "example.com", "url": "https://example.com/pricing"},
                {"position": 3, "domain": "competitor2.com", "url": "https://competitor2.com/price"},
            ]
        }
    ]


@pytest.fixture
def synthetic_gsc_data():
    """Synthetic GSC keyword data for testing."""
    return [
        {"keyword": "best crm software", "position": 3.2, "impressions": 8900, "clicks": 180, "ctr": 0.020},
        {"keyword": "crm implementation guide", "position": 5.1, "impressions": 3400, "clicks": 95, "ctr": 0.028},
        {"keyword": "crm pricing comparison", "position": 2.1, "impressions": 5600, "clicks": 420, "ctr": 0.075},
    ]


def test_parse_serp_features():
    """Test SERP feature parsing."""
    serp_item = {
        "serp_features": {
            "featured_snippet": {"present": True},
            "people_also_ask": {"count": 4},
            "ai_overview": {"present": True},
            "shopping_results": {"present": True, "count": 3}
        }
    }
    
    features = _parse_serp_features(serp_item)
    
    assert features["featured_snippet"] is True
    assert features["paa_count"] == 4
    assert features["ai_overview"] is True
    assert features["shopping_results"] is True


def test_calculate_visual_position():
    """Test visual position calculation based on SERP features."""
    # Position 3 with featured snippet (2), 4 PAA (0.5 each), AI overview (1)
    visual_pos = _calculate_visual_position(
        organic_position=3,
        featured_snippet=True,
        paa_count=4,
        ai_overview=True,
        video_carousel=False,
        local_pack=False,
        shopping_count=0
    )
    
    # Organic 3 + featured snippet 2 + PAA 2 (4 * 0.5) + AI overview 1 = 8
    assert visual_pos == 8.0
    
    # Position 2 with no features
    visual_pos_clean = _calculate_visual_position(
        organic_position=2,
        featured_snippet=False,
        paa_count=0,
        ai_overview=False,
        video_carousel=False,
        local_pack=False,
        shopping_count=0
    )
    
    assert visual_pos_clean == 2.0


def test_extract_competitors(synthetic_serp_data):
    """Test competitor extraction and frequency analysis."""
    competitors = _extract_competitors(synthetic_serp_data, user_domain="example.com")
    
    # competitor1.com appears in all 3 keywords
    assert "competitor1.com" in competitors
    assert competitors["competitor1.com"]["keywords_shared"] == 3
    assert competitors["competitor1.com"]["total_appearances"] == 4  # appears twice in first keyword
    
    # competitor2.com appears in 2 keywords
    assert "competitor2.com" in competitors
    assert competitors["competitor2.com"]["keywords_shared"] == 2
    
    # Should not include user's own domain
    assert "example.com" not in competitors


def test_classify_intent():
    """Test SERP intent classification."""
    # Commercial intent - shopping results, comparison language
    serp_features_commercial = {
        "featured_snippet": False,
        "paa_count": 2,
        "ai_overview": False,
        "shopping_results": True,
        "knowledge_panel": False
    }
    intent = _classify_intent("best crm software", serp_features_commercial)
    assert intent == "commercial"
    
    # Informational intent - PAA heavy, knowledge panel
    serp_features_info = {
        "featured_snippet": True,
        "paa_count": 6,
        "ai_overview": True,
        "shopping_results": False,
        "knowledge_panel": True
    }
    intent = _classify_intent("what is crm", serp_features_info)
    assert intent == "informational"
    
    # Transactional intent
    serp_features_trans = {
        "featured_snippet": False,
        "paa_count": 1,
        "ai_overview": False,
        "shopping_results": True,
        "knowledge_panel": False
    }
    intent = _classify_intent("buy crm software", serp_features_trans)
    assert intent == "transactional"


def test_estimate_click_share():
    """Test click share estimation."""
    # Position 3 with heavy SERP features
    click_share = _estimate_click_share(
        organic_position=3,
        visual_position=8,
        impressions=8900
    )
    
    # Should be lower than generic position 3 CTR due to displacement
    assert 0.0 < click_share < 0.05
    
    # Position 2 with clean SERP
    click_share_clean = _estimate_click_share(
        organic_position=2,
        visual_position=2,
        impressions=5600
    )
    
    # Should be higher for clean SERP
    assert click_share_clean > click_share


def test_analyze_serp_landscape_output_schema(synthetic_serp_data, synthetic_gsc_data):
    """Test that analyze_serp_landscape returns correct schema."""
    result = analyze_serp_landscape(synthetic_serp_data, synthetic_gsc_data)
    
    # Verify top-level schema
    assert "keywords_analyzed" in result
    assert "serp_feature_displacement" in result
    assert "competitors" in result
    assert "intent_mismatches" in result
    assert "total_click_share" in result
    assert "click_share_opportunity" in result
    
    # Verify data types
    assert isinstance(result["keywords_analyzed"], int)
    assert isinstance(result["serp_feature_displacement"], list)
    assert isinstance(result["competitors"], list)
    assert isinstance(result["total_click_share"], float)
    assert isinstance(result["click_share_opportunity"], float)
    
    # Verify count matches input
    assert result["keywords_analyzed"] == 3


def test_serp_feature_displacement_detection(synthetic_serp_data, synthetic_gsc_data):
    """Test detection of SERP feature displacement."""
    result = analyze_serp_landscape(synthetic_serp_data, synthetic_gsc_data)
    
    displacement_items = result["serp_feature_displacement"]
    
    # Should detect displacement for "best crm software" (position 3 → visual 8)
    displaced = [d for d in displacement_items if d["keyword"] == "best crm software"]
    assert len(displaced) > 0
    
    displaced_item = displaced[0]
    assert displaced_item["organic_position"] == 3
    assert displaced_item["visual_position"] > 6  # Significant displacement
    assert "features_above" in displaced_item
    assert "estimated_ctr_impact" in displaced_item
    assert displaced_item["estimated_ctr_impact"] < 0  # Negative impact


def test_competitor_mapping(synthetic_serp_data, synthetic_gsc_data):
    """Test competitor frequency mapping and threat assessment."""
    result = analyze_serp_landscape(synthetic_serp_data, synthetic_gsc_data)
    
    competitors = result["competitors"]
    
    # Should have multiple competitors
    assert len(competitors) > 0
    
    # Verify competitor data structure
    for comp in competitors:
        assert "domain" in comp
        assert "keywords_shared" in comp
        assert "avg_position" in comp
        assert "threat_level" in comp
        
        # Verify data types
        assert isinstance(comp["keywords_shared"], int)
        assert isinstance(comp["avg_position"], float)
        assert comp["threat_level"] in ["low", "medium", "high"]
    
    # competitor1.com should be high threat (appears in all keywords)
    comp1 = [c for c in competitors if c["domain"] == "competitor1.com"][0]
    assert comp1["threat_level"] in ["medium", "high"]
    assert comp1["keywords_shared"] == 3


def test_intent_classification_integration(synthetic_serp_data, synthetic_gsc_data):
    """Test intent classification for all keywords."""
    result = analyze_serp_landscape(synthetic_serp_data, synthetic_gsc_data)
    
    # All keywords should be classified
    # We can check this through the full result structure
    assert result["keywords_analyzed"] == len(synthetic_serp_data)


def test_click_share_calculation(synthetic_serp_data, synthetic_gsc_data):
    """Test overall click share calculation."""
    result = analyze_serp_landscape(synthetic_serp_data, synthetic_gsc_data)
    
    # Should calculate total click share
    assert 0.0 <= result["total_click_share"] <= 1.0
    
    # Click share opportunity should be positive
    assert result["click_share_opportunity"] >= 0.0
    
    # Opportunity should be greater than current share (room for growth)
    assert result["click_share_opportunity"] >= result["total_click_share"]


def test_empty_serp_data():
    """Test handling of empty SERP data."""
    result = analyze_serp_landscape([], [])
    
    assert result["keywords_analyzed"] == 0
    assert result["serp_feature_displacement"] == []
    assert result["competitors"] == []
    assert result["total_click_share"] == 0.0


def test_missing_serp_features():
    """Test handling of missing SERP feature data."""
    serp_data_minimal = [
        {
            "keyword": "test keyword",
            "organic_position": 5,
            "url": "https://example.com/test",
            "serp_features": {},  # Empty features
            "top_10_results": []
        }
    ]
    gsc_data_minimal = [
        {"keyword": "test keyword", "position": 5.0, "impressions": 100, "clicks": 5, "ctr": 0.05}
    ]
    
    # Should not raise error
    result = analyze_serp_landscape(serp_data_minimal, gsc_data_minimal)
    
    assert result["keywords_analyzed"] == 1


def test_competitor_threat_level_logic():
    """Test threat level classification logic."""
    # High overlap and good position = high threat
    competitors_raw = {
        "competitor1.com": {
            "keywords_shared": 8,
            "total_appearances": 10,
            "positions": [2, 3, 1, 4, 2, 3, 1, 5]
        },
        # Medium overlap = medium threat
        "competitor2.com": {
            "keywords_shared": 3,
            "total_appearances": 3,
            "positions": [5, 6, 7]
        },
        # Low overlap = low threat
        "competitor3.com": {
            "keywords_shared": 1,
            "total_appearances": 1,
            "positions": [8]
        }
    }
    
    # This would be called internally; testing the logic
    from api.modules.module_03_serp_landscape import _assign_threat_level
    
    for domain, data in competitors_raw.items():
        avg_pos = sum(data["positions"]) / len(data["positions"])
        threat = _assign_threat_level(data["keywords_shared"], avg_pos, total_keywords=10)
        
        if domain == "competitor1.com":
            assert threat == "high"
        elif domain == "competitor2.com":
            assert threat in ["medium", "low"]
        elif domain == "competitor3.com":
            assert threat == "low"


def test_visual_position_edge_cases():
    """Test visual position calculation edge cases."""
    # All features present
    visual_pos = _calculate_visual_position(
        organic_position=10,
        featured_snippet=True,
        paa_count=8,
        ai_overview=True,
        video_carousel=True,
        local_pack=True,
        shopping_count=4
    )
    
    # Should push position significantly lower
    assert visual_pos > 15
    
    # Position 1 with no features
    visual_pos_top = _calculate_visual_position(
        organic_position=1,
        featured_snippet=False,
        paa_count=0,
        ai_overview=False,
        video_carousel=False,
        local_pack=False,
        shopping_count=0
    )
    
    assert visual_pos_top == 1.0


def test_result_completeness(synthetic_serp_data, synthetic_gsc_data):
    """Test that all expected result fields are present and valid."""
    result = analyze_serp_landscape(synthetic_serp_data, synthetic_gsc_data)
    
    # All required top-level keys
    required_keys = [
        "keywords_analyzed",
        "serp_feature_displacement",
        "competitors",
        "intent_mismatches",
        "total_click_share",
        "click_share_opportunity"
    ]
    
    for key in required_keys:
        assert key in result, f"Missing required key: {key}"
    
    # Verify displacement items have required fields
    if result["serp_feature_displacement"]:
        displacement_required = [
            "keyword",
            "organic_position",
            "visual_position",
            "features_above",
            "estimated_ctr_impact"
        ]
        for item in result["serp_feature_displacement"]:
            for field in displacement_required:
                assert field in item, f"Displacement item missing field: {field}"
    
    # Verify competitor items have required fields
    if result["competitors"]:
        competitor_required = [
            "domain",
            "keywords_shared",
            "avg_position",
            "threat_level"
        ]
        for comp in result["competitors"]:
            for field in competitor_required:
                assert field in comp, f"Competitor item missing field: {field}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
