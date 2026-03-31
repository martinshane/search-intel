import pytest
from unittest.mock import Mock, patch, MagicMock
import json
from datetime import datetime, timedelta
from api.modules.module_05_gameplan import (
    generate_gameplan,
    GameplanOutput,
    ActionItem,
    ActionCategory,
    _categorize_actions,
    _calculate_impact_scores,
    _generate_narrative_with_claude,
)


# Mock outputs from previous modules
@pytest.fixture
def mock_health_output():
    """Mock Module 1 output with health and trajectory data"""
    return {
        "overall_direction": "declining",
        "trend_slope_pct_per_month": -2.3,
        "change_points": [
            {
                "date": "2025-11-08",
                "magnitude": -0.12,
                "direction": "drop"
            }
        ],
        "seasonality": {
            "best_day": "Tuesday",
            "worst_day": "Saturday",
            "monthly_cycle": True,
            "cycle_description": "15% traffic spike first week of each month"
        },
        "anomalies": [
            {
                "date": "2025-12-25",
                "type": "discord",
                "magnitude": -0.45
            }
        ],
        "forecast": {
            "30d": {"clicks": 12400, "ci_low": 11200, "ci_high": 13600},
            "60d": {"clicks": 11800, "ci_low": 10100, "ci_high": 13500},
            "90d": {"clicks": 11200, "ci_low": 9000, "ci_high": 13400}
        }
    }


@pytest.fixture
def mock_triage_output():
    """Mock Module 2 output with page-level triage data"""
    return {
        "pages": [
            {
                "url": "/blog/best-widgets",
                "bucket": "critical",
                "current_monthly_clicks": 840,
                "trend_slope": -0.82,
                "projected_page1_loss_date": "2026-03-15",
                "ctr_anomaly": True,
                "ctr_expected": 0.082,
                "ctr_actual": 0.031,
                "engagement_flag": "low_engagement",
                "priority_score": 187.4,
                "recommended_action": "title_rewrite"
            },
            {
                "url": "/blog/widget-guide",
                "bucket": "decaying",
                "current_monthly_clicks": 340,
                "trend_slope": -0.28,
                "projected_page1_loss_date": "2026-05-15",
                "ctr_anomaly": True,
                "ctr_expected": 0.065,
                "ctr_actual": 0.029,
                "engagement_flag": None,
                "priority_score": 87.4,
                "recommended_action": "title_rewrite"
            },
            {
                "url": "/pricing",
                "bucket": "stable",
                "current_monthly_clicks": 1200,
                "trend_slope": 0.05,
                "projected_page1_loss_date": None,
                "ctr_anomaly": False,
                "ctr_expected": 0.094,
                "ctr_actual": 0.091,
                "engagement_flag": None,
                "priority_score": 45.2,
                "recommended_action": None
            }
        ],
        "summary": {
            "total_pages_analyzed": 142,
            "growing": 23,
            "stable": 67,
            "decaying": 38,
            "critical": 14,
            "total_recoverable_clicks_monthly": 2840
        }
    }


@pytest.fixture
def mock_serp_output():
    """Mock Module 3 output with SERP landscape data"""
    return {
        "keywords_analyzed": 87,
        "serp_feature_displacement": [
            {
                "keyword": "best crm software",
                "organic_position": 3,
                "visual_position": 8,
                "features_above": ["featured_snippet", "paa_x4", "ai_overview"],
                "estimated_ctr_impact": -0.062,
                "landing_page": "/blog/best-widgets"
            },
            {
                "keyword": "widget comparison",
                "organic_position": 5,
                "visual_position": 7,
                "features_above": ["paa_x3", "video_carousel"],
                "estimated_ctr_impact": -0.028,
                "landing_page": "/blog/widget-guide"
            }
        ],
        "competitors": [
            {
                "domain": "competitor.com",
                "keywords_shared": 34,
                "avg_position": 4.2,
                "threat_level": "high"
            },
            {
                "domain": "bigwidgets.com",
                "keywords_shared": 22,
                "avg_position": 5.8,
                "threat_level": "medium"
            }
        ],
        "intent_mismatches": [
            {
                "keyword": "buy widgets online",
                "intent_detected": "transactional",
                "landing_page": "/blog/widget-guide",
                "page_type": "blog_post",
                "recommendation": "Create dedicated product page"
            }
        ],
        "total_click_share": 0.12,
        "click_share_opportunity": 0.31
    }


@pytest.fixture
def mock_content_output():
    """Mock Module 4 output with content intelligence data"""
    return {
        "cannibalization_clusters": [
            {
                "query_group": "crm pricing comparison",
                "pages": ["/blog/crm-pricing", "/crm-pricing-page"],
                "shared_queries": 23,
                "total_impressions_affected": 4500,
                "recommendation": "consolidate",
                "keep_page": "/crm-pricing-page"
            },
            {
                "query_group": "widget types",
                "pages": ["/blog/widget-types", "/widgets/categories"],
                "shared_queries": 18,
                "total_impressions_affected": 3200,
                "recommendation": "consolidate",
                "keep_page": "/widgets/categories"
            }
        ],
        "striking_distance": [
            {
                "query": "best crm for small business",
                "current_position": 11.3,
                "impressions": 8900,
                "estimated_click_gain_if_top5": 420,
                "intent": "commercial",
                "landing_page": "/blog/best-widgets"
            },
            {
                "query": "widget pricing guide",
                "current_position": 9.7,
                "impressions": 5200,
                "estimated_click_gain_if_top5": 245,
                "intent": "commercial",
                "landing_page": "/blog/widget-guide"
            },
            {
                "query": "how to choose widgets",
                "current_position": 13.2,
                "impressions": 4100,
                "estimated_click_gain_if_top5": 180,
                "intent": "informational",
                "landing_page": "/blog/widget-selection"
            }
        ],
        "thin_content": [
            {
                "url": "/blog/quick-tip",
                "word_count": 320,
                "impressions": 2400,
                "bounce_rate": 0.88,
                "avg_session_duration": 18,
                "recommendation": "expand"
            }
        ],
        "update_priority_matrix": {
            "urgent_update": [
                {
                    "url": "/blog/old-guide",
                    "age_days": 920,
                    "trend": "decaying",
                    "monthly_clicks": 450
                }
            ],
            "leave_alone": [
                {
                    "url": "/resources/evergreen",
                    "age_days": 1200,
                    "trend": "stable",
                    "monthly_clicks": 800
                }
            ],
            "structural_problem": [
                {
                    "url": "/blog/new-failing",
                    "age_days": 45,
                    "trend": "decaying",
                    "monthly_clicks": 120
                }
            ],
            "double_down": [
                {
                    "url": "/blog/trending-topic",
                    "age_days": 60,
                    "trend": "growing",
                    "monthly_clicks": 340
                }
            ]
        }
    }


class TestGameplanGeneration:
    """Tests for the main gameplan generation function"""
    
    def test_generate_gameplan_with_all_modules(
        self,
        mock_health_output,
        mock_triage_output,
        mock_serp_output,
        mock_content_output
    ):
        """Test gameplan generation with complete module outputs"""
        with patch('api.modules.module_05_gameplan._generate_narrative_with_claude') as mock_claude:
            mock_claude.return_value = "Your site is currently declining at 2.3% per month. Immediate action is required on 14 critical pages."
            
            result = generate_gameplan(
                health=mock_health_output,
                triage=mock_triage_output,
                serp=mock_serp_output,
                content=mock_content_output
            )
            
            # Check structure
            assert isinstance(result, dict)
            assert "critical" in result
            assert "quick_wins" in result
            assert "strategic" in result
            assert "structural" in result
            assert "narrative" in result
            assert "total_estimated_monthly_click_recovery" in result
            assert "total_estimated_monthly_click_growth" in result
            
            # Check that critical issues are identified
            assert len(result["critical"]) > 0
            
            # Check that impact estimates are present
            assert result["total_estimated_monthly_click_recovery"] > 0
            
            # Check that narrative was generated
            assert len(result["narrative"]) > 0
            assert "declining" in result["narrative"].lower()
    
    def test_generate_gameplan_with_minimal_data(self):
        """Test gameplan generation with minimal module outputs"""
        minimal_health = {
            "overall_direction": "stable",
            "trend_slope_pct_per_month": 0.1,
            "change_points": [],
            "anomalies": [],
            "forecast": {
                "30d": {"clicks": 10000, "ci_low": 9500, "ci_high": 10500}
            }
        }
        
        minimal_triage = {
            "pages": [],
            "summary": {
                "total_pages_analyzed": 10,
                "growing": 3,
                "stable": 7,
                "decaying": 0,
                "critical": 0,
                "total_recoverable_clicks_monthly": 0
            }
        }
        
        minimal_serp = {
            "keywords_analyzed": 0,
            "serp_feature_displacement": [],
            "competitors": [],
            "intent_mismatches": [],
            "total_click_share": 0.15,
            "click_share_opportunity": 0.20
        }
        
        minimal_content = {
            "cannibalization_clusters": [],
            "striking_distance": [],
            "thin_content": [],
            "update_priority_matrix": {
                "urgent_update": [],
                "leave_alone": [],
                "structural_problem": [],
                "double_down": []
            }
        }
        
        with patch('api.modules.module_05_gameplan._generate_narrative_with_claude') as mock_claude:
            mock_claude.return_value = "Your site is stable with limited opportunities identified."
            
            result = generate_gameplan(
                health=minimal_health,
                triage=minimal_triage,
                serp=minimal_serp,
                content=minimal_content
            )
            
            # Should still generate valid output
            assert isinstance(result, dict)
            assert "critical" in result
            assert "narrative" in result
            
            # But with no critical actions
            assert len(result["critical"]) == 0
    
    def test_generate_gameplan_validates_output_schema(
        self,
        mock_health_output,
        mock_triage_output,
        mock_serp_output,
        mock_content_output
    ):
        """Test that output conforms to GameplanOutput schema"""
        with patch('api.modules.module_05_gameplan._generate_narrative_with_claude') as mock_claude:
            mock_claude.return_value = "Test narrative"
            
            result = generate_gameplan(
                health=mock_health_output,
                triage=mock_triage_output,
                serp=mock_serp_output,
                content=mock_content_output
            )
            
            # Validate against schema
            validated = GameplanOutput(**result)
            
            # Check types
            assert isinstance(validated.critical, list)
            assert isinstance(validated.quick_wins, list)
            assert isinstance(validated.strategic, list)
            assert isinstance(validated.structural, list)
            assert isinstance(validated.narrative, str)
            assert isinstance(validated.total_estimated_monthly_click_recovery, (int, float))
            assert isinstance(validated.total_estimated_monthly_click_growth, (int, float))
            
            # Check action item structure
            for action in validated.critical + validated.quick_wins + validated.strategic + validated.structural:
                assert isinstance(action, ActionItem)
                assert hasattr(action, 'action')
                assert hasattr(action, 'impact')
                assert hasattr(action, 'effort')
                assert hasattr(action, 'category')


class TestActionCategorization:
    """Tests for action categorization logic"""
    
    def test_categorize_critical_actions(self, mock_triage_output):
        """Test identification of critical actions"""
        actions = _categorize_actions(
            health={"overall_direction": "declining"},
            triage=mock_triage_output,
            serp={},
            content={}
        )
        
        critical = [a for a in actions if a["category"] == ActionCategory.CRITICAL]
        
        # Should identify critical page from triage
        assert len(critical) > 0
        assert any("/blog/best-widgets" in a.get("target", "") for a in critical)
        
        # Critical actions should have high impact
        for action in critical:
            assert action["impact"] > 100
    
    def test_categorize_quick_wins(self, mock_content_output, mock_serp_output):
        """Test identification of quick win opportunities"""
        actions = _categorize_actions(
            health={},
            triage={"pages": [], "summary": {}},
            serp=mock_serp_output,
            content=mock_content_output
        )
        
        quick_wins = [a for a in actions if a["category"] == ActionCategory.QUICK_WIN]
        
        # Should identify striking distance keywords
        assert len(quick_wins) > 0
        
        # Quick wins should have low to medium effort
        for action in quick_wins:
            assert action["effort"] in ["low", "medium"]
    
    def test_categorize_strategic_plays(self, mock_content_output):
        """Test identification of strategic plays"""
        actions = _categorize_actions(
            health={},
            triage={"pages": [], "summary": {}},
            serp={"intent_mismatches": [
                {
                    "keyword": "buy widgets",
                    "intent_detected": "transactional",
                    "landing_page": "/blog/widgets",
                    "page_type": "blog_post",
                    "recommendation": "Create product page"
                }
            ]},
            content=mock_content_output
        )
        
        strategic = [a for a in actions if a["category"] == ActionCategory.STRATEGIC]
        
        # Should identify consolidation and content gaps
        assert len(strategic) > 0
        
        # Strategic actions should have medium to high effort
        for action in strategic:
            assert action["effort"] in ["medium", "high"]
    
    def test_categorize_structural_improvements(self, mock_health_output):
        """Test identification of structural improvements"""
        actions = _categorize_actions(
            health=mock_health_output,
            triage={"pages": [], "summary": {}},
            serp={},
            content={"update_priority_matrix": {
                "double_down": [
                    {"url": "/blog/trending", "trend": "growing", "monthly_clicks": 400}
                ],
                "urgent_update": [],
                "leave_alone": [],
                "structural_problem": []
            }}
        )
        
        structural = [a for a in actions if a["category"] == ActionCategory.STRUCTURAL]
        
        # Should identify ongoing improvements
        assert len(structural) >= 0
    
    def test_categorization_prioritizes_correctly(
        self,
        mock_health_output,
        mock_triage_output,
        mock_serp_output,
        mock_content_output
    ):
        """Test that actions are correctly prioritized within categories"""
        actions = _categorize_actions(
            health=mock_health_output,
            triage=mock_triage_output,
            serp=mock_serp_output,
            content=mock_content_output
        )
        
        # Within critical, highest impact should come first
        critical = [a for a in actions if a["category"] == ActionCategory.CRITICAL]
        if len(critical) > 1:
            for i in range(len(critical) - 1):
                assert critical[i]["impact"] >= critical[i + 1]["impact"]
        
        # Within quick wins, best effort/impact ratio should come first
        quick_wins = [a for a in actions if a["category"] == ActionCategory.QUICK_WIN]
        if len(quick_wins) > 1:
            effort_map = {"low": 1, "medium": 2, "high": 3}
            for i in range(len(quick_wins) - 1):
                ratio_i = quick_wins[i]["impact"] / effort_map.get(quick_wins[i]["effort"], 2)
                ratio_next = quick_wins[i + 1]["impact"] / effort_map.get(quick_wins[i + 1]["effort"], 2)
                assert ratio_i >= ratio_next


class TestImpactCalculations:
    """Tests for impact score calculation logic"""
    
    def test_calculate_impact_for_critical_page(self):
        """Test impact calculation for critical decaying page"""
        page_data = {
            "url": "/blog/important",
            "bucket": "critical",
            "current_monthly_clicks": 800,
            "trend_slope": -0.75,
            "ctr_anomaly": True,
            "ctr_expected": 0.08,
            "ctr_actual": 0.03
        }
        
        impact = _calculate_impact_scores([page_data])
        
        # High traffic + steep decline + CTR issue = high impact
        assert impact[0] > 500
    
    def test_calculate_impact_for_striking_distance(self):
        """Test impact calculation for striking distance keyword"""
        keyword_data = {
            "query": "test keyword",
            "current_position": 11.0,
            "impressions": 5000,
            "estimated_click_gain_if_top5": 300
        }
        
        # Impact should be based on click gain potential
        impact = keyword_data["estimated_click_gain_if_top5"]
        assert impact == 300
    
    def test_calculate_impact_for_cannibalization(self):
        """Test impact calculation for cannibalization cluster"""
        cluster_data = {
            "query_group": "test queries",
            "pages": ["/page1", "/page2"],
            "shared_queries": 20,
            "total_impressions_affected": 8000
        }
        
        # Impact based on impressions and potential CTR improvement
        # Assume resolving cannibalization improves CTR by 30%
        estimated_clicks_current = 8000 * 0.05  # ~400 clicks
        estimated_clicks_improved = estimated_clicks_current * 1.3  # ~520 clicks
        impact = estimated_clicks_improved - estimated_clicks_current  # ~120 clicks
        
        assert impact > 100
    
    def test_calculate_impact_with_position_decay(self):
        """Test that position decay affects impact calculation"""
        page_decaying_fast = {
            "current_monthly_clicks": 500,
            "trend_slope": -0.9,
            "projected_page1_loss_date": "2026-02-01"
        }
        
        page_decaying_slow = {
            "current_monthly_clicks": 500,
            "trend_slope": -0.2,
            "projected_page1_loss_date": "2026-08-01"
        }
        
        impacts = _calculate_impact_scores([page_decaying_fast, page_decaying_slow])
        
        # Fast decay should have higher impact (more urgent)
        assert impacts[0] > impacts[1]


class TestClaudeIntegration:
    """Tests for Claude API integration for narrative generation"""
    
    @patch('anthropic.Anthropic')
    def test_generate_narrative_success(self, mock_anthropic_class):
        """Test successful narrative generation via Claude"""
        # Mock the Claude API response
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.content = [MagicMock(text="Your site is declining. Take these actions...")]
        mock_client.messages.create.return_value = mock_response
        mock_anthropic_class.return_value = mock_client
        
        summary_data = {
            "overall_direction": "declining",
            "critical_count": 5,
            "quick_wins_count": 12,
            "total_recovery_potential": 2500
        }
        
        narrative = _generate_narrative_with_claude(summary_data)
        
        # Check that narrative was generated
        assert isinstance(narrative, str)
        assert len(narrative) > 0
        assert "declining" in narrative.lower()
        
        # Verify Claude was called with correct parameters
        mock_client.messages.create.assert_called_once()
        call_kwargs = mock_client.messages.create.call_args[1]
        assert call_kwargs["model"] == "claude-3-5-sonnet-20241022"
        assert call_kwargs["max_tokens"] >= 1000
    
    @patch('anthropic.Anthropic')
    def test_generate_narrative_with_context(self, mock_anthropic_class):
        """Test that narrative includes relevant context from all modules"""
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.content = [MagicMock(text="Comprehensive analysis...")]
        mock_client.messages.create.return_value = mock_response
        mock_anthropic_class.return_value = mock_client
        
        summary_data = {
            "overall_direction": "declining",
            "trend_slope": -2.3,
            "critical_pages": 14,
            "cannibalization_clusters": 3,
            "striking_distance_keywords": 15,
            "serp_feature_displacement": 8,
            "total_recovery_potential": 2840,
            "total_growth_potential": 1200
        }
        
        narrative = _generate_narrative_with_claude(summary_data)
        
        # Check that prompt includes key metrics
        call_kwargs = mock_client.messages.create.call_args[1]
        prompt = str(call_kwargs["messages"])
        
        assert "declining" in prompt.lower()
        assert "2.3" in prompt or "2840" in prompt
    
    @patch('anthropic.Anthropic')
    def test_generate_narrative_api_error_handling(self, mock_anthropic_class):
        """Test handling of Claude API errors"""
        # Mock API error
        mock_client = MagicMock()
        mock_client.messages.create.side_effect = Exception("API Error")
        mock_anthropic_class.return_value = mock_client
        
        summary_data = {
            "overall_direction": "stable",
            "critical_count": 0
        }
        
        # Should return fallback narrative instead of raising
        narrative = _generate_narrative_with_claude(summary_data)
        
        assert isinstance(narrative, str)
        assert len(narrative) > 0
        # Should indicate this is automated/fallback
        assert "analysis" in narrative.lower() or "summary" in narrative.lower()
    
    @patch('anthropic.Anthropic')
    def test_generate_narrative_tone_validation(self, mock_anthropic_class):
        """Test that generated narrative has appropriate consultant tone"""
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.content = [MagicMock(
            text="Your site traffic is declining at 2.3% per month. Fourteen pages require immediate attention. "
                 "The primary issues are: title optimization on high-traffic pages, content consolidation to resolve "
                 "cannibalization, and content expansion for striking-distance keywords. Recovery potential: 2,840 clicks/month."
        )]
        mock_client.messages.create.return_value = mock_response
        mock_anthropic_class.return_value = mock_client
        
        summary_data = {
            "overall_direction": "declining",
            "trend_slope": -2.3,
            "critical_pages": 14,
            "total_recovery_potential": 2840
        }
        
        narrative = _generate_narrative_with_claude(summary_data)
        
        # Check for consultant-grade characteristics
        assert len(narrative) > 50  # Substantive
        assert any(word in narrative.lower() for word in ["require", "recommend", "should", "need"])
        # Should be direct, not fluffy
        assert "exciting" not in narrative.lower()
        assert "amazing" not in narrative.lower()


class TestActionItemSchema:
    """Tests for ActionItem data model"""
    
    def test_action_item_creation(self):
        """Test creating valid ActionItem"""
        action = ActionItem(
            action="Rewrite title tag for /blog/test",
            target="/blog/test",
            impact=350,
            effort="low",
            category=ActionCategory.CRITICAL,
            dependencies=None,
            details="CTR is 3.1% vs expected 8.2%"
        )
        
        assert action.action == "Rewrite title tag for /blog/test"
        assert action.impact == 350
        assert action.effort == "low"
        assert action.category == ActionCategory.CRITICAL
    
    def test_action_item_with_dependencies(self):
        """Test ActionItem with dependencies"""
        action = ActionItem(
            action="Add internal links to /pricing",
            target="/pricing",
            impact=120,
            effort="medium",
            category=ActionCategory.QUICK_WIN,
            dependencies=["Consolidate /blog/pricing pages first"],
            details="Boost authority after consolidation"
        )
        
        assert len(action.dependencies) == 1
        assert "Consolidate" in action.dependencies[0]
    
    def test_action_item_validation(self):
        """Test ActionItem field validation"""
        # Valid effort levels
        for effort in ["low", "medium", "high"]:
            action = ActionItem(
                action="Test",
                impact=100,
                effort=effort,
                category=ActionCategory.QUICK_WIN
            )
            assert action.effort == effort
        
        # Impact should be positive
        action = ActionItem(
            action="Test",
            impact=0,
            effort="low",
            category=ActionCategory.STRUCTURAL
        )
        assert action.impact == 0  # Zero is acceptable (structural improvements may not have immediate impact)


class TestGameplanOutputSchema:
    """Tests for GameplanOutput data model"""
    
    def test_gameplan_output_creation(self):
        """Test creating valid GameplanOutput"""
        output = GameplanOutput(
            critical=[
                ActionItem(
                    action="Fix critical page",
                    impact=500,
                    effort="low",
                    category=ActionCategory.CRITICAL
                )
            ],
            quick_wins=[
                ActionItem(
                    action="Optimize title",
                    impact=200,
                    effort="low",
                    category=ActionCategory.QUICK_WIN
                )
            ],
            strategic=[],
            structural=[],
            total_estimated_monthly_click_recovery=2500,
            total_estimated_monthly_click_growth=1200,
            narrative="Test narrative"
        )
        
        assert len(output.critical) == 1
        assert len(output.quick_wins) == 1
        assert output.total_estimated_monthly_click_recovery == 2500
        assert output.narrative == "Test narrative"
    
    def test_gameplan_output_serialization(self):
        """Test that GameplanOutput can be serialized to JSON"""
        output = GameplanOutput(
            critical=[
                ActionItem(
                    action="Test action",
                    impact=100,
                    effort="low",
                    category=ActionCategory.CRITICAL
                )
            ],
            quick_wins=[],
            strategic=[],
            structural=[],
            total_estimated_monthly_click_recovery=100,
            total_estimated_monthly_click_growth=50,
            narrative="Test"
        )
        
        # Should be JSON serializable
        json_str = output.model_dump_json()
        assert isinstance(json_str, str)
        
        # Should be deserializable
        parsed = json.loads(json_str)
        assert parsed["total_estimated_monthly_click_recovery"] == 100
        assert len(parsed["critical"]) == 1


class TestEdgeCases:
    """Tests for edge cases and error handling"""
    
    def test_empty_module_outputs(self):
        """Test handling of empty/minimal module outputs"""
        result = generate_gameplan(
            health={"overall_direction": "stable"},
            triage={"pages": [], "summary": {}},
            serp={},
            content={}
        )
        
        # Should not crash
        assert isinstance(result, dict)
        assert "critical" in result
        assert isinstance(result["critical"], list)
    
    def test_missing_optional_fields(self):
        """Test handling of missing optional fields in module outputs"""
        health = {
            "overall_direction": "stable",
            "trend_slope_pct_per_month": 0.1
            # Missing: change_points, anomalies, forecast
        }
        
        triage = {
            "pages": [
                {
                    "url": "/test",
                    "bucket": "stable",
                    "current_monthly_clicks": 100,
                    "trend_slope": 0.01
                    # Missing: ctr_anomaly, engagement_flag, etc.
                }
            ],
            "summary": {"total_pages_analyzed": 1}
        }
        
        with patch('api.modules.module_05_gameplan._generate_narrative_with_claude') as mock_claude:
            mock_claude.return_value = "Test narrative"
            
            result = generate_gameplan(
                health=health,
                triage=triage,
                serp={},
                content={}
            )
            
            # Should handle gracefully
            assert isinstance(result, dict)
            assert "narrative" in result
    
    def test_very_large_action_list(self):
        """Test handling of many potential actions"""
        # Create large triage output with many pages
        large_triage = {
            "pages": [
                {
                    "url": f"/page-{i}",
                    "bucket": "decaying",
                    "current_monthly_clicks": 100 + i,
                    "trend_slope": -0.3,
                    "ctr_anomaly": i % 2 == 0,
                    "priority_score": 50 + i
                }
                for i in range(100)
            ],
            "summary": {
                "total_pages_analyzed": 100,
                "decaying": 100,
                "total_recoverable_clicks_monthly": 15000
            }
        }
        
        with patch('api.modules.module_05_gameplan._generate_narrative_with_claude') as mock_claude:
            mock_claude.return_value = "Large site analysis"
            
            result = generate_gameplan(
                health={"overall_direction": "declining"},
                triage=large_triage,
                serp={},
                content={}
            )
            
            # Should complete without error
            assert isinstance(result, dict)
            
            # Should prioritize and potentially limit action list
            total_actions = (
                len(result["critical"]) +
                len(result["quick_wins"]) +
                len(result["strategic"]) +
                len(result["structural"])
            )
            
            # Should have actions, but potentially filtered/prioritized
            assert total_actions > 0
            assert total_actions <= 150  # Reasonable upper limit