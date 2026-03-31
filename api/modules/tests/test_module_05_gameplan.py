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
            },
            {
                "url": "/blog/widget-basics",
                "bucket": "critical",
                "current_monthly_clicks": 520,
                "trend_slope": -0.95,
                "projected_page1_loss_date": "2026-02-28",
                "ctr_anomaly": False,
                "ctr_expected": 0.072,
                "ctr_actual": 0.068,
                "engagement_flag": "low_engagement",
                "priority_score": 156.8,
                "recommended_action": "content_refresh"
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
    """Mock Module 3 output with SERP landscape analysis"""
    return {
        "keywords_analyzed": 87,
        "serp_feature_displacement": [
            {
                "keyword": "best crm software",
                "organic_position": 3,
                "visual_position": 8,
                "features_above": ["featured_snippet", "paa_x4", "ai_overview"],
                "estimated_ctr_impact": -0.062
            },
            {
                "keyword": "widget comparison",
                "organic_position": 5,
                "visual_position": 7,
                "features_above": ["paa_x3", "video_carousel"],
                "estimated_ctr_impact": -0.034
            }
        ],
        "competitors": [
            {
                "domain": "competitor.com",
                "keywords_shared": 34,
                "avg_position": 4.2,
                "threat_level": "high"
            }
        ],
        "intent_mismatches": [
            {
                "keyword": "buy widgets",
                "current_intent": "transactional",
                "landing_page": "/blog/widget-guide",
                "landing_page_type": "informational",
                "recommended_landing": "/pricing"
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
                "query_group": "widget pricing comparison",
                "pages": ["/blog/widget-pricing", "/pricing"],
                "shared_queries": 23,
                "total_impressions_affected": 4500,
                "recommendation": "consolidate",
                "keep_page": "/pricing"
            }
        ],
        "striking_distance": [
            {
                "query": "best widgets for small business",
                "current_position": 11.3,
                "impressions": 8900,
                "estimated_click_gain_if_top5": 420,
                "intent": "commercial",
                "landing_page": "/blog/best-widgets"
            },
            {
                "query": "widget comparison tool",
                "current_position": 9.8,
                "impressions": 5600,
                "estimated_click_gain_if_top5": 280,
                "intent": "commercial",
                "landing_page": "/tools/compare"
            }
        ],
        "thin_content": [
            {
                "url": "/blog/widget-basics",
                "word_count": 420,
                "bounce_rate": 0.87,
                "avg_session_duration": 18,
                "impressions": 3200,
                "recommendation": "expand"
            }
        ],
        "update_priority_matrix": {
            "urgent_update": [
                {
                    "url": "/blog/widget-basics",
                    "age_days": 780,
                    "status": "decaying"
                }
            ],
            "leave_alone": [
                {
                    "url": "/blog/evergreen-guide",
                    "age_days": 920,
                    "status": "stable"
                }
            ],
            "structural_problem": [],
            "double_down": [
                {
                    "url": "/blog/new-widget-trends",
                    "age_days": 45,
                    "status": "growing"
                }
            ]
        }
    }


class TestGameplanGeneration:
    """Test the main gameplan generation function"""

    @patch('api.modules.module_05_gameplan._generate_narrative_with_claude')
    def test_generate_gameplan_basic(
        self,
        mock_claude,
        mock_health_output,
        mock_triage_output,
        mock_serp_output,
        mock_content_output
    ):
        """Test basic gameplan generation with all module inputs"""
        mock_claude.return_value = "Your site is currently declining at 2.3% per month..."

        result = generate_gameplan(
            health=mock_health_output,
            triage=mock_triage_output,
            serp=mock_serp_output,
            content=mock_content_output
        )

        # Verify output structure
        assert isinstance(result, dict)
        assert "critical" in result
        assert "quick_wins" in result
        assert "strategic" in result
        assert "structural" in result
        assert "total_estimated_monthly_click_recovery" in result
        assert "total_estimated_monthly_click_growth" in result
        assert "narrative" in result

        # Verify narrative was generated
        assert result["narrative"] == "Your site is currently declining at 2.3% per month..."

    @patch('api.modules.module_05_gameplan._generate_narrative_with_claude')
    def test_critical_actions_identified(
        self,
        mock_claude,
        mock_health_output,
        mock_triage_output,
        mock_serp_output,
        mock_content_output
    ):
        """Test that critical issues are properly identified and prioritized"""
        mock_claude.return_value = "Test narrative"

        result = generate_gameplan(
            health=mock_health_output,
            triage=mock_triage_output,
            serp=mock_serp_output,
            content=mock_content_output
        )

        critical_actions = result["critical"]
        
        # Should have critical actions from pages in critical bucket
        assert len(critical_actions) > 0
        
        # Verify critical actions include high-impact pages
        critical_urls = [action["page_url"] for action in critical_actions if "page_url" in action]
        assert "/blog/best-widgets" in critical_urls or any(
            "best-widgets" in action.get("description", "") for action in critical_actions
        )

        # Verify actions have required fields
        for action in critical_actions:
            assert "action" in action or "description" in action
            assert "estimated_impact" in action
            assert "effort" in action
            assert action["effort"] in ["low", "medium", "high"]

    @patch('api.modules.module_05_gameplan._generate_narrative_with_claude')
    def test_quick_wins_from_striking_distance(
        self,
        mock_claude,
        mock_health_output,
        mock_triage_output,
        mock_serp_output,
        mock_content_output
    ):
        """Test that striking distance keywords become quick wins"""
        mock_claude.return_value = "Test narrative"

        result = generate_gameplan(
            health=mock_health_output,
            triage=mock_triage_output,
            serp=mock_serp_output,
            content=mock_content_output
        )

        quick_wins = result["quick_wins"]
        
        # Should have quick wins from striking distance
        assert len(quick_wins) > 0
        
        # Check for striking distance keyword mentions
        quick_win_text = " ".join([
            action.get("description", "") + action.get("action", "")
            for action in quick_wins
        ])
        assert "best widgets for small business" in quick_win_text or "striking distance" in quick_win_text.lower()

    @patch('api.modules.module_05_gameplan._generate_narrative_with_claude')
    def test_strategic_consolidation_actions(
        self,
        mock_claude,
        mock_health_output,
        mock_triage_output,
        mock_serp_output,
        mock_content_output
    ):
        """Test that cannibalization issues become strategic actions"""
        mock_claude.return_value = "Test narrative"

        result = generate_gameplan(
            health=mock_health_output,
            triage=mock_triage_output,
            serp=mock_serp_output,
            content=mock_content_output
        )

        strategic_actions = result["strategic"]
        
        # Should have strategic actions
        assert len(strategic_actions) > 0
        
        # Should include consolidation from cannibalization
        strategic_text = " ".join([
            action.get("description", "") + action.get("action", "")
            for action in strategic_actions
        ])
        assert "consolidate" in strategic_text.lower() or "cannibalization" in strategic_text.lower()

    @patch('api.modules.module_05_gameplan._generate_narrative_with_claude')
    def test_impact_calculations(
        self,
        mock_claude,
        mock_health_output,
        mock_triage_output,
        mock_serp_output,
        mock_content_output
    ):
        """Test that impact values are calculated correctly"""
        mock_claude.return_value = "Test narrative"

        result = generate_gameplan(
            health=mock_health_output,
            triage=mock_triage_output,
            serp=mock_serp_output,
            content=mock_content_output
        )

        total_recovery = result["total_estimated_monthly_click_recovery"]
        total_growth = result["total_estimated_monthly_click_growth"]

        # Should have positive impact values
        assert total_recovery >= 0
        assert total_growth >= 0

        # Recovery should align with triage summary
        assert total_recovery <= mock_triage_output["summary"]["total_recoverable_clicks_monthly"] + 1000

        # Individual action impacts should sum to something reasonable
        all_actions = (
            result["critical"] +
            result["quick_wins"] +
            result["strategic"] +
            result["structural"]
        )
        total_action_impact = sum(
            action.get("estimated_impact", 0) for action in all_actions
        )
        assert total_action_impact > 0


class TestActionCategorization:
    """Test the action categorization logic"""

    def test_categorize_critical_actions(self):
        """Test identification of critical actions"""
        triage_data = {
            "pages": [
                {
                    "url": "/critical-page",
                    "bucket": "critical",
                    "current_monthly_clicks": 500,
                    "ctr_anomaly": True,
                    "recommended_action": "title_rewrite"
                }
            ]
        }
        content_data = {"cannibalization_clusters": [], "striking_distance": []}
        
        actions = _categorize_actions(
            triage=triage_data,
            serp={},
            content=content_data,
            health={}
        )

        assert len(actions["critical"]) > 0
        critical_action = actions["critical"][0]
        assert critical_action["estimated_impact"] == 500 or "/critical-page" in str(critical_action)

    def test_categorize_quick_wins(self):
        """Test identification of quick win opportunities"""
        content_data = {
            "striking_distance": [
                {
                    "query": "test query",
                    "estimated_click_gain_if_top5": 250,
                    "landing_page": "/test-page"
                }
            ],
            "cannibalization_clusters": []
        }
        triage_data = {"pages": []}
        
        actions = _categorize_actions(
            triage=triage_data,
            serp={},
            content=content_data,
            health={}
        )

        assert len(actions["quick_wins"]) > 0
        quick_win = actions["quick_wins"][0]
        assert quick_win["estimated_impact"] == 250 or "test query" in str(quick_win)

    def test_categorize_strategic_actions(self):
        """Test identification of strategic plays"""
        content_data = {
            "cannibalization_clusters": [
                {
                    "query_group": "test queries",
                    "pages": ["/page1", "/page2"],
                    "total_impressions_affected": 4500,
                    "recommendation": "consolidate"
                }
            ],
            "striking_distance": []
        }
        
        actions = _categorize_actions(
            triage={"pages": []},
            serp={},
            content=content_data,
            health={}
        )

        assert len(actions["strategic"]) > 0
        strategic_action = actions["strategic"][0]
        assert "consolidate" in str(strategic_action).lower() or strategic_action.get("effort") in ["medium", "high"]

    def test_empty_inputs(self):
        """Test categorization with empty module outputs"""
        actions = _categorize_actions(
            triage={"pages": []},
            serp={},
            content={"cannibalization_clusters": [], "striking_distance": []},
            health={}
        )

        # Should return structure even with no actions
        assert "critical" in actions
        assert "quick_wins" in actions
        assert "strategic" in actions
        assert "structural" in actions


class TestImpactCalculations:
    """Test impact score calculations"""

    def test_calculate_recovery_impact(self):
        """Test calculation of recoverable clicks"""
        triage_data = {
            "pages": [
                {"bucket": "critical", "current_monthly_clicks": 500},
                {"bucket": "decaying", "current_monthly_clicks": 300},
                {"bucket": "stable", "current_monthly_clicks": 1000}
            ],
            "summary": {"total_recoverable_clicks_monthly": 2000}
        }
        content_data = {"striking_distance": []}

        impact = _calculate_impact_scores(triage=triage_data, content=content_data)

        assert "recovery" in impact
        assert impact["recovery"] > 0
        assert impact["recovery"] <= 2000

    def test_calculate_growth_impact(self):
        """Test calculation of growth opportunities"""
        content_data = {
            "striking_distance": [
                {"estimated_click_gain_if_top5": 400},
                {"estimated_click_gain_if_top5": 250},
                {"estimated_click_gain_if_top5": 180}
            ]
        }
        triage_data = {"pages": [], "summary": {"total_recoverable_clicks_monthly": 0}}

        impact = _calculate_impact_scores(triage=triage_data, content=content_data)

        assert "growth" in impact
        assert impact["growth"] == 830  # 400 + 250 + 180

    def test_zero_impact(self):
        """Test impact calculation with no opportunities"""
        triage_data = {"pages": [], "summary": {"total_recoverable_clicks_monthly": 0}}
        content_data = {"striking_distance": []}

        impact = _calculate_impact_scores(triage=triage_data, content=content_data)

        assert impact["recovery"] == 0
        assert impact["growth"] == 0


class TestClaudeIntegration:
    """Test Claude API integration for narrative generation"""

    @patch('api.modules.module_05_gameplan.anthropic.Anthropic')
    def test_claude_narrative_generation(self, mock_anthropic_class):
        """Test narrative generation via Claude API"""
        # Mock the Claude API response
        mock_client = MagicMock()
        mock_anthropic_class.return_value = mock_client
        
        mock_message = MagicMock()
        mock_message.content = [MagicMock(text="Generated narrative about site health and recommendations.")]
        mock_client.messages.create.return_value = mock_message

        context = {
            "health_summary": "declining at 2.3% per month",
            "critical_count": 5,
            "total_impact": 5000
        }

        narrative = _generate_narrative_with_claude(context)

        # Verify Claude was called
        mock_client.messages.create.assert_called_once()
        call_args = mock_client.messages.create.call_args

        # Verify request structure
        assert call_args[1]["model"].startswith("claude-")
        assert call_args[1]["max_tokens"] > 0
        assert len(call_args[1]["messages"]) > 0
        
        # Verify narrative was returned
        assert isinstance(narrative, str)
        assert len(narrative) > 0

    @patch('api.modules.module_05_gameplan.anthropic.Anthropic')
    def test_claude_error_handling(self, mock_anthropic_class):
        """Test graceful handling of Claude API errors"""
        mock_client = MagicMock()
        mock_anthropic_class.return_value = mock_client
        mock_client.messages.create.side_effect = Exception("API Error")

        context = {"health_summary": "test"}

        # Should not raise exception, return fallback
        narrative = _generate_narrative_with_claude(context)
        
        assert isinstance(narrative, str)
        # Should have some fallback content
        assert len(narrative) > 0

    @patch('api.modules.module_05_gameplan.anthropic.Anthropic')
    def test_claude_prompt_structure(self, mock_anthropic_class):
        """Test that Claude prompt includes necessary context"""
        mock_client = MagicMock()
        mock_anthropic_class.return_value = mock_client
        
        mock_message = MagicMock()
        mock_message.content = [MagicMock(text="Test response")]
        mock_client.messages.create.return_value = mock_message

        context = {
            "health_direction": "declining",
            "trend_slope": -2.3,
            "critical_actions": 5,
            "quick_wins": 8,
            "total_recovery": 2800,
            "total_growth": 5200
        }

        _generate_narrative_with_claude(context)

        # Verify the prompt includes key context elements
        call_args = mock_client.messages.create.call_args
        messages = call_args[1]["messages"]
        prompt_text = " ".join([m["content"] for m in messages])

        assert "declining" in prompt_text or "-2.3" in prompt_text
        assert any(str(val) in prompt_text for val in [5, 8, 2800, 5200])


class TestOutputSchema:
    """Test output schema compliance"""

    @patch('api.modules.module_05_gameplan._generate_narrative_with_claude')
    def test_output_schema_structure(
        self,
        mock_claude,
        mock_health_output,
        mock_triage_output,
        mock_serp_output,
        mock_content_output
    ):
        """Test that output matches expected schema"""
        mock_claude.return_value = "Test narrative"

        result = generate_gameplan(
            health=mock_health_output,
            triage=mock_triage_output,
            serp=mock_serp_output,
            content=mock_content_output
        )

        # Required top-level fields
        required_fields = [
            "critical",
            "quick_wins",
            "strategic",
            "structural",
            "total_estimated_monthly_click_recovery",
            "total_estimated_monthly_click_growth",
            "narrative"
        ]
        for field in required_fields:
            assert field in result, f"Missing required field: {field}"

        # Verify types
        assert isinstance(result["critical"], list)
        assert isinstance(result["