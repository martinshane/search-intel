"""
Comprehensive test suite for api/services/consulting_ctas.py

Tests the Consulting CTA service — contextual, data-driven consulting
call-to-action generation for Search Intelligence Reports.  Covers:
  - Constants and configuration (URLs, service catalogue, module map)
  - Helper functions (_severity_label, _extract_metric, _format_number)
  - Per-module CTA generators (modules 1-12)
  - Public API (generate_module_cta, generate_report_ctas)
  - PDF-specific CTAs (generate_pdf_ctas)
  - Email-specific CTAs (generate_email_ctas)
  - Service catalogue (get_available_services)
  - Edge cases and integration scenarios
"""

import pytest
from typing import Any, Dict, List, Optional


# ===================================================================
# Import the module under test
# ===================================================================
from api.services.consulting_ctas import (
    CONTACT_URL,
    BOOKING_URL,
    AUDIT_URL,
    CTA_STYLES,
    MODULE_SERVICE_MAP,
    SERVICE_DETAILS,
    _severity_label,
    _extract_metric,
    _format_number,
    _cta_module_1,
    _cta_module_2,
    _cta_module_3,
    _cta_module_4,
    _cta_module_5,
    _cta_module_8,
    _cta_module_9,
    _cta_module_10,
    _cta_module_12,
    _MODULE_CTA_GENERATORS,
    generate_module_cta,
    generate_report_ctas,
    generate_pdf_ctas,
    generate_email_ctas,
    get_available_services,
)


# ===================================================================
# 1. Constants and configuration
# ===================================================================

class TestConstants:
    """Verify constants are properly defined."""

    def test_contact_url_is_clankermarketing(self):
        assert "clankermarketing.com" in CONTACT_URL

    def test_booking_url_is_clankermarketing(self):
        assert "clankermarketing.com" in BOOKING_URL

    def test_audit_url_is_clankermarketing(self):
        assert "clankermarketing.com" in AUDIT_URL

    def test_urls_are_https(self):
        for url in (CONTACT_URL, BOOKING_URL, AUDIT_URL):
            assert url.startswith("https://"), f"{url} should start with https://"

    def test_cta_styles_has_expected_keys(self):
        expected = {"banner", "inline", "card", "sidebar", "modal"}
        assert set(CTA_STYLES.keys()) == expected

    def test_module_service_map_covers_all_12(self):
        assert set(MODULE_SERVICE_MAP.keys()) == set(range(1, 13))

    def test_all_service_map_values_in_details(self):
        for mod, svc in MODULE_SERVICE_MAP.items():
            assert svc in SERVICE_DETAILS, f"Module {mod} maps to {svc} not in SERVICE_DETAILS"

    def test_service_details_have_required_fields(self):
        required = {"name", "description", "duration", "price_hint"}
        for svc_id, details in SERVICE_DETAILS.items():
            assert required.issubset(details.keys()), f"{svc_id} missing fields: {required - details.keys()}"

    def test_service_details_count(self):
        assert len(SERVICE_DETAILS) == 12

    def test_all_prices_have_dollar_sign(self):
        for svc_id, details in SERVICE_DETAILS.items():
            assert "$" in details["price_hint"], f"{svc_id} price_hint missing $"


# ===================================================================
# 2. _severity_label
# ===================================================================

class TestSeverityLabel:
    def test_critical_below_40(self):
        assert _severity_label(10) == "critical"

    def test_critical_at_zero(self):
        assert _severity_label(0) == "critical"

    def test_needs_attention_between_40_70(self):
        assert _severity_label(50) == "needs_attention"

    def test_healthy_above_70(self):
        assert _severity_label(80) == "healthy"

    def test_boundary_at_40(self):
        assert _severity_label(40) == "needs_attention"

    def test_boundary_at_70(self):
        assert _severity_label(70) == "healthy"

    def test_custom_thresholds(self):
        assert _severity_label(25, thresholds=(20, 50)) == "needs_attention"
        assert _severity_label(15, thresholds=(20, 50)) == "critical"
        assert _severity_label(60, thresholds=(20, 50)) == "healthy"

    def test_boundary_exact_low_threshold(self):
        assert _severity_label(39.9) == "critical"


# ===================================================================
# 3. _extract_metric
# ===================================================================

class TestExtractMetric:
    def test_single_key(self):
        assert _extract_metric({"a": 1}, "a") == 1

    def test_nested_keys(self):
        assert _extract_metric({"a": {"b": {"c": 42}}}, "a", "b", "c") == 42

    def test_missing_key_returns_default(self):
        assert _extract_metric({"a": 1}, "b", default="N/A") == "N/A"

    def test_missing_nested_key(self):
        assert _extract_metric({"a": {"b": 1}}, "a", "c", default=0) == 0

    def test_default_is_none(self):
        assert _extract_metric({}, "x") is None

    def test_non_dict_intermediate(self):
        assert _extract_metric({"a": "string"}, "a", "b", default="fallback") == "fallback"

    def test_empty_dict(self):
        assert _extract_metric({}, "a", "b", default=99) == 99

    def test_value_is_zero(self):
        assert _extract_metric({"a": 0}, "a") == 0

    def test_value_is_false(self):
        assert _extract_metric({"a": False}, "a") is False

    def test_value_is_empty_list(self):
        assert _extract_metric({"a": []}, "a") == []


# ===================================================================
# 4. _format_number
# ===================================================================

class TestFormatNumber:
    def test_none_returns_na(self):
        assert _format_number(None) == "N/A"

    def test_million(self):
        assert _format_number(2_500_000) == "2.5M"

    def test_thousand(self):
        assert _format_number(5_300) == "5.3K"

    def test_integer_renders_no_decimal(self):
        assert _format_number(42) == "42"

    def test_float_renders_one_decimal(self):
        assert _format_number(3.7) == "3.7"

    def test_zero(self):
        assert _format_number(0) == "0"

    def test_string_number(self):
        assert _format_number("1500") == "1.5K"

    def test_non_numeric_string(self):
        assert _format_number("hello") == "hello"

    def test_exactly_1000(self):
        assert _format_number(1000) == "1.0K"

    def test_exactly_1000000(self):
        assert _format_number(1_000_000) == "1.0M"

    def test_small_float(self):
        assert _format_number(0.5) == "0.5"

    def test_negative_number(self):
        result = _format_number(-500)
        assert result  # Should not crash


# ===================================================================
# 5. Per-module CTA generators
# ===================================================================

class TestCtaModule1:
    """Health Trajectory — triggered by declining trends."""

    def test_declining_text_summary(self):
        result = _cta_module_1({"summary": "Traffic is declining sharply"})
        assert result is not None
        assert result["module"] == 1
        assert result["urgency"] == "high"

    def test_declining_dict_summary(self):
        result = _cta_module_1({"summary": {"trend_direction": "declining"}})
        assert result is not None

    def test_stable_returns_none(self):
        result = _cta_module_1({"summary": {"trend_direction": "stable"}})
        assert result is None

    def test_increasing_returns_none(self):
        result = _cta_module_1({"summary": {"trend_direction": "increasing"}})
        assert result is None

    def test_drop_keyword_in_summary(self):
        result = _cta_module_1({"summary": "We see a significant drop in visibility"})
        assert result is not None

    def test_cta_has_required_fields(self):
        result = _cta_module_1({"summary": "declining traffic"})
        for key in ("module", "trigger", "urgency", "headline", "body", "cta_text", "cta_url", "service"):
            assert key in result, f"Missing key: {key}"

    def test_cta_url_contains_booking(self):
        result = _cta_module_1({"summary": "declining"})
        assert BOOKING_URL in result["cta_url"]

    def test_empty_results(self):
        result = _cta_module_1({})
        assert result is None

    def test_decreasing_direction(self):
        result = _cta_module_1({"summary": {"trend_direction": "decreasing"}})
        assert result is not None

    def test_negative_direction(self):
        result = _cta_module_1({"summary": {"trend_direction": "negative"}})
        assert result is not None


class TestCtaModule2:
    """Page Triage — triggered by many underperforming pages."""

    def test_many_underperforming(self):
        pages = [{"url": f"/page-{i}"} for i in range(25)]
        result = _cta_module_2({"underperforming_pages": pages})
        assert result is not None
        assert result["module"] == 2
        assert result["urgency"] == "medium"

    def test_over_50_is_high_urgency(self):
        pages = [{"url": f"/p{i}"} for i in range(55)]
        result = _cta_module_2({"underperforming_pages": pages})
        assert result["urgency"] == "high"

    def test_few_pages_returns_none(self):
        pages = [{"url": f"/p{i}"} for i in range(5)]
        result = _cta_module_2({"underperforming_pages": pages})
        assert result is None

    def test_fallback_to_pages_to_fix(self):
        pages = [{"url": f"/p{i}"} for i in range(15)]
        result = _cta_module_2({"pages_to_fix": pages})
        assert result is not None

    def test_empty_results(self):
        result = _cta_module_2({})
        assert result is None

    def test_exactly_10_returns_cta(self):
        pages = [{"url": f"/p{i}"} for i in range(10)]
        result = _cta_module_2({"underperforming_pages": pages})
        assert result is not None

    def test_exactly_9_returns_none(self):
        pages = [{"url": f"/p{i}"} for i in range(9)]
        result = _cta_module_2({"underperforming_pages": pages})
        assert result is None


class TestCtaModule3:
    """SERP Landscape — triggered by many competitors."""

    def test_many_competitors(self):
        comps = [{"domain": f"comp{i}.com"} for i in range(5)]
        result = _cta_module_3({"competitors": comps})
        assert result is not None
        assert result["module"] == 3

    def test_few_competitors_returns_none(self):
        comps = [{"domain": "a.com"}, {"domain": "b.com"}]
        result = _cta_module_3({"competitors": comps})
        assert result is None

    def test_fallback_to_competitor_domains(self):
        comps = [f"comp{i}.com" for i in range(4)]
        result = _cta_module_3({"competitor_domains": comps})
        assert result is not None

    def test_empty_results(self):
        result = _cta_module_3({})
        assert result is None

    def test_exactly_3_returns_cta(self):
        comps = ["a.com", "b.com", "c.com"]
        result = _cta_module_3({"competitors": comps})
        assert result is not None


class TestCtaModule4:
    """Content Intelligence — triggered by content gaps."""

    def test_many_gaps(self):
        gaps = [{"keyword": f"kw{i}"} for i in range(10)]
        result = _cta_module_4({"content_gaps": gaps})
        assert result is not None
        assert result["module"] == 4

    def test_few_gaps_returns_none(self):
        gaps = [{"keyword": "kw1"}]
        result = _cta_module_4({"content_gaps": gaps})
        assert result is None

    def test_fallback_to_opportunities(self):
        opps = [f"opp{i}" for i in range(7)]
        result = _cta_module_4({"opportunities": opps})
        assert result is not None

    def test_empty_results(self):
        result = _cta_module_4({})
        assert result is None

    def test_exactly_5_triggers(self):
        gaps = [f"g{i}" for i in range(5)]
        result = _cta_module_4({"content_gaps": gaps})
        assert result is not None


class TestCtaModule5:
    """Gameplan — triggered by multiple recommendations."""

    def test_many_recommendations(self):
        recs = [{"action": f"a{i}"} for i in range(5)]
        result = _cta_module_5({"recommendations": recs})
        assert result is not None
        assert result["module"] == 5

    def test_few_recommendations_returns_none(self):
        recs = [{"action": "a1"}]
        result = _cta_module_5({"recommendations": recs})
        assert result is None

    def test_exactly_3_triggers(self):
        recs = ["r1", "r2", "r3"]
        result = _cta_module_5({"recommendations": recs})
        assert result is not None

    def test_empty_results(self):
        result = _cta_module_5({})
        assert result is None

    def test_headline_mentions_count(self):
        recs = [f"r{i}" for i in range(8)]
        result = _cta_module_5({"recommendations": recs})
        assert "8" in result["body"]


class TestCtaModule8:
    """Technical Health — triggered by technical issues."""

    def test_many_issues(self):
        issues = [{"type": f"issue{i}"} for i in range(10)]
        result = _cta_module_8({"issues": issues})
        assert result is not None
        assert result["module"] == 8

    def test_over_20_is_high_urgency(self):
        issues = [{"type": f"i{i}"} for i in range(25)]
        result = _cta_module_8({"issues": issues})
        assert result["urgency"] == "high"

    def test_few_issues_returns_none(self):
        issues = [{"type": "i1"}]
        result = _cta_module_8({"issues": issues})
        assert result is None

    def test_fallback_to_technical_issues(self):
        issues = [f"ti{i}" for i in range(8)]
        result = _cta_module_8({"technical_issues": issues})
        assert result is not None

    def test_empty_results(self):
        result = _cta_module_8({})
        assert result is None

    def test_exactly_5_triggers(self):
        issues = list(range(5))
        result = _cta_module_8({"issues": issues})
        assert result is not None


class TestCtaModule9:
    """Site Architecture — triggered by orphans/bottlenecks."""

    def test_many_orphans(self):
        orphans = [{"url": f"/o{i}"} for i in range(10)]
        result = _cta_module_9({"orphan_pages": orphans})
        assert result is not None
        assert result["module"] == 9

    def test_many_bottlenecks(self):
        bottlenecks = [{"url": f"/b{i}"} for i in range(5)]
        result = _cta_module_9({"equity_bottlenecks": bottlenecks})
        assert result is not None

    def test_over_20_orphans_high_urgency(self):
        orphans = [{"url": f"/o{i}"} for i in range(25)]
        result = _cta_module_9({"orphan_pages": orphans})
        assert result["urgency"] == "high"

    def test_low_counts_returns_none(self):
        orphans = [{"url": "/o1"}]
        bottlenecks = [{"url": "/b1"}]
        result = _cta_module_9({"orphan_pages": orphans, "equity_bottlenecks": bottlenecks})
        assert result is None

    def test_empty_results(self):
        result = _cta_module_9({})
        assert result is None

    def test_5_orphans_triggers(self):
        orphans = [f"o{i}" for i in range(5)]
        result = _cta_module_9({"orphan_pages": orphans})
        assert result is not None


class TestCtaModule10:
    """Branded Split — triggered by high brand dependency."""

    def test_high_dependency(self):
        result = _cta_module_10({"brand_dependency": {"dependency_score": 85}})
        assert result is not None
        assert result["module"] == 10
        assert result["urgency"] == "high"

    def test_low_dependency_returns_none(self):
        result = _cta_module_10({"brand_dependency": {"dependency_score": 30}})
        assert result is None

    def test_boundary_at_60(self):
        result = _cta_module_10({"brand_dependency": {"dependency_score": 60}})
        assert result is not None

    def test_below_60_returns_none(self):
        result = _cta_module_10({"brand_dependency": {"dependency_score": 59}})
        assert result is None

    def test_80_plus_is_high(self):
        result = _cta_module_10({"brand_dependency": {"dependency_score": 80}})
        assert result["urgency"] == "high"

    def test_60_to_79_is_medium(self):
        result = _cta_module_10({"brand_dependency": {"dependency_score": 65}})
        assert result["urgency"] == "medium"

    def test_string_score_converted(self):
        result = _cta_module_10({"brand_dependency": {"dependency_score": "75"}})
        assert result is not None

    def test_empty_results(self):
        result = _cta_module_10({})
        assert result is None

    def test_non_numeric_score(self):
        result = _cta_module_10({"brand_dependency": {"dependency_score": "invalid"}})
        assert result is None


class TestCtaModule12:
    """Revenue Attribution — triggered by revenue at risk."""

    def test_high_revenue_at_risk(self):
        at_risk = [
            {"estimated_revenue": 5000, "reason": "declining"},
            {"estimated_revenue": 3000, "reason": "bounce"},
        ]
        result = _cta_module_12({"revenue_at_risk": at_risk})
        assert result is not None
        assert result["module"] == 12
        assert result["urgency"] == "critical"

    def test_low_revenue_returns_none(self):
        at_risk = [{"estimated_revenue": 200}]
        result = _cta_module_12({"revenue_at_risk": at_risk})
        assert result is None

    def test_empty_at_risk_list(self):
        result = _cta_module_12({"revenue_at_risk": []})
        assert result is None

    def test_empty_results(self):
        result = _cta_module_12({})
        assert result is None

    def test_exactly_1000_triggers(self):
        at_risk = [{"estimated_revenue": 1000}]
        result = _cta_module_12({"revenue_at_risk": at_risk})
        assert result is not None

    def test_below_1000_no_trigger(self):
        at_risk = [{"estimated_revenue": 999}]
        result = _cta_module_12({"revenue_at_risk": at_risk})
        assert result is None

    def test_headline_mentions_dollar(self):
        at_risk = [{"estimated_revenue": 5000}]
        result = _cta_module_12({"revenue_at_risk": at_risk})
        assert "$" in result["headline"]


# ===================================================================
# 6. Module CTA generator map
# ===================================================================

class TestModuleCtaGeneratorMap:
    """Verify the generator map is correctly configured."""

    def test_generators_for_modules_1_2_3_4_5_8_9_10_12(self):
        expected = {1, 2, 3, 4, 5, 8, 9, 10, 12}
        assert set(_MODULE_CTA_GENERATORS.keys()) == expected

    def test_modules_6_7_11_have_no_generator(self):
        for mod in (6, 7, 11):
            assert mod not in _MODULE_CTA_GENERATORS


# ===================================================================
# 7. generate_module_cta (public API)
# ===================================================================

class TestGenerateModuleCta:
    def test_returns_cta_for_triggered_module(self):
        result = generate_module_cta(1, {"summary": "declining trends"})
        assert result is not None
        assert result["module"] == 1

    def test_returns_none_for_non_triggered(self):
        result = generate_module_cta(1, {"summary": "everything is great"})
        assert result is None

    def test_returns_none_for_unmapped_module(self):
        result = generate_module_cta(6, {"something": "data"})
        assert result is None

    def test_returns_none_for_invalid_module(self):
        result = generate_module_cta(99, {})
        assert result is None

    def test_handles_exception_gracefully(self):
        # Pass something that might cause an internal error
        result = generate_module_cta(1, None)
        # Should return None rather than raising
        # (depends on implementation — _extract_metric handles non-dict)
        assert result is None or isinstance(result, dict)


# ===================================================================
# 8. generate_report_ctas (public API)
# ===================================================================

class TestGenerateReportCtas:
    def _build_triggering_results(self) -> Dict[int, Dict[str, Any]]:
        """Build module results that trigger CTAs for multiple modules."""
        return {
            1: {"summary": "declining traffic significantly"},
            2: {"underperforming_pages": [{"url": f"/p{i}"} for i in range(20)]},
            3: {"competitors": [f"c{i}.com" for i in range(5)]},
            4: {"content_gaps": [f"g{i}" for i in range(10)]},
            5: {"recommendations": [f"r{i}" for i in range(6)]},
            8: {"issues": [f"i{i}" for i in range(15)]},
            9: {"orphan_pages": [f"o{i}" for i in range(10)]},
            10: {"brand_dependency": {"dependency_score": 85}},
            12: {"revenue_at_risk": [{"estimated_revenue": 10000}]},
        }

    def test_returns_dict_with_required_keys(self):
        result = generate_report_ctas({})
        assert "ctas" in result
        assert "executive_cta" in result
        assert "total_generated" in result
        assert "contact_url" in result
        assert "booking_url" in result
        assert "audit_url" in result

    def test_empty_results_gives_fallback_executive(self):
        result = generate_report_ctas({})
        assert result["executive_cta"]["module"] is None
        assert result["total_generated"] == 0

    def test_max_ctas_respected(self):
        results = self._build_triggering_results()
        out = generate_report_ctas(results, max_ctas=3)
        assert len(out["ctas"]) <= 3

    def test_default_max_is_5(self):
        results = self._build_triggering_results()
        out = generate_report_ctas(results)
        assert len(out["ctas"]) <= 5

    def test_sorted_by_urgency(self):
        results = self._build_triggering_results()
        out = generate_report_ctas(results)
        urgency_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
        urgencies = [urgency_order.get(c["urgency"], 99) for c in out["ctas"]]
        assert urgencies == sorted(urgencies)

    def test_executive_cta_is_highest_urgency(self):
        results = self._build_triggering_results()
        out = generate_report_ctas(results)
        if out["ctas"]:
            assert out["executive_cta"] == out["ctas"][0]

    def test_total_generated_counts_all(self):
        results = self._build_triggering_results()
        out = generate_report_ctas(results)
        assert out["total_generated"] >= len(out["ctas"])

    def test_single_module_triggering(self):
        results = {12: {"revenue_at_risk": [{"estimated_revenue": 5000}]}}
        out = generate_report_ctas(results)
        assert len(out["ctas"]) == 1
        assert out["ctas"][0]["module"] == 12

    def test_no_modules_triggering(self):
        results = {1: {"summary": "stable"}, 2: {"underperforming_pages": []}}
        out = generate_report_ctas(results)
        assert len(out["ctas"]) == 0
        assert out["total_generated"] == 0

    def test_urls_in_output(self):
        out = generate_report_ctas({})
        assert out["contact_url"] == CONTACT_URL
        assert out["booking_url"] == BOOKING_URL
        assert out["audit_url"] == AUDIT_URL


# ===================================================================
# 9. generate_pdf_ctas
# ===================================================================

class TestGeneratePdfCtas:
    def _build_triggering_results(self) -> Dict[int, Dict[str, Any]]:
        return {
            1: {"summary": "declining"},
            2: {"underperforming_pages": [{"url": f"/p{i}"} for i in range(20)]},
            12: {"revenue_at_risk": [{"estimated_revenue": 5000}]},
        }

    def test_returns_required_keys(self):
        out = generate_pdf_ctas({})
        assert "module_ctas" in out
        assert "closing_cta" in out
        assert "contact_url" in out
        assert "booking_url" in out

    def test_module_ctas_capped_at_3(self):
        results = self._build_triggering_results()
        out = generate_pdf_ctas(results)
        assert len(out["module_ctas"]) <= 3

    def test_pdf_ctas_have_style_hints(self):
        results = self._build_triggering_results()
        out = generate_pdf_ctas(results)
        for cta in out["module_ctas"]:
            assert "placement" in cta
            assert "style" in cta
            assert "background_color" in cta
            assert "accent_color" in cta
            assert "text_color" in cta

    def test_closing_cta_has_required_fields(self):
        out = generate_pdf_ctas({})
        closing = out["closing_cta"]
        assert "headline" in closing
        assert "body" in closing
        assert "cta_text" in closing
        assert "cta_url" in closing
        assert "secondary_cta_text" in closing
        assert "secondary_cta_url" in closing

    def test_closing_cta_style_is_full_page(self):
        out = generate_pdf_ctas({})
        assert out["closing_cta"]["style"] == "full_page"

    def test_closing_cta_has_email_fallback(self):
        out = generate_pdf_ctas({})
        assert "mailto:" in out["closing_cta"]["secondary_cta_url"]


# ===================================================================
# 10. generate_email_ctas
# ===================================================================

class TestGenerateEmailCtas:
    def _build_triggering_results(self) -> Dict[int, Dict[str, Any]]:
        return {
            1: {"summary": "declining"},
            8: {"issues": [f"i{i}" for i in range(10)]},
        }

    def test_returns_required_keys(self):
        out = generate_email_ctas({})
        assert "header_cta" in out
        assert "footer_cta" in out
        assert "inline_ctas" in out
        assert "contact_url" in out

    def test_header_cta_has_url(self):
        out = generate_email_ctas({}, domain="example.com")
        assert "url" in out["header_cta"]
        assert "example.com" in out["header_cta"]["url"]

    def test_footer_cta_has_required_fields(self):
        out = generate_email_ctas({})
        footer = out["footer_cta"]
        assert "headline" in footer
        assert "body" in footer
        assert "cta_text" in footer
        assert "cta_url" in footer

    def test_inline_ctas_from_triggered_modules(self):
        results = self._build_triggering_results()
        out = generate_email_ctas(results)
        assert len(out["inline_ctas"]) > 0

    def test_inline_ctas_have_module_field(self):
        results = self._build_triggering_results()
        out = generate_email_ctas(results)
        for cta in out["inline_ctas"]:
            assert "module" in cta
            assert "text" in cta
            assert "url" in cta

    def test_domain_param_appears_in_urls(self):
        out = generate_email_ctas({}, domain="test.com")
        assert "test.com" in out["header_cta"]["url"]
        assert "test.com" in out["footer_cta"]["cta_url"]

    def test_empty_domain(self):
        out = generate_email_ctas({}, domain="")
        assert out["header_cta"]["url"]  # Should not crash

    def test_max_2_inline_ctas(self):
        results = {
            1: {"summary": "declining"},
            2: {"underperforming_pages": [{"url": f"/p{i}"} for i in range(20)]},
            3: {"competitors": [f"c{i}.com" for i in range(5)]},
            8: {"issues": [f"i{i}" for i in range(10)]},
            12: {"revenue_at_risk": [{"estimated_revenue": 5000}]},
        }
        out = generate_email_ctas(results)
        # generate_email_ctas passes max_ctas=2 to generate_report_ctas
        assert len(out["inline_ctas"]) <= 2


# ===================================================================
# 11. get_available_services
# ===================================================================

class TestGetAvailableServices:
    def test_returns_list(self):
        services = get_available_services()
        assert isinstance(services, list)

    def test_returns_12_services(self):
        services = get_available_services()
        assert len(services) == 12

    def test_each_service_has_required_fields(self):
        services = get_available_services()
        required = {"id", "name", "description", "duration", "price_hint", "booking_url", "modules"}
        for svc in services:
            assert required.issubset(svc.keys()), f"{svc.get('id')} missing: {required - svc.keys()}"

    def test_booking_urls_contain_service_id(self):
        services = get_available_services()
        for svc in services:
            assert svc["id"] in svc["booking_url"]

    def test_modules_field_is_list(self):
        services = get_available_services()
        for svc in services:
            assert isinstance(svc["modules"], list)

    def test_every_module_mapped_to_a_service(self):
        services = get_available_services()
        all_modules = set()
        for svc in services:
            all_modules.update(svc["modules"])
        assert set(range(1, 13)).issubset(all_modules)


# ===================================================================
# 12. Edge cases and integration
# ===================================================================

class TestEdgeCases:
    def test_none_module_results_in_report_ctas(self):
        # generate_report_ctas expects a dict; empty dict is fine
        out = generate_report_ctas({})
        assert out["total_generated"] == 0

    def test_module_results_with_non_int_keys(self):
        # Module results with string keys should not crash
        out = generate_report_ctas({"1": {"summary": "declining"}})
        # String keys won't match int generators, so 0 CTAs expected
        assert out["total_generated"] == 0

    def test_very_large_module_results(self):
        # Huge page list
        big_results = {
            2: {"underperforming_pages": [{"url": f"/p{i}"} for i in range(10000)]},
        }
        out = generate_report_ctas(big_results)
        assert len(out["ctas"]) == 1
        assert "10K" in out["ctas"][0]["headline"] or "10,000" in out["ctas"][0]["headline"] or "10.0K" in out["ctas"][0]["headline"]

    def test_all_modules_triggering(self):
        results = {
            1: {"summary": "declining traffic"},
            2: {"underperforming_pages": [{"url": f"/p{i}"} for i in range(50)]},
            3: {"competitors": [f"c{i}.com" for i in range(10)]},
            4: {"content_gaps": [f"g{i}" for i in range(20)]},
            5: {"recommendations": [f"r{i}" for i in range(10)]},
            8: {"issues": [f"i{i}" for i in range(30)]},
            9: {"orphan_pages": [f"o{i}" for i in range(25)]},
            10: {"brand_dependency": {"dependency_score": 90}},
            12: {"revenue_at_risk": [{"estimated_revenue": 50000}]},
        }
        out = generate_report_ctas(results)
        assert out["total_generated"] == 9
        assert len(out["ctas"]) == 5  # default max

    def test_unicode_in_results(self):
        result = _cta_module_1({"summary": "tráfico está declinando rápidamente"})
        assert result is not None  # "declin" substring match

    def test_pdf_and_email_ctas_consistent(self):
        results = {12: {"revenue_at_risk": [{"estimated_revenue": 5000}]}}
        pdf = generate_pdf_ctas(results)
        email = generate_email_ctas(results)
        # Both should have at least one CTA
        assert len(pdf["module_ctas"]) >= 1
        assert len(email["inline_ctas"]) >= 1

    def test_cta_service_details_match_catalogue(self):
        result = _cta_module_1({"summary": "declining"})
        assert result["service"] == SERVICE_DETAILS["search_health_audit"]

    def test_generate_module_cta_for_each_generator(self):
        """Smoke test: every generator returns CTA or None without error."""
        dummy_results = {
            1: {"summary": "declining"},
            2: {"underperforming_pages": list(range(20))},
            3: {"competitors": list(range(5))},
            4: {"content_gaps": list(range(10))},
            5: {"recommendations": list(range(5))},
            8: {"issues": list(range(10))},
            9: {"orphan_pages": list(range(10))},
            10: {"brand_dependency": {"dependency_score": 75}},
            12: {"revenue_at_risk": [{"estimated_revenue": 2000}]},
        }
        for mod, results in dummy_results.items():
            cta = generate_module_cta(mod, results)
            assert cta is None or isinstance(cta, dict), f"Module {mod} returned unexpected type"
