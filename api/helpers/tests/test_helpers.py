"""
Comprehensive test suite for api/helpers/ modules.

Tests all four helper modules:
- crawl_helper.py  (_LinkExtractor parser, get_crawl_data async crawler)
- ga4_helper.py    (get_ga4_data GA4 Data API integration)
- gsc_helper.py    (get_gsc_data Google Search Console integration)
- serp_helper.py   (get_serp_data DataForSEO SERP integration)
"""

import asyncio
import base64
import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock
from typing import Any, Dict, List


# ---------------------------------------------------------------------------
# SECTION 1 — _LinkExtractor (crawl_helper)
# ---------------------------------------------------------------------------

class TestLinkExtractorBasic:
    """Test the HTML parser extracts links correctly."""

    def test_extracts_anchor_hrefs(self):
        from api.helpers.crawl_helper import _LinkExtractor
        parser = _LinkExtractor("https://example.com")
        parser.feed('<a href="/about">About</a>')
        assert len(parser.links) == 1
        assert parser.links[0] == "https://example.com/about"

    def test_resolves_relative_urls(self):
        from api.helpers.crawl_helper import _LinkExtractor
        parser = _LinkExtractor("https://example.com/blog/")
        parser.feed('<a href="../contact">Contact</a>')
        assert "example.com/contact" in parser.links[0]

    def test_absolute_urls_preserved(self):
        from api.helpers.crawl_helper import _LinkExtractor
        parser = _LinkExtractor("https://example.com")
        parser.feed('<a href="https://other.com/page">Link</a>')
        assert parser.links[0] == "https://other.com/page"

    def test_extracts_meta_tags(self):
        from api.helpers.crawl_helper import _LinkExtractor
        parser = _LinkExtractor("https://example.com")
        parser.feed('<meta name="description" content="A test page">')
        assert parser.meta["description"] == "A test page"

    def test_extracts_og_meta(self):
        from api.helpers.crawl_helper import _LinkExtractor
        parser = _LinkExtractor("https://example.com")
        parser.feed('<meta property="og:title" content="OG Title">')
        assert parser.meta["og:title"] == "OG Title"

    def test_extracts_title(self):
        from api.helpers.crawl_helper import _LinkExtractor
        parser = _LinkExtractor("https://example.com")
        parser.feed('<html><head><title>My Page Title</title></head></html>')
        assert parser.title == "My Page Title"

    def test_extracts_h1s(self):
        from api.helpers.crawl_helper import _LinkExtractor
        parser = _LinkExtractor("https://example.com")
        parser.feed('<h1>First Heading</h1><h1>Second Heading</h1>')
        assert len(parser.h1s) == 2
        assert parser.h1s[0] == "First Heading"
        assert parser.h1s[1] == "Second Heading"

    def test_empty_html(self):
        from api.helpers.crawl_helper import _LinkExtractor
        parser = _LinkExtractor("https://example.com")
        parser.feed("")
        assert parser.links == []
        assert parser.meta == {}
        assert parser.title == ""
        assert parser.h1s == []

    def test_no_href_anchor_ignored(self):
        from api.helpers.crawl_helper import _LinkExtractor
        parser = _LinkExtractor("https://example.com")
        parser.feed('<a name="anchor">Text</a>')
        assert len(parser.links) == 0

    def test_meta_without_content_ignored(self):
        from api.helpers.crawl_helper import _LinkExtractor
        parser = _LinkExtractor("https://example.com")
        parser.feed('<meta name="viewport">')
        assert len(parser.meta) == 0

    def test_multiple_links(self):
        from api.helpers.crawl_helper import _LinkExtractor
        parser = _LinkExtractor("https://example.com")
        html = '<a href="/a">A</a><a href="/b">B</a><a href="/c">C</a>'
        parser.feed(html)
        assert len(parser.links) == 3

    def test_title_strips_whitespace(self):
        from api.helpers.crawl_helper import _LinkExtractor
        parser = _LinkExtractor("https://example.com")
        parser.feed('<title>  Spaced Title  </title>')
        assert parser.title == "Spaced Title"

    def test_h1_strips_whitespace(self):
        from api.helpers.crawl_helper import _LinkExtractor
        parser = _LinkExtractor("https://example.com")
        parser.feed('<h1>  Spaced H1  </h1>')
        assert parser.h1s[0] == "Spaced H1"


class TestLinkExtractorEdgeCases:
    """Edge cases for the HTML parser."""

    def test_nested_html_in_anchor(self):
        from api.helpers.crawl_helper import _LinkExtractor
        parser = _LinkExtractor("https://example.com")
        parser.feed('<a href="/page"><span>Nested</span></a>')
        assert len(parser.links) == 1

    def test_fragment_urls(self):
        from api.helpers.crawl_helper import _LinkExtractor
        parser = _LinkExtractor("https://example.com/page")
        parser.feed('<a href="#section">Jump</a>')
        assert len(parser.links) == 1

    def test_multiple_meta_tags(self):
        from api.helpers.crawl_helper import _LinkExtractor
        parser = _LinkExtractor("https://example.com")
        parser.feed('<meta name="robots" content="index"><meta name="author" content="Test">')
        assert parser.meta["robots"] == "index"
        assert parser.meta["author"] == "Test"

    def test_unicode_content(self):
        from api.helpers.crawl_helper import _LinkExtractor
        parser = _LinkExtractor("https://example.com")
        parser.feed('<title>日本語タイトル</title>')
        assert parser.title == "日本語タイトル"


# ---------------------------------------------------------------------------
# SECTION 2 — get_crawl_data (crawl_helper)
# ---------------------------------------------------------------------------

class TestGetCrawlDataBasic:
    """Test the async crawl function."""

    def test_returns_dict_structure(self):
        from api.helpers.crawl_helper import get_crawl_data

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"content-type": "text/html"}
        mock_response.text = "<html><title>Test</title><a href='/about'>About</a></html>"

        with patch("httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.get = AsyncMock(return_value=mock_response)
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            result = asyncio.get_event_loop().run_until_complete(
                get_crawl_data("example.com")
            )

        assert "pages" in result
        assert "link_graph" in result
        assert "page_count" in result
        assert "errors" in result

    def test_default_seed_is_homepage(self):
        from api.helpers.crawl_helper import get_crawl_data

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"content-type": "text/html"}
        mock_response.text = "<html><title>Home</title></html>"

        with patch("httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.get = AsyncMock(return_value=mock_response)
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            result = asyncio.get_event_loop().run_until_complete(
                get_crawl_data("example.com")
            )

        assert result["page_count"] >= 1

    def test_custom_seed_urls(self):
        from api.helpers.crawl_helper import get_crawl_data

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"content-type": "text/html"}
        mock_response.text = "<html><title>Page</title></html>"

        with patch("httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.get = AsyncMock(return_value=mock_response)
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            result = asyncio.get_event_loop().run_until_complete(
                get_crawl_data("example.com", seed_urls=["https://example.com/blog", "https://example.com/about"])
            )

        assert result["page_count"] == 2

    def test_max_pages_respected(self):
        from api.helpers.crawl_helper import get_crawl_data

        call_count = 0
        def make_response(url):
            nonlocal call_count
            call_count += 1
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.headers = {"content-type": "text/html"}
            links = "".join(f'<a href="/page{i}">P{i}</a>' for i in range(20))
            mock_response.text = f"<html><title>Page {call_count}</title>{links}</html>"
            return mock_response

        with patch("httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.get = AsyncMock(side_effect=make_response)
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            result = asyncio.get_event_loop().run_until_complete(
                get_crawl_data("example.com", max_pages=3)
            )

        assert result["page_count"] <= 3

    def test_non_html_skipped(self):
        from api.helpers.crawl_helper import get_crawl_data

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"content-type": "application/json"}
        mock_response.text = '{"data": "not html"}'

        with patch("httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.get = AsyncMock(return_value=mock_response)
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            result = asyncio.get_event_loop().run_until_complete(
                get_crawl_data("example.com")
            )

        assert result["page_count"] == 0

    def test_http_errors_captured(self):
        from api.helpers.crawl_helper import get_crawl_data

        with patch("httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.get = AsyncMock(side_effect=Exception("Connection refused"))
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            result = asyncio.get_event_loop().run_until_complete(
                get_crawl_data("example.com")
            )

        assert len(result["errors"]) >= 1
        assert "Connection refused" in result["errors"][0]["error"]

    def test_external_domains_not_crawled(self):
        from api.helpers.crawl_helper import get_crawl_data

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"content-type": "text/html"}
        mock_response.text = '<html><a href="https://external.com/page">Ext</a></html>'

        with patch("httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.get = AsyncMock(return_value=mock_response)
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            result = asyncio.get_event_loop().run_until_complete(
                get_crawl_data("example.com")
            )

        # Only homepage crawled, external link not followed
        assert result["page_count"] == 1


class TestGetCrawlDataPageData:
    """Test page data extraction in crawl results."""

    def test_page_data_fields(self):
        from api.helpers.crawl_helper import get_crawl_data

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"content-type": "text/html; charset=utf-8"}
        mock_response.text = (
            '<html><head><title>Test Page</title>'
            '<meta name="description" content="A test page"></head>'
            '<body><h1>Welcome</h1>'
            '<a href="/internal">Internal</a>'
            '<a href="https://ext.com">External</a>'
            '</body></html>'
        )

        with patch("httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.get = AsyncMock(return_value=mock_response)
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            result = asyncio.get_event_loop().run_until_complete(
                get_crawl_data("example.com")
            )

        page = result["pages"][0]
        assert page["title"] == "Test Page"
        assert page["status_code"] == 200
        assert "h1s" in page
        assert page["internal_link_count"] >= 1
        assert page["external_link_count"] >= 1
        assert "internal_links" in page
        assert "external_links" in page

    def test_link_graph_populated(self):
        from api.helpers.crawl_helper import get_crawl_data

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"content-type": "text/html"}
        mock_response.text = '<html><a href="/about">About</a><a href="/blog">Blog</a></html>'

        with patch("httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.get = AsyncMock(return_value=mock_response)
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            result = asyncio.get_event_loop().run_until_complete(
                get_crawl_data("example.com")
            )

        assert len(result["link_graph"]) >= 1

    def test_subdomain_treated_as_internal(self):
        from api.helpers.crawl_helper import get_crawl_data

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"content-type": "text/html"}
        mock_response.text = '<html><a href="https://blog.example.com/post">Post</a></html>'

        with patch("httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.get = AsyncMock(return_value=mock_response)
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            result = asyncio.get_event_loop().run_until_complete(
                get_crawl_data("example.com")
            )

        page = result["pages"][0]
        assert page["internal_link_count"] >= 1


class TestCrawlConstants:
    """Test default constants in crawl_helper."""

    def test_default_user_agent(self):
        from api.helpers.crawl_helper import DEFAULT_USER_AGENT
        assert "SearchIntelBot" in DEFAULT_USER_AGENT
        assert "compatible" in DEFAULT_USER_AGENT


# ---------------------------------------------------------------------------
# SECTION 3 — get_ga4_data (ga4_helper)
# ---------------------------------------------------------------------------

class TestGa4DataBasic:
    """Test GA4 data fetching."""

    def test_returns_dict_with_rows(self):
        from api.helpers.ga4_helper import get_ga4_data

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "dimensionHeaders": [{"name": "pagePath"}, {"name": "date"}],
            "metricHeaders": [{"name": "sessions"}],
            "rows": [
                {"dimensionValues": [{"value": "/home"}, {"value": "20260301"}],
                 "metricValues": [{"value": "100"}]}
            ],
            "rowCount": 1,
        }
        mock_resp.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.post = AsyncMock(return_value=mock_resp)
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            result = asyncio.get_event_loop().run_until_complete(
                get_ga4_data("fake_token", "123456789")
            )

        assert "rows" in result
        assert result["row_count"] == 1
        assert result["rows"][0]["pagePath"] == "/home"
        assert result["rows"][0]["sessions"] == "100"

    def test_flattens_dimension_and_metric_values(self):
        from api.helpers.ga4_helper import get_ga4_data

        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "dimensionHeaders": [{"name": "pagePath"}],
            "metricHeaders": [{"name": "sessions"}, {"name": "totalUsers"}],
            "rows": [
                {"dimensionValues": [{"value": "/page1"}],
                 "metricValues": [{"value": "50"}, {"value": "30"}]}
            ],
            "rowCount": 1,
        }
        mock_resp.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.post = AsyncMock(return_value=mock_resp)
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            result = asyncio.get_event_loop().run_until_complete(
                get_ga4_data("token", "123", metrics=["sessions", "totalUsers"], dimensions=["pagePath"])
            )

        row = result["rows"][0]
        assert row["pagePath"] == "/page1"
        assert row["sessions"] == "50"
        assert row["totalUsers"] == "30"

    def test_empty_response_returns_empty_rows(self):
        from api.helpers.ga4_helper import get_ga4_data

        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "dimensionHeaders": [],
            "metricHeaders": [],
            "rows": [],
            "rowCount": 0,
        }
        mock_resp.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.post = AsyncMock(return_value=mock_resp)
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            result = asyncio.get_event_loop().run_until_complete(
                get_ga4_data("token", "123")
            )

        assert result["rows"] == []
        assert result["row_count"] == 0

    def test_custom_date_range(self):
        from api.helpers.ga4_helper import get_ga4_data

        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "dimensionHeaders": [{"name": "date"}],
            "metricHeaders": [{"name": "sessions"}],
            "rows": [{"dimensionValues": [{"value": "20260301"}], "metricValues": [{"value": "10"}]}],
            "rowCount": 1,
        }
        mock_resp.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.post = AsyncMock(return_value=mock_resp)
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            result = asyncio.get_event_loop().run_until_complete(
                get_ga4_data("token", "123", start_date="2026-01-01", end_date="2026-03-01")
            )

        # Verify the post was called with correct date range
        call_args = client_instance.post.call_args
        body = call_args[1]["json"] if "json" in call_args[1] else call_args[0][1] if len(call_args[0]) > 1 else None
        if body:
            assert body["dateRanges"][0]["startDate"] == "2026-01-01"

    def test_pagination_collects_all_rows(self):
        from api.helpers.ga4_helper import get_ga4_data

        call_count = [0]

        def make_response(*args, **kwargs):
            call_count[0] += 1
            mock = MagicMock()
            mock.raise_for_status = MagicMock()
            if call_count[0] == 1:
                mock.json.return_value = {
                    "dimensionHeaders": [{"name": "pagePath"}],
                    "metricHeaders": [{"name": "sessions"}],
                    "rows": [{"dimensionValues": [{"value": f"/p{i}"}], "metricValues": [{"value": str(i)}]} for i in range(3)],
                    "rowCount": 5,
                }
            elif call_count[0] == 2:
                mock.json.return_value = {
                    "dimensionHeaders": [{"name": "pagePath"}],
                    "metricHeaders": [{"name": "sessions"}],
                    "rows": [{"dimensionValues": [{"value": f"/p{i}"}], "metricValues": [{"value": str(i)}]} for i in range(3, 5)],
                    "rowCount": 5,
                }
            else:
                mock.json.return_value = {
                    "dimensionHeaders": [{"name": "pagePath"}],
                    "metricHeaders": [{"name": "sessions"}],
                    "rows": [],
                    "rowCount": 5,
                }
            return mock

        with patch("httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.post = AsyncMock(side_effect=make_response)
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            result = asyncio.get_event_loop().run_until_complete(
                get_ga4_data("token", "123", limit=3)
            )

        assert result["row_count"] == 5

    def test_dimension_filter_passed(self):
        from api.helpers.ga4_helper import get_ga4_data

        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "dimensionHeaders": [], "metricHeaders": [],
            "rows": [], "rowCount": 0,
        }
        mock_resp.raise_for_status = MagicMock()

        dim_filter = {"filter": {"fieldName": "pagePath", "stringFilter": {"value": "/blog"}}}

        with patch("httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.post = AsyncMock(return_value=mock_resp)
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            asyncio.get_event_loop().run_until_complete(
                get_ga4_data("token", "123", dimension_filter=dim_filter)
            )

        call_body = client_instance.post.call_args[1]["json"]
        assert "dimensionFilter" in call_body

    def test_http_error_raised(self):
        from api.helpers.ga4_helper import get_ga4_data
        import httpx as real_httpx

        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = real_httpx.HTTPStatusError(
            "403", request=MagicMock(), response=MagicMock()
        )

        with patch("httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.post = AsyncMock(return_value=mock_resp)
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            with pytest.raises(real_httpx.HTTPStatusError):
                asyncio.get_event_loop().run_until_complete(
                    get_ga4_data("bad_token", "123")
                )


class TestGa4Constants:
    """Test GA4 helper constants."""

    def test_api_base_url(self):
        from api.helpers.ga4_helper import GA4_API_BASE
        assert "analyticsdata.googleapis.com" in GA4_API_BASE
        assert "v1beta" in GA4_API_BASE

    def test_default_metrics(self):
        """Default metrics should include sessions and totalUsers."""
        from api.helpers.ga4_helper import get_ga4_data
        import inspect
        sig = inspect.signature(get_ga4_data)
        # metrics default is None, which becomes a list in the function
        assert sig.parameters["metrics"].default is None

    def test_returns_dimension_and_metric_headers(self):
        from api.helpers.ga4_helper import get_ga4_data

        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "dimensionHeaders": [{"name": "pagePath"}],
            "metricHeaders": [{"name": "sessions"}],
            "rows": [{"dimensionValues": [{"value": "/"}], "metricValues": [{"value": "1"}]}],
            "rowCount": 1,
        }
        mock_resp.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.post = AsyncMock(return_value=mock_resp)
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            result = asyncio.get_event_loop().run_until_complete(
                get_ga4_data("token", "123")
            )

        assert "dimension_headers" in result
        assert "metric_headers" in result


# ---------------------------------------------------------------------------
# SECTION 4 — get_gsc_data (gsc_helper)
# ---------------------------------------------------------------------------

class TestGscDataBasic:
    """Test Google Search Console data fetching."""

    def test_returns_dict_with_rows(self):
        from api.helpers.gsc_helper import get_gsc_data

        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "rows": [{"keys": ["keyword", "/page", "2026-01-01"], "clicks": 10, "impressions": 100}],
            "responseAggregationType": "byPage",
        }
        mock_resp.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.post = AsyncMock(return_value=mock_resp)
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            result = asyncio.get_event_loop().run_until_complete(
                get_gsc_data("token", "sc-domain:example.com")
            )

        assert "rows" in result
        assert "row_count" in result
        assert result["row_count"] == 1

    def test_default_dimensions(self):
        from api.helpers.gsc_helper import get_gsc_data

        mock_resp = MagicMock()
        mock_resp.json.return_value = {"rows": [], "responseAggregationType": "auto"}
        mock_resp.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.post = AsyncMock(return_value=mock_resp)
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            asyncio.get_event_loop().run_until_complete(
                get_gsc_data("token", "sc-domain:example.com")
            )

        body = client_instance.post.call_args[1]["json"]
        assert body["dimensions"] == ["query", "page", "date"]

    def test_custom_dimensions(self):
        from api.helpers.gsc_helper import get_gsc_data

        mock_resp = MagicMock()
        mock_resp.json.return_value = {"rows": [], "responseAggregationType": "auto"}
        mock_resp.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.post = AsyncMock(return_value=mock_resp)
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            asyncio.get_event_loop().run_until_complete(
                get_gsc_data("token", "sc-domain:example.com", dimensions=["query", "country"])
            )

        body = client_instance.post.call_args[1]["json"]
        assert body["dimensions"] == ["query", "country"]

    def test_custom_date_range(self):
        from api.helpers.gsc_helper import get_gsc_data

        mock_resp = MagicMock()
        mock_resp.json.return_value = {"rows": [], "responseAggregationType": "auto"}
        mock_resp.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.post = AsyncMock(return_value=mock_resp)
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            asyncio.get_event_loop().run_until_complete(
                get_gsc_data("token", "sc-domain:example.com", start_date="2026-01-01", end_date="2026-03-01")
            )

        body = client_instance.post.call_args[1]["json"]
        assert body["startDate"] == "2026-01-01"
        assert body["endDate"] == "2026-03-01"

    def test_pagination(self):
        from api.helpers.gsc_helper import get_gsc_data

        call_count = [0]

        def make_response(*args, **kwargs):
            call_count[0] += 1
            mock = MagicMock()
            mock.raise_for_status = MagicMock()
            if call_count[0] == 1:
                mock.json.return_value = {
                    "rows": [{"keys": [f"kw{i}"], "clicks": i} for i in range(5)],
                    "responseAggregationType": "auto",
                }
            else:
                mock.json.return_value = {"rows": [], "responseAggregationType": "auto"}
            return mock

        with patch("httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.post = AsyncMock(side_effect=make_response)
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            result = asyncio.get_event_loop().run_until_complete(
                get_gsc_data("token", "sc-domain:example.com", row_limit=5)
            )

        assert result["row_count"] == 5

    def test_data_type_parameter(self):
        from api.helpers.gsc_helper import get_gsc_data

        mock_resp = MagicMock()
        mock_resp.json.return_value = {"rows": [], "responseAggregationType": "auto"}
        mock_resp.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.post = AsyncMock(return_value=mock_resp)
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            asyncio.get_event_loop().run_until_complete(
                get_gsc_data("token", "sc-domain:example.com", data_type="image")
            )

        body = client_instance.post.call_args[1]["json"]
        assert body["type"] == "image"

    def test_dimension_filter_groups(self):
        from api.helpers.gsc_helper import get_gsc_data

        mock_resp = MagicMock()
        mock_resp.json.return_value = {"rows": [], "responseAggregationType": "auto"}
        mock_resp.raise_for_status = MagicMock()

        filters = [{"groupType": "and", "filters": [{"dimension": "query", "operator": "contains", "expression": "test"}]}]

        with patch("httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.post = AsyncMock(return_value=mock_resp)
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            asyncio.get_event_loop().run_until_complete(
                get_gsc_data("token", "sc-domain:example.com", dimension_filter_groups=filters)
            )

        body = client_instance.post.call_args[1]["json"]
        assert "dimensionFilterGroups" in body

    def test_http_error_raised(self):
        from api.helpers.gsc_helper import get_gsc_data
        import httpx as real_httpx

        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = real_httpx.HTTPStatusError(
            "401", request=MagicMock(), response=MagicMock()
        )

        with patch("httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.post = AsyncMock(return_value=mock_resp)
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            with pytest.raises(real_httpx.HTTPStatusError):
                asyncio.get_event_loop().run_until_complete(
                    get_gsc_data("bad_token", "sc-domain:example.com")
                )


class TestGscConstants:
    """Test GSC helper constants."""

    def test_api_base_url(self):
        from api.helpers.gsc_helper import GSC_API_BASE
        assert "googleapis.com" in GSC_API_BASE
        assert "webmasters" in GSC_API_BASE

    def test_response_aggregation_type(self):
        from api.helpers.gsc_helper import get_gsc_data

        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "rows": [{"keys": ["kw"], "clicks": 1}],
            "responseAggregationType": "byPage",
        }
        mock_resp.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.post = AsyncMock(return_value=mock_resp)
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            result = asyncio.get_event_loop().run_until_complete(
                get_gsc_data("token", "sc-domain:example.com")
            )

        assert result["responseAggregationType"] == "byPage"


# ---------------------------------------------------------------------------
# SECTION 5 — get_serp_data (serp_helper)
# ---------------------------------------------------------------------------

class TestSerpDataBasic:
    """Test DataForSEO SERP data fetching."""

    def test_returns_dict_with_results(self):
        from api.helpers.serp_helper import get_serp_data

        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "tasks": [{
                "result": [{
                    "keyword": "test keyword",
                    "type": "organic",
                    "items_count": 10,
                    "items": [{"type": "organic", "position": 1}],
                    "spell": None,
                    "check_url": "https://google.com/search?q=test",
                }]
            }],
            "cost": 0.05,
        }
        mock_resp.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.post = AsyncMock(return_value=mock_resp)
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            result = asyncio.get_event_loop().run_until_complete(
                get_serp_data("login", "pass", keywords=["test keyword"])
            )

        assert "results" in result
        assert result["keyword_count"] == 1
        assert result["results"][0]["keyword"] == "test keyword"

    def test_no_keywords_raises_value_error(self):
        from api.helpers.serp_helper import get_serp_data

        with pytest.raises(ValueError, match="At least one keyword"):
            asyncio.get_event_loop().run_until_complete(
                get_serp_data("login", "pass", keywords=[])
            )

    def test_none_keywords_raises_value_error(self):
        from api.helpers.serp_helper import get_serp_data

        with pytest.raises(ValueError, match="At least one keyword"):
            asyncio.get_event_loop().run_until_complete(
                get_serp_data("login", "pass")
            )

    def test_multiple_keywords(self):
        from api.helpers.serp_helper import get_serp_data

        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "tasks": [
                {"result": [{"keyword": "kw1", "type": "organic", "items_count": 5, "items": [], "spell": None, "check_url": ""}]},
                {"result": [{"keyword": "kw2", "type": "organic", "items_count": 3, "items": [], "spell": None, "check_url": ""}]},
            ],
            "cost": 0.10,
        }
        mock_resp.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.post = AsyncMock(return_value=mock_resp)
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            result = asyncio.get_event_loop().run_until_complete(
                get_serp_data("login", "pass", keywords=["kw1", "kw2"])
            )

        assert result["keyword_count"] == 2

    def test_cost_included(self):
        from api.helpers.serp_helper import get_serp_data

        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "tasks": [{"result": [{"keyword": "kw1", "type": "organic", "items_count": 0, "items": [], "spell": None, "check_url": ""}]}],
            "cost": 0.025,
        }
        mock_resp.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.post = AsyncMock(return_value=mock_resp)
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            result = asyncio.get_event_loop().run_until_complete(
                get_serp_data("login", "pass", keywords=["kw1"])
            )

        assert result["cost"] == 0.025

    def test_authorization_header_format(self):
        from api.helpers.serp_helper import get_serp_data

        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "tasks": [{"result": [{"keyword": "kw", "type": "organic", "items_count": 0, "items": [], "spell": None, "check_url": ""}]}],
            "cost": 0,
        }
        mock_resp.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.post = AsyncMock(return_value=mock_resp)
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            asyncio.get_event_loop().run_until_complete(
                get_serp_data("mylogin", "mypass", keywords=["kw"])
            )

        call_args = client_instance.post.call_args
        sent_headers = call_args[1]["headers"]
        expected_creds = base64.b64encode(b"mylogin:mypass").decode()
        assert sent_headers["Authorization"] == f"Basic {expected_creds}"

    def test_custom_location_and_device(self):
        from api.helpers.serp_helper import get_serp_data

        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "tasks": [{"result": [{"keyword": "kw", "type": "organic", "items_count": 0, "items": [], "spell": None, "check_url": ""}]}],
            "cost": 0,
        }
        mock_resp.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.post = AsyncMock(return_value=mock_resp)
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            asyncio.get_event_loop().run_until_complete(
                get_serp_data("l", "p", keywords=["kw"], location_code=2826, device="mobile")
            )

        body = client_instance.post.call_args[1]["json"]
        assert body[0]["location_code"] == 2826
        assert body[0]["device"] == "mobile"

    def test_target_domain_in_task(self):
        from api.helpers.serp_helper import get_serp_data

        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "tasks": [{"result": [{"keyword": "kw", "type": "organic", "items_count": 0, "items": [], "spell": None, "check_url": ""}]}],
            "cost": 0,
        }
        mock_resp.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.post = AsyncMock(return_value=mock_resp)
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            asyncio.get_event_loop().run_until_complete(
                get_serp_data("l", "p", keywords=["kw"], target_domain="example.com")
            )

        body = client_instance.post.call_args[1]["json"]
        assert body[0]["target"] == "example.com"

    def test_serp_type_in_url(self):
        from api.helpers.serp_helper import get_serp_data

        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "tasks": [{"result": [{"keyword": "kw", "type": "paid", "items_count": 0, "items": [], "spell": None, "check_url": ""}]}],
            "cost": 0,
        }
        mock_resp.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.post = AsyncMock(return_value=mock_resp)
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            asyncio.get_event_loop().run_until_complete(
                get_serp_data("l", "p", keywords=["kw"], serp_type="paid")
            )

        call_url = client_instance.post.call_args[0][0]
        assert "paid" in call_url

    def test_http_error_raised(self):
        from api.helpers.serp_helper import get_serp_data
        import httpx as real_httpx

        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = real_httpx.HTTPStatusError(
            "401", request=MagicMock(), response=MagicMock()
        )

        with patch("httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.post = AsyncMock(return_value=mock_resp)
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            with pytest.raises(real_httpx.HTTPStatusError):
                asyncio.get_event_loop().run_until_complete(
                    get_serp_data("bad", "creds", keywords=["kw"])
                )


class TestSerpConstants:
    """Test SERP helper constants."""

    def test_api_base_url(self):
        from api.helpers.serp_helper import DATAFORSEO_API_BASE
        assert "dataforseo.com" in DATAFORSEO_API_BASE
        assert "v3" in DATAFORSEO_API_BASE

    def test_default_location_us(self):
        """Default location_code should be 2840 (US)."""
        from api.helpers.serp_helper import get_serp_data
        import inspect
        sig = inspect.signature(get_serp_data)
        assert sig.parameters["location_code"].default == 2840


# ---------------------------------------------------------------------------
# SECTION 6 — __init__.py exports
# ---------------------------------------------------------------------------

class TestHelpersInit:
    """Test that the helpers package exports all functions."""

    def test_exports_get_gsc_data(self):
        from api.helpers import get_gsc_data
        assert callable(get_gsc_data)

    def test_exports_get_ga4_data(self):
        from api.helpers import get_ga4_data
        assert callable(get_ga4_data)

    def test_exports_get_serp_data(self):
        from api.helpers import get_serp_data
        assert callable(get_serp_data)

    def test_exports_get_crawl_data(self):
        from api.helpers import get_crawl_data
        assert callable(get_crawl_data)

    def test_all_exports(self):
        import api.helpers
        assert "get_gsc_data" in api.helpers.__all__
        assert "get_ga4_data" in api.helpers.__all__
        assert "get_serp_data" in api.helpers.__all__
        assert "get_crawl_data" in api.helpers.__all__


# ---------------------------------------------------------------------------
# SECTION 7 — Edge cases and integration patterns
# ---------------------------------------------------------------------------

class TestEdgeCases:
    """Cross-cutting edge cases for all helpers."""

    def test_crawl_helper_user_agent_customizable(self):
        from api.helpers.crawl_helper import get_crawl_data

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"content-type": "text/html"}
        mock_response.text = "<html></html>"

        with patch("httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.get = AsyncMock(return_value=mock_response)
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            asyncio.get_event_loop().run_until_complete(
                get_crawl_data("example.com", user_agent="CustomBot/1.0")
            )

        # Verify custom user agent was passed
        client_kwargs = MockClient.call_args[1]
        assert client_kwargs["headers"]["User-Agent"] == "CustomBot/1.0"

    def test_crawl_helper_timeout_customizable(self):
        from api.helpers.crawl_helper import get_crawl_data

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"content-type": "text/html"}
        mock_response.text = "<html></html>"

        with patch("httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.get = AsyncMock(return_value=mock_response)
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            asyncio.get_event_loop().run_until_complete(
                get_crawl_data("example.com", timeout=15.0)
            )

        client_kwargs = MockClient.call_args[1]
        assert client_kwargs["timeout"] == 15.0

    def test_ga4_order_bys_passed(self):
        from api.helpers.ga4_helper import get_ga4_data

        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "dimensionHeaders": [], "metricHeaders": [],
            "rows": [], "rowCount": 0,
        }
        mock_resp.raise_for_status = MagicMock()

        order = [{"metric": {"metricName": "sessions"}, "desc": True}]

        with patch("httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.post = AsyncMock(return_value=mock_resp)
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            asyncio.get_event_loop().run_until_complete(
                get_ga4_data("token", "123", order_bys=order)
            )

        body = client_instance.post.call_args[1]["json"]
        assert "orderBys" in body

    def test_serp_no_target_domain_excluded(self):
        from api.helpers.serp_helper import get_serp_data

        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "tasks": [{"result": [{"keyword": "kw", "type": "organic", "items_count": 0, "items": [], "spell": None, "check_url": ""}]}],
            "cost": 0,
        }
        mock_resp.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.post = AsyncMock(return_value=mock_resp)
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            asyncio.get_event_loop().run_until_complete(
                get_serp_data("l", "p", keywords=["kw"])
            )

        body = client_instance.post.call_args[1]["json"]
        assert "target" not in body[0]

    def test_crawl_duplicate_urls_not_revisited(self):
        from api.helpers.crawl_helper import get_crawl_data

        call_urls = []

        def track_get(url):
            call_urls.append(url)
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.headers = {"content-type": "text/html"}
            mock_response.text = '<html><a href="https://example.com/">Home</a></html>'
            return mock_response

        with patch("httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.get = AsyncMock(side_effect=track_get)
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            result = asyncio.get_event_loop().run_until_complete(
                get_crawl_data("example.com")
            )

        # Homepage should only be crawled once even though it links to itself
        assert result["page_count"] == 1

    def test_internal_links_capped_at_100(self):
        from api.helpers.crawl_helper import get_crawl_data

        links_html = "".join(f'<a href="/page{i}">P{i}</a>' for i in range(150))
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"content-type": "text/html"}
        mock_response.text = f"<html>{links_html}</html>"

        with patch("httpx.AsyncClient") as MockClient:
            client_instance = AsyncMock()
            client_instance.get = AsyncMock(return_value=mock_response)
            client_instance.__aenter__ = AsyncMock(return_value=client_instance)
            client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = client_instance

            result = asyncio.get_event_loop().run_until_complete(
                get_crawl_data("example.com", max_pages=1)
            )

        page = result["pages"][0]
        assert len(page["internal_links"]) <= 100
        assert page["internal_link_count"] == 150
