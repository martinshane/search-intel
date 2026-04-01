"""
Site crawler — lightweight sitemap-based internal link analysis.

Fetches a site's sitemap (or auto-discovers via robots.txt), then crawls
each URL to extract page metadata and internal links.  Produces two outputs
consumed by the analysis pipeline:

  1. **page_data** — per-page metadata suitable for Module 4 (Content Intelligence):
     url, title, h1, meta_description, word_count, last_modified, canonical,
     schema_types

  2. **link_graph** — internal link adjacency suitable for Module 9 (Site Architecture):
     { "pages": [...], "link_graph": {src_url: [dst_url, ...], ...} }

Budget-aware: caps at ``max_pages`` (default 200) to stay within Railway's
4 GB RAM and keep background-task wall time under 5 minutes.
"""

import logging
import re
import time
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse
from xml.etree import ElementTree

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_MAX_PAGES = 200
REQUEST_TIMEOUT = 15  # seconds per page
CRAWL_DELAY = 0.25  # seconds between requests — polite crawl
USER_AGENT = (
    "SearchIntelBot/1.0 (+https://search-intel-api-production.up.railway.app; "
    "site-analysis-tool)"
)

# Sitemap XML namespaces
SITEMAP_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

# ---------------------------------------------------------------------------
# Sitemap discovery & parsing
# ---------------------------------------------------------------------------


def _fetch_url(url: str, *, timeout: int = REQUEST_TIMEOUT) -> Optional[requests.Response]:
    """Fetch a URL with our user-agent. Returns None on failure."""
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": USER_AGENT},
            timeout=timeout,
            allow_redirects=True,
        )
        resp.raise_for_status()
        return resp
    except Exception as exc:
        logger.debug("Failed to fetch %s: %s", url, exc)
        return None


def discover_sitemap_urls(domain: str) -> List[str]:
    """
    Auto-discover sitemap URLs for a domain.

    Strategy:
      1. Check robots.txt for ``Sitemap:`` directives
      2. Try common paths: /sitemap.xml, /sitemap_index.xml
      3. If a sitemap index is found, recursively fetch child sitemaps
    """
    base = f"https://{domain}" if not domain.startswith("http") else domain
    base = base.rstrip("/")

    sitemap_candidates: List[str] = []

    # 1. Parse robots.txt
    robots_resp = _fetch_url(f"{base}/robots.txt")
    if robots_resp:
        for line in robots_resp.text.splitlines():
            stripped = line.strip()
            if stripped.lower().startswith("sitemap:"):
                sm_url = stripped.split(":", 1)[1].strip()
                if sm_url:
                    sitemap_candidates.append(sm_url)

    # 2. Common paths
    for path in ["/sitemap.xml", "/sitemap_index.xml"]:
        candidate = f"{base}{path}"
        if candidate not in sitemap_candidates:
            sitemap_candidates.append(candidate)

    # 3. Fetch & expand sitemap indexes
    page_urls: List[str] = []
    visited_sitemaps: Set[str] = set()

    def _parse_sitemap(sm_url: str) -> None:
        if sm_url in visited_sitemaps:
            return
        visited_sitemaps.add(sm_url)

        resp = _fetch_url(sm_url)
        if not resp:
            return

        try:
            root = ElementTree.fromstring(resp.content)
        except ElementTree.ParseError:
            logger.debug("Invalid XML from %s", sm_url)
            return

        tag = root.tag.split("}")[-1] if "}" in root.tag else root.tag

        if tag == "sitemapindex":
            # Sitemap index — recurse into child sitemaps
            for sitemap_elem in root.findall("sm:sitemap/sm:loc", SITEMAP_NS):
                child_url = (sitemap_elem.text or "").strip()
                if child_url:
                    _parse_sitemap(child_url)
        elif tag == "urlset":
            for url_elem in root.findall("sm:url/sm:loc", SITEMAP_NS):
                loc = (url_elem.text or "").strip()
                if loc:
                    page_urls.append(loc)

    for sm in sitemap_candidates:
        _parse_sitemap(sm)
        if page_urls:
            break  # Found a valid sitemap — no need to try others

    logger.info("Discovered %d URLs from sitemaps for %s", len(page_urls), domain)
    return page_urls


# ---------------------------------------------------------------------------
# Page crawling & extraction
# ---------------------------------------------------------------------------


def _extract_page_data(
    url: str,
    soup: BeautifulSoup,
    domain: str,
) -> Tuple[Dict[str, Any], List[str]]:
    """
    Extract metadata and internal links from a parsed HTML page.

    Returns:
        (page_metadata_dict, list_of_internal_link_urls)
    """
    # --- Title ---
    title_tag = soup.find("title")
    title = title_tag.get_text(strip=True) if title_tag else ""

    # --- H1 ---
    h1_tag = soup.find("h1")
    h1 = h1_tag.get_text(strip=True) if h1_tag else ""

    # --- Meta description ---
    meta_desc = ""
    meta_tag = soup.find("meta", attrs={"name": re.compile(r"^description$", re.I)})
    if meta_tag:
        meta_desc = meta_tag.get("content", "")

    # --- Canonical ---
    canonical = ""
    canon_tag = soup.find("link", attrs={"rel": "canonical"})
    if canon_tag:
        canonical = canon_tag.get("href", "")

    # --- Word count (main text) ---
    # Remove script/style tags before counting
    for tag in soup(["script", "style", "nav", "header", "footer"]):
        tag.decompose()
    body_text = soup.get_text(separator=" ", strip=True)
    word_count = len(body_text.split())

    # --- Schema markup types ---
    schema_types: List[str] = []
    for script_tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
        try:
            import json
            ld = json.loads(script_tag.string or "{}")
            if isinstance(ld, dict) and "@type" in ld:
                schema_types.append(ld["@type"])
            elif isinstance(ld, list):
                for item in ld:
                    if isinstance(item, dict) and "@type" in item:
                        schema_types.append(item["@type"])
        except Exception:
            pass

    # --- Internal links ---
    parsed_url = urlparse(url)
    target_domain = parsed_url.netloc.lower()
    internal_links: List[str] = []

    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        resolved = urljoin(url, href)
        resolved_parsed = urlparse(resolved)

        # Keep only same-domain HTTP(S) links, strip fragments
        if resolved_parsed.netloc.lower() == target_domain and resolved_parsed.scheme in ("http", "https"):
            clean_url = f"{resolved_parsed.scheme}://{resolved_parsed.netloc}{resolved_parsed.path}"
            if clean_url.rstrip("/") != url.rstrip("/"):  # Skip self-links
                internal_links.append(clean_url)

    page_meta = {
        "url": url,
        "title": title,
        "h1": h1,
        "meta_description": meta_desc,
        "word_count": word_count,
        "canonical": canonical,
        "schema_types": schema_types,
        "last_modified": None,  # Will be set from HTTP headers if available
    }

    return page_meta, internal_links


def crawl_site(
    domain: str,
    *,
    max_pages: int = DEFAULT_MAX_PAGES,
    sitemap_urls: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Crawl a site and return page metadata + internal link graph.

    Args:
        domain: The site domain (e.g. "example.com")
        max_pages: Maximum number of pages to crawl (budget control)
        sitemap_urls: Pre-fetched list of URLs from sitemap; if None,
                      will auto-discover from robots.txt / sitemap.xml

    Returns:
        {
            "pages": [
                {"url": ..., "title": ..., "h1": ..., "meta_description": ...,
                 "word_count": ..., "canonical": ..., "schema_types": [...],
                 "last_modified": ...},
                ...
            ],
            "link_graph": {
                "https://example.com/page-a": [
                    "https://example.com/page-b",
                    "https://example.com/page-c",
                ],
                ...
            },
            "sitemap_urls": ["https://example.com/page-a", ...],
            "stats": {
                "urls_in_sitemap": N,
                "pages_crawled": M,
                "pages_failed": F,
                "total_internal_links": L,
                "crawl_time_seconds": T,
            }
        }
    """
    start_time = time.time()

    # Discover sitemap URLs if not provided
    if sitemap_urls is None:
        sitemap_urls = discover_sitemap_urls(domain)

    # If sitemap is empty, try homepage + crawl discovery
    urls_to_crawl: List[str] = list(sitemap_urls[:max_pages])
    if not urls_to_crawl:
        base = f"https://{domain}" if not domain.startswith("http") else domain
        urls_to_crawl = [base.rstrip("/") + "/"]
        logger.info("No sitemap found — starting from homepage only")

    pages: List[Dict[str, Any]] = []
    link_graph: Dict[str, List[str]] = {}
    crawled: Set[str] = set()
    failed_count = 0
    total_links = 0

    for i, url in enumerate(urls_to_crawl):
        if len(crawled) >= max_pages:
            logger.info("Reached max_pages=%d, stopping crawl", max_pages)
            break

        normalised = url.rstrip("/") or url
        if normalised in crawled:
            continue

        resp = _fetch_url(url)
        if not resp:
            failed_count += 1
            continue

        crawled.add(normalised)

        # Parse Last-Modified from headers
        last_modified = resp.headers.get("Last-Modified")

        try:
            soup = BeautifulSoup(resp.content, "html.parser")
        except Exception as exc:
            logger.debug("Failed to parse HTML for %s: %s", url, exc)
            failed_count += 1
            continue

        page_meta, internal_links = _extract_page_data(url, soup, domain)
        page_meta["last_modified"] = last_modified

        pages.append(page_meta)
        link_graph[url] = list(set(internal_links))  # Deduplicate
        total_links += len(link_graph[url])

        # Polite crawl delay
        if i < len(urls_to_crawl) - 1:
            time.sleep(CRAWL_DELAY)

    elapsed = time.time() - start_time

    stats = {
        "urls_in_sitemap": len(sitemap_urls),
        "pages_crawled": len(pages),
        "pages_failed": failed_count,
        "total_internal_links": total_links,
        "crawl_time_seconds": round(elapsed, 1),
    }

    logger.info(
        "Crawl complete for %s: %d pages, %d links, %.1fs",
        domain, stats["pages_crawled"], stats["total_internal_links"], elapsed,
    )

    return {
        "pages": pages,
        "link_graph": link_graph,
        "sitemap_urls": sitemap_urls,
        "stats": stats,
    }
