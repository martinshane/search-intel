"""
Site crawl data helper.

Provides get_crawl_data() which performs a lightweight crawl of target pages
to extract internal link graphs, meta tags, headings, and content structure.
Used by Module 8 (Technical Health) and Module 9 (Site Architecture).
"""

import logging
from typing import Any, Dict, List, Optional, Set
from urllib.parse import urljoin, urlparse
from html.parser import HTMLParser

import httpx

logger = logging.getLogger(__name__)

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (compatible; SearchIntelBot/1.0; +https://searchintel.app)"
)


class _LinkExtractor(HTMLParser):
    """Simple HTML parser that extracts anchor hrefs and meta tags."""

    def __init__(self, base_url: str) -> None:
        super().__init__()
        self.base_url = base_url
        self.links: List[str] = []
        self.meta: Dict[str, str] = {}
        self.title: str = ""
        self._in_title = False
        self.h1s: List[str] = []
        self._in_h1 = False
        self._current_text = ""

    def handle_starttag(self, tag: str, attrs: list) -> None:
        attr_dict = dict(attrs)
        if tag == "a" and "href" in attr_dict:
            href = attr_dict["href"]
            full_url = urljoin(self.base_url, href)
            self.links.append(full_url)
        elif tag == "meta":
            name = attr_dict.get("name", attr_dict.get("property", ""))
            content = attr_dict.get("content", "")
            if name and content:
                self.meta[name] = content
        elif tag == "title":
            self._in_title = True
            self._current_text = ""
        elif tag == "h1":
            self._in_h1 = True
            self._current_text = ""

    def handle_data(self, data: str) -> None:
        if self._in_title or self._in_h1:
            self._current_text += data

    def handle_endtag(self, tag: str) -> None:
        if tag == "title" and self._in_title:
            self.title = self._current_text.strip()
            self._in_title = False
        elif tag == "h1" and self._in_h1:
            self.h1s.append(self._current_text.strip())
            self._in_h1 = False


async def get_crawl_data(
    domain: str,
    *,
    seed_urls: Optional[List[str]] = None,
    max_pages: int = 100,
    timeout: float = 30.0,
    user_agent: str = DEFAULT_USER_AGENT,
) -> Dict[str, Any]:
    """
    Perform a lightweight crawl of a domain.

    Args:
        domain: Target domain (e.g. "example.com").
        seed_urls: Optional list of URLs to start from. Defaults to homepage.
        max_pages: Maximum pages to crawl.
        timeout: HTTP request timeout in seconds.
        user_agent: User-Agent string for requests.

    Returns:
        Dict with:
        - "pages": list of page data dicts (url, title, h1s, meta, internal_links, external_links, status_code)
        - "link_graph": dict mapping source URL to list of internal link targets
        - "page_count": total pages crawled
        - "errors": list of URLs that returned errors
    """
    base_url = f"https://{domain}"
    if seed_urls is None:
        seed_urls = [base_url + "/"]

    visited: Set[str] = set()
    queue: List[str] = list(seed_urls)
    pages: List[Dict[str, Any]] = []
    link_graph: Dict[str, List[str]] = {}
    errors: List[Dict[str, Any]] = []

    headers = {"User-Agent": user_agent}

    async with httpx.AsyncClient(
        timeout=timeout, follow_redirects=True, headers=headers
    ) as client:
        while queue and len(visited) < max_pages:
            url = queue.pop(0)
            normalized = url.rstrip("/")
            if normalized in visited:
                continue
            visited.add(normalized)

            parsed = urlparse(url)
            if parsed.netloc and parsed.netloc != domain and not parsed.netloc.endswith(f".{domain}"):
                continue

            try:
                resp = await client.get(url)
                content_type = resp.headers.get("content-type", "")
                if "text/html" not in content_type:
                    continue

                parser = _LinkExtractor(url)
                parser.feed(resp.text)

                internal_links = []
                external_links = []
                for link in parser.links:
                    link_parsed = urlparse(link)
                    link_domain = link_parsed.netloc
                    if link_domain == domain or link_domain.endswith(f".{domain}") or not link_domain:
                        internal_links.append(link)
                    else:
                        external_links.append(link)

                page_data = {
                    "url": url,
                    "status_code": resp.status_code,
                    "title": parser.title,
                    "h1s": parser.h1s,
                    "meta": parser.meta,
                    "internal_link_count": len(internal_links),
                    "external_link_count": len(external_links),
                    "internal_links": internal_links[:100],  # cap stored links
                    "external_links": external_links[:50],
                }
                pages.append(page_data)
                link_graph[url] = internal_links

                # Add discovered internal links to queue
                for ilink in internal_links:
                    if ilink.rstrip("/") not in visited:
                        queue.append(ilink)

            except Exception as e:
                logger.warning("Crawl error for %s: %s", url, str(e))
                errors.append({"url": url, "error": str(e)})

    logger.info("Crawled %d pages for %s (%d errors)", len(pages), domain, len(errors))
    return {
        "pages": pages,
        "link_graph": link_graph,
        "page_count": len(pages),
        "errors": errors,
    }
