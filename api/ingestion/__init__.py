"""
Ingestion module for Search Intelligence Report.

Handles data collection from:
- Google Search Console API
- Google Analytics 4 Data API
- DataForSEO SERP API
- Site crawling for internal link graph
"""

from api.ingestion.gsc import GSCClient
from api.ingestion.ga4 import GA4Client
from api.ingestion.serp import SERPClient
from api.ingestion.crawler import SiteCrawler
from api.ingestion.cache import IngestionCache

__all__ = [
    "GSCClient",
    "GA4Client",
    "SERPClient",
    "SiteCrawler",
    "IngestionCache",
]