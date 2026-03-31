"""
Ingestion module for data collection and processing.

Handles:
- Google Search Console API integration
- Google Analytics 4 API integration
- DataForSEO SERP data collection
- Site crawling for internal link graph
- Data caching and storage
"""

from api.ingestion.gsc import GSCClient
from api.ingestion.ga4 import GA4Client
from api.ingestion.dataforseo import DataForSEOClient
from api.ingestion.crawler import SiteCrawler

__all__ = [
    'GSCClient',
    'GA4Client',
    'DataForSEOClient',
    'SiteCrawler',
]