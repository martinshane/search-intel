"""
Ingestion package for Search Intelligence Report.

Handles data collection from:
- Google Search Console API
- Google Analytics 4 Data API
- DataForSEO SERP API
- Site crawling for internal link graph
"""

from .gsc import GSCClient, GSCDataPuller
from .ga4 import GA4Client, GA4DataPuller

__all__ = [
    "GSCClient",
    "GSCDataPuller",
    "GA4Client",
    "GA4DataPuller",
]