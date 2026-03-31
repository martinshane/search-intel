# api/ingestion/__init__.py
"""
Ingestion package for Search Intelligence Report.

This package handles data collection from external APIs:
- Google Search Console (GSC)
- Google Analytics 4 (GA4)
- DataForSEO (SERP data)
- Site crawling (internal link graph)
"""

from .gsc import GSCIngestion

__all__ = ["GSCIngestion"]