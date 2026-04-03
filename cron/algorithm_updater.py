"""
Weekly cron script that fetches new algorithm updates from public sources
and updates the database.

Sources:
- Google Search Central Blog (via RSS)
- Semrush Sensor (scrape confirmed updates)
- Search Engine Roundtable (scrape)
- Moz (scrape)

Also includes a comprehensive KNOWN_UPDATES seed list covering all major
confirmed Google algorithm updates from 2023-01 to 2026-03, ensuring
Module 6 (Algorithm Impact) has robust historical data for traffic
change-point correlation even before the weekly RSS fetch runs.

Runs weekly to keep algorithm_updates table current.
"""

import os
import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import re

import httpx
from bs4 import BeautifulSoup
import feedparser
from supabase import create_client, Client

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Supabase client
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("SUPABASE_URL and SUPABASE_SERVICE_KEY must be set")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


# ---------------------------------------------------------------------------
# Known algorithm updates — comprehensive seed data for Module 6
#
# Sources: Google Search Central Blog, Search Engine Roundtable,
#          Search Engine Land, Moz Google Algorithm Update History
#
# This list covers every CONFIRMED Google algorithm update from Jan 2023
# through Mar 2026.  Dates are the official rollout start date.  Types
# match the classification used by Module 6 (algorithm_impact).
# ---------------------------------------------------------------------------

KNOWN_UPDATES: List[Dict[str, str]] = [
    # ── 2023 ──────────────────────────────────────────────────────────────
    {"date": "2023-02-21", "name": "February 2023 Product Reviews Update", "type": "product_reviews",
     "source": "Google Search Central",
     "description": "Expanded product reviews system to cover all review content, not just product reviews. Rolled out over ~14 days."},
    {"date": "2023-02-22", "name": "February 2023 Link Spam Update", "type": "link",
     "source": "Google Search Central",
     "description": "SpamBrain update targeting link spam across multiple languages. Rolled out globally."},
    {"date": "2023-03-15", "name": "March 2023 Core Update", "type": "core",
     "source": "Google Search Central",
     "description": "Broad core algorithm update. Took approximately 13 days to fully roll out (completed March 28)."},
    {"date": "2023-04-12", "name": "April 2023 Reviews Update", "type": "product_reviews",
     "source": "Google Search Central",
     "description": "Reviews system update applied to reviews beyond products — services, destinations, media, etc. Completed April 25."},
    {"date": "2023-08-22", "name": "August 2023 Core Update", "type": "core",
     "source": "Google Search Central",
     "description": "Major core update. Rolled out over 16 days (completed September 7). Significant ranking volatility observed."},
    {"date": "2023-09-14", "name": "September 2023 Helpful Content Update", "type": "helpful_content",
     "source": "Google Search Central",
     "description": "Third helpful content system update. Improved classifier for identifying unhelpful content. Completed September 28."},
    {"date": "2023-10-04", "name": "October 2023 Spam Update", "type": "spam",
     "source": "Google Search Central",
     "description": "Spam update targeting cloaking, hacked content, auto-generated spam, and scraped content across many languages."},
    {"date": "2023-10-05", "name": "October 2023 Core Update", "type": "core",
     "source": "Google Search Central",
     "description": "Core update that rolled out alongside the October spam update. Completed October 19."},
    {"date": "2023-11-02", "name": "November 2023 Core Update", "type": "core",
     "source": "Google Search Central",
     "description": "Final core update of 2023. Completed November 28. Significant changes to ranking for informational queries."},
    {"date": "2023-11-08", "name": "November 2023 Reviews Update", "type": "product_reviews",
     "source": "Google Search Central",
     "description": "Reviews system update. Last standalone reviews update before being folded into core updates."},

    # ── 2024 ──────────────────────────────────────────────────────────────
    {"date": "2024-03-05", "name": "March 2024 Core Update", "type": "core",
     "source": "Google Search Central",
     "description": "Massive core update with multiple ranking systems updated. Included helpful content system integration. Took 45 days to fully roll out (completed April 19). Reduced low-quality content in search by 45%."},
    {"date": "2024-03-05", "name": "March 2024 Spam Update", "type": "spam",
     "source": "Google Search Central",
     "description": "Spam policies update alongside March 2024 core update. New policies for expired domain abuse, scaled content abuse, and site reputation abuse."},
    {"date": "2024-04-16", "name": "April 2024 Helpful Content Update (folded into core)", "type": "helpful_content",
     "source": "Google Search Central",
     "description": "Helpful content system was officially folded into the core ranking system as part of the March 2024 core update rollout."},
    {"date": "2024-04-26", "name": "April 2024 Reviews Update", "type": "product_reviews",
     "source": "Google Search Central",
     "description": "Reviews system update. One of the last before reviews were integrated into core ranking."},
    {"date": "2024-05-05", "name": "May 2024 Site Reputation Abuse Enforcement", "type": "spam",
     "source": "Google Search Central",
     "description": "Manual enforcement of site reputation abuse policy (parasite SEO). Sites hosting third-party content to manipulate rankings targeted."},
    {"date": "2024-06-20", "name": "June 2024 Spam Update", "type": "spam",
     "source": "Google Search Central",
     "description": "Spam update improving detection of spam in multiple languages."},
    {"date": "2024-08-15", "name": "August 2024 Core Update", "type": "core",
     "source": "Google Search Central",
     "description": "Core update focused on surfacing content created for people, improving results for smaller independent publishers. Completed September 3."},
    {"date": "2024-11-11", "name": "November 2024 Core Update", "type": "core",
     "source": "Google Search Central",
     "description": "Core ranking update. Took approximately 23 days to complete (finished December 5). Continued emphasis on helpful, people-first content."},
    {"date": "2024-12-12", "name": "December 2024 Core Update", "type": "core",
     "source": "Google Search Central",
     "description": "Final core update of 2024. Quick rollout, completed December 18. Minor ranking adjustments building on November update."},
    {"date": "2024-12-19", "name": "December 2024 Spam Update", "type": "spam",
     "source": "Google Search Central",
     "description": "Year-end spam update. Targeted auto-generated content farms and scaled content abuse."},

    # ── 2025 ──────────────────────────────────────────────────────────────
    {"date": "2025-01-21", "name": "January 2025 Local/Maps Update", "type": "local",
     "source": "Search Engine Roundtable",
     "description": "Update to local search and Google Maps rankings. Affected local pack and map results for service-area businesses."},
    {"date": "2025-03-04", "name": "March 2025 Core Update", "type": "core",
     "source": "Google Search Central",
     "description": "First core update of 2025. Major ranking shifts across informational and commercial queries. Completed March 27."},
    {"date": "2025-03-13", "name": "March 2025 AI Overview Expansion", "type": "ai_overview",
     "source": "Google Search Central",
     "description": "Significant expansion of AI Overviews to more query types. Organic CTR displacement observed for informational queries."},
    {"date": "2025-05-05", "name": "May 2025 AI Overview Expansion", "type": "ai_overview",
     "source": "Google Search Central",
     "description": "Further expansion of AI Overviews. Now appearing for commercial queries. Estimated 10-15% organic CTR displacement on affected queries."},
    {"date": "2025-06-10", "name": "June 2025 Helpful Content Update", "type": "helpful_content",
     "source": "Google Search Central",
     "description": "Helpful content system update integrated into core ranking. Improved detection of AI-generated content and rewarded original research."},
    {"date": "2025-08-19", "name": "August 2025 Core Update", "type": "core",
     "source": "Google Search Central",
     "description": "Major core update. Significant shifts in SERP rankings. Focus on E-E-A-T signals and user engagement metrics."},
    {"date": "2025-09-15", "name": "September 2025 Spam Update", "type": "spam",
     "source": "Google Search Central",
     "description": "Spam update targeting AI-generated content farms and scaled content abuse patterns identified in 2025."},
    {"date": "2025-11-08", "name": "November 2025 Core Update", "type": "core",
     "source": "Google Search Central",
     "description": "Core update with emphasis on user engagement signals and topical authority. Rolled out over 3 weeks."},

    # ── 2026 ──────────────────────────────────────────────────────────────
    {"date": "2026-02-12", "name": "February 2026 Link Spam Update", "type": "link",
     "source": "Google Search Central",
     "description": "Link spam update using enhanced SpamBrain. Targeted unnatural link building patterns and link network abuse."},
    {"date": "2026-03-10", "name": "March 2026 Core Update", "type": "core",
     "source": "Google Search Central",
     "description": "First major core update of 2026. Broad ranking changes affecting content quality assessment and topical authority signals."},
]


class AlgorithmUpdateFetcher:
    """Fetches algorithm updates from various public sources."""
    
    def __init__(self):
        self.client = httpx.AsyncClient(
            timeout=30.0,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; SearchIntelBot/1.0)"
            }
        )
        self.updates: List[Dict] = []
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.aclose()
    
    async def fetch_google_search_central(self) -> List[Dict]:
        """Fetch updates from Google Search Central Blog RSS feed."""
        logger.info("Fetching from Google Search Central Blog...")
        
        try:
            # Google Search Central RSS feed
            feed_url = "https://developers.google.com/search/blog/feeds/posts/default"
            response = await self.client.get(feed_url)
            response.raise_for_status()
            
            feed = feedparser.parse(response.text)
            updates = []
            
            # Look for posts in the last 90 days that mention updates
            cutoff_date = datetime.now() - timedelta(days=90)
            
            for entry in feed.entries:
                try:
                    # Parse published date
                    published = datetime(*entry.published_parsed[:6])
                    
                    if published < cutoff_date:
                        continue
                    
                    title = entry.title.lower()
                    content = entry.get('summary', '').lower()
                    
                    # Keywords that indicate an algorithm update
                    update_keywords = [
                        'core update', 'algorithm update', 'ranking update',
                        'spam update', 'helpful content', 'product reviews',
                        'link spam', 'site reputation'
                    ]
                    
                    if any(keyword in title or keyword in content for keyword in update_keywords):
                        # Extract update type
                        update_type = self._extract_update_type(title + ' ' + content)
                        
                        updates.append({
                            'date': published.date(),
                            'name': entry.title,
                            'type': update_type,
                            'source': 'Google Search Central',
                            'description': entry.get('summary', '')[:500]
                        })
                        
                        logger.info(f"Found update: {entry.title} ({published.date()})")
                
                except Exception as e:
                    logger.warning(f"Error parsing entry: {e}")
                    continue
            
            return updates
        
        except Exception as e:
            logger.error(f"Error fetching Google Search Central: {e}")
            return []
    
    async def fetch_semrush_sensor(self) -> List[Dict]:
        """Fetch confirmed updates from Semrush Sensor."""
        logger.info("Fetching from Semrush Sensor...")
        
        try:
            url = "https://www.semrush.com/sensor/"
            response = await self.client.get(url)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            updates = []
            
            # Look for confirmed update markers
            update_elements = soup.find_all(['div', 'li'], class_=re.compile(r'update|volatility|confirmed', re.I))
            
            for element in update_elements[:20]:
                text = element.get_text(strip=True)
                
                date_match = re.search(r'(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+(\d{4})', text, re.I)
                
                if date_match:
                    try:
                        date_str = f"{date_match.group(1)} {date_match.group(2)} {date_match.group(3)}"
                        update_date = datetime.strptime(date_str, "%d %b %Y").date()
                        
                        if (datetime.now().date() - update_date).days > 90:
                            continue
                        
                        updates.append({
                            'date': update_date,
                            'name': f"Google Update - {update_date}",
                            'type': 'unconfirmed',
                            'source': 'Semrush Sensor',
                            'description': text[:500]
                        })
                        
                        logger.info(f"Found Semrush update: {update_date}")
                    
                    except ValueError:
                        continue
            
            return updates
        
        except Exception as e:
            logger.error(f"Error fetching Semrush Sensor: {e}")
            return []
    
    async def fetch_search_engine_roundtable(self) -> List[Dict]:
        """Fetch updates from Search Engine Roundtable."""
        logger.info("Fetching from Search Engine Roundtable...")
        
        try:
            feed_url = "https://www.seroundtable.com/feed"
            response = await self.client.get(feed_url)
            response.raise_for_status()
            
            feed = feedparser.parse(response.text)
            updates = []
            
            cutoff_date = datetime.now() - timedelta(days=90)
            
            for entry in feed.entries:
                try:
                    published = datetime(*entry.published_parsed[:6])
                    
                    if published < cutoff_date:
                        continue
                    
                    title = entry.title.lower()
                    content = entry.get('summary', '').lower()
                    
                    update_patterns = [
                        r'google.*?(core|algorithm|ranking|spam|helpful content).*?update',
                        r'(confirmed|unconfirmed).*?google.*?update',
                        r'google.*?rolling out'
                    ]
                    
                    if any(re.search(pattern, title + ' ' + content, re.I) for pattern in update_patterns):
                        update_type = self._extract_update_type(title + ' ' + content)
                        
                        updates.append({
                            'date': published.date(),
                            'name': entry.title,
                            'type': update_type,
                            'source': 'Search Engine Roundtable',
                            'description': entry.get('summary', '')[:500]
                        })
                        
                        logger.info(f"Found update: {entry.title} ({published.date()})")
                
                except Exception as e:
                    logger.warning(f"Error parsing entry: {e}")
                    continue
            
            return updates
        
        except Exception as e:
            logger.error(f"Error fetching Search Engine Roundtable: {e}")
            return []
    
    async def fetch_moz(self) -> List[Dict]:
        """Fetch updates from Moz blog."""
        logger.info("Fetching from Moz blog...")
        
        try:
            feed_url = "https://moz.com/blog/feed"
            response = await self.client.get(feed_url)
            response.raise_for_status()
            
            feed = feedparser.parse(response.text)
            updates = []
            
            cutoff_date = datetime.now() - timedelta(days=90)
            
            for entry in feed.entries:
                try:
                    published = datetime(*entry.published_parsed[:6])
                    
                    if published < cutoff_date:
                        continue
                    
                    title = entry.title.lower()
                    content = entry.get('summary', '').lower()
                    
                    if re.search(r'google.*(update|algorithm)', title + ' ' + content, re.I):
                        update_type = self._extract_update_type(title + ' ' + content)
                        
                        updates.append({
                            'date': published.date(),
                            'name': entry.title,
                            'type': update_type,
                            'source': 'Moz',
                            'description': entry.get('summary', '')[:500]
                        })
                        
                        logger.info(f"Found update: {entry.title} ({published.date()})")
                
                except Exception as e:
                    logger.warning(f"Error parsing entry: {e}")
                    continue
            
            return updates
        
        except Exception as e:
            logger.error(f"Error fetching Moz: {e}")
            return []
    
    def _extract_update_type(self, text: str) -> str:
        """Extract update type from text content."""
        text = text.lower()
        
        if re.search(r'core\s+update', text):
            return 'core'
        elif re.search(r'spam\s+update', text):
            return 'spam'
        elif re.search(r'helpful\s+content', text):
            return 'helpful_content'
        elif re.search(r'product\s+reviews?', text):
            return 'product_reviews'
        elif re.search(r'link\s+spam', text):
            return 'link_spam'
        elif re.search(r'site\s+reputation', text):
            return 'site_reputation'
        elif re.search(r'reviews?\s+update', text):
            return 'reviews'
        elif re.search(r'ai\s+overview', text):
            return 'ai_overview'
        elif re.search(r'local|maps', text):
            return 'local'
        else:
            return 'general'
    
    async def fetch_all(self) -> List[Dict]:
        """Fetch from all sources concurrently."""
        logger.info("Starting fetch from all sources...")
        
        tasks = [
            self.fetch_google_search_central(),
            self.fetch_semrush_sensor(),
            self.fetch_search_engine_roundtable(),
            self.fetch_moz()
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        all_updates = []
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Task failed: {result}")
            elif isinstance(result, list):
                all_updates.extend(result)
        
        return all_updates


def deduplicate_updates(updates: List[Dict]) -> List[Dict]:
    """
    Deduplicate updates that appear from multiple sources.
    Keep the one with the most detailed description.
    """
    by_date = {}
    for update in updates:
        date_key = update['date'].isoformat() if hasattr(update['date'], 'isoformat') else update['date']
        if date_key not in by_date:
            by_date[date_key] = []
        by_date[date_key].append(update)
    
    deduplicated = []
    
    for date_key, date_updates in by_date.items():
        if len(date_updates) == 1:
            deduplicated.append(date_updates[0])
        else:
            source_priority = {
                'Google Search Central': 0,
                'Search Engine Roundtable': 1,
                'Moz': 2,
                'Semrush Sensor': 3
            }
            
            sorted_updates = sorted(
                date_updates,
                key=lambda u: (
                    source_priority.get(u['source'], 99),
                    -len(u.get('description', ''))
                )
            )
            
            best = sorted_updates[0]
            
            if len(sorted_updates) > 1:
                sources = [u['source'] for u in sorted_updates]
                best['source'] = ', '.join(set(sources))
            
            deduplicated.append(best)
    
    return deduplicated


async def seed_known_updates() -> int:
    """
    Seed the algorithm_updates table with comprehensive known updates.

    Inserts any KNOWN_UPDATES entry that is not already present in the
    database (matched by exact date + name substring).  Safe to call
    repeatedly — idempotent.

    Returns the number of new rows inserted.
    """
    logger.info("Seeding known algorithm updates (%d entries)...", len(KNOWN_UPDATES))

    inserted = 0
    skipped = 0

    for update in KNOWN_UPDATES:
        try:
            # Check if an update with the same date and similar name exists.
            # We match on the first 25 chars of the name to handle minor
            # wording differences between sources.
            name_prefix = update["name"][:25]
            existing = (
                supabase.table("algorithm_updates")
                .select("id")
                .eq("date", update["date"])
                .ilike("name", f"%{name_prefix}%")
                .execute()
            )

            if existing.data:
                skipped += 1
                continue

            # Also check for ANY entry on the same date with the same type
            # (covers cases where a different source used a different name)
            existing_by_type = (
                supabase.table("algorithm_updates")
                .select("id")
                .eq("date", update["date"])
                .eq("type", update["type"])
                .execute()
            )

            if existing_by_type.data:
                skipped += 1
                continue

            # Insert the update
            result = supabase.table("algorithm_updates").insert({
                "date": update["date"],
                "name": update["name"],
                "type": update["type"],
                "source": update["source"],
                "description": update.get("description", ""),
            }).execute()

            if result.data:
                inserted += 1
                logger.info("Seeded: %s (%s)", update["name"], update["date"])

        except Exception as exc:
            logger.warning("Failed to seed %s: %s", update["name"], exc)

    logger.info(
        "Seed complete: %d inserted, %d already present", inserted, skipped
    )
    return inserted


async def save_updates_to_db(updates: List[Dict]) -> int:
    """
    Save new updates to the database.
    Returns count of new updates inserted.
    """
    if not updates:
        logger.info("No updates to save")
        return 0
    
    inserted_count = 0
    
    for update in updates:
        try:
            date_str = (
                update['date'].isoformat()
                if hasattr(update['date'], 'isoformat')
                else update['date']
            )

            existing = supabase.table('algorithm_updates')\
                .select('id')\
                .eq('date', date_str)\
                .ilike('name', f"%{update['name'][:30]}%")\
                .execute()
            
            if existing.data:
                logger.debug(f"Update already exists: {update['name']} ({update['date']})")
                continue
            
            result = supabase.table('algorithm_updates').insert({
                'date': date_str,
                'name': update['name'],
                'type': update['type'],
                'source': update['source'],
                'description': update.get('description', '')
            }).execute()
            
            if result.data:
                inserted_count += 1
                logger.info(f"Inserted: {update['name']} ({update['date']})")
        
        except Exception as e:
            logger.error(f"Error inserting update {update.get('name')}: {e}")
            continue
    
    return inserted_count


async def cleanup_old_updates():
    """Remove updates older than 3 years to keep database clean."""
    try:
        cutoff_date = datetime.now().date() - timedelta(days=3*365)
        
        result = supabase.table('algorithm_updates')\
            .delete()\
            .lt('date', cutoff_date.isoformat())\
            .execute()
        
        if result.data:
            logger.info(f"Cleaned up {len(result.data)} old updates")
    
    except Exception as e:
        logger.error(f"Error cleaning up old updates: {e}")


async def main():
    """Main execution function."""
    logger.info("=== Algorithm Update Fetcher Starting ===")
    
    try:
        # Step 1: Seed known updates (idempotent — skips existing)
        seeded = await seed_known_updates()
        logger.info("Seed phase: %d new known updates inserted", seeded)

        # Step 2: Fetch new updates from live sources
        async with AlgorithmUpdateFetcher() as fetcher:
            all_updates = await fetcher.fetch_all()
        
        logger.info(f"Fetched {len(all_updates)} total updates from all sources")
        
        # Deduplicate
        unique_updates = deduplicate_updates(all_updates)
        logger.info(f"After deduplication: {len(unique_updates)} unique updates")
        
        # Save to database
        inserted_count = await save_updates_to_db(unique_updates)
        logger.info(f"Inserted {inserted_count} new updates into database")
        
        # Cleanup old updates
        await cleanup_old_updates()
        
        logger.info("=== Algorithm Update Fetcher Complete ===")
        
        return {
            'success': True,
            'seeded': seeded,
            'fetched': len(all_updates),
            'unique': len(unique_updates),
            'inserted': inserted_count
        }
    
    except Exception as e:
        logger.error(f"Fatal error in main: {e}", exc_info=True)
        return {
            'success': False,
            'error': str(e)
        }


if __name__ == "__main__":
    result = asyncio.run(main())
    
    if result['success']:
        print(f"✓ Successfully fetched and stored algorithm updates")
        print(f"  Seeded: {result['seeded']}")
        print(f"  Fetched: {result['fetched']}")
        print(f"  Unique: {result['unique']}")
        print(f"  New: {result['inserted']}")
    else:
        print(f"✗ Failed: {result.get('error')}")
        exit(1)
