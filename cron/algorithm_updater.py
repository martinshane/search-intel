"""
Weekly cron script that fetches new algorithm updates from public sources
and updates the database.

Sources:
- Google Search Central Blog (via RSS)
- Semrush Sensor (scrape confirmed updates)
- Search Engine Roundtable (scrape)
- Moz (scrape)

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
            # Note: This is a simplified scraper - actual implementation may need adjustments
            # based on Semrush's current HTML structure
            update_elements = soup.find_all(['div', 'li'], class_=re.compile(r'update|volatility|confirmed', re.I))
            
            for element in update_elements[:20]:  # Limit to recent updates
                text = element.get_text(strip=True)
                
                # Try to extract date from text
                date_match = re.search(r'(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+(\d{4})', text, re.I)
                
                if date_match:
                    try:
                        date_str = f"{date_match.group(1)} {date_match.group(2)} {date_match.group(3)}"
                        update_date = datetime.strptime(date_str, "%d %b %Y").date()
                        
                        # Only include updates from last 90 days
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
            # Search Engine Roundtable RSS feed
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
                    
                    # Look for Google update announcements
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
            # Moz blog RSS feed
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
                    
                    # Look for Google update content
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
    # Group by date
    by_date = {}
    for update in updates:
        date_key = update['date'].isoformat()
        if date_key not in by_date:
            by_date[date_key] = []
        by_date[date_key].append(update)
    
    deduplicated = []
    
    for date_key, date_updates in by_date.items():
        if len(date_updates) == 1:
            deduplicated.append(date_updates[0])
        else:
            # Multiple updates on same date - check if they're the same update
            # Keep the one with longest description and prefer official Google source
            
            # Sort by: 1) source priority, 2) description length
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
            
            # Check if top 2 are talking about the same update
            # (similar type and within 3 days)
            best = sorted_updates[0]
            
            # Merge descriptions if multiple sources confirm
            if len(sorted_updates) > 1:
                sources = [u['source'] for u in sorted_updates]
                best['source'] = ', '.join(set(sources))
            
            deduplicated.append(best)
    
    return deduplicated


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
            # Check if update already exists (by date and similar name)
            existing = supabase.table('algorithm_updates')\
                .select('id')\
                .eq('date', update['date'].isoformat())\
                .ilike('name', f"%{update['name'][:30]}%")\
                .execute()
            
            if existing.data:
                logger.debug(f"Update already exists: {update['name']} ({update['date']})")
                continue
            
            # Insert new update
            result = supabase.table('algorithm_updates').insert({
                'date': update['date'].isoformat(),
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
        # Fetch updates from all sources
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
        print(f"  Fetched: {result['fetched']}")
        print(f"  Unique: {result['unique']}")
        print(f"  New: {result['inserted']}")
    else:
        print(f"✗ Failed: {result.get('error')}")
        exit(1)
