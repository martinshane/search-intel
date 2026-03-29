"""
Module 7: Query Intent Migration Tracking

Tracks how search intent distribution evolves over time and estimates
AI Overview impact on informational queries.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import pandas as pd
import numpy as np
from collections import defaultdict
import hashlib
import json

logger = logging.getLogger(__name__)


class IntentMigrationAnalyzer:
    """Analyzes query intent migration patterns over time."""
    
    INTENT_TYPES = ["informational", "commercial", "navigational", "transactional"]
    
    # Intent keywords for rule-based classification fallback
    INTENT_PATTERNS = {
        "informational": [
            "what", "how", "why", "when", "where", "who",
            "guide", "tutorial", "learn", "example", "definition"
        ],
        "commercial": [
            "best", "top", "review", "vs", "versus", "compare",
            "alternative", "comparison", "cheapest", "affordable"
        ],
        "navigational": [
            "login", "sign in", "account", "portal", "dashboard",
            "official", "website"
        ],
        "transactional": [
            "buy", "price", "pricing", "cost", "purchase", "order",
            "discount", "coupon", "deal", "trial", "demo"
        ]
    }
    
    def __init__(self, llm_client=None, cache_db=None):
        """
        Initialize analyzer.
        
        Args:
            llm_client: Optional Claude API client for intent classification
            cache_db: Optional database connection for intent caching
        """
        self.llm_client = llm_client
        self.cache_db = cache_db
        self.intent_cache = {}
    
    def analyze(
        self,
        gsc_query_date_data: pd.DataFrame,
        brand_terms: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Analyze query intent migration over time.
        
        Args:
            gsc_query_date_data: DataFrame with columns [query, date, clicks, impressions, ctr, position]
            brand_terms: Optional list of brand terms to filter out
        
        Returns:
            Dict containing intent migration analysis results
        """
        try:
            logger.info("Starting intent migration analysis")
            
            # Validate input data
            required_columns = ["query", "date", "clicks", "impressions", "ctr"]
            if not all(col in gsc_query_date_data.columns for col in required_columns):
                raise ValueError(f"Missing required columns. Need: {required_columns}")
            
            if gsc_query_date_data.empty:
                logger.warning("Empty query data provided")
                return self._empty_result()
            
            # Prepare data
            df = gsc_query_date_data.copy()
            df["date"] = pd.to_datetime(df["date"])
            
            # Filter out branded queries if brand terms provided
            if brand_terms:
                df = self._filter_branded_queries(df, brand_terms)
            
            # Get unique queries for classification
            unique_queries = df["query"].unique().tolist()
            logger.info(f"Classifying {len(unique_queries)} unique queries")
            
            # Classify query intents
            query_intents = self._classify_queries(unique_queries)
            
            # Add intent to dataframe
            df["intent"] = df["query"].map(query_intents)
            
            # Calculate intent distribution over time
            intent_timeline = self._calculate_intent_timeline(df)
            
            # Detect AI Overview impact
            ai_impact = self._detect_ai_overview_impact(df, query_intents)
            
            # Calculate current vs historical distribution
            distribution_current = self._calculate_distribution(
                df[df["date"] >= df["date"].max() - timedelta(days=30)]
            )
            
            distribution_6mo_ago = self._calculate_distribution(
                df[
                    (df["date"] >= df["date"].max() - timedelta(days=210)) &
                    (df["date"] < df["date"].max() - timedelta(days=150))
                ]
            )
            
            # Generate strategic recommendation
            recommendation = self._generate_recommendation(
                distribution_current,
                distribution_6mo_ago,
                intent_timeline,
                ai_impact
            )
            
            # Build result
            result = {
                "intent_distribution_current": distribution_current,
                "intent_distribution_6mo_ago": distribution_6mo_ago,
                "intent_timeline": intent_timeline,
                "ai_overview_impact": ai_impact,
                "strategic_recommendation": recommendation,
                "queries_analyzed": len(unique_queries),
                "date_range": {
                    "start": df["date"].min().isoformat(),
                    "end": df["date"].max().isoformat()
                }
            }
            
            logger.info("Intent migration analysis complete")
            return result
            
        except Exception as e:
            logger.error(f"Error in intent migration analysis: {str(e)}")
            raise
    
    def _filter_branded_queries(
        self,
        df: pd.DataFrame,
        brand_terms: List[str]
    ) -> pd.DataFrame:
        """Filter out branded queries."""
        brand_pattern = "|".join([term.lower() for term in brand_terms])
        mask = ~df["query"].str.lower().str.contains(brand_pattern, na=False)
        return df[mask]
    
    def _classify_queries(self, queries: List[str]) -> Dict[str, str]:
        """
        Classify queries by intent.
        
        Uses LLM if available, falls back to rule-based classification.
        """
        query_intents = {}
        
        # Check cache first
        queries_to_classify = []
        for query in queries:
            cached_intent = self._get_cached_intent(query)
            if cached_intent:
                query_intents[query] = cached_intent
            else:
                queries_to_classify.append(query)
        
        if not queries_to_classify:
            return query_intents
        
        # Try LLM classification if available
        if self.llm_client:
            try:
                llm_intents = self._classify_with_llm(queries_to_classify)
                query_intents.update(llm_intents)
                
                # Cache results
                for query, intent in llm_intents.items():
                    self._cache_intent(query, intent)
                
                return query_intents
            except Exception as e:
                logger.warning(f"LLM classification failed: {str(e)}. Falling back to rule-based.")
        
        # Fallback to rule-based classification
        for query in queries_to_classify:
            intent = self._classify_rule_based(query)
            query_intents[query] = intent
            self._cache_intent(query, intent)
        
        return query_intents
    
    def _classify_with_llm(self, queries: List[str]) -> Dict[str, str]:
        """Classify queries using LLM in batches."""
        query_intents = {}
        batch_size = 50
        
        for i in range(0, len(queries), batch_size):
            batch = queries[i:i + batch_size]
            
            prompt = self._build_classification_prompt(batch)
            
            try:
                response = self.llm_client.messages.create(
                    model="claude-3-haiku-20240307",
                    max_tokens=2000,
                    temperature=0,
                    messages=[{"role": "user", "content": prompt}]
                )
                
                # Parse response
                content = response.content[0].text
                batch_intents = self._parse_classification_response(content, batch)
                query_intents.update(batch_intents)
                
            except Exception as e:
                logger.error(f"Error classifying batch: {str(e)}")
                # Fallback to rule-based for this batch
                for query in batch:
                    query_intents[query] = self._classify_rule_based(query)
        
        return query_intents
    
    def _build_classification_prompt(self, queries: List[str]) -> str:
        """Build prompt for LLM intent classification."""
        queries_text = "\n".join([f"{i+1}. {q}" for i, q in enumerate(queries)])
        
        return f"""Classify each search query by intent. Return ONLY a JSON object mapping query numbers to intent types.

Intent types:
- informational: User wants to learn/understand something
- commercial: User is researching/comparing before buying
- navigational: User wants to find a specific site/page
- transactional: User is ready to take action (buy, sign up, download)

Queries:
{queries_text}

Return format (JSON only, no explanation):
{{"1": "informational", "2": "commercial", ...}}"""
    
    def _parse_classification_response(
        self,
        response: str,
        queries: List[str]
    ) -> Dict[str, str]:
        """Parse LLM classification response."""
        try:
            # Extract JSON from response
            response = response.strip()
            if "```json" in response:
                response = response.split("```json")[1].split("```")[0]
            elif "```" in response:
                response = response.split("```")[1].split("```")[0]
            
            classifications = json.loads(response)
            
            # Map back to queries
            result = {}
            for i, query in enumerate(queries):
                key = str(i + 1)
                if key in classifications:
                    intent = classifications[key]
                    if intent in self.INTENT_TYPES:
                        result[query] = intent
                    else:
                        result[query] = self._classify_rule_based(query)
                else:
                    result[query] = self._classify_rule_based(query)
            
            return result
            
        except Exception as e:
            logger.error(f"Error parsing classification response: {str(e)}")
            return {q: self._classify_rule_based(q) for q in queries}
    
    def _classify_rule_based(self, query: str) -> str:
        """Rule-based intent classification using keyword patterns."""
        query_lower = query.lower()
        
        # Score each intent type
        scores = defaultdict(int)
        
        for intent, patterns in self.INTENT_PATTERNS.items():
            for pattern in patterns:
                if pattern in query_lower:
                    scores[intent] += 1
        
        # Return highest scoring intent, default to informational
        if not scores:
            return "informational"
        
        return max(scores.items(), key=lambda x: x[1])[0]
    
    def _calculate_intent_timeline(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Calculate intent distribution over time by month."""
        df = df.copy()
        df["month"] = df["date"].dt.to_period("M")
        
        timeline = []
        
        for month, month_data in df.groupby("month"):
            month_clicks = month_data.groupby("intent")["clicks"].sum()
            total_clicks = month_clicks.sum()
            
            if total_clicks == 0:
                continue
            
            distribution = {
                intent: float(month_clicks.get(intent, 0) / total_clicks)
                for intent in self.INTENT_TYPES
            }
            
            timeline.append({
                "month": str(month),
                "date": month.to_timestamp().isoformat(),
                "distribution": distribution,
                "total_clicks": int(total_clicks)
            })
        
        return sorted(timeline, key=lambda x: x["date"])
    
    def _detect_ai_overview_impact(
        self,
        df: pd.DataFrame,
        query_intents: Dict[str, str]
    ) -> Dict[str, Any]:
        """Detect potential AI Overview impact on informational queries."""
        # Focus on informational queries
        info_df = df[df["intent"] == "informational"].copy()
        
        if info_df.empty:
            return {
                "queries_affected": 0,
                "estimated_monthly_clicks_lost": 0,
                "affected_queries": []
            }
        
        # Group by query to analyze trends
        query_stats = []
        
        for query, query_data in info_df.groupby("query"):
            query_data = query_data.sort_values("date")
            
            # Split into old and recent periods
            midpoint = query_data["date"].min() + (
                query_data["date"].max() - query_data["date"].min()
            ) / 2
            
            old_data = query_data[query_data["date"] < midpoint]
            recent_data = query_data[query_data["date"] >= midpoint]
            
            if old_data.empty or recent_data.empty:
                continue
            
            # Calculate metrics
            old_impressions = old_data["impressions"].mean()
            recent_impressions = recent_data["impressions"].mean()
            old_ctr = old_data["ctr"].mean()
            recent_ctr = recent_data["ctr"].mean()
            
            # Check for AI Overview pattern: stable/growing impressions, declining CTR
            impressions_stable = (
                recent_impressions >= old_impressions * 0.9
            )  # Within 10%
            ctr_declining = recent_ctr < old_ctr * 0.85  # >15% decline
            
            if impressions_stable and ctr_declining:
                monthly_clicks_old = old_data["clicks"].mean() * 30
                monthly_clicks_recent = recent_data["clicks"].mean() * 30
                clicks_lost = max(0, monthly_clicks_old - monthly_clicks_recent)
                
                query_stats.append({
                    "query": query,
                    "impressions_old": float(old_impressions),
                    "impressions_recent": float(recent_impressions),
                    "ctr_old": float(old_ctr),
                    "ctr_recent": float(recent_ctr),
                    "ctr_decline_pct": float((old_ctr - recent_ctr) / old_ctr * 100),
                    "estimated_monthly_clicks_lost": float(clicks_lost)
                })
        
        # Sort by clicks lost
        query_stats.sort(key=lambda x: x["estimated_monthly_clicks_lost"], reverse=True)
        
        total_clicks_lost = sum(q["estimated_monthly_clicks_lost"] for q in query_stats)
        
        return {
            "queries_affected": len(query_stats),
            "estimated_monthly_clicks_lost": float(total_clicks_lost),
            "affected_queries": query_stats[:20]  # Top 20
        }
    
    def _calculate_distribution(self, df: pd.DataFrame) -> Dict[str, float]:
        """Calculate intent distribution for a time period."""
        if df.empty:
            return {intent: 0.0 for intent in self.INTENT_TYPES}
        
        intent_clicks = df.groupby("intent")["clicks"].sum()
        total_clicks = intent_clicks.sum()
        
        if total_clicks == 0:
            return {intent: 0.0 for intent in self.INTENT_TYPES}
        
        return {
            intent: float(intent_clicks.get(intent, 0) / total_clicks)
            for intent in self.INTENT_TYPES
        }
    
    def _generate_recommendation(
        self,
        current: Dict[str, float],
        historical: Dict[str, float],
        timeline: List[Dict[str, Any]],
        ai_impact: Dict[str, Any]
    ) -> str:
        """Generate strategic recommendation based on intent migration."""
        recommendations = []
        
        # Analyze informational decline
        info_change = current["informational"] - historical["informational"]
        if info_change < -0.10:  # >10% decline
            if ai_impact["queries_affected"] > 0:
                recommendations.append(
                    f"Informational queries have declined {abs(info_change)*100:.1f}% "
                    f"(from {historical['informational']*100:.1f}% to {current['informational']*100:.1f}% of clicks), "
                    f"with {ai_impact['queries_affected']} queries showing AI Overview displacement patterns. "
                    f"Consider pivoting content strategy toward commercial and transactional intent."
                )
            else:
                recommendations.append(
                    f"Informational queries have declined {abs(info_change)*100:.1f}%. "
                    f"This may indicate changing user behavior or market maturity. "
                    f"Evaluate whether your informational content needs updating or if you should shift focus."
                )
        
        # Analyze commercial growth
        commercial_change = current["commercial"] - historical["commercial"]
        if commercial_change > 0.05:  # >5% growth
            recommendations.append(
                f"Commercial intent queries are growing ({commercial_change*100:.1f}% increase). "
                f"This is a positive signal - users are researching your space. "
                f"Invest in comparison content, buying guides, and product reviews to capture this demand."
            )
        
        # Analyze transactional weakness
        if current["transactional"] < 0.15 and current["commercial"] > 0.25:
            recommendations.append(
                f"You have strong commercial intent traffic ({current['commercial']*100:.1f}%) "
                f"but low transactional ({current['transactional']*100:.1f}%). "
                f"Create more bottom-funnel content: pricing pages, product comparisons, "
                f"free trial offers, and calculator tools."
            )
        
        # Analyze navigational dominance
        if current["navigational"] > 0.50:
            recommendations.append(
                f"Navigational queries dominate your traffic ({current['navigational']*100:.1f}%). "
                f"Your brand is strong, but you're not capturing new users through discovery. "
                f"Build out non-branded content to reach users earlier in their journey."
            )
        
        # AI Overview specific recommendations
        if ai_impact["estimated_monthly_clicks_lost"] > 500:
            recommendations.append(
                f"AI Overviews are impacting approximately {ai_impact['queries_affected']} queries, "
                f"costing an estimated {ai_impact['estimated_monthly_clicks_lost']:.0f} clicks/month. "
                f"For affected queries, consider: (1) targeting featured snippets with structured content, "
                f"(2) adding unique data/research that AI can't generate, "
                f"(3) pivoting to commercial angles where AI Overviews are less prevalent."
            )
        
        # Default recommendation
        if not recommendations:
            recommendations.append(
                "Your intent distribution is relatively stable. "
                "Monitor for emerging shifts and maintain content balance across all intent types."
            )
        
        return " ".join(recommendations)
    
    def _get_cached_intent(self, query: str) -> Optional[str]:
        """Get cached intent classification for a query."""
        # Check in-memory cache first
        if query in self.intent_cache:
            return self.intent_cache[query]
        
        # Check database cache if available
        if self.cache_db:
            try:
                query_hash = hashlib.md5(query.encode()).hexdigest()
                result = self.cache_db.table("query_intents").select("intent").eq(
                    "query_hash", query_hash
                ).execute()
                
                if result.data:
                    intent = result.data[0]["intent"]
                    self.intent_cache[query] = intent
                    return intent
            except Exception as e:
                logger.warning(f"Error fetching cached intent: {str(e)}")
        
        return None
    
    def _cache_intent(self, query: str, intent: str) -> None:
        """Cache intent classification for a query."""
        # Store in memory
        self.intent_cache[query] = intent
        
        # Store in database if available
        if self.cache_db:
            try:
                query_hash = hashlib.md5(query.encode()).hexdigest()
                self.cache_db.table("query_intents").upsert({
                    "query_hash": query_hash,
                    "query": query,
                    "intent": intent,
                    "confidence": 0.85,  # Rule-based or LLM confidence
                    "classified_at": datetime.utcnow().isoformat()
                }).execute()
            except Exception as e:
                logger.warning(f"Error caching intent: {str(e)}")
    
    def _empty_result(self) -> Dict[str, Any]:
        """Return empty result structure."""
        return {
            "intent_distribution_current": {intent: 0.0 for intent in self.INTENT_TYPES},
            "intent_distribution_6mo_ago": {intent: 0.0 for intent in self.INTENT_TYPES},
            "intent_timeline": [],
            "ai_overview_impact": {
                "queries_affected": 0,
                "estimated_monthly_clicks_lost": 0,
                "affected_queries": []
            },
            "strategic_recommendation": "Insufficient data for intent migration analysis.",
            "queries_analyzed": 0,
            "date_range": {"start": None, "end": None}
        }


def analyze_intent_migration(
    gsc_query_date_data: pd.DataFrame,
    brand_terms: Optional[List[str]] = None,
    llm_client=None,
    cache_db=None
) -> Dict[str, Any]:
    """
    Convenience function for intent migration analysis.
    
    Args:
        gsc_query_date_data: DataFrame with GSC query data by date
        brand_terms: Optional list of brand terms to filter out
        llm_client: Optional Claude API client
        cache_db: Optional Supabase client for caching
    
    Returns:
        Intent migration analysis results
    """
    analyzer = IntentMigrationAnalyzer(llm_client=llm_client, cache_db=cache_db)
    return analyzer.analyze(gsc_query_date_data, brand_terms=brand_terms)
