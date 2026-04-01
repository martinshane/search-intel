"""
Module 8: SERP CTR Modeling

Analyzes SERP data to calculate expected vs actual CTR based on position and features,
identifies CTR optimization opportunities, and provides structured insights about which
queries have CTR gaps and why.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class SERPCTRModeler:
    """
    Models CTR expectations based on SERP features and position,
    identifies gaps between expected and actual CTR.
    """
    
    # Baseline CTR curves by position (organic, no features)
    # Based on industry averages (Advanced Web Ranking 2024)
    BASELINE_CTR = {
        1: 0.396,
        2: 0.181,
        3: 0.103,
        4: 0.067,
        5: 0.048,
        6: 0.037,
        7: 0.029,
        8: 0.024,
        9: 0.020,
        10: 0.017,
        11: 0.012,
        12: 0.010,
        13: 0.008,
        14: 0.007,
        15: 0.006,
        16: 0.005,
        17: 0.004,
        18: 0.004,
        19: 0.003,
        20: 0.003
    }
    
    # SERP feature impact multipliers
    # Negative = reduces CTR, positive = increases CTR
    FEATURE_IMPACT = {
        'featured_snippet': {
            'user_owns': 1.8,  # Owning featured snippet dramatically increases CTR
            'competitor_owns': 0.65,  # Competitor snippet reduces CTR for positions below
            'visual_displacement': -0.15  # Per position displaced visually
        },
        'ai_overview': {
            'present': 0.70,  # AI overview reduces all organic CTR
            'user_cited': 1.15  # Being cited in AI overview helps
        },
        'people_also_ask': {
            'per_question': 0.96,  # Each PAA question reduces CTR by 4%
            'user_appears': 1.10  # Appearing in PAA helps
        },
        'knowledge_panel': {
            'present': 0.88,
            'user_owns': 1.25
        },
        'local_pack': {
            'present': 0.75,  # Local pack significantly reduces organic CTR
            'user_in_pack': 1.40
        },
        'video_carousel': {
            'present': 0.92,
            'user_video': 1.15
        },
        'image_pack': {
            'present': 0.95,
            'user_image': 1.08
        },
        'shopping_results': {
            'present': 0.82,
            'user_product': 1.20
        },
        'top_stories': {
            'present': 0.90,
            'user_in_stories': 1.30
        },
        'reddit_threads': {
            'present': 0.93
        },
        'twitter_carousel': {
            'present': 0.94
        }
    }
    
    # Intent-based CTR adjustments
    INTENT_MULTIPLIERS = {
        'navigational': 1.4,  # High CTR for branded/navigational queries
        'transactional': 1.1,  # Slightly higher CTR for buyer intent
        'commercial': 1.0,  # Baseline
        'informational': 0.95  # Slightly lower CTR
    }
    
    # Title/description quality factors
    QUALITY_FACTORS = {
        'has_number': 1.08,
        'has_year': 1.12,
        'has_question': 1.06,
        'has_power_word': 1.10,  # best, top, ultimate, complete, etc.
        'exact_match': 1.15,  # Title contains exact query
        'truncated': 0.92,  # Title or description truncated
        'poor_description': 0.88  # Missing or thin meta description
    }

    def __init__(self, gsc_data: pd.DataFrame, serp_data: List[Dict], 
                 crawl_data: Optional[pd.DataFrame] = None):
        """
        Initialize the SERP CTR modeler.
        
        Args:
            gsc_data: GSC performance data with query, clicks, impressions, ctr, position
            serp_data: Live SERP data from DataForSEO
            crawl_data: Optional crawl data with title, description, etc.
        """
        self.gsc_data = gsc_data
        self.serp_data = serp_data
        self.crawl_data = crawl_data
        
        # Build SERP lookup
        self.serp_lookup = {item['keyword']: item for item in serp_data}
        
        # Build crawl lookup if available
        self.crawl_lookup = {}
        if crawl_data is not None and not crawl_data.empty:
            self.crawl_lookup = crawl_data.set_index('url').to_dict('index')

    def calculate_expected_ctr(self, keyword: str, position: float, 
                               user_url: str) -> Dict[str, Any]:
        """
        Calculate expected CTR based on position, SERP features, and page quality.
        
        Returns:
            {
                'baseline_ctr': float,
                'feature_adjusted_ctr': float,
                'quality_adjusted_ctr': float,
                'final_expected_ctr': float,
                'adjustments': [...],
                'visual_position': int
            }
        """
        # Start with baseline CTR for position
        position_int = max(1, min(20, round(position)))
        baseline_ctr = self.BASELINE_CTR.get(position_int, 0.002)
        
        adjustments = []
        ctr_multiplier = 1.0
        visual_position = position_int
        
        # Get SERP data for this keyword
        serp = self.serp_lookup.get(keyword)
        
        if serp:
            # Analyze SERP features
            features = serp.get('features', {})
            organic_results = serp.get('organic_results', [])
            
            # Find user's result
            user_result = None
            user_rank = None
            for rank, result in enumerate(organic_results, 1):
                if result.get('url', '').startswith(user_url) or user_url in result.get('url', ''):
                    user_result = result
                    user_rank = rank
                    break
            
            # Featured snippet impact
            if features.get('featured_snippet'):
                snippet = features['featured_snippet']
                if snippet.get('url', '').startswith(user_url):
                    # User owns the snippet
                    snippet_multiplier = self.FEATURE_IMPACT['featured_snippet']['user_owns']
                    ctr_multiplier *= snippet_multiplier
                    adjustments.append({
                        'feature': 'featured_snippet_owned',
                        'impact': snippet_multiplier,
                        'description': 'Owns featured snippet'
                    })
                else:
                    # Competitor owns snippet
                    snippet_multiplier = self.FEATURE_IMPACT['featured_snippet']['competitor_owns']
                    ctr_multiplier *= snippet_multiplier
                    visual_position += 2  # Snippet pushes results down
                    adjustments.append({
                        'feature': 'featured_snippet_competitor',
                        'impact': snippet_multiplier,
                        'description': 'Competitor owns featured snippet'
                    })
            
            # AI Overview impact
            if features.get('ai_overview'):
                ai_overview = features['ai_overview']
                citations = ai_overview.get('citations', [])
                user_cited = any(user_url in citation for citation in citations)
                
                if user_cited:
                    ai_multiplier = self.FEATURE_IMPACT['ai_overview']['user_cited']
                    adjustments.append({
                        'feature': 'ai_overview_cited',
                        'impact': ai_multiplier,
                        'description': 'Cited in AI Overview'
                    })
                else:
                    ai_multiplier = self.FEATURE_IMPACT['ai_overview']['present']
                    adjustments.append({
                        'feature': 'ai_overview_present',
                        'impact': ai_multiplier,
                        'description': 'AI Overview present (not cited)'
                    })
                
                ctr_multiplier *= ai_multiplier
                visual_position += 3  # AI overview significantly displaces
            
            # People Also Ask impact
            paa_count = features.get('people_also_ask_count', 0)
            if paa_count > 0:
                paa_multiplier = self.FEATURE_IMPACT['people_also_ask']['per_question'] ** paa_count
                ctr_multiplier *= paa_multiplier
                visual_position += int(paa_count * 0.5)  # Each PAA ~0.5 positions
                adjustments.append({
                    'feature': 'people_also_ask',
                    'impact': paa_multiplier,
                    'description': f'{paa_count} PAA questions present'
                })
            
            # Local Pack impact
            if features.get('local_pack'):
                local_pack = features['local_pack']
                user_in_pack = any(user_url in item.get('url', '') 
                                  for item in local_pack.get('items', []))
                
                if user_in_pack:
                    local_multiplier = self.FEATURE_IMPACT['local_pack']['user_in_pack']
                    adjustments.append({
                        'feature': 'local_pack_user',
                        'impact': local_multiplier,
                        'description': 'Appears in local pack'
                    })
                else:
                    local_multiplier = self.FEATURE_IMPACT['local_pack']['present']
                    visual_position += 3
                    adjustments.append({
                        'feature': 'local_pack_present',
                        'impact': local_multiplier,
                        'description': 'Local pack present (not in it)'
                    })
                
                ctr_multiplier *= local_multiplier
            
            # Video carousel impact
            if features.get('video_carousel'):
                videos = features['video_carousel']
                user_has_video = any(user_url in video.get('url', '') 
                                    for video in videos)
                
                if user_has_video:
                    video_multiplier = self.FEATURE_IMPACT['video_carousel']['user_video']
                    adjustments.append({
                        'feature': 'video_carousel_user',
                        'impact': video_multiplier,
                        'description': 'Video in carousel'
                    })
                else:
                    video_multiplier = self.FEATURE_IMPACT['video_carousel']['present']
                    visual_position += 1
                    adjustments.append({
                        'feature': 'video_carousel_present',
                        'impact': video_multiplier,
                        'description': 'Video carousel present'
                    })
                
                ctr_multiplier *= video_multiplier
            
            # Image pack impact
            if features.get('image_pack'):
                image_pack = features['image_pack']
                user_has_image = any(user_url in img.get('source_url', '') 
                                    for img in image_pack.get('images', []))
                
                if user_has_image:
                    img_multiplier = self.FEATURE_IMPACT['image_pack']['user_image']
                    adjustments.append({
                        'feature': 'image_pack_user',
                        'impact': img_multiplier,
                        'description': 'Image in pack'
                    })
                else:
                    img_multiplier = self.FEATURE_IMPACT['image_pack']['present']
                    visual_position += 1
                    adjustments.append({
                        'feature': 'image_pack_present',
                        'impact': img_multiplier,
                        'description': 'Image pack present'
                    })
                
                ctr_multiplier *= img_multiplier
            
            # Shopping results impact
            if features.get('shopping_results'):
                shopping = features['shopping_results']
                user_has_product = any(user_url in prod.get('url', '') 
                                      for prod in shopping.get('items', []))
                
                if user_has_product:
                    shop_multiplier = self.FEATURE_IMPACT['shopping_results']['user_product']
                    adjustments.append({
                        'feature': 'shopping_user',
                        'impact': shop_multiplier,
                        'description': 'Product in shopping results'
                    })
                else:
                    shop_multiplier = self.FEATURE_IMPACT['shopping_results']['present']
                    visual_position += 2
                    adjustments.append({
                        'feature': 'shopping_present',
                        'impact': shop_multiplier,
                        'description': 'Shopping results present'
                    })
                
                ctr_multiplier *= shop_multiplier
            
            # Top Stories impact
            if features.get('top_stories'):
                stories = features['top_stories']
                user_in_stories = any(user_url in story.get('url', '') 
                                     for story in stories.get('items', []))
                
                if user_in_stories:
                    stories_multiplier = self.FEATURE_IMPACT['top_stories']['user_in_stories']
                    adjustments.append({
                        'feature': 'top_stories_user',
                        'impact': stories_multiplier,
                        'description': 'Appears in Top Stories'
                    })
                else:
                    stories_multiplier = self.FEATURE_IMPACT['top_stories']['present']
                    visual_position += 2
                    adjustments.append({
                        'feature': 'top_stories_present',
                        'impact': stories_multiplier,
                        'description': 'Top Stories present'
                    })
                
                ctr_multiplier *= stories_multiplier
            
            # Reddit threads impact
            if features.get('reddit_threads'):
                reddit_multiplier = self.FEATURE_IMPACT['reddit_threads']['present']
                ctr_multiplier *= reddit_multiplier
                visual_position += 1
                adjustments.append({
                    'feature': 'reddit_threads',
                    'impact': reddit_multiplier,
                    'description': 'Reddit threads present'
                })
            
            # Knowledge panel impact
            if features.get('knowledge_panel'):
                kp = features['knowledge_panel']
                user_owns_kp = user_url in kp.get('url', '')
                
                if user_owns_kp:
                    kp_multiplier = self.FEATURE_IMPACT['knowledge_panel']['user_owns']
                    adjustments.append({
                        'feature': 'knowledge_panel_owned',
                        'impact': kp_multiplier,
                        'description': 'Owns knowledge panel'
                    })
                else:
                    kp_multiplier = self.FEATURE_IMPACT['knowledge_panel']['present']
                    adjustments.append({
                        'feature': 'knowledge_panel_present',
                        'impact': kp_multiplier,
                        'description': 'Knowledge panel present'
                    })
                
                ctr_multiplier *= kp_multiplier
            
            # Intent-based adjustment
            intent = serp.get('intent', 'commercial')
            intent_multiplier = self.INTENT_MULTIPLIERS.get(intent, 1.0)
            if intent_multiplier != 1.0:
                ctr_multiplier *= intent_multiplier
                adjustments.append({
                    'feature': f'intent_{intent}',
                    'impact': intent_multiplier,
                    'description': f'Search intent: {intent}'
                })
        
        feature_adjusted_ctr = baseline_ctr * ctr_multiplier
        
        # Quality adjustments based on title/description
        quality_multiplier = 1.0
        quality_adjustments = []
        
        if user_url in self.crawl_lookup:
            page_data = self.crawl_lookup[user_url]
            title = page_data.get('title', '').lower()
            description = page_data.get('meta_description', '').lower()
            keyword_lower = keyword.lower()
            
            # Check for power words
            power_words = ['best', 'top', 'ultimate', 'complete', 'definitive', 
                          'essential', 'perfect', 'ultimate', 'comprehensive']
            if any(word in title for word in power_words):
                quality_multiplier *= self.QUALITY_FACTORS['has_power_word']
                quality_adjustments.append({
                    'factor': 'power_word',
                    'impact': self.QUALITY_FACTORS['has_power_word'],
                    'description': 'Power word in title'
                })
            
            # Check for numbers
            if any(char.isdigit() for char in title):
                quality_multiplier *= self.QUALITY_FACTORS['has_number']
                quality_adjustments.append({
                    'factor': 'has_number',
                    'impact': self.QUALITY_FACTORS['has_number'],
                    'description': 'Number in title'
                })
            
            # Check for current year
            current_year = str(datetime.now().year)
            if current_year in title:
                quality_multiplier *= self.QUALITY_FACTORS['has_year']
                quality_adjustments.append({
                    'factor': 'has_year',
                    'impact': self.QUALITY_FACTORS['has_year'],
                    'description': 'Current year in title'
                })
            
            # Check for question format
            if any(q in title for q in ['how', 'what', 'why', 'when', 'where', 'who']):
                quality_multiplier *= self.QUALITY_FACTORS['has_question']
                quality_adjustments.append({
                    'factor': 'has_question',
                    'impact': self.QUALITY_FACTORS['has_question'],
                    'description': 'Question format'
                })
            
            # Check for exact match
            if keyword_lower in title:
                quality_multiplier *= self.QUALITY_FACTORS['exact_match']
                quality_adjustments.append({
                    'factor': 'exact_match',
                    'impact': self.QUALITY_FACTORS['exact_match'],
                    'description': 'Exact keyword match in title'
                })
            
            # Check for truncation
            if len(title) > 60:
                quality_multiplier *= self.QUALITY_FACTORS['truncated']
                quality_adjustments.append({
                    'factor': 'truncated_title',
                    'impact': self.QUALITY_FACTORS['truncated'],
                    'description': 'Title likely truncated in SERPs'
                })
            
            # Check for poor description
            if not description or len(description) < 50:
                quality_multiplier *= self.QUALITY_FACTORS['poor_description']
                quality_adjustments.append({
                    'factor': 'poor_description',
                    'impact': self.QUALITY_FACTORS['poor_description'],
                    'description': 'Missing or thin meta description'
                })
        
        final_expected_ctr = feature_adjusted_ctr * quality_multiplier
        
        return {
            'baseline_ctr': baseline_ctr,
            'feature_adjusted_ctr': feature_adjusted_ctr,
            'quality_adjusted_ctr': final_expected_ctr,
            'final_expected_ctr': final_expected_ctr,
            'feature_adjustments': adjustments,
            'quality_adjustments': quality_adjustments,
            'visual_position': visual_position,
            'organic_position': position_int,
            'total_multiplier': ctr_multiplier * quality_multiplier
        }

    def identify_ctr_gaps(self, min_impressions: int = 100) -> List[Dict[str, Any]]:
        """
        Identify queries with significant CTR gaps between expected and actual.
        
        Returns list of queries with CTR gaps, sorted by opportunity size.
        """
        gaps = []
        
        for _, row in self.gsc_data.iterrows():
            keyword = row.get('query', '')
            actual_ctr = row.get('ctr', 0)
            position = row.get('position', 20)
            impressions = row.get('impressions', 0)
            clicks = row.get('clicks', 0)
            page = row.get('page', '')
            
            # Skip low-impression queries
            if impressions < min_impressions:
                continue
            
            # Calculate expected CTR
            expected_data = self.calculate_expected_ctr(keyword, position, page)
            expected_ctr = expected_data['final_expected_ctr']
            
            # Calculate gap
            ctr_gap = actual_ctr - expected_ctr
            ctr_gap_pct = (ctr_gap / expected_ctr * 100) if expected_ctr > 0 else 0
            
            # Calculate opportunity (missed clicks per month)
            missed_clicks = impressions * ctr_gap if ctr_gap < 0 else 0
            gained_clicks = impressions * ctr_gap if ctr_gap > 0 else 0
            
            # Determine primary issue
            primary_issue = self._determine_primary_issue(
                ctr_gap_pct, 
                expected_data,
                page
            )
            
            gap_data = {
                'keyword': keyword,
                'page': page,
                'position': position,
                'visual_position': expected_data['visual_position'],
                'impressions': impressions,
                'clicks': clicks,
                'actual_ctr': actual_ctr,
                'expected_ctr': expected_ctr,
                'ctr_gap': ctr_gap,
                'ctr_gap_pct': ctr_gap_pct,
                'missed_clicks_monthly': abs(missed_clicks),
                'opportunity_score': abs(missed_clicks) * (1 if ctr_gap < 0 else 0.5),
                'primary_issue': primary_issue,
                'feature_adjustments': expected_data['feature_adjustments'],
                'quality_adjustments': expected_data['quality_adjustments'],
                'recommendation': self._generate_recommendation(
                    primary_issue,
                    expected_data,
                    ctr_gap_pct
                )
            }
            
            gaps.append(gap_data)
        
        # Sort by opportunity score
        gaps.sort(key=lambda x: x['opportunity_score'], reverse=True)
        
        return gaps

    def _determine_primary_issue(self, ctr_gap_pct: float, 
                                 expected_data: Dict, 
                                 page: str) -> str:
        """
        Determine the primary issue causing CTR underperformance.
        """
        if ctr_gap_pct >= -5:
            return 'performing_well'
        
        # Check for major SERP feature displacement
        visual_gap = expected_data['visual_position'] - expected_data['organic_position']
        if visual_gap >= 3:
            return 'serp_feature_displacement'
        
        # Check for quality issues
        quality_adjustments = expected_data.get('quality_adjustments', [])
        negative_quality = [adj for adj in quality_adjustments if adj['impact'] < 1.0]
        
        if len(negative_quality) >= 2:
            return 'title_description_quality'
        
        # Check for specific feature issues
        feature_adjustments = expected_data.get('feature_adjustments', [])
        
        for adj in feature_adjustments:
            if 'featured_snippet_competitor' in adj['feature']:
                return 'missing_featured_snippet'
            if 'ai_overview' in adj['feature'] and 'cited' not in adj['feature']:
                return 'not_cited_in_ai'
            if 'local_pack' in adj['feature'] and 'user' not in adj['feature']:
                return 'missing_local_pack'
        
        # Check if title is missing exact match
        has_exact_match = any(adj['factor'] == 'exact_match' 
                             for adj in quality_adjustments)
        if not has_exact_match:
            return 'title_keyword_mismatch'
        
        return 'general_optimization_needed'

    def _generate_recommendation(self, issue: str, expected_data: Dict, 
                                 ctr_gap_pct: float) -> Dict[str, Any]:
        """
        Generate specific recommendation based on primary issue.
        """
        recommendations = {
            'performing_well': {
                'action': 'monitor',
                'priority': 'low',
                'description': 'CTR is meeting or exceeding expectations',
                'specific_steps': []
            },
            'serp_feature_displacement': {
                'action': 'feature_optimization',
                'priority': 'high',
                'description': 'SERP features are pushing your result down visually',
                'specific_steps': [
                    'Analyze which SERP features are present',
                    'Optimize content to capture relevant features (FAQ schema, video, images)',
                    'Consider content format changes to match SERP intent'
                ]
            },
            'title_description_quality': {
                'action': 'title_rewrite',
                'priority': 'high',
                'description': 'Title and/or meta description need optimization',
                'specific_steps': [
                    'Rewrite title to include target keyword',
                    'Add compelling power words or numbers',
                    'Ensure title is under 60 characters',
                    'Write descriptive meta description (150-160 chars)'
                ]
            },
            'missing_featured_snippet': {
                'action': 'snippet_optimization',
                'priority': 'high',
                'description': 'Competitor owns featured snippet',
                'specific_steps': [
                    'Add concise answer paragraph at top of content',
                    'Use proper heading hierarchy (H2 for questions)',
                    'Add FAQ schema markup',
                    'Format lists and tables for snippet eligibility'
                ]
            },
            'not_cited_in_ai': {
                'action': 'ai_optimization',
                'priority': 'medium',
                'description': 'Not cited in AI Overview',
                'specific_steps': [
                    'Ensure content has clear, authoritative answers',
                    'Add structured data (Article, HowTo, FAQ schema)',
                    'Include expert credentials and citations',
                    'Update content to be current and comprehensive'
                ]
            },
            'missing_local_pack': {
                'action': 'local_optimization',
                'priority': 'high',
                'description': 'Missing from local pack for local query',
                'specific_steps': [
                    'Optimize Google Business Profile',
                    'Add LocalBusiness schema to website',
                    'Build local citations',
                    'Collect more Google reviews'
                ]
            },
            'title_keyword_mismatch': {
                'action': 'title_optimization',
                'priority': 'medium',
                'description': 'Title does not contain exact keyword match',
                'specific_steps': [
                    'Rewrite title to include exact keyword phrase',
                    'Keep keyword closer to beginning of title',
                    'Maintain title under 60 characters'
                ]
            },
            'general_optimization_needed': {
                'action': 'comprehensive_optimization',
                'priority': 'medium',
                'description': 'Multiple optimization opportunities exist',
                'specific_steps': [
                    'Improve title tag with keyword and power words',
                    'Write compelling meta description',
                    'Add relevant schema markup',
                    'Optimize for SERP features present in results'
                ]
            }
        }
        
        recommendation = recommendations.get(issue, recommendations['general_optimization_needed'])
        
        # Adjust priority based on gap severity
        if ctr_gap_pct < -30:
            recommendation['priority'] = 'critical'
        elif ctr_gap_pct < -20:
            recommendation['priority'] = 'high'
        
        return recommendation

    def generate_summary_stats(self, gaps: List[Dict]) -> Dict[str, Any]:
        """
        Generate summary statistics across all CTR gaps.
        """
        if not gaps:
            return {
                'total_queries_analyzed': 0,
                'queries_with_gaps': 0,
                'total_missed_clicks_monthly': 0,
                'average_ctr_gap_pct': 0,
                'issues_breakdown': {},
                'top_opportunities': []
            }
        
        underperforming = [g for g in gaps if g['ctr_gap'] < 0]
        
        issues_breakdown = {}
        for gap in underperforming:
            issue = gap['primary_issue']
            if issue not in issues_breakdown:
                issues_breakdown[issue] = {
                    'count': 0,
                    'total_missed_clicks': 0,
                    'queries': []
                }
            issues_breakdown[issue]['count'] += 1
            issues_breakdown[issue]['total_missed_clicks'] += gap['missed_clicks_monthly']
            issues_breakdown[issue]['queries'].append(gap['keyword'])
        
        return {
            'total_queries_analyzed': len(gaps),
            'queries_underperforming': len(underperforming),
            'queries_overperforming': len([g for g in gaps if g['ctr_gap'] > 0]),
            'total_missed_clicks_monthly': sum(g['missed_clicks_monthly'] 
                                              for g in underperforming),
            'average_ctr_gap_pct': np.mean([g['ctr_gap_pct'] for g in underperforming]) 
                                  if underperforming else 0,
            'median_ctr_gap_pct': np.median([g['ctr_gap_pct'] for g in underperforming])
                                 if underperforming else 0,
            'issues_breakdown': issues_breakdown,
            'top_opportunities': gaps[:20]  # Top 20 by opportunity score
        }


def analyze_serp_ctr(gsc_data: pd.DataFrame, serp_data: List[Dict],
                     crawl_data: Optional[pd.DataFrame] = None,
                     min_impressions: int = 100) -> Dict[str, Any]:
    """
    Main entry point for SERP CTR analysis.
    
    Args:
        gsc_data: GSC performance data
        serp_data: Live SERP data from DataForSEO
        crawl_data: Optional crawl data
        min_impressions: Minimum impressions threshold
    
    Returns:
        Structured analysis results matching module spec
    """
    try:
        logger.info("Starting SERP CTR analysis")
        
        # Initialize modeler
        modeler = SERPCTRModeler(gsc_data, serp_data, crawl_data)
        
        # Identify CTR gaps
        gaps = modeler.identify_ctr_gaps(min_impressions)
        
        # Generate summary statistics
        summary = modeler.generate_summary_stats(gaps)
        
        # Calculate feature opportunity matrix
        feature_opportunities = _calculate_feature_opportunities(gaps)
        
        # Identify quick wins
        quick_wins = _identify_quick_wins(gaps)
        
        # Segment by intent
        intent_analysis = _analyze_by_intent(gaps, serp_data)
        
        result = {
            'summary': summary,
            'ctr_gaps': gaps[:100],  # Top 100 opportunities
            'feature_opportunities': feature_opportunities,
            'quick_wins': quick_wins,
            'intent_analysis': intent_analysis,
            'metadata': {
                'queries_analyzed': len(gaps),
                'min_impressions_threshold': min_impressions,
                'analysis_date': datetime.now().isoformat(),
                'serp_data_keywords': len(serp_data)
            }
        }
        
        logger.info(f"SERP CTR analysis complete. Found {len(gaps)} opportunities.")
        return result
        
    except Exception as e:
        logger.error(f"Error in SERP CTR analysis: {str(e)}", exc_info=True)
        raise


def _calculate_feature_opportunities(gaps: List[Dict]) -> Dict[str, Any]:
    """
    Calculate opportunities by SERP feature type.
    """
    feature_opps = {}
    
    for gap in gaps:
        if gap['ctr_gap'] >= 0:
            continue
            
        for adj in gap.get('feature_adjustments', []):
            feature = adj['feature']
            
            # Group similar features
            if 'snippet' in feature:
                feature_key = 'featured_snippet'
            elif 'ai_overview' in feature:
                feature_key = 'ai_overview'
            elif 'paa' in feature or 'people_also_ask' in feature:
                feature_key = 'people_also_ask'
            elif 'local' in feature:
                feature_key = 'local_pack'
            elif 'video' in feature:
                feature_key = 'video'
            elif 'image' in feature:
                feature_key = 'image_pack'
            elif 'shopping' in feature:
                feature_key = 'shopping'
            else:
                continue
            
            if feature_key not in feature_opps:
                feature_opps[feature_key] = {
                    'queries': [],
                    'total_missed_clicks': 0,
                    'avg_impact': []
                }
            
            feature_opps[feature_key]['queries'].append(gap['keyword'])
            feature_opps[feature_key]['total_missed_clicks'] += gap['missed_clicks_monthly']
            feature_opps[feature_key]['avg_impact'].append(adj['impact'])
    
    # Calculate averages
    for feature, data in feature_opps.items():
        data['query_count'] = len(data['queries'])
        data['avg_impact'] = np.mean(data['avg_impact'])
        data['queries'] = data['queries'][:10]  # Keep top 10 examples
    
    return feature_opps


def _identify_quick_wins(gaps: List[Dict]) -> List[Dict]:
    """
    Identify quick win opportunities (high impact, low effort).
    """
    quick_wins = []
    
    for gap in gaps:
        if gap['ctr_gap'] >= 0:
            continue
        
        rec = gap['recommendation']
        
        # Quick wins = title/description fixes or simple schema additions
        if rec['action'] in ['title_rewrite', 'title_optimization', 
                            'snippet_optimization']:
            if gap['missed_clicks_monthly'] >= 20:  # Meaningful impact
                quick_wins.append({
                    'keyword': gap['keyword'],
                    'page': gap['page'],
                    'action': rec['action'],
                    'missed_clicks_monthly': gap['missed_clicks_monthly'],
                    'specific_steps': rec['specific_steps'],
                    'estimated_effort_hours': 0.5 if 'title' in rec['action'] else 2,
                    'roi_score': gap['missed_clicks_monthly'] / 
                                (0.5 if 'title' in rec['action'] else 2)
                })
    
    # Sort by ROI
    quick_wins.sort(key=lambda x: x['roi_score'], reverse=True)
    
    return quick_wins[:20]


def _analyze_by_intent(gaps: List[Dict], serp_data: List[Dict]) -> Dict[str, Any]:
    """
    Segment CTR analysis by search intent.
    """
    serp_lookup = {item['keyword']: item for item in serp_data}
    
    intent_segments = {
        'informational': [],
        'commercial': [],
        'transactional': [],
        'navigational': []
    }
    
    for gap in gaps:
        if gap['ctr_gap'] >= 0:
            continue
            
        serp = serp_lookup.get(gap['keyword'])
        intent = serp.get('intent', 'commercial') if serp else 'commercial'
        
        if intent in intent_segments:
            intent_segments[intent].append(gap)
    
    # Calculate stats per intent
    result = {}
    for intent, intent_gaps in intent_segments.items():
        if not intent_gaps:
            continue
            
        result[intent] = {
            'query_count': len(intent_gaps),
            'total_missed_clicks': sum(g['missed_clicks_monthly'] for g in intent_gaps),
            'avg_ctr_gap_pct': np.mean([g['ctr_gap_pct'] for g in intent_gaps]),
            'top_queries': [
                {
                    'keyword': g['keyword'],
                    'missed_clicks': g['missed_clicks_monthly'],
                    'primary_issue': g['primary_issue']
                }
                for g in sorted(intent_gaps, 
                              key=lambda x: x['missed_clicks_monthly'],
                              reverse=True)[:5]
            ]
        }
    
    return result
