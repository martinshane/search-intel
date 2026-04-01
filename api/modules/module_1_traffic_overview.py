import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import pandas as pd
import numpy as np
from api.utils.db import get_supabase_client
from api.utils.gsc_client import get_gsc_data
from api.utils.ga4_client import get_ga4_data

logger = logging.getLogger(__name__)

def fetch_gsc_traffic_data(
    site_url: str,
    access_token: str,
    start_date: datetime,
    end_date: datetime
) -> Optional[pd.DataFrame]:
    """
    Fetch daily traffic data from Google Search Console.
    
    Args:
        site_url: GSC property URL
        access_token: OAuth access token
        start_date: Start date for data fetch
        end_date: End date for data fetch
    
    Returns:
        DataFrame with columns: date, clicks, impressions, ctr, position
    """
    try:
        logger.info(f"Fetching GSC data for {site_url} from {start_date} to {end_date}")
        
        # Fetch daily performance data
        gsc_data = get_gsc_data(
            site_url=site_url,
            access_token=access_token,
            start_date=start_date.strftime('%Y-%m-%d'),
            end_date=end_date.strftime('%Y-%m-%d'),
            dimensions=['date']
        )
        
        if not gsc_data or 'rows' not in gsc_data:
            logger.warning("No GSC data returned")
            return None
        
        # Parse into DataFrame
        rows = []
        for row in gsc_data['rows']:
            rows.append({
                'date': datetime.strptime(row['keys'][0], '%Y-%m-%d'),
                'clicks': row['clicks'],
                'impressions': row['impressions'],
                'ctr': row['ctr'],
                'position': row['position']
            })
        
        df = pd.DataFrame(rows)
        df = df.sort_values('date').reset_index(drop=True)
        
        logger.info(f"Fetched {len(df)} days of GSC data")
        return df
        
    except Exception as e:
        logger.error(f"Error fetching GSC data: {str(e)}", exc_info=True)
        return None


def fetch_ga4_traffic_data(
    property_id: str,
    access_token: str,
    start_date: datetime,
    end_date: datetime
) -> Optional[pd.DataFrame]:
    """
    Fetch daily traffic data from Google Analytics 4.
    
    Args:
        property_id: GA4 property ID
        access_token: OAuth access token
        start_date: Start date for data fetch
        end_date: End date for data fetch
    
    Returns:
        DataFrame with columns: date, sessions, users, bounce_rate, avg_session_duration
    """
    try:
        logger.info(f"Fetching GA4 data for {property_id} from {start_date} to {end_date}")
        
        # Fetch daily metrics
        ga4_data = get_ga4_data(
            property_id=property_id,
            access_token=access_token,
            start_date=start_date.strftime('%Y-%m-%d'),
            end_date=end_date.strftime('%Y-%m-%d'),
            dimensions=['date'],
            metrics=[
                'sessions',
                'totalUsers',
                'bounceRate',
                'averageSessionDuration'
            ]
        )
        
        if not ga4_data or 'rows' not in ga4_data:
            logger.warning("No GA4 data returned")
            return None
        
        # Parse into DataFrame
        rows = []
        for row in ga4_data['rows']:
            rows.append({
                'date': datetime.strptime(row['dimensionValues'][0]['value'], '%Y%m%d'),
                'sessions': int(row['metricValues'][0]['value']),
                'users': int(row['metricValues'][1]['value']),
                'bounce_rate': float(row['metricValues'][2]['value']),
                'avg_session_duration': float(row['metricValues'][3]['value'])
            })
        
        df = pd.DataFrame(rows)
        df = df.sort_values('date').reset_index(drop=True)
        
        logger.info(f"Fetched {len(df)} days of GA4 data")
        return df
        
    except Exception as e:
        logger.error(f"Error fetching GA4 data: {str(e)}", exc_info=True)
        return None


def calculate_totals(df: pd.DataFrame, metric_cols: List[str]) -> Dict[str, float]:
    """Calculate total values for specified metrics."""
    totals = {}
    for col in metric_cols:
        if col in df.columns:
            if col in ['ctr', 'bounce_rate', 'position']:
                # Average for rate/position metrics
                totals[col] = float(df[col].mean())
            else:
                # Sum for count metrics
                totals[col] = float(df[col].sum())
    return totals


def calculate_trends(
    current_df: pd.DataFrame,
    previous_df: pd.DataFrame,
    metric_cols: List[str]
) -> Dict[str, Dict[str, float]]:
    """
    Calculate period-over-period trends.
    
    Returns dict with structure:
    {
        'metric_name': {
            'current': float,
            'previous': float,
            'change': float,
            'change_pct': float
        }
    }
    """
    trends = {}
    
    for col in metric_cols:
        if col not in current_df.columns or col not in previous_df.columns:
            continue
        
        if col in ['ctr', 'bounce_rate', 'position']:
            current_val = float(current_df[col].mean())
            previous_val = float(previous_df[col].mean())
        else:
            current_val = float(current_df[col].sum())
            previous_val = float(previous_df[col].sum())
        
        change = current_val - previous_val
        change_pct = (change / previous_val * 100) if previous_val > 0 else 0
        
        trends[col] = {
            'current': current_val,
            'previous': previous_val,
            'change': change,
            'change_pct': change_pct
        }
    
    return trends


def prepare_time_series(df: pd.DataFrame, source: str) -> List[Dict[str, Any]]:
    """
    Convert DataFrame to time series array format.
    
    Args:
        df: DataFrame with date column and metric columns
        source: 'gsc' or 'ga4' to determine which columns to include
    
    Returns:
        List of dicts with date and metric values
    """
    time_series = []
    
    for _, row in df.iterrows():
        entry = {
            'date': row['date'].strftime('%Y-%m-%d')
        }
        
        if source == 'gsc':
            entry.update({
                'clicks': int(row['clicks']),
                'impressions': int(row['impressions']),
                'ctr': float(row['ctr']),
                'position': float(row['position'])
            })
        elif source == 'ga4':
            entry.update({
                'sessions': int(row['sessions']),
                'users': int(row['users']),
                'bounce_rate': float(row['bounce_rate']),
                'avg_session_duration': float(row['avg_session_duration'])
            })
        
        time_series.append(entry)
    
    return time_series


def analyze_traffic_overview(
    site_url: str,
    ga4_property_id: Optional[str],
    access_token: str,
    user_id: str
) -> Dict[str, Any]:
    """
    Complete implementation of Module 1: Traffic Overview
    
    Fetches 90-day traffic data from GSC and GA4, compares to previous 90-day period,
    and returns comprehensive traffic metrics with trends.
    
    Args:
        site_url: GSC property URL
        ga4_property_id: GA4 property ID (optional)
        access_token: OAuth access token
        user_id: User ID for caching
    
    Returns:
        dict containing:
        - gsc_data: GSC metrics (clicks, impressions, CTR, position)
        - ga4_data: GA4 metrics (sessions, users, bounce rate, duration)
        - time_series: Daily data arrays
        - totals: Aggregate values for current period
        - trends: Period-over-period comparison
        - data_quality: Information about data availability
    """
    logger.info(f"Starting Module 1 analysis for {site_url}")
    
    # Define date ranges
    end_date = datetime.now().date()
    start_date_current = end_date - timedelta(days=90)
    start_date_previous = start_date_current - timedelta(days=90)
    end_date_previous = start_date_current - timedelta(days=1)
    
    result = {
        'module': 'traffic_overview',
        'generated_at': datetime.now().isoformat(),
        'date_range': {
            'current': {
                'start': start_date_current.strftime('%Y-%m-%d'),
                'end': end_date.strftime('%Y-%m-%d')
            },
            'previous': {
                'start': start_date_previous.strftime('%Y-%m-%d'),
                'end': end_date_previous.strftime('%Y-%m-%d')
            }
        },
        'gsc_data': None,
        'ga4_data': None,
        'data_quality': {
            'gsc_available': False,
            'ga4_available': False,
            'warnings': []
        }
    }
    
    # Fetch GSC data
    try:
        gsc_current = fetch_gsc_traffic_data(
            site_url=site_url,
            access_token=access_token,
            start_date=datetime.combine(start_date_current, datetime.min.time()),
            end_date=datetime.combine(end_date, datetime.min.time())
        )
        
        gsc_previous = fetch_gsc_traffic_data(
            site_url=site_url,
            access_token=access_token,
            start_date=datetime.combine(start_date_previous, datetime.min.time()),
            end_date=datetime.combine(end_date_previous, datetime.min.time())
        )
        
        if gsc_current is not None and len(gsc_current) > 0:
            result['data_quality']['gsc_available'] = True
            
            gsc_metrics = ['clicks', 'impressions', 'ctr', 'position']
            
            # Calculate totals
            gsc_totals = calculate_totals(gsc_current, gsc_metrics)
            
            # Calculate trends if previous period available
            gsc_trends = None
            if gsc_previous is not None and len(gsc_previous) > 0:
                gsc_trends = calculate_trends(gsc_current, gsc_previous, gsc_metrics)
            else:
                result['data_quality']['warnings'].append(
                    "Previous period GSC data not available for comparison"
                )
            
            # Prepare time series
            gsc_time_series = prepare_time_series(gsc_current, 'gsc')
            
            result['gsc_data'] = {
                'totals': gsc_totals,
                'trends': gsc_trends,
                'time_series': gsc_time_series,
                'days_of_data': len(gsc_current)
            }
            
            logger.info(f"GSC data processed: {len(gsc_current)} days")
        else:
            result['data_quality']['warnings'].append(
                "No GSC data available for current period"
            )
            
    except Exception as e:
        logger.error(f"Error processing GSC data: {str(e)}", exc_info=True)
        result['data_quality']['warnings'].append(f"GSC data fetch failed: {str(e)}")
    
    # Fetch GA4 data if property ID provided
    if ga4_property_id:
        try:
            ga4_current = fetch_ga4_traffic_data(
                property_id=ga4_property_id,
                access_token=access_token,
                start_date=datetime.combine(start_date_current, datetime.min.time()),
                end_date=datetime.combine(end_date, datetime.min.time())
            )
            
            ga4_previous = fetch_ga4_traffic_data(
                property_id=ga4_property_id,
                access_token=access_token,
                start_date=datetime.combine(start_date_previous, datetime.min.time()),
                end_date=datetime.combine(end_date_previous, datetime.min.time())
            )
            
            if ga4_current is not None and len(ga4_current) > 0:
                result['data_quality']['ga4_available'] = True
                
                ga4_metrics = ['sessions', 'users', 'bounce_rate', 'avg_session_duration']
                
                # Calculate totals
                ga4_totals = calculate_totals(ga4_current, ga4_metrics)
                
                # Calculate trends if previous period available
                ga4_trends = None
                if ga4_previous is not None and len(ga4_previous) > 0:
                    ga4_trends = calculate_trends(ga4_current, ga4_previous, ga4_metrics)
                else:
                    result['data_quality']['warnings'].append(
                        "Previous period GA4 data not available for comparison"
                    )
                
                # Prepare time series
                ga4_time_series = prepare_time_series(ga4_current, 'ga4')
                
                result['ga4_data'] = {
                    'totals': ga4_totals,
                    'trends': ga4_trends,
                    'time_series': ga4_time_series,
                    'days_of_data': len(ga4_current)
                }
                
                logger.info(f"GA4 data processed: {len(ga4_current)} days")
            else:
                result['data_quality']['warnings'].append(
                    "No GA4 data available for current period"
                )
                
        except Exception as e:
            logger.error(f"Error processing GA4 data: {str(e)}", exc_info=True)
            result['data_quality']['warnings'].append(f"GA4 data fetch failed: {str(e)}")
    else:
        result['data_quality']['warnings'].append("GA4 property ID not provided")
    
    # Cache results in Supabase
    try:
        supabase = get_supabase_client()
        supabase.table('report_cache').upsert({
            'user_id': user_id,
            'site_url': site_url,
            'module': 'traffic_overview',
            'data': result,
            'created_at': datetime.now().isoformat(),
            'expires_at': (datetime.now() + timedelta(hours=24)).isoformat()
        }).execute()
        logger.info("Module 1 results cached successfully")
    except Exception as e:
        logger.error(f"Error caching results: {str(e)}", exc_info=True)
    
    return result


def get_summary_insights(result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generate human-readable summary insights from traffic overview data.
    
    Args:
        result: Output from analyze_traffic_overview
    
    Returns:
        Dict with summary insights and key findings
    """
    insights = {
        'key_metrics': {},
        'trends_summary': {},
        'alerts': []
    }
    
    # GSC insights
    if result.get('gsc_data'):
        gsc_data = result['gsc_data']
        totals = gsc_data['totals']
        trends = gsc_data.get('trends')
        
        insights['key_metrics']['gsc'] = {
            'total_clicks': int(totals.get('clicks', 0)),
            'total_impressions': int(totals.get('impressions', 0)),
            'avg_ctr': round(totals.get('ctr', 0) * 100, 2),
            'avg_position': round(totals.get('position', 0), 1)
        }
        
        if trends:
            insights['trends_summary']['gsc'] = {}
            
            # Clicks trend
            clicks_change = trends.get('clicks', {}).get('change_pct', 0)
            if abs(clicks_change) >= 10:
                direction = 'increased' if clicks_change > 0 else 'decreased'
                insights['alerts'].append(
                    f"Clicks {direction} by {abs(round(clicks_change, 1))}% vs previous period"
                )
            insights['trends_summary']['gsc']['clicks_change_pct'] = round(clicks_change, 1)
            
            # Position trend
            position_change = trends.get('position', {}).get('change', 0)
            if abs(position_change) >= 1:
                direction = 'improved' if position_change < 0 else 'declined'
                insights['alerts'].append(
                    f"Average position {direction} by {abs(round(position_change, 1))} spots"
                )
            insights['trends_summary']['gsc']['position_change'] = round(position_change, 1)
            
            # CTR trend
            ctr_change = trends.get('ctr', {}).get('change_pct', 0)
            if abs(ctr_change) >= 15:
                direction = 'improved' if ctr_change > 0 else 'declined'
                insights['alerts'].append(
                    f"CTR {direction} by {abs(round(ctr_change, 1))}% - investigate title/snippet changes"
                )
            insights['trends_summary']['gsc']['ctr_change_pct'] = round(ctr_change, 1)
    
    # GA4 insights
    if result.get('ga4_data'):
        ga4_data = result['ga4_data']
        totals = ga4_data['totals']
        trends = ga4_data.get('trends')
        
        insights['key_metrics']['ga4'] = {
            'total_sessions': int(totals.get('sessions', 0)),
            'total_users': int(totals.get('users', 0)),
            'avg_bounce_rate': round(totals.get('bounce_rate', 0) * 100, 2),
            'avg_session_duration': round(totals.get('avg_session_duration', 0), 1)
        }
        
        if trends:
            insights['trends_summary']['ga4'] = {}
            
            # Sessions trend
            sessions_change = trends.get('sessions', {}).get('change_pct', 0)
            if abs(sessions_change) >= 10:
                direction = 'increased' if sessions_change > 0 else 'decreased'
                insights['alerts'].append(
                    f"Sessions {direction} by {abs(round(sessions_change, 1))}%"
                )
            insights['trends_summary']['ga4']['sessions_change_pct'] = round(sessions_change, 1)
            
            # Bounce rate trend
            bounce_change = trends.get('bounce_rate', {}).get('change_pct', 0)
            if abs(bounce_change) >= 15:
                direction = 'increased' if bounce_change > 0 else 'decreased'
                status = 'concerning' if bounce_change > 0 else 'positive'
                insights['alerts'].append(
                    f"Bounce rate {direction} by {abs(round(bounce_change, 1))}% - {status}"
                )
            insights['trends_summary']['ga4']['bounce_rate_change_pct'] = round(bounce_change, 1)
            
            # Session duration trend
            duration_change = trends.get('avg_session_duration', {}).get('change_pct', 0)
            if abs(duration_change) >= 20:
                direction = 'increased' if duration_change > 0 else 'decreased'
                status = 'positive' if duration_change > 0 else 'concerning'
                insights['alerts'].append(
                    f"Avg session duration {direction} by {abs(round(duration_change, 1))}% - {status}"
                )
            insights['trends_summary']['ga4']['duration_change_pct'] = round(duration_change, 1)
    
    # Data quality warnings
    if result.get('data_quality', {}).get('warnings'):
        insights['data_warnings'] = result['data_quality']['warnings']
    
    return insights