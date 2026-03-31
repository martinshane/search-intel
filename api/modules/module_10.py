"""
Module 10: Revenue Attribution Analysis

Analyzes GA4 e-commerce data to map SEO traffic to revenue, calculates revenue per landing page,
identifies high-value keywords, and models revenue opportunity from organic growth.
Includes cohort analysis, conversion funnel metrics, and revenue forecasting.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from scipy import stats
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
import warnings

warnings.filterwarnings('ignore')


@dataclass
class RevenueMetrics:
    """Container for revenue metrics"""
    total_revenue: float
    transaction_count: int
    avg_order_value: float
    revenue_per_session: float
    conversion_rate: float
    revenue_per_user: float


@dataclass
class LandingPageRevenue:
    """Revenue metrics for a landing page"""
    url: str
    revenue: float
    transactions: int
    sessions: int
    users: int
    avg_order_value: float
    conversion_rate: float
    revenue_per_session: float
    revenue_growth_rate: float
    top_converting_queries: List[Dict[str, Any]]
    revenue_share: float


@dataclass
class KeywordRevenue:
    """Revenue attribution for a keyword"""
    query: str
    attributed_revenue: float
    transactions: int
    impressions: int
    clicks: int
    avg_position: float
    conversion_rate: float
    revenue_per_click: float
    landing_pages: List[str]
    assisted_conversions: int


@dataclass
class CohortMetrics:
    """Cohort analysis metrics"""
    cohort_period: str
    acquisition_date: str
    users: int
    revenue_day_0: float
    revenue_day_7: float
    revenue_day_30: float
    revenue_day_60: float
    revenue_day_90: float
    ltv_30d: float
    ltv_60d: float
    ltv_90d: float
    retention_rate_30d: float


@dataclass
class FunnelStage:
    """Conversion funnel stage metrics"""
    stage: str
    users: int
    drop_off_rate: float
    conversion_rate: float
    avg_time_to_next_stage: float
    revenue_at_stage: float


@dataclass
class RevenueProjection:
    """Revenue forecast"""
    period: str
    projected_revenue: float
    confidence_interval_low: float
    confidence_interval_high: float
    projected_transactions: int
    growth_rate: float
    assumptions: Dict[str, Any]


def calculate_revenue_metrics(ga4_ecommerce_data: pd.DataFrame) -> RevenueMetrics:
    """
    Calculate overall revenue metrics from GA4 e-commerce data.
    
    Args:
        ga4_ecommerce_data: DataFrame with columns [date, sessions, users, transactions, revenue]
        
    Returns:
        RevenueMetrics object with aggregated metrics
    """
    if ga4_ecommerce_data.empty:
        return RevenueMetrics(
            total_revenue=0.0,
            transaction_count=0,
            avg_order_value=0.0,
            revenue_per_session=0.0,
            conversion_rate=0.0,
            revenue_per_user=0.0
        )
    
    total_revenue = ga4_ecommerce_data['revenue'].sum()
    transaction_count = int(ga4_ecommerce_data['transactions'].sum())
    total_sessions = ga4_ecommerce_data['sessions'].sum()
    total_users = ga4_ecommerce_data['users'].sum()
    
    avg_order_value = total_revenue / transaction_count if transaction_count > 0 else 0.0
    revenue_per_session = total_revenue / total_sessions if total_sessions > 0 else 0.0
    conversion_rate = transaction_count / total_sessions if total_sessions > 0 else 0.0
    revenue_per_user = total_revenue / total_users if total_users > 0 else 0.0
    
    return RevenueMetrics(
        total_revenue=round(total_revenue, 2),
        transaction_count=transaction_count,
        avg_order_value=round(avg_order_value, 2),
        revenue_per_session=round(revenue_per_session, 2),
        conversion_rate=round(conversion_rate, 4),
        revenue_per_user=round(revenue_per_user, 2)
    )


def analyze_landing_page_revenue(
    ga4_landing_page_data: pd.DataFrame,
    gsc_page_data: pd.DataFrame,
    query_page_mapping: pd.DataFrame,
    min_sessions: int = 10
) -> List[LandingPageRevenue]:
    """
    Calculate revenue attribution by landing page.
    
    Args:
        ga4_landing_page_data: DataFrame with [landing_page, date, sessions, users, transactions, revenue]
        gsc_page_data: DataFrame with [page, date, clicks, impressions]
        query_page_mapping: DataFrame with [query, page, clicks, impressions]
        min_sessions: Minimum sessions to include a page
        
    Returns:
        List of LandingPageRevenue objects
    """
    results = []
    
    # Ensure date columns are datetime
    if 'date' in ga4_landing_page_data.columns:
        ga4_landing_page_data['date'] = pd.to_datetime(ga4_landing_page_data['date'])
    
    # Aggregate by landing page
    page_agg = ga4_landing_page_data.groupby('landing_page').agg({
        'revenue': 'sum',
        'transactions': 'sum',
        'sessions': 'sum',
        'users': 'sum'
    }).reset_index()
    
    # Filter by minimum sessions
    page_agg = page_agg[page_agg['sessions'] >= min_sessions]
    
    total_revenue = page_agg['revenue'].sum()
    
    for _, row in page_agg.iterrows():
        url = row['landing_page']
        revenue = row['revenue']
        transactions = int(row['transactions'])
        sessions = row['sessions']
        users = row['users']
        
        avg_order_value = revenue / transactions if transactions > 0 else 0.0
        conversion_rate = transactions / sessions if sessions > 0 else 0.0
        revenue_per_session = revenue / sessions if sessions > 0 else 0.0
        revenue_share = revenue / total_revenue if total_revenue > 0 else 0.0
        
        # Calculate growth rate
        page_timeseries = ga4_landing_page_data[
            ga4_landing_page_data['landing_page'] == url
        ].sort_values('date')
        
        growth_rate = 0.0
        if len(page_timeseries) >= 30:
            recent_revenue = page_timeseries.tail(30)['revenue'].sum()
            older_revenue = page_timeseries.head(30)['revenue'].sum()
            if older_revenue > 0:
                growth_rate = (recent_revenue - older_revenue) / older_revenue
        
        # Find top converting queries for this page
        top_queries = []
        if not query_page_mapping.empty:
            page_queries = query_page_mapping[
                query_page_mapping['page'] == url
            ].sort_values('clicks', ascending=False).head(5)
            
            for _, q_row in page_queries.iterrows():
                top_queries.append({
                    'query': q_row['query'],
                    'clicks': int(q_row['clicks']),
                    'impressions': int(q_row['impressions'])
                })
        
        results.append(LandingPageRevenue(
            url=url,
            revenue=round(revenue, 2),
            transactions=transactions,
            sessions=int(sessions),
            users=int(users),
            avg_order_value=round(avg_order_value, 2),
            conversion_rate=round(conversion_rate, 4),
            revenue_per_session=round(revenue_per_session, 2),
            revenue_growth_rate=round(growth_rate, 4),
            top_converting_queries=top_queries,
            revenue_share=round(revenue_share, 4)
        ))
    
    # Sort by revenue descending
    results.sort(key=lambda x: x.revenue, reverse=True)
    
    return results


def attribute_revenue_to_keywords(
    gsc_query_data: pd.DataFrame,
    query_page_mapping: pd.DataFrame,
    landing_page_revenue: List[LandingPageRevenue],
    ga4_conversion_paths: Optional[pd.DataFrame] = None
) -> List[KeywordRevenue]:
    """
    Attribute revenue to individual keywords using multiple attribution models.
    
    Args:
        gsc_query_data: DataFrame with [query, clicks, impressions, position]
        query_page_mapping: DataFrame with [query, page, clicks]
        landing_page_revenue: List of LandingPageRevenue objects
        ga4_conversion_paths: Optional DataFrame with conversion path data
        
    Returns:
        List of KeywordRevenue objects
    """
    results = []
    
    # Create revenue lookup by page
    page_revenue_map = {lpr.url: lpr for lpr in landing_page_revenue}
    
    # Aggregate query data
    query_agg = gsc_query_data.groupby('query').agg({
        'clicks': 'sum',
        'impressions': 'sum',
        'position': 'mean'
    }).reset_index()
    
    for _, query_row in query_agg.iterrows():
        query = query_row['query']
        total_clicks = query_row['clicks']
        impressions = query_row['impressions']
        avg_position = query_row['position']
        
        # Find all landing pages for this query
        query_pages = query_page_mapping[query_page_mapping['query'] == query]
        
        if query_pages.empty:
            continue
        
        attributed_revenue = 0.0
        transactions = 0
        landing_pages = []
        
        # Distribute revenue based on click share to each landing page
        for _, qp_row in query_pages.iterrows():
            page = qp_row['page']
            page_clicks_from_query = qp_row['clicks']
            
            if page in page_revenue_map:
                lpr = page_revenue_map[page]
                landing_pages.append(page)
                
                # Attribution: proportional to clicks from this query vs total sessions
                if lpr.sessions > 0:
                    attribution_weight = page_clicks_from_query / lpr.sessions
                    attributed_revenue += lpr.revenue * attribution_weight
                    transactions += int(lpr.transactions * attribution_weight)
        
        # Calculate assisted conversions if conversion path data available
        assisted_conversions = 0
        if ga4_conversion_paths is not None and not ga4_conversion_paths.empty:
            # Count conversions where this query appeared in the path but wasn't last click
            assisted = ga4_conversion_paths[
                (ga4_conversion_paths['query_in_path'] == query) &
                (ga4_conversion_paths['last_click_query'] != query)
            ]
            assisted_conversions = len(assisted)
        
        conversion_rate = transactions / total_clicks if total_clicks > 0 else 0.0
        revenue_per_click = attributed_revenue / total_clicks if total_clicks > 0 else 0.0
        
        results.append(KeywordRevenue(
            query=query,
            attributed_revenue=round(attributed_revenue, 2),
            transactions=transactions,
            impressions=int(impressions),
            clicks=int(total_clicks),
            avg_position=round(avg_position, 2),
            conversion_rate=round(conversion_rate, 4),
            revenue_per_click=round(revenue_per_click, 2),
            landing_pages=landing_pages,
            assisted_conversions=assisted_conversions
        ))
    
    # Sort by attributed revenue
    results.sort(key=lambda x: x.attributed_revenue, reverse=True)
    
    return results


def perform_cohort_analysis(
    ga4_user_acquisition_data: pd.DataFrame,
    ga4_user_revenue_data: pd.DataFrame,
    cohort_period: str = 'month'
) -> List[CohortMetrics]:
    """
    Perform cohort analysis to track user lifetime value over time.
    
    Args:
        ga4_user_acquisition_data: DataFrame with [user_id, acquisition_date, source]
        ga4_user_revenue_data: DataFrame with [user_id, date, revenue]
        cohort_period: 'week' or 'month'
        
    Returns:
        List of CohortMetrics objects
    """
    results = []
    
    if ga4_user_acquisition_data.empty or ga4_user_revenue_data.empty:
        return results
    
    # Ensure dates are datetime
    ga4_user_acquisition_data['acquisition_date'] = pd.to_datetime(
        ga4_user_acquisition_data['acquisition_date']
    )
    ga4_user_revenue_data['date'] = pd.to_datetime(ga4_user_revenue_data['date'])
    
    # Create cohort assignment
    if cohort_period == 'week':
        ga4_user_acquisition_data['cohort'] = ga4_user_acquisition_data['acquisition_date'].dt.to_period('W')
    else:  # month
        ga4_user_acquisition_data['cohort'] = ga4_user_acquisition_data['acquisition_date'].dt.to_period('M')
    
    # Merge acquisition with revenue
    merged = ga4_user_revenue_data.merge(
        ga4_user_acquisition_data[['user_id', 'acquisition_date', 'cohort']],
        on='user_id',
        how='left'
    )
    
    # Calculate days since acquisition
    merged['days_since_acquisition'] = (
        merged['date'] - merged['acquisition_date']
    ).dt.days
    
    # Group by cohort
    cohorts = merged.groupby('cohort')
    
    for cohort_name, cohort_data in cohorts:
        cohort_date = str(cohort_name)
        users_in_cohort = cohort_data['user_id'].nunique()
        
        # Revenue by day buckets
        revenue_day_0 = cohort_data[
            cohort_data['days_since_acquisition'] == 0
        ]['revenue'].sum()
        
        revenue_day_7 = cohort_data[
            (cohort_data['days_since_acquisition'] >= 0) &
            (cohort_data['days_since_acquisition'] <= 7)
        ]['revenue'].sum()
        
        revenue_day_30 = cohort_data[
            (cohort_data['days_since_acquisition'] >= 0) &
            (cohort_data['days_since_acquisition'] <= 30)
        ]['revenue'].sum()
        
        revenue_day_60 = cohort_data[
            (cohort_data['days_since_acquisition'] >= 0) &
            (cohort_data['days_since_acquisition'] <= 60)
        ]['revenue'].sum()
        
        revenue_day_90 = cohort_data[
            (cohort_data['days_since_acquisition'] >= 0) &
            (cohort_data['days_since_acquisition'] <= 90)
        ]['revenue'].sum()
        
        # LTV calculations
        ltv_30d = revenue_day_30 / users_in_cohort if users_in_cohort > 0 else 0.0
        ltv_60d = revenue_day_60 / users_in_cohort if users_in_cohort > 0 else 0.0
        ltv_90d = revenue_day_90 / users_in_cohort if users_in_cohort > 0 else 0.0
        
        # Retention rate (users active in day 30 period)
        users_active_30d = cohort_data[
            (cohort_data['days_since_acquisition'] >= 20) &
            (cohort_data['days_since_acquisition'] <= 30)
        ]['user_id'].nunique()
        retention_rate_30d = users_active_30d / users_in_cohort if users_in_cohort > 0 else 0.0
        
        results.append(CohortMetrics(
            cohort_period=cohort_period,
            acquisition_date=cohort_date,
            users=users_in_cohort,
            revenue_day_0=round(revenue_day_0, 2),
            revenue_day_7=round(revenue_day_7, 2),
            revenue_day_30=round(revenue_day_30, 2),
            revenue_day_60=round(revenue_day_60, 2),
            revenue_day_90=round(revenue_day_90, 2),
            ltv_30d=round(ltv_30d, 2),
            ltv_60d=round(ltv_60d, 2),
            ltv_90d=round(ltv_90d, 2),
            retention_rate_30d=round(retention_rate_30d, 4)
        ))
    
    return results


def analyze_conversion_funnel(
    ga4_funnel_data: pd.DataFrame
) -> List[FunnelStage]:
    """
    Analyze conversion funnel with drop-off rates and revenue attribution by stage.
    
    Args:
        ga4_funnel_data: DataFrame with [stage, stage_order, users, revenue, avg_time_seconds]
        
    Returns:
        List of FunnelStage objects
    """
    results = []
    
    if ga4_funnel_data.empty:
        return results
    
    # Sort by stage order
    funnel_data = ga4_funnel_data.sort_values('stage_order')
    
    total_users_start = funnel_data.iloc[0]['users'] if len(funnel_data) > 0 else 0
    
    for i, row in funnel_data.iterrows():
        stage = row['stage']
        users = row['users']
        revenue = row['revenue']
        avg_time = row.get('avg_time_seconds', 0)
        
        # Calculate drop-off rate from previous stage
        if i > 0:
            prev_users = funnel_data.iloc[i - 1]['users']
            drop_off_rate = (prev_users - users) / prev_users if prev_users > 0 else 0.0
        else:
            drop_off_rate = 0.0
        
        # Conversion rate from start
        conversion_rate = users / total_users_start if total_users_start > 0 else 0.0
        
        results.append(FunnelStage(
            stage=stage,
            users=int(users),
            drop_off_rate=round(drop_off_rate, 4),
            conversion_rate=round(conversion_rate, 4),
            avg_time_to_next_stage=round(avg_time, 2),
            revenue_at_stage=round(revenue, 2)
        ))
    
    return results


def forecast_revenue(
    historical_revenue_data: pd.DataFrame,
    traffic_projections: Dict[str, float],
    current_conversion_rate: float,
    current_avg_order_value: float,
    forecast_periods: List[int] = [30, 60, 90],
    growth_scenarios: Optional[Dict[str, float]] = None
) -> List[RevenueProjection]:
    """
    Forecast revenue based on traffic projections and current conversion metrics.
    
    Args:
        historical_revenue_data: DataFrame with [date, revenue, sessions]
        traffic_projections: Dict with keys like '30d', '60d', '90d' and projected session values
        current_conversion_rate: Current conversion rate
        current_avg_order_value: Current AOV
        forecast_periods: List of days to forecast
        growth_scenarios: Optional dict with conversion_rate_growth and aov_growth rates
        
    Returns:
        List of RevenueProjection objects
    """
    results = []
    
    if growth_scenarios is None:
        growth_scenarios = {
            'conversion_rate_growth': 0.0,
            'aov_growth': 0.0
        }
    
    # Fit time series model on historical revenue
    if not historical_revenue_data.empty and len(historical_revenue_data) >= 30:
        historical_revenue_data = historical_revenue_data.sort_values('date')
        historical_revenue_data['days'] = (
            historical_revenue_data['date'] - historical_revenue_data['date'].min()
        ).dt.days
        
        X = historical_revenue_data[['days']].values
        y = historical_revenue_data['revenue'].values
        
        # Fit linear regression for trend
        model = LinearRegression()
        model.fit(X, y)
        
        # Calculate prediction intervals using residuals
        predictions = model.predict(X)
        residuals = y - predictions
        std_residuals = np.std(residuals)
    else:
        std_residuals = 0.0
        model = None
    
    for period in forecast_periods:
        period_key = f'{period}d'
        
        # Get projected sessions
        projected_sessions = traffic_projections.get(period_key, 0)
        
        if projected_sessions == 0:
            continue
        
        # Apply growth scenarios to conversion rate and AOV
        days_factor = period / 365.0
        projected_conversion_rate = current_conversion_rate * (
            1 + growth_scenarios['conversion_rate_growth'] * days_factor
        )
        projected_aov = current_avg_order_value * (
            1 + growth_scenarios['aov_growth'] * days_factor
        )
        
        # Calculate projected revenue
        projected_transactions = projected_sessions * projected_conversion_rate
        projected_revenue = projected_transactions * projected_aov
        
        # Calculate confidence intervals (±2 standard deviations)
        if std_residuals > 0:
            # Scale std by forecast horizon
            forecast_std = std_residuals * np.sqrt(1 + period / 30.0)
            ci_low = projected_revenue - (2 * forecast_std)
            ci_high = projected_revenue + (2 * forecast_std)
        else:
            # Use ±20% as default if no historical variance
            ci_low = projected_revenue * 0.8
            ci_high = projected_revenue * 1.2
        
        # Calculate growth rate vs current
        if len(historical_revenue_data) >= 30:
            recent_30d_revenue = historical_revenue_data.tail(30)['revenue'].sum()
            growth_rate = (projected_revenue - recent_30d_revenue) / recent_30d_revenue if recent_30d_revenue > 0 else 0.0
        else:
            growth_rate = 0.0
        
        results.append(RevenueProjection(
            period=period_key,
            projected_revenue=round(projected_revenue, 2),
            confidence_interval_low=round(max(0, ci_low), 2),
            confidence_interval_high=round(ci_high, 2),
            projected_transactions=int(projected_transactions),
            growth_rate=round(growth_rate, 4),
            assumptions={
                'projected_sessions': int(projected_sessions),
                'conversion_rate': round(projected_conversion_rate, 4),
                'avg_order_value': round(projected_aov, 2),
                'conversion_rate_growth': growth_scenarios['conversion_rate_growth'],
                'aov_growth': growth_scenarios['aov_growth']
            }
        ))
    
    return results


def calculate_keyword_opportunity_value(
    keyword_revenue_data: List[KeywordRevenue],
    striking_distance_keywords: List[Dict[str, Any]],
    avg_ctr_by_position: Dict[int, float]
) -> List[Dict[str, Any]]:
    """
    Calculate revenue opportunity for keywords in striking distance.
    
    Args:
        keyword_revenue_data: List of KeywordRevenue objects
        striking_distance_keywords: List of dicts with query, position, impressions
        avg_ctr_by_position: Dict mapping position to average CTR
        
    Returns:
        List of opportunity dicts with revenue projections
    """
    results = []
    
    # Create lookup for existing keyword revenue data
    keyword_revenue_map = {kr.query: kr for kr in keyword_revenue_data}
    
    for keyword in striking_distance_keywords:
        query = keyword['query']
        current_position = keyword['current_position']
        impressions = keyword['impressions']
        
        # Get current revenue data if exists
        current_revenue_per_click = 0.0
        if query in keyword_revenue_map:
            kr = keyword_revenue_map[query]
            current_revenue_per_click = kr.revenue_per_click
        
        # If no revenue data, use portfolio average
        if current_revenue_per_click == 0.0 and keyword_revenue_data:
            total_revenue = sum(kr.attributed_revenue for kr in keyword_revenue_data)
            total_clicks = sum(kr.clicks for kr in keyword_revenue_data)
            current_revenue_per_click = total_revenue / total_clicks if total_clicks > 0 else 0.0
        
        # Calculate current clicks
        current_ctr = avg_ctr_by_position.get(int(current_position), 0.02)
        current_clicks = impressions * current_ctr
        
        # Calculate potential clicks at position 3 (target)
        target_position = 3
        target_ctr = avg_ctr_by_position.get(target_position, 0.15)
        potential_clicks = impressions * target_ctr
        
        # Revenue opportunity
        click_increase = potential_clicks - current_clicks
        revenue_opportunity = click_increase * current_revenue_per_click
        
        # Monthly projection
        monthly_revenue_opportunity = revenue_opportunity * 30
        
        results.append({
            'query': query,
            'current_position': current_position,
            'target_position': target_position,
            'impressions': int(impressions),
            'current_clicks': int(current_clicks),
            'potential_clicks': int(potential_clicks),
            'click_increase': int(click_increase),
            'revenue_per_click': round(current_revenue_per_click, 2),
            'daily_revenue_opportunity': round(revenue_opportunity, 2),
            'monthly_revenue_opportunity': round(monthly_revenue_opportunity, 2)
        })
    
    # Sort by monthly opportunity
    results.sort(key=lambda x: x['monthly_revenue_opportunity'], reverse=True)
    
    return results


def segment_revenue_by_channel(
    ga4_channel_data: pd.DataFrame,
    focus_on_organic: bool = True
) -> Dict[str, RevenueMetrics]:
    """
    Segment revenue metrics by traffic channel.
    
    Args:
        ga4_channel_data: DataFrame with [channel, sessions, users, transactions, revenue]
        focus_on_organic: Whether to separate organic search subcategories
        
    Returns:
        Dict mapping channel name to RevenueMetrics
    """
    results = {}
    
    if ga4_channel_data.empty:
        return results
    
    # Aggregate by channel
    channel_agg = ga4_channel_data.groupby('channel').agg({
        'revenue': 'sum',
        'transactions': 'sum',
        'sessions': 'sum',
        'users': 'sum'
    }).reset_index()
    
    for _, row in channel_agg.iterrows():
        channel = row['channel']
        
        metrics = RevenueMetrics(
            total_revenue=round(row['revenue'], 2),
            transaction_count=int(row['transactions']),
            avg_order_value=round(
                row['revenue'] / row['transactions'] if row['transactions'] > 0 else 0.0,
                2
            ),
            revenue_per_session=round(
                row['revenue'] / row['sessions'] if row['sessions'] > 0 else 0.0,
                2
            ),
            conversion_rate=round(
                row['transactions'] / row['sessions'] if row['sessions'] > 0 else 0.0,
                4
            ),
            revenue_per_user=round(
                row['revenue'] / row['users'] if row['users'] > 0 else 0.0,
                2
            )
        )
        
        results[channel] = metrics
    
    return results


def analyze_revenue_attribution(
    ga4_ecommerce_data: pd.DataFrame,
    ga4_landing_page_data: pd.DataFrame,
    gsc_query_data: pd.DataFrame,
    gsc_page_data: pd.DataFrame,
    query_page_mapping: pd.DataFrame,
    traffic_projections: Dict[str, float],
    ga4_user_acquisition_data: Optional[pd.DataFrame] = None,
    ga4_user_revenue_data: Optional[pd.DataFrame] = None,
    ga4_funnel_data: Optional[pd.DataFrame] = None,
    ga4_channel_data: Optional[pd.DataFrame] = None,
    striking_distance_keywords: Optional[List[Dict[str, Any]]] = None
) -> dict:
    """
    Main analysis function for Module 10 - Revenue Attribution.
    
    Combines all revenue analysis components into a comprehensive report.
    
    Args:
        ga4_ecommerce_data: Overall e-commerce metrics by date
        ga4_landing_page_data: Landing page performance with revenue
        gsc_query_data: GSC query performance data
        gsc_page_data: GSC page performance data
        query_page_mapping: Mapping of queries to pages
        traffic_projections: Traffic forecast from Module 1
        ga4_user_acquisition_data: Optional user acquisition data for cohorts
        ga4_user_revenue_data: Optional user-level revenue data
        ga4_funnel_data: Optional conversion funnel data
        ga4_channel_data: Optional channel breakdown data
        striking_distance_keywords: Optional list from Module 4
        
    Returns:
        Dictionary with comprehensive revenue attribution analysis
    """
    
    # Calculate overall revenue metrics
    overall_metrics = calculate_revenue_metrics(ga4_ecommerce_data)
    
    # Analyze landing page revenue
    landing_page_revenue = analyze_landing_page_revenue(
        ga4_landing_page_data,
        gsc_page_data,
        query_page_mapping
    )
    
    # Attribute revenue to keywords
    keyword_revenue = attribute_revenue_to_keywords(
        gsc_query_data,
        query_page_mapping,
        landing_page_revenue
    )
    
    # Perform cohort analysis if data available
    cohort_analysis = []
    if ga4_user_acquisition_data is not None and ga4_user_revenue_data is not None:
        cohort_analysis = perform_cohort_analysis(
            ga4_user_acquisition_data,
            ga4_user_revenue_data
        )
    
    # Analyze conversion funnel if data available
    funnel_stages = []
    if ga4_funnel_data is not None:
        funnel_stages = analyze_conversion_funnel(ga4_funnel_data)
    
    # Forecast revenue
    revenue_forecasts = forecast_revenue(
        ga4_ecommerce_data,
        traffic_projections,
        overall_metrics.conversion_rate,
        overall_metrics.avg_order_value
    )
    
    # Calculate keyword opportunity value
    keyword_opportunities = []
    if striking_distance_keywords:
        # Default CTR curve by position
        avg_ctr_by_position = {
            1: 0.287, 2: 0.157, 3: 0.103, 4: 0.073, 5: 0.053,
            6: 0.041, 7: 0.033, 8: 0.027, 9: 0.023, 10: 0.020,
            11: 0.015, 12: 0.012, 13: 0.010, 14: 0.009, 15: 0.008,
            16: 0.007, 17: 0.006, 18: 0.006, 19: 0.005, 20: 0.005
        }
        
        keyword_opportunities = calculate_keyword_opportunity_value(
            keyword_revenue,
            striking_distance_keywords,
            avg_ctr_by_position
        )
    
    # Segment by channel
    channel_metrics = {}
    if ga4_channel_data is not None:
        channel_metrics = segment_revenue_by_channel(ga4_channel_data)
    
    # Calculate total opportunity
    total_keyword_opportunity = sum(
        ko['monthly_revenue_opportunity'] for ko in keyword_opportunities
    )
    
    # Identify high-value segments
    high_value_pages = [
        lpr for lpr in landing_page_revenue
        if lpr.revenue_per_session > overall_metrics.revenue_per_session * 1.5
    ][:10]
    
    high_value_keywords = [
        kr for kr in keyword_revenue
        if kr.revenue_per_click > 0 and kr.clicks >= 10
    ][:20]
    
    # Calculate LTV trends if cohort data available
    ltv_trend = None
    if cohort_analysis and len(cohort_analysis) >= 3:
        recent_cohorts = sorted(cohort_analysis, key=lambda x: x.acquisition_date)[-3:]
        avg_ltv_30d = np.mean([c.ltv_30d for c in recent_cohorts])
        ltv_trend = {
            'avg_ltv_30d': round(avg_ltv_30d, 2),
            'trend_direction': 'stable'  # Could calculate slope here
        }
    
    return {
        'overall_metrics': asdict(overall_metrics),
        'landing_page_revenue': [asdict(lpr) for lpr in landing_page_revenue[:50]],
        'keyword_revenue': [asdict(kr) for kr in keyword_revenue[:100]],
        'high_value_pages': [asdict(lpr) for lpr in high_value_pages],
        'high_value_keywords': [asdict(kr) for kr in high_value_keywords],
        'cohort_analysis': [asdict(cm) for cm in cohort_analysis] if cohort_analysis else [],
        'ltv_trend': ltv_trend,
        'funnel_analysis': [asdict(fs) for fs in funnel_stages] if funnel_stages else [],
        'revenue_forecasts': [asdict(rp) for rp in revenue_forecasts],
        'keyword_opportunities': keyword_opportunities[:30],
        'channel_metrics': {k: asdict(v) for k, v in channel_metrics.items()},
        'summary': {
            'total_revenue_analyzed': overall_metrics.total_revenue,
            'total_transactions': overall_metrics.transaction_count,
            'avg_order_value': overall_metrics.avg_order_value,
            'organic_conversion_rate': overall_metrics.conversion_rate,
            'revenue_per_session': overall_metrics.revenue_per_session,
            'pages_driving_revenue': len(landing_page_revenue),
            'keywords_driving_revenue': len([kr for kr in keyword_revenue if kr.attributed_revenue > 0]),
            'total_monthly_opportunity': round(total_keyword_opportunity, 2),
            'top_revenue_page': landing_page_revenue[0].url if landing_page_revenue else None,
            'top_revenue_keyword': keyword_revenue[0].query if keyword_revenue else None,
            'projected_revenue_30d': revenue_forecasts[0].projected_revenue if revenue_forecasts else 0.0,
            'projected_revenue_90d': revenue_forecasts[-1].projected_revenue if len(revenue_forecasts) >= 3 else 0.0
        },
        'recommendations': generate_revenue_recommendations(
            overall_metrics,
            landing_page_revenue,
            keyword_revenue,
            keyword_opportunities,
            funnel_stages
        )
    }


def generate_revenue_recommendations(
    overall_metrics: RevenueMetrics,
    landing_page_revenue: List[LandingPageRevenue],
    keyword_revenue: List[KeywordRevenue],
    keyword_opportunities: List[Dict[str, Any]],
    funnel_stages: List[FunnelStage]
) -> List[Dict[str, Any]]:
    """
    Generate actionable recommendations based on revenue analysis.
    
    Returns:
        List of recommendation dicts with priority, action, and expected impact
    """
    recommendations = []
    
    # Recommendation 1: Focus on high-value keywords in striking distance
    if keyword_opportunities:
        top_opportunities = keyword_opportunities[:5]
        total_opportunity = sum(ko['monthly_revenue_opportunity'] for ko in top_opportunities)
        
        recommendations.append({
            'priority': 'critical',
            'category': 'keyword_optimization',
            'title': 'Optimize High-Value Keywords in Striking Distance',
            'description': f'Focus on improving rankings for {len(top_opportunities)} keywords that could generate ${total_opportunity:,.0f}/month in additional revenue',
            'keywords': [ko['query'] for ko in top_opportunities],
            'estimated_monthly_impact': round(total_opportunity, 2),
            'effort': 'medium',
            'timeframe': '30-60 days'
        })
    
    # Recommendation 2: Improve conversion rate on high-traffic pages
    if landing_page_revenue:
        below_avg_converting = [
            lpr for lpr in landing_page_revenue
            if lpr.conversion_rate < overall_metrics.conversion_rate * 0.7
            and lpr.sessions >= 100
        ][:5]
        
        if below_avg_converting:
            potential_revenue = sum(
                lpr.sessions * (overall_metrics.conversion_rate - lpr.conversion_rate) * overall_metrics.avg_order_value
                for lpr in below_avg_converting
            ) * 30
            
            recommendations.append({
                'priority': 'high',
                'category': 'conversion_optimization',
                'title': 'Optimize Conversion Rate on High-Traffic Pages',
                'description': f'Improve conversion rates on {len(below_avg_converting)} pages currently underperforming',
                'pages': [lpr.url for lpr in below_avg_converting],
                'estimated_monthly_impact': round(potential_revenue, 2),
                'effort': 'medium',
                'timeframe': '30-45 days'
            })
    
    # Recommendation 3: Increase AOV on top revenue pages
    if landing_page_revenue:
        high_traffic_pages = [
            lpr for lpr in landing_page_revenue
            if lpr.transactions >= 10
        ][:10]
        
        if high_traffic_pages:
            # Estimate 10% AOV increase impact
            aov_increase_impact = sum(
                lpr.transactions * overall_metrics.avg_order_value * 0.10
                for lpr in high_traffic_pages
            ) * 30
            
            recommendations.append({
                'priority': 'medium',
                'category': 'aov_optimization',
                'title': 'Implement AOV Increase Strategies',
                'description': f'Add upsells, cross-sells, and bundles on top {len(high_traffic_pages)} converting pages',
                'pages': [lpr.url for lpr in high_traffic_pages],
                'estimated_monthly_impact': round(aov_increase_impact, 2),
                'effort': 'high',
                'timeframe': '45-90 days'
            })
    
    # Recommendation 4: Fix funnel drop-offs
    if funnel_stages and len(funnel_stages) > 1:
        high_drop_off_stages = [
            fs for fs in funnel_stages[1:]  # Skip first stage
            if fs.drop_off_rate > 0.5
        ]
        
        if high_drop_off_stages:
            recommendations.append({
                'priority': 'high',
                'category': 'funnel_optimization',
                'title': 'Reduce Conversion Funnel Drop-offs',
                'description': f'Address high drop-off rates at {len(high_drop_off_stages)} funnel stages',
                'stages': [fs.stage for fs in high_drop_off_stages],
                'effort': 'medium',
                'timeframe': '30-60 days'
            })
    
    # Recommendation 5: Scale successful keyword patterns
    if keyword_revenue:
        high_performing = [
            kr for kr in keyword_revenue
            if kr.revenue_per_click > 0 and kr.clicks >= 20
        ][:10]
        
        if high_performing:
            recommendations.append({
                'priority': 'medium',
                'category': 'content_expansion',
                'title': 'Create Content for Similar High-Value Keywords',
                'description': f'Identify and target keywords similar to your top {len(high_performing)} revenue-generating queries',
                'example_keywords': [kr.query for kr in high_performing[:5]],
                'effort': 'high',
                'timeframe': '60-90 days'
            })
    
    return recommendations
