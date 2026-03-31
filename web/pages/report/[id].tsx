import { useRouter } from 'next/router';
import { useEffect, useState } from 'react';
import Head from 'next/head';
import {
  LineChart,
  Line,
  ScatterChart,
  Scatter,
  BarChart,
  Bar,
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  ReferenceLine,
  Cell,
} from 'recharts';
import { supabase } from '../../lib/supabase';

interface Report {
  id: string;
  status: string;
  progress: Record<string, string>;
  report_data: ReportData | null;
  gsc_property: string;
  ga4_property: string | null;
  created_at: string;
  completed_at: string | null;
}

interface ReportData {
  health_trajectory?: HealthTrajectory;
  page_triage?: PageTriage;
  serp_landscape?: SerpLandscape;
  content_intelligence?: ContentIntelligence;
  gameplan?: Gameplan;
  algorithm_impacts?: AlgorithmImpacts;
  intent_migration?: IntentMigration;
  ctr_modeling?: CtrModeling;
  site_architecture?: SiteArchitecture;
  branded_split?: BrandedSplit;
  competitive_threats?: CompetitiveThreats;
  revenue_attribution?: RevenueAttribution;
}

interface HealthTrajectory {
  overall_direction: string;
  trend_slope_pct_per_month: number;
  change_points: ChangePoint[];
  seasonality: Seasonality;
  anomalies: Anomaly[];
  forecast: Forecast;
}

interface ChangePoint {
  date: string;
  magnitude: number;
  direction: string;
}

interface Seasonality {
  best_day: string;
  worst_day: string;
  monthly_cycle: boolean;
  cycle_description: string;
}

interface Anomaly {
  date: string;
  type: string;
  magnitude: number;
}

interface Forecast {
  '30d': ForecastPeriod;
  '60d': ForecastPeriod;
  '90d': ForecastPeriod;
}

interface ForecastPeriod {
  clicks: number;
  ci_low: number;
  ci_high: number;
}

interface PageTriage {
  pages: PageAnalysis[];
  summary: PageSummary;
}

interface PageAnalysis {
  url: string;
  bucket: string;
  current_monthly_clicks: number;
  trend_slope: number;
  projected_page1_loss_date?: string;
  ctr_anomaly: boolean;
  ctr_expected?: number;
  ctr_actual?: number;
  engagement_flag?: string;
  priority_score: number;
  recommended_action: string;
}

interface PageSummary {
  total_pages_analyzed: number;
  growing: number;
  stable: number;
  decaying: number;
  critical: number;
  total_recoverable_clicks_monthly: number;
}

interface SerpLandscape {
  keywords_analyzed: number;
  serp_feature_displacement: SerpDisplacement[];
  competitors: Competitor[];
  intent_mismatches: IntentMismatch[];
  total_click_share: number;
  click_share_opportunity: number;
}

interface SerpDisplacement {
  keyword: string;
  organic_position: number;
  visual_position: number;
  features_above: string[];
  estimated_ctr_impact: number;
}

interface Competitor {
  domain: string;
  keywords_shared: number;
  avg_position: number;
  threat_level: string;
}

interface IntentMismatch {
  keyword: string;
  serp_intent: string;
  page_type: string;
  recommendation: string;
}

interface ContentIntelligence {
  cannibalization_clusters: CannibalizationCluster[];
  striking_distance: StrikingDistance[];
  thin_content: ThinContent[];
  update_priority_matrix: UpdatePriorityMatrix;
}

interface CannibalizationCluster {
  query_group: string;
  pages: string[];
  shared_queries: number;
  total_impressions_affected: number;
  recommendation: string;
  keep_page?: string;
}

interface StrikingDistance {
  query: string;
  current_position: number;
  impressions: number;
  estimated_click_gain_if_top5: number;
  intent: string;
  landing_page: string;
}

interface ThinContent {
  url: string;
  word_count: number;
  impressions: number;
  bounce_rate: number;
  recommendation: string;
}

interface UpdatePriorityMatrix {
  urgent_update: string[];
  leave_alone: string[];
  structural_problem: string[];
  double_down: string[];
}

interface Gameplan {
  critical: Action[];
  quick_wins: Action[];
  strategic: Action[];
  structural: Action[];
  total_estimated_monthly_click_recovery: number;
  total_estimated_monthly_click_growth: number;
  narrative: string;
}

interface Action {
  action: string;
  pages_affected?: string[];
  keywords_affected?: string[];
  impact: number;
  effort: string;
  dependencies?: string[];
}

interface AlgorithmImpacts {
  updates_impacting_site: AlgorithmUpdate[];
  vulnerability_score: number;
  recommendation: string;
}

interface AlgorithmUpdate {
  update_name: string;
  date: string;
  site_impact: string;
  click_change_pct: number;
  pages_most_affected: string[];
  common_characteristics: string[];
  recovery_status: string;
}

interface IntentMigration {
  intent_distribution_current: IntentDistribution;
  intent_distribution_6mo_ago: IntentDistribution;
  ai_overview_impact: AIOverviewImpact;
  strategic_recommendation: string;
}

interface IntentDistribution {
  informational: number;
  commercial: number;
  navigational: number;
  transactional: number;
}

interface AIOverviewImpact {
  queries_affected: number;
  estimated_monthly_clicks_lost: number;
  affected_queries: string[];
}

interface CtrModeling {
  ctr_model_accuracy: number;
  keyword_ctr_analysis: KeywordCtrAnalysis[];
  feature_opportunities: FeatureOpportunity[];
}

interface KeywordCtrAnalysis {
  keyword: string;
  position: number;
  expected_ctr_generic: number;
  expected_ctr_contextual: number;
  actual_ctr: number;
  performance: string;
  serp_features_present: string[];
}

interface FeatureOpportunity {
  keyword: string;
  feature: string;
  current_holder?: string;
  estimated_click_gain: number;
  difficulty: string;
}

interface SiteArchitecture {
  pagerank_distribution: PageRankDistribution;
  authority_flow_to_conversion: number;
  orphan_pages: string[];
  content_silos: ContentSilo[];
  link_recommendations: LinkRecommendation[];
  network_graph_data?: NetworkGraphData;
}

interface PageRankDistribution {
  top_authority_pages: AuthorityPage[];
  starved_pages: AuthorityPage[];
  authority_sinks: AuthorityPage[];
}

interface AuthorityPage {
  url: string;
  pagerank: number;
  clicks?: number;
}

interface ContentSilo {
  name: string;
  pages: number;
  internal_pagerank_share: number;
}

interface LinkRecommendation {
  target_page: string;
  link_from: string;
  suggested_anchor: string;
  estimated_pagerank_boost: number;
}

interface NetworkGraphData {
  nodes: NetworkNode[];
  edges: NetworkEdge[];
}

interface NetworkNode {
  id: string;
  pagerank: number;
  clicks: number;
}

interface NetworkEdge {
  source: string;
  target: string;
  anchor?: string;
}

interface BrandedSplit {
  branded_ratio: number;
  dependency_level: string;
  branded_trend: TrendSummary;
  non_branded_trend: TrendSummary;
  non_branded_opportunity: NonBrandedOpportunity;
}

interface TrendSummary {
  direction: string;
  slope: number;
}

interface NonBrandedOpportunity {
  current_monthly_clicks: number;
  potential_monthly_clicks: number;
  gap: number;
  months_to_meaningful_at_current_rate: number;
  months_to_meaningful_with_actions: number;
}

interface CompetitiveThreats {
  primary_competitors: PrimaryCompetitor[];
  emerging_threats: EmergingThreat[];
  keyword_vulnerability: KeywordVulnerability[];
}

interface PrimaryCompetitor {
  domain: string;
  keyword_overlap: number;
  avg_position: number;
}

interface EmergingThreat {
  domain: string;
  first_seen: string;
  keywords_entered: number;
  avg_entry_position: number;
  current_avg_position: number;
  trajectory: string;
  threat_level: string;
}

interface KeywordVulnerability {
  keyword: string;
  your_position: number;
  competitors_within_3: number;
  gap_trend: string;
}

interface RevenueAttribution {
  total_search_attributed_revenue_monthly: number;
  revenue_at_risk_90d: number;
  top_revenue_keywords: RevenueKeyword[];
  action_roi: ActionROI;
}

interface RevenueKeyword {
  keyword: string;
  current_revenue_monthly: number;
  potential_revenue_if_top3: number;
  gap: number;
}

interface ActionROI {
  critical_fixes_monthly_value: number;
  quick_wins_monthly_value: number;
  strategic_plays_monthly_value: number;
  total_opportunity: number;
}

export default function ReportPage() {
  const router = useRouter();
  const { id } = router.query;
  const [report, setReport] = useState<Report | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [expandedSections, setExpandedSections] = useState<Set<string>>(new Set(['health']));

  useEffect(() => {
    if (!id || typeof id !== 'string') return;

    const fetchReport = async () => {
      try {
        setLoading(true);
        setError(null);

        const { data, error: fetchError } = await supabase
          .from('reports')
          .select('*')
          .eq('id', id)
          .single();

        if (fetchError) throw fetchError;

        setReport(data);
      } catch (err) {
        console.error('Error fetching report:', err);
        setError(err instanceof Error ? err.message : 'Failed to load report');
      } finally {
        setLoading(false);
      }
    };

    fetchReport();

    // Poll for updates if report is in progress
    const pollInterval = setInterval(() => {
      if (report && ['pending', 'ingesting', 'analyzing', 'generating'].includes(report.status)) {
        fetchReport();
      }
    }, 5000);

    return () => clearInterval(pollInterval);
  }, [id, report?.status]);

  const toggleSection = (section: string) => {
    setExpandedSections((prev) => {
      const newSet = new Set(prev);
      if (newSet.has(section)) {
        newSet.delete(section);
      } else {
        newSet.add(section);
      }
      return newSet;
    });
  };

  const formatNumber = (num: number | null | undefined): string => {
    if (num === null || num === undefined || isNaN(num)) return 'N/A';
    return new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 }).format(num);
  };

  const formatPercent = (num: number | null | undefined): string => {
    if (num === null || num === undefined || isNaN(num)) return 'N/A';
    return `${(num * 100).toFixed(1)}%`;
  };

  const formatCurrency = (num: number | null | undefined): string => {
    if (num === null || num === undefined || isNaN(num)) return 'N/A';
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD',
      maximumFractionDigits: 0,
    }).format(num);
  };

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin rounded-full h-16 w-16 border-b-2 border-blue-600 mx-auto mb-4"></div>
          <p className="text-gray-600">Loading report...</p>
        </div>
      </div>
    );
  }

  if (error || !report) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <h2 className="text-2xl font-bold text-gray-900 mb-2">Error Loading Report</h2>
          <p className="text-gray-600">{error || 'Report not found'}</p>
          <button
            onClick={() => router.push('/')}
            className="mt-4 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700"
          >
            Back to Home
          </button>
        </div>
      </div>
    );
  }

  if (report.status !== 'complete') {
    const progressSteps = ['pending', 'ingesting', 'analyzing', 'generating'];
    const currentStepIndex = progressSteps.indexOf(report.status);
    const progressPercent = ((currentStepIndex + 1) / progressSteps.length) * 100;

    return (
      <div className="min-h-screen bg-gray-50">
        <Head>
          <title>Generating Report... | Search Intelligence Report</title>
        </Head>
        <div className="max-w-4xl mx-auto px-4 py-16">
          <div className="bg-white rounded-lg shadow-sm p-8">
            <h1 className="text-3xl font-bold text-gray-900 mb-6">Generating Your Report</h1>
            <div className="mb-4">
              <div className="w-full bg-gray-200 rounded-full h-4">
                <div
                  className="bg-blue-600 h-4 rounded-full transition-all duration-500"
                  style={{ width: `${progressPercent}%` }}
                ></div>
              </div>
            </div>
            <p className="text-gray-600 mb-4">
              Status: <span className="font-semibold capitalize">{report.status}</span>
            </p>
            {report.progress && Object.keys(report.progress).length > 0 && (
              <div className="mt-6">
                <h3 className="text-sm font-semibold text-gray-700 mb-2">Module Progress:</h3>
                <div className="space-y-1">
                  {Object.entries(report.progress).map(([module, status]) => (
                    <div key={module} className="flex items-center text-sm">
                      <span className="w-32 text-gray-600">{module}:</span>
                      <span className={`font-medium ${status === 'complete' ? 'text-green-600' : 'text-blue-600'}`}>
                        {status}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            )}
            <p className="text-sm text-gray-500 mt-6">
              This usually takes 2-5 minutes. This page will refresh automatically when complete.
            </p>
          </div>
        </div>
      </div>
    );
  }

  const data = report.report_data;

  if (!data) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <h2 className="text-2xl font-bold text-gray-900 mb-2">Report Data Not Available</h2>
          <p className="text-gray-600">The report completed but data is missing.</p>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50">
      <Head>
        <title>Search Intelligence Report | {report.gsc_property}</title>
      </Head>

      {/* Header */}
      <div className="bg-white border-b border-gray-200 sticky top-0 z-10">
        <div className="max-w-7xl mx-auto px-4 py-6">
          <h1 className="text-3xl font-bold text-gray-900">Search Intelligence Report</h1>
          <p className="text-gray-600 mt-1">{report.gsc_property}</p>
          <p className="text-sm text-gray-500 mt-1">
            Generated {new Date(report.created_at).toLocaleDateString()}
          </p>
        </div>
      </div>

      <div className="max-w-7xl mx-auto px-4 py-8">
        {/* Health & Trajectory */}
        {data.health_trajectory && (
          <Section
            title="Health & Trajectory"
            id="health"
            expanded={expandedSections.has('health')}
            onToggle={() => toggleSection('health')}
            summary={`${data.health_trajectory.overall_direction} — ${data.health_trajectory.trend_slope_pct_per_month >= 0 ? '+' : ''}${data.health_trajectory.trend_slope_pct_per_month.toFixed(1)}% per month`}
          >
            <div className="space-y-6">
              <div>
                <h4 className="font-semibold text-gray-900 mb-2">Trend Direction</h4>
                <p className="text-gray-700">
                  Your site is <span className="font-semibold">{data.health_trajectory.overall_direction}</span> at a
                  rate of{' '}
                  <span className="font-semibold">
                    {data.health_trajectory.trend_slope_pct_per_month >= 0 ? '+' : ''}
                    {data.health_trajectory.trend_slope_pct_per_month.toFixed(1)}%
                  </span>{' '}
                  per month.
                </p>
              </div>

              {data.health_trajectory.forecast && (
                <div>
                  <h4 className="font-semibold text-gray-900 mb-3">90-Day Forecast</h4>
                  <div className="grid grid-cols-3 gap-4">
                    {['30d', '60d', '90d'].map((period) => {
                      const forecast = data.health_trajectory!.forecast[period as keyof Forecast];
                      if (!forecast) return null;
                      return (
                        <div key={period} className="bg-gray-50 rounded-lg p-4">
                          <div className="text-sm text-gray-600 mb-1">{period}</div>
                          <div className="text-2xl font-bold text-gray-900">
                            {formatNumber(forecast.clicks)}
                          </div>
                          <div className="text-xs text-gray-500 mt-1">
                            {formatNumber(forecast.ci_low)} - {formatNumber(forecast.ci_high)}
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </div>
              )}

              {data.health_trajectory.change_points && data.health_trajectory.change_points.length > 0 && (
                <div>
                  <h4 className="font-semibold text-gray-900 mb-3">Change Points Detected</h4>
                  <div className="space-y-2">
                    {data.health_trajectory.change_points.map((cp, i) => (
                      <div key={i} className="flex items-center justify-between p-3 bg-gray-50 rounded">
                        <div>
                          <span className="font-medium">{cp.date}</span>
                          <span className="ml-2 text-gray-600">
                            {cp.direction} ({(cp.magnitude * 100).toFixed(1)}%)
                          </span>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {data.health_trajectory.seasonality && (
                <div>
                  <h4 className="font-semibold text-gray-900 mb-2">Seasonality</h4>
                  <p className="text-gray-700">
                    Best day: <span className="font-semibold">{data.health_trajectory.seasonality.best_day}</span>
                    {' • '}
                    Worst day: <span className="font-semibold">{data.health_trajectory.seasonality.worst_day}</span>
                  </p>
                  {data.health_trajectory.seasonality.cycle_description && (
                    <p className="text-gray-600 mt-2">{data.health_trajectory.seasonality.cycle_description}</p>
                  )}
                </div>
              )}
            </div>
          </Section>
        )}

        {/* Page-Level Triage */}
        {data.page_triage && (
          <Section
            title="Page-Level Triage"
            id="triage"
            expanded={expandedSections.has('triage')}
            onToggle={() => toggleSection('triage')}
            summary={`${data.page_triage.summary.critical} critical pages, ${formatNumber(data.page_triage.summary.total_recoverable_clicks_monthly)} recoverable clicks/month`}
          >
            <div className="space-y-6">
              {data.page_triage.summary && (
                <div className="grid grid-cols-5 gap-4">
                  <div className="bg-green-50 rounded-lg p-4">
                    <div className="text-sm text-green-700 mb-1">Growing</div>
                    <div className="text-2xl font-bold text-green-900">{data.page_triage.summary.growing}</div>
                  </div>
                  <div className="bg-blue-50 rounded-lg p-4">
                    <div className="text-sm text-blue-700 mb-1">Stable</div>
                    <div className="text-2xl font-bold text-blue-900">{data.page_triage.summary