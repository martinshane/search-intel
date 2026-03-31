import { GetServerSideProps } from 'next';
import Head from 'next/head';
import { useState } from 'react';
import { ChevronDown, ChevronUp, TrendingUp, TrendingDown, Minus, AlertCircle, CheckCircle, Clock } from 'lucide-react';

// Mock report data structure
const MOCK_REPORT = {
  id: '550e8400-e29b-41d4-a716-446655440000',
  gsc_property: 'https://example.com',
  ga4_property: 'GA4-XXXXX',
  status: 'complete',
  created_at: '2024-03-15T10:30:00Z',
  completed_at: '2024-03-15T10:35:00Z',
  report_data: {
    module_1_health_trajectory: {
      overall_direction: 'declining',
      trend_slope_pct_per_month: -2.3,
      change_points: [
        { date: '2025-11-08', magnitude: -0.12, direction: 'drop' }
      ],
      seasonality: {
        best_day: 'Tuesday',
        worst_day: 'Saturday',
        monthly_cycle: true,
        cycle_description: '15% traffic spike first week of each month'
      },
      anomalies: [
        { date: '2025-12-25', type: 'discord', magnitude: -0.45, description: 'Holiday traffic drop' }
      ],
      forecast: {
        '30d': { clicks: 12400, ci_low: 11200, ci_high: 13600 },
        '60d': { clicks: 11800, ci_low: 10100, ci_high: 13500 },
        '90d': { clicks: 11200, ci_low: 9000, ci_high: 13400 }
      }
    },
    module_2_page_triage: {
      pages: [
        {
          url: '/blog/best-widgets',
          bucket: 'decaying',
          current_monthly_clicks: 340,
          trend_slope: -0.28,
          projected_page1_loss_date: '2026-05-15',
          ctr_anomaly: true,
          ctr_expected: 0.082,
          ctr_actual: 0.031,
          engagement_flag: 'low_engagement',
          priority_score: 87.4,
          recommended_action: 'title_rewrite'
        },
        {
          url: '/products/enterprise',
          bucket: 'growing',
          current_monthly_clicks: 890,
          trend_slope: 0.45,
          ctr_anomaly: false,
          ctr_expected: 0.051,
          ctr_actual: 0.053,
          engagement_flag: 'healthy',
          priority_score: 23.1,
          recommended_action: 'double_down'
        },
        {
          url: '/blog/ultimate-guide',
          bucket: 'critical',
          current_monthly_clicks: 120,
          trend_slope: -0.82,
          projected_page1_loss_date: '2026-02-20',
          ctr_anomaly: true,
          ctr_expected: 0.074,
          ctr_actual: 0.028,
          engagement_flag: 'high_bounce',
          priority_score: 94.7,
          recommended_action: 'urgent_rewrite'
        }
      ],
      summary: {
        total_pages_analyzed: 142,
        growing: 23,
        stable: 67,
        decaying: 38,
        critical: 14,
        total_recoverable_clicks_monthly: 2840
      }
    },
    module_3_serp_landscape: {
      keywords_analyzed: 87,
      serp_feature_displacement: [
        {
          keyword: 'best crm software',
          organic_position: 3,
          visual_position: 8,
          features_above: ['featured_snippet', 'paa_x4', 'ai_overview'],
          estimated_ctr_impact: -0.062
        },
        {
          keyword: 'crm pricing comparison',
          organic_position: 5,
          visual_position: 7,
          features_above: ['paa_x3', 'shopping'],
          estimated_ctr_impact: -0.034
        }
      ],
      competitors: [
        {
          domain: 'competitor.com',
          keywords_shared: 34,
          avg_position: 4.2,
          threat_level: 'high'
        },
        {
          domain: 'bigcompetitor.com',
          keywords_shared: 28,
          avg_position: 6.8,
          threat_level: 'medium'
        }
      ],
      total_click_share: 0.12,
      click_share_opportunity: 0.31
    },
    module_4_content_intelligence: {
      cannibalization_clusters: [
        {
          query_group: 'crm pricing comparison',
          pages: ['/blog/crm-pricing', '/crm-pricing-page'],
          shared_queries: 23,
          total_impressions_affected: 4500,
          recommendation: 'consolidate',
          keep_page: '/crm-pricing-page'
        }
      ],
      striking_distance: [
        {
          query: 'best crm for small business',
          current_position: 11.3,
          impressions: 8900,
          estimated_click_gain_if_top5: 420,
          intent: 'commercial',
          landing_page: '/blog/best-crm'
        },
        {
          query: 'crm implementation guide',
          current_position: 9.8,
          impressions: 5600,
          estimated_click_gain_if_top5: 280,
          intent: 'informational',
          landing_page: '/resources/implementation'
        }
      ],
      thin_content: [
        {
          url: '/blog/short-post',
          word_count: 420,
          impressions: 2300,
          bounce_rate: 0.89,
          recommended_action: 'expand_content'
        }
      ]
    },
    module_5_gameplan: {
      critical: [
        {
          action: 'Rewrite title and meta description for /blog/ultimate-guide',
          pages_affected: ['/blog/ultimate-guide'],
          impact_monthly_clicks: 120,
          effort: 'low',
          reasoning: 'CTR is 62% below expected for position 4. Current title is generic.'
        },
        {
          action: 'Consolidate /blog/crm-pricing into /crm-pricing-page',
          pages_affected: ['/blog/crm-pricing', '/crm-pricing-page'],
          impact_monthly_clicks: 180,
          effort: 'medium',
          reasoning: 'Both pages competing for same 23 queries, splitting authority and confusing users.'
        }
      ],
      quick_wins: [
        {
          action: 'Add FAQ schema to /blog/best-crm to target featured snippet',
          pages_affected: ['/blog/best-crm'],
          impact_monthly_clicks: 340,
          effort: 'low',
          reasoning: 'Query "best crm for small business" shows PAA box, no current holder.'
        },
        {
          action: 'Add 3 internal links to /blog/best-widgets from high-authority blog posts',
          pages_affected: ['/blog/best-widgets'],
          impact_monthly_clicks: 85,
          effort: 'low',
          reasoning: 'Page has low PageRank (0.012) but high traffic potential.'
        }
      ],
      strategic: [
        {
          action: 'Create comprehensive comparison page for "crm vs project management"',
          pages_affected: [],
          impact_monthly_clicks: 450,
          effort: 'high',
          reasoning: 'Gap keyword with 12K monthly impressions, no strong competitor content.'
        }
      ],
      structural: [
        {
          action: 'Reorganize internal linking to flow more authority to conversion pages',
          pages_affected: ['site-wide'],
          impact_monthly_clicks: 0,
          effort: 'high',
          reasoning: '73% of PageRank trapped in blog section, only 4% reaches /pricing and /demo.'
        }
      ],
      total_estimated_monthly_click_recovery: 2840,
      total_estimated_monthly_click_growth: 5200,
      narrative: 'Your site is currently declining at 2.3% per month, primarily due to the November 2025 Core Update which hit thin blog content hardest. However, there are significant quick-win opportunities: 14 pages in critical decay represent 1,200 clicks/month that can be recovered with title rewrites and content updates. The striking distance opportunity set (pages ranking 8-20) represents an additional 3,800 clicks/month with modest content improvements.'
    },
    module_6_algorithm_updates: {
      updates_impacting_site: [
        {
          update_name: 'November 2025 Core Update',
          date: '2025-11-08',
          site_impact: 'negative',
          click_change_pct: -12.3,
          pages_most_affected: ['/blog/thin-post', '/blog/another-thin'],
          common_characteristics: ['thin_content', 'no_schema', 'low_engagement'],
          recovery_status: 'not_recovered'
        }
      ],
      vulnerability_score: 0.72,
      recommendation: 'Focus on content depth for blog section. Pages under 800 words with high bounce rates are most vulnerable.'
    },
    module_7_intent_migration: {
      intent_distribution_current: {
        informational: 0.45,
        commercial: 0.30,
        navigational: 0.15,
        transactional: 0.10
      },
      intent_distribution_6mo_ago: {
        informational: 0.60,
        commercial: 0.22,
        navigational: 0.12,
        transactional: 0.06
      },
      ai_overview_impact: {
        queries_affected: 34,
        estimated_monthly_clicks_lost: 890,
        affected_queries: ['how to choose crm', 'what is crm software', 'crm implementation steps']
      },
      strategic_recommendation: 'Shift content investment toward commercial intent. Informational queries declining due to AI Overview displacement. Commercial/transactional content is more defensible.'
    },
    module_8_ctr_modeling: {
      ctr_model_accuracy: 0.84,
      keyword_ctr_analysis: [
        {
          keyword: 'best crm software',
          position: 3,
          expected_ctr_generic: 0.082,
          expected_ctr_contextual: 0.021,
          actual_ctr: 0.018,
          performance: 'in_line',
          serp_features_present: ['featured_snippet', 'paa_x4', 'shopping']
        }
      ],
      feature_opportunities: [
        {
          keyword: 'crm implementation guide',
          feature: 'featured_snippet',
          current_holder: 'competitor.com',
          estimated_click_gain: 340,
          difficulty: 'medium'
        }
      ]
    },
    module_9_site_architecture: {
      pagerank_distribution: {
        top_authority_pages: [
          { url: '/', pagerank: 0.089 },
          { url: '/blog/', pagerank: 0.034 }
        ],
        starved_pages: [
          { url: '/pricing', pagerank: 0.003, potential: 'high' },
          { url: '/demo', pagerank: 0.002, potential: 'high' }
        ],
        authority_sinks: [
          { url: '/blog/old-popular-post', pagerank: 0.028, traffic_value: 'low' }
        ]
      },
      authority_flow_to_conversion: 0.04,
      orphan_pages: ['/resources/hidden-guide'],
      link_recommendations: [
        {
          target_page: '/pricing',
          link_from: '/blog/crm-guide',
          suggested_anchor: 'CRM pricing comparison',
          estimated_pagerank_boost: 0.023
        }
      ]
    },
    module_10_branded_split: {
      branded_ratio: 0.94,
      dependency_level: 'critical',
      branded_trend: { direction: 'stable', slope: 0.001 },
      non_branded_trend: { direction: 'growing', slope: 0.08 },
      non_branded_opportunity: {
        current_monthly_clicks: 340,
        potential_monthly_clicks: 2800,
        gap: 2460,
        months_to_meaningful_at_current_rate: 14,
        months_to_meaningful_with_actions: 6
      }
    },
    module_11_competitive_threats: {
      primary_competitors: [
        {
          domain: 'competitor.com',
          keyword_overlap: 34,
          avg_position: 4.2
        }
      ],
      emerging_threats: [
        {
          domain: 'newcomer.com',
          first_seen: '2026-02-15',
          keywords_entered: 8,
          avg_entry_position: 12.3,
          current_avg_position: 7.1,
          trajectory: 'rapidly_improving',
          threat_level: 'high'
        }
      ],
      keyword_vulnerability: [
        {
          keyword: 'best crm software',
          your_position: 5,
          competitors_within_3: 4,
          gap_trend: 'narrowing'
        }
      ]
    },
    module_12_revenue_attribution: {
      total_search_attributed_revenue_monthly: 34000,
      revenue_at_risk_90d: 4200,
      top_revenue_keywords: [
        {
          keyword: 'crm pricing',
          current_revenue_monthly: 2100,
          potential_revenue_if_top3: 5400,
          gap: 3300
        }
      ],
      action_roi: {
        critical_fixes_monthly_value: 2800,
        quick_wins_monthly_value: 5200,
        strategic_plays_monthly_value: 12000,
        total_opportunity: 20000
      }
    }
  }
};

interface CollapsibleSectionProps {
  title: string;
  tldr: string;
  icon?: React.ReactNode;
  defaultOpen?: boolean;
  children: React.ReactNode;
}

const CollapsibleSection: React.FC<CollapsibleSectionProps> = ({
  title,
  tldr,
  icon,
  defaultOpen = false,
  children
}) => {
  const [isOpen, setIsOpen] = useState(defaultOpen);

  return (
    <div className="bg-white rounded-lg shadow-md border border-gray-200 mb-6 overflow-hidden">
      <button
        onClick={() => setIsOpen(!isOpen)}
        className="w-full px-6 py-4 flex items-center justify-between hover:bg-gray-50 transition-colors"
      >
        <div className="flex items-center gap-3 flex-1 text-left">
          {icon && <div className="text-blue-600">{icon}</div>}
          <div className="flex-1">
            <h2 className="text-xl font-semibold text-gray-900">{title}</h2>
            <p className="text-sm text-gray-600 mt-1">{tldr}</p>
          </div>
        </div>
        <div className="ml-4">
          {isOpen ? (
            <ChevronUp className="w-5 h-5 text-gray-500" />
          ) : (
            <ChevronDown className="w-5 h-5 text-gray-500" />
          )}
        </div>
      </button>
      {isOpen && (
        <div className="px-6 py-4 border-t border-gray-200 bg-gray-50">
          {children}
        </div>
      )}
    </div>
  );
};

const TrendIcon: React.FC<{ direction: string }> = ({ direction }) => {
  if (direction === 'declining' || direction === 'strong_decline') {
    return <TrendingDown className="w-5 h-5 text-red-600" />;
  }
  if (direction === 'growth' || direction === 'strong_growth') {
    return <TrendingUp className="w-5 h-5 text-green-600" />;
  }
  return <Minus className="w-5 h-5 text-gray-600" />;
};

const StatusBadge: React.FC<{ status: string }> = ({ status }) => {
  const colors: Record<string, string> = {
    critical: 'bg-red-100 text-red-800 border-red-200',
    decaying: 'bg-orange-100 text-orange-800 border-orange-200',
    stable: 'bg-gray-100 text-gray-800 border-gray-200',
    growing: 'bg-green-100 text-green-800 border-green-200'
  };

  return (
    <span className={`px-2 py-1 rounded text-xs font-medium border ${colors[status] || colors.stable}`}>
      {status}
    </span>
  );
};

interface ReportPageProps {
  report: typeof MOCK_REPORT;
}

export default function ReportPage({ report }: ReportPageProps) {
  const data = report.report_data;

  return (
    <>
      <Head>
        <title>Search Intelligence Report - {report.gsc_property}</title>
        <meta name="description" content="Comprehensive SEO analysis and recommendations" />
      </Head>

      <div className="min-h-screen bg-gray-100">
        {/* Header */}
        <div className="bg-white border-b border-gray-200">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
            <div className="flex items-center justify-between">
              <div>
                <h1 className="text-3xl font-bold text-gray-900">Search Intelligence Report</h1>
                <p className="text-gray-600 mt-1">{report.gsc_property}</p>
              </div>
              <div className="text-right">
                <div className="text-sm text-gray-500">Generated</div>
                <div className="text-sm font-medium text-gray-900">
                  {new Date(report.created_at).toLocaleDateString()}
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* Main Content */}
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
          
          {/* Module 1: Health & Trajectory */}
          <CollapsibleSection
            title="1. Health & Trajectory"
            tldr={`Your site is ${data.module_1_health_trajectory.overall_direction} at ${Math.abs(data.module_1_health_trajectory.trend_slope_pct_per_month)}% per month`}
            icon={<TrendIcon direction={data.module_1_health_trajectory.overall_direction} />}
            defaultOpen={true}
          >
            <div className="space-y-4">
              <div className="bg-white p-4 rounded border border-gray-200">
                <h3 className="font-semibold mb-3 text-gray-900">Trend Analysis</h3>
                <div className="grid grid-cols-3 gap-4">
                  <div>
                    <div className="text-sm text-gray-500">Direction</div>
                    <div className="text-lg font-semibold capitalize">{data.module_1_health_trajectory.overall_direction}</div>
                  </div>
                  <div>
                    <div className="text-sm text-gray-500">Monthly Change</div>
                    <div className="text-lg font-semibold">{data.module_1_health_trajectory.trend_slope_pct_per_month}%</div>
                  </div>
                  <div>
                    <div className="text-sm text-gray-500">Best Day</div>
                    <div className="text-lg font-semibold">{data.module_1_health_trajectory.seasonality.best_day}</div>
                  </div>
                </div>
              </div>

              <div className="bg-white p-4 rounded border border-gray-200">
                <h3 className="font-semibold mb-3 text-gray-900">Forecast (Next 90 Days)</h3>
                <div className="space-y-2">
                  {Object.entries(data.module_1_health_trajectory.forecast).map(([period, values]) => (
                    <div key={period} className="flex justify-between items-center py-2 border-b border-gray-100 last:border-0">
                      <span className="text-sm font-medium text-gray-700">{period}</span>
                      <span className="text-sm text-gray-900">
                        {values.clicks.toLocaleString()} clicks 
                        <span className="text-gray-500 ml-2">
                          (±{Math.round((values.ci_high - values.ci_low) / 2).toLocaleString()})
                        </span>
                      </span>
                    </div>
                  ))}
                </div>
              </div>

              {data.module_1_health_trajectory.change_points.length > 0 && (
                <div className="bg-white p-4 rounded border border-gray-200">
                  <h3 className="font-semibold mb-3 text-gray-900">Change Points</h3>
                  <div className="space-y-2">
                    {data.module_1_health_trajectory.change_points.map((cp, idx) => (
                      <div key={idx} className="flex items-center gap-3 p-2 bg-yellow-50 rounded border border-yellow-200">
                        <AlertCircle className="w-4 h-4 text-yellow-600 flex-shrink-0" />
                        <div className="flex-1">
                          <div className="text-sm font-medium text-gray-900">{new Date(cp.date).toLocaleDateString()}</div>
                          <div className="text-xs text-gray-600">
                            {cp.direction} of {Math.abs(cp.magnitude * 100).toFixed(1)}%
                          </div>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              <div className="bg-blue-50 p-4 rounded border border-blue-200">
                <div className="text-sm text-gray-700">
                  <strong>Visualization Placeholder:</strong> Line chart showing clicks over time with trend line, 
                  seasonal decomposition, forecast with confidence intervals, and change point markers.
                </div>
              </div>
            </div>
          </CollapsibleSection>

          {