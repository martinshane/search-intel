import React, { useState, useMemo } from 'react';
import {
  BarChart,
  Bar,
  ScatterChart,
  Scatter,
  PieChart,
  Pie,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  Cell,
  RadarChart,
  PolarGrid,
  PolarAngleAxis,
  PolarRadiusAxis,
  Radar,
} from 'recharts';
import { Shield, AlertTriangle, Target, Eye, TrendingUp, Search, ChevronDown, ChevronUp } from 'lucide-react';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface DisplacementEntry {
  keyword: string;
  organic_position: number;
  visual_position: number;
  displacement: number;
  features_above: string[];
  estimated_ctr_impact: number;
}

interface Competitor {
  domain: string;
  keywords_shared: number;
  overlap_percentage: number;
  avg_position: number;
  threat_level: 'critical' | 'high' | 'medium' | 'low';
}

interface IntentAnalysis {
  intent_distribution: Record<string, number>;
  intent_mismatches: Array<{
    keyword: string;
    serp_intent: string;
    page_type: string;
    user_position: number;
    recommendation: string;
  }>;
}

interface ClickShare {
  total_click_share: number;
  current_monthly_clicks: number;
  potential_monthly_clicks: number;
  click_opportunity: number;
  keyword_breakdown: Array<{
    keyword: string;
    clicks: number;
    potential_clicks: number;
    click_share: number;
    position: number;
  }>;
}

interface FeatureSummary {
  feature_prevalence: Record<string, { count: number; pct: number }>;
  feature_sample_keywords: Record<string, string[]>;
}

interface SerpSummary {
  keywords_analyzed: number;
  keywords_with_significant_displacement: number;
  avg_visual_displacement: number;
  primary_competitors_count: number;
  total_click_share: number;
  click_opportunity_size: number;
  dominant_intent: string;
  intent_mismatches_found: number;
}

interface Module3Data {
  keywords_analyzed: number;
  serp_feature_displacement: DisplacementEntry[];
  serp_feature_summary: FeatureSummary;
  competitors: Competitor[];
  intent_analysis: IntentAnalysis;
  click_share: ClickShare;
  summary: SerpSummary;
}

interface Module3SerpLandscapeProps {
  data: Module3Data | null;
  loading?: boolean;
  error?: string | null;
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const THREAT_COLORS: Record<string, string> = {
  critical: '#ef4444',
  high: '#f97316',
  medium: '#eab308',
  low: '#6b7280',
};

const INTENT_COLORS: Record<string, string> = {
  informational: '#3b82f6',
  commercial: '#8b5cf6',
  transactional: '#10b981',
  navigational: '#f59e0b',
};

const FEATURE_LABELS: Record<string, string> = {
  featured_snippet: 'Featured Snippet',
  knowledge_panel: 'Knowledge Panel',
  ai_overview: 'AI Overview',
  local_pack: 'Local Pack',
  people_also_ask: 'People Also Ask',
  video_carousel: 'Video Carousel',
  image_pack: 'Image Pack',
  shopping_results: 'Shopping Results',
  top_stories: 'Top Stories',
  reddit_threads: 'Reddit Threads',
};

const formatNumber = (num: number): string => {
  if (num >= 1000000) return (num / 1000000).toFixed(1) + 'M';
  if (num >= 1000) return (num / 1000).toFixed(1) + 'K';
  return num.toFixed(0);
};

const formatPct = (num: number): string => `${(num * 100).toFixed(1)}%`;

// ---------------------------------------------------------------------------
// Sub-components
// ---------------------------------------------------------------------------

function SummaryCards({ summary }: { summary: SerpSummary }) {
  return (
    <div className="grid grid-cols-2 lg:grid-cols-4 gap-3 mb-6">
      <div className="bg-blue-50 rounded-lg p-4">
        <div className="flex items-center space-x-2 mb-1">
          <Search className="w-4 h-4 text-blue-600" />
          <span className="text-xs font-medium text-blue-600 uppercase">Keywords</span>
        </div>
        <div className="text-2xl font-bold text-blue-900">{summary.keywords_analyzed}</div>
        <div className="text-xs text-blue-700 mt-1">analyzed</div>
      </div>
      <div className="bg-orange-50 rounded-lg p-4">
        <div className="flex items-center space-x-2 mb-1">
          <Eye className="w-4 h-4 text-orange-600" />
          <span className="text-xs font-medium text-orange-600 uppercase">Displacement</span>
        </div>
        <div className="text-2xl font-bold text-orange-900">{summary.keywords_with_significant_displacement}</div>
        <div className="text-xs text-orange-700 mt-1">visually displaced keywords</div>
      </div>
      <div className="bg-red-50 rounded-lg p-4">
        <div className="flex items-center space-x-2 mb-1">
          <Shield className="w-4 h-4 text-red-600" />
          <span className="text-xs font-medium text-red-600 uppercase">Competitors</span>
        </div>
        <div className="text-2xl font-bold text-red-900">{summary.primary_competitors_count}</div>
        <div className="text-xs text-red-700 mt-1">high-threat competitors</div>
      </div>
      <div className="bg-green-50 rounded-lg p-4">
        <div className="flex items-center space-x-2 mb-1">
          <TrendingUp className="w-4 h-4 text-green-600" />
          <span className="text-xs font-medium text-green-600 uppercase">Click Share</span>
        </div>
        <div className="text-2xl font-bold text-green-900">{formatPct(summary.total_click_share)}</div>
        <div className="text-xs text-green-700 mt-1">
          {formatNumber(summary.click_opportunity_size)} clicks recoverable
        </div>
      </div>
    </div>
  );
}

function SerpFeatureChart({ featureSummary }: { featureSummary: FeatureSummary }) {
  const chartData = useMemo(() => {
    return Object.entries(featureSummary.feature_prevalence)
      .map(([feature, data]) => ({
        feature: FEATURE_LABELS[feature] || feature,
        pct: data.pct,
        count: data.count,
      }))
      .sort((a, b) => b.pct - a.pct);
  }, [featureSummary]);

  if (chartData.length === 0) return null;

  const COLORS_LIST = ['#3b82f6', '#8b5cf6', '#10b981', '#f59e0b', '#ef4444', '#06b6d4', '#ec4899', '#84cc16'];

  return (
    <div className="mb-6">
      <h4 className="text-sm font-semibold text-gray-900 mb-3">SERP Feature Prevalence</h4>
      <div className="h-64">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={chartData} layout="vertical" margin={{ top: 5, right: 20, left: 120, bottom: 5 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
            <XAxis type="number" tick={{ fontSize: 11 }} tickFormatter={(v) => `${v}%`} />
            <YAxis type="category" dataKey="feature" tick={{ fontSize: 11 }} width={110} />
            <Tooltip
              formatter={(value: number) => [`${value}%`, 'Prevalence']}
              contentStyle={{ fontSize: '12px' }}
            />
            <Bar dataKey="pct" radius={[0, 4, 4, 0]}>
              {chartData.map((_, idx) => (
                <Cell key={idx} fill={COLORS_LIST[idx % COLORS_LIST.length]} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

function DisplacementChart({ displacement }: { displacement: DisplacementEntry[] }) {
  const chartData = useMemo(() => {
    return displacement.slice(0, 20).map((d) => ({
      keyword: d.keyword.length > 25 ? d.keyword.slice(0, 22) + '...' : d.keyword,
      fullKeyword: d.keyword,
      organic: d.organic_position,
      visual: d.visual_position,
      displacement: d.displacement,
    }));
  }, [displacement]);

  if (chartData.length === 0) return null;

  return (
    <div className="mb-6">
      <h4 className="text-sm font-semibold text-gray-900 mb-1">SERP Feature Displacement</h4>
      <p className="text-xs text-gray-500 mb-3">
        Organic rank vs. visual position after SERP features push your listing down
      </p>
      <div className="h-80">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={chartData} layout="vertical" margin={{ top: 5, right: 20, left: 140, bottom: 5 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
            <XAxis type="number" tick={{ fontSize: 11 }} label={{ value: 'Position', position: 'insideBottom', offset: -5, fontSize: 11 }} />
            <YAxis type="category" dataKey="keyword" tick={{ fontSize: 10 }} width={130} />
            <Tooltip
              content={({ active, payload }) => {
                if (!active || !payload?.length) return null;
                const d = payload[0].payload;
                return (
                  <div className="bg-white p-3 border border-gray-200 rounded shadow-lg text-xs">
                    <p className="font-semibold mb-1">{d.fullKeyword}</p>
                    <p>Organic rank: <span className="font-medium text-blue-600">#{d.organic}</span></p>
                    <p>Visual position: <span className="font-medium text-red-600">#{d.visual.toFixed(1)}</span></p>
                    <p>Displacement: <span className="font-medium text-orange-600">+{d.displacement.toFixed(1)}</span></p>
                  </div>
                );
              }}
            />
            <Legend wrapperStyle={{ fontSize: '11px' }} />
            <Bar dataKey="organic" name="Organic Rank" fill="#3b82f6" radius={[0, 4, 4, 0]} />
            <Bar dataKey="visual" name="Visual Position" fill="#ef4444" radius={[0, 4, 4, 0]} />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

function CompetitorTable({ competitors }: { competitors: Competitor[] }) {
  if (competitors.length === 0) return null;

  return (
    <div className="mb-6">
      <h4 className="text-sm font-semibold text-gray-900 mb-3">Competitor Threat Map</h4>
      <div className="overflow-x-auto -mx-4 sm:mx-0">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Domain</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Overlap</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Avg Position</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Threat</th>
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {competitors.slice(0, 15).map((c, idx) => (
              <tr key={idx} className={idx % 2 === 0 ? 'bg-white' : 'bg-gray-50'}>
                <td className="px-4 py-3 text-sm font-medium text-gray-900">{c.domain}</td>
                <td className="px-4 py-3 text-sm text-gray-700">
                  {c.keywords_shared} kws ({c.overlap_percentage}%)
                </td>
                <td className="px-4 py-3 text-sm text-gray-700">#{c.avg_position.toFixed(1)}</td>
                <td className="px-4 py-3">
                  <span
                    className="px-2 py-1 rounded-full text-xs font-medium"
                    style={{
                      backgroundColor: THREAT_COLORS[c.threat_level] + '20',
                      color: THREAT_COLORS[c.threat_level],
                    }}
                  >
                    {c.threat_level}
                  </span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function IntentChart({ intentAnalysis }: { intentAnalysis: IntentAnalysis }) {
  const pieData = useMemo(() => {
    return Object.entries(intentAnalysis.intent_distribution)
      .filter(([_, v]) => v > 0)
      .map(([intent, value]) => ({
        name: intent.charAt(0).toUpperCase() + intent.slice(1),
        value: Math.round(value * 100),
        fill: INTENT_COLORS[intent] || '#6b7280',
      }));
  }, [intentAnalysis]);

  return (
    <div className="mb-6">
      <h4 className="text-sm font-semibold text-gray-900 mb-3">Search Intent Distribution</h4>
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div className="h-56">
          <ResponsiveContainer width="100%" height="100%">
            <PieChart>
              <Pie
                data={pieData}
                cx="50%"
                cy="50%"
                innerRadius={50}
                outerRadius={80}
                paddingAngle={3}
                dataKey="value"
                label={({ name, value }) => `${name} ${value}%`}
              >
                {pieData.map((entry, idx) => (
                  <Cell key={idx} fill={entry.fill} />
                ))}
              </Pie>
              <Tooltip
                formatter={(value: number) => [`${value}%`, 'Share']}
                contentStyle={{ fontSize: '12px' }}
              />
            </PieChart>
          </ResponsiveContainer>
        </div>
        {intentAnalysis.intent_mismatches.length > 0 && (
          <div>
            <h5 className="text-xs font-semibold text-orange-700 mb-2 flex items-center">
              <AlertTriangle className="w-3.5 h-3.5 mr-1" />
              Intent Mismatches ({intentAnalysis.intent_mismatches.length})
            </h5>
            <div className="space-y-2 max-h-48 overflow-y-auto">
              {intentAnalysis.intent_mismatches.slice(0, 8).map((m, idx) => (
                <div key={idx} className="bg-orange-50 rounded p-2 text-xs">
                  <span className="font-medium text-gray-900">{m.keyword}</span>
                  <div className="text-gray-600 mt-0.5">
                    SERP intent: <span className="font-medium">{m.serp_intent}</span> | Your page: <span className="font-medium">{m.page_type}</span>
                  </div>
                  <div className="text-orange-700 mt-0.5">{m.recommendation}</div>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

function ClickShareChart({ clickShare }: { clickShare: ClickShare }) {
  const gaugeData = useMemo(() => {
    const share = Math.round(clickShare.total_click_share * 100);
    return [
      { name: 'Your Click Share', value: share },
      { name: 'Opportunity', value: 100 - share },
    ];
  }, [clickShare]);

  return (
    <div className="mb-6">
      <h4 className="text-sm font-semibold text-gray-900 mb-3">Click Share Analysis</h4>
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4 mb-4">
        <div className="bg-blue-50 rounded-lg p-3 text-center">
          <div className="text-xl font-bold text-blue-900">
            {formatNumber(clickShare.current_monthly_clicks)}
          </div>
          <div className="text-xs text-blue-700">Current Monthly Clicks</div>
        </div>
        <div className="bg-purple-50 rounded-lg p-3 text-center">
          <div className="text-xl font-bold text-purple-900">
            {formatNumber(clickShare.potential_monthly_clicks)}
          </div>
          <div className="text-xs text-purple-700">Potential Monthly Clicks</div>
        </div>
        <div className="bg-green-50 rounded-lg p-3 text-center">
          <div className="text-xl font-bold text-green-900">
            +{formatNumber(clickShare.click_opportunity)}
          </div>
          <div className="text-xs text-green-700">Click Opportunity</div>
        </div>
      </div>

      {clickShare.keyword_breakdown.length > 0 && (
        <div className="h-64">
          <ResponsiveContainer width="100%" height="100%">
            <ScatterChart margin={{ top: 10, right: 10, left: 0, bottom: 10 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
              <XAxis
                type="number"
                dataKey="position"
                name="Position"
                tick={{ fontSize: 11 }}
                label={{ value: 'Position', position: 'insideBottom', offset: -5, fontSize: 11 }}
                reversed
                domain={[1, 'auto']}
              />
              <YAxis
                type="number"
                dataKey="click_share"
                name="Click Share"
                tick={{ fontSize: 11 }}
                tickFormatter={(v) => `${(v * 100).toFixed(0)}%`}
                label={{ value: 'Click Share', angle: -90, position: 'insideLeft', fontSize: 11 }}
              />
              <Tooltip
                content={({ active, payload }) => {
                  if (!active || !payload?.length) return null;
                  const d = payload[0].payload;
                  return (
                    <div className="bg-white p-3 border border-gray-200 rounded shadow-lg text-xs">
                      <p className="font-semibold mb-1">{d.keyword}</p>
                      <p>Position: #{d.position.toFixed(1)}</p>
                      <p>Clicks: {formatNumber(d.clicks)}</p>
                      <p>Potential: {formatNumber(d.potential_clicks)}</p>
                      <p>Click Share: {formatPct(d.click_share)}</p>
                    </div>
                  );
                }}
              />
              <Scatter
                data={clickShare.keyword_breakdown.slice(0, 30)}
                fill="#3b82f6"
              >
                {clickShare.keyword_breakdown.slice(0, 30).map((entry, idx) => (
                  <Cell
                    key={idx}
                    fill={entry.click_share > 0.5 ? '#10b981' : entry.click_share > 0.2 ? '#3b82f6' : '#ef4444'}
                    opacity={0.7}
                  />
                ))}
              </Scatter>
            </ScatterChart>
          </ResponsiveContainer>
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------

export default function Module3SerpLandscape({ data, loading, error }: Module3SerpLandscapeProps) {
  const [expandedSection, setExpandedSection] = useState<string | null>('overview');

  if (loading) {
    return (
      <div className="bg-white rounded-lg border border-gray-200 p-6">
        <div className="animate-pulse space-y-4">
          <div className="h-6 bg-gray-200 rounded w-1/3"></div>
          <div className="grid grid-cols-4 gap-3">
            {[...Array(4)].map((_, i) => (
              <div key={i} className="h-20 bg-gray-100 rounded"></div>
            ))}
          </div>
          <div className="h-64 bg-gray-100 rounded"></div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-white rounded-lg border border-red-200 p-6">
        <div className="flex items-center space-x-2 text-red-700">
          <AlertTriangle className="w-5 h-5" />
          <span className="font-medium">SERP analysis failed</span>
        </div>
        <p className="text-sm text-red-600 mt-2">{error}</p>
      </div>
    );
  }

  if (!data || data.keywords_analyzed === 0) {
    return (
      <div className="bg-white rounded-lg border border-gray-200 p-6">
        <div className="text-center text-gray-500">
          <Search className="w-8 h-8 mx-auto mb-2 text-gray-400" />
          <p className="text-sm">No SERP data available. This module requires DataForSEO integration.</p>
        </div>
      </div>
    );
  }

  const sections = [
    { id: 'overview', label: 'SERP Features' },
    { id: 'displacement', label: 'Feature Displacement' },
    { id: 'competitors', label: 'Competitor Map' },
    { id: 'intent', label: 'Intent Analysis' },
    { id: 'clickshare', label: 'Click Share' },
  ];

  return (
    <div className="space-y-4">
      <SummaryCards summary={data.summary} />

      {sections.map((section) => (
        <div key={section.id} className="bg-white rounded-lg border border-gray-200 overflow-hidden">
          <button
            onClick={() =>
              setExpandedSection(expandedSection === section.id ? null : section.id)
            }
            className="w-full px-4 py-3 flex items-center justify-between hover:bg-gray-50 transition text-left"
          >
            <span className="text-sm font-semibold text-gray-900">{section.label}</span>
            {expandedSection === section.id ? (
              <ChevronUp className="w-4 h-4 text-gray-400" />
            ) : (
              <ChevronDown className="w-4 h-4 text-gray-400" />
            )}
          </button>

          {expandedSection === section.id && (
            <div className="px-4 pb-4 border-t border-gray-100 pt-4">
              {section.id === 'overview' && (
                <SerpFeatureChart featureSummary={data.serp_feature_summary} />
              )}
              {section.id === 'displacement' && (
                <DisplacementChart displacement={data.serp_feature_displacement} />
              )}
              {section.id === 'competitors' && (
                <CompetitorTable competitors={data.competitors} />
              )}
              {section.id === 'intent' && (
                <IntentChart intentAnalysis={data.intent_analysis} />
              )}
              {section.id === 'clickshare' && (
                <ClickShareChart clickShare={data.click_share} />
              )}
            </div>
          )}
        </div>
      ))}
    </div>
  );
}
