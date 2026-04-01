import React from 'react';
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  Cell,
  PieChart,
  Pie,
  RadarChart,
  PolarGrid,
  PolarAngleAxis,
  PolarRadiusAxis,
  Radar,
} from 'recharts';

interface CoreWebVital {
  metric: string;
  score: number;
  rating: 'good' | 'needs-improvement' | 'poor';
  value: string;
  threshold_good: string;
  threshold_poor: string;
}

interface MobileUsability {
  score: number;
  issues_count: number;
  mobile_traffic_pct: number;
  desktop_traffic_pct: number;
  mobile_clicks: number;
  desktop_clicks: number;
}

interface PageSpeedMetrics {
  avg_fcp: number;
  avg_ttfb: number;
  avg_tti: number;
  pages_analyzed: number;
  speed_distribution: {
    fast: number;
    moderate: number;
    slow: number;
  };
}

interface IndexabilityStatus {
  total_pages_crawled: number;
  indexed_pages: number;
  not_indexed_pages: number;
  indexability_rate: number;
  coverage_issues: {
    type: string;
    count: number;
    severity: 'critical' | 'warning' | 'info';
  }[];
}

interface CriticalIssue {
  type: string;
  severity: 'critical' | 'high' | 'medium';
  affected_pages: number;
  description: string;
  impact: string;
}

interface Module1Data {
  core_web_vitals: CoreWebVital[];
  mobile_usability: MobileUsability;
  page_speed: PageSpeedMetrics;
  indexability: IndexabilityStatus;
  critical_issues: CriticalIssue[];
}

interface Module1TechnicalHealthProps {
  data: Module1Data;
}

const Module1TechnicalHealth: React.FC<Module1TechnicalHealthProps> = ({ data }) => {
  if (!data) {
    return (
      <div className="p-8 bg-white rounded-lg shadow-sm border border-gray-200">
        <p className="text-gray-500">No technical health data available</p>
      </div>
    );
  }

  const getRatingColor = (rating: string): string => {
    switch (rating) {
      case 'good':
        return '#10b981';
      case 'needs-improvement':
        return '#f59e0b';
      case 'poor':
        return '#ef4444';
      default:
        return '#6b7280';
    }
  };

  const getSeverityColor = (severity: string): string => {
    switch (severity) {
      case 'critical':
        return '#dc2626';
      case 'high':
        return '#ea580c';
      case 'medium':
        return '#f59e0b';
      case 'warning':
        return '#f59e0b';
      case 'info':
        return '#3b82f6';
      default:
        return '#6b7280';
    }
  };

  const trafficData = data.mobile_usability
    ? [
        {
          name: 'Mobile',
          value: data.mobile_usability.mobile_traffic_pct,
          clicks: data.mobile_usability.mobile_clicks,
        },
        {
          name: 'Desktop',
          value: data.mobile_usability.desktop_traffic_pct,
          clicks: data.mobile_usability.desktop_clicks,
        },
      ]
    : [];

  const speedDistributionData = data.page_speed?.speed_distribution
    ? [
        { name: 'Fast', value: data.page_speed.speed_distribution.fast, fill: '#10b981' },
        { name: 'Moderate', value: data.page_speed.speed_distribution.moderate, fill: '#f59e0b' },
        { name: 'Slow', value: data.page_speed.speed_distribution.slow, fill: '#ef4444' },
      ]
    : [];

  const indexabilityData = data.indexability
    ? [
        { name: 'Indexed', value: data.indexability.indexed_pages, fill: '#10b981' },
        { name: 'Not Indexed', value: data.indexability.not_indexed_pages, fill: '#ef4444' },
      ]
    : [];

  const pageSpeedMetricsData = data.page_speed
    ? [
        {
          metric: 'FCP',
          value: data.page_speed.avg_fcp,
          fullName: 'First Contentful Paint',
        },
        {
          metric: 'TTFB',
          value: data.page_speed.avg_ttfb,
          fullName: 'Time to First Byte',
        },
        {
          metric: 'TTI',
          value: data.page_speed.avg_tti,
          fullName: 'Time to Interactive',
        },
      ]
    : [];

  const renderCoreWebVitalsGauge = (vital: CoreWebVital) => {
    const percentage = vital.score * 100;
    const rotation = (percentage / 100) * 180 - 90;

    return (
      <div key={vital.metric} className="flex flex-col items-center p-4 bg-gray-50 rounded-lg">
        <div className="relative w-32 h-16 mb-2">
          <svg viewBox="0 0 100 50" className="w-full h-full">
            {/* Background arc */}
            <path
              d="M 10 45 A 40 40 0 0 1 90 45"
              fill="none"
              stroke="#e5e7eb"
              strokeWidth="8"
              strokeLinecap="round"
            />
            {/* Colored segments */}
            <path
              d="M 10 45 A 40 40 0 0 1 50 5"
              fill="none"
              stroke="#10b981"
              strokeWidth="8"
              strokeLinecap="round"
            />
            <path
              d="M 50 5 A 40 40 0 0 1 70 11"
              fill="none"
              stroke="#f59e0b"
              strokeWidth="8"
              strokeLinecap="round"
            />
            <path
              d="M 70 11 A 40 40 0 0 1 90 45"
              fill="none"
              stroke="#ef4444"
              strokeWidth="8"
              strokeLinecap="round"
            />
            {/* Needle */}
            <g transform={`rotate(${rotation} 50 45)`}>
              <line
                x1="50"
                y1="45"
                x2="50"
                y2="15"
                stroke="#1f2937"
                strokeWidth="2"
                strokeLinecap="round"
              />
              <circle cx="50" cy="45" r="3" fill="#1f2937" />
            </g>
          </svg>
        </div>
        <div className="text-center">
          <div className="text-sm font-semibold text-gray-900">{vital.metric}</div>
          <div className="text-2xl font-bold" style={{ color: getRatingColor(vital.rating) }}>
            {vital.value}
          </div>
          <div className="text-xs text-gray-500 mt-1">
            Good: &lt; {vital.threshold_good}
          </div>
          <div className="text-xs text-gray-500">
            Poor: &gt; {vital.threshold_poor}
          </div>
        </div>
      </div>
    );
  };

  return (
    <div className="space-y-6">
      {/* Core Web Vitals */}
      <section className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
        <h3 className="text-xl font-bold text-gray-900 mb-4">Core Web Vitals</h3>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          {data.core_web_vitals?.map((vital) => renderCoreWebVitalsGauge(vital))}
        </div>
      </section>

      {/* Mobile Usability & Traffic */}
      <section className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
        <h3 className="text-xl font-bold text-gray-900 mb-4">Mobile Usability</h3>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <div className="flex flex-col items-center">
            <div className="text-center mb-4">
              <div className="text-5xl font-bold text-gray-900">
                {data.mobile_usability?.score || 0}
              </div>
              <div className="text-sm text-gray-600 mt-1">Usability Score</div>
              {data.mobile_usability?.issues_count > 0 && (
                <div className="text-sm text-orange-600 mt-2">
                  {data.mobile_usability.issues_count} issue
                  {data.mobile_usability.issues_count !== 1 ? 's' : ''} found
                </div>
              )}
            </div>
          </div>
          <div>
            <h4 className="text-sm font-semibold text-gray-700 mb-3">Traffic Breakdown</h4>
            <ResponsiveContainer width="100%" height={200}>
              <PieChart>
                <Pie
                  data={trafficData}
                  cx="50%"
                  cy="50%"
                  labelLine={false}
                  label={({ name, value }) => `${name}: ${value.toFixed(1)}%`}
                  outerRadius={80}
                  fill="#8884d8"
                  dataKey="value"
                >
                  {trafficData.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={index === 0 ? '#3b82f6' : '#8b5cf6'} />
                  ))}
                </Pie>
                <Tooltip
                  formatter={(value: any, name: string, props: any) => [
                    `${value.toFixed(1)}% (${props.payload.clicks.toLocaleString()} clicks)`,
                    name,
                  ]}
                />
              </PieChart>
            </ResponsiveContainer>
          </div>
        </div>
      </section>

      {/* Page Speed Metrics */}
      <section className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
        <h3 className="text-xl font-bold text-gray-900 mb-4">Page Speed Metrics</h3>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <div>
            <h4 className="text-sm font-semibold text-gray-700 mb-3">Average Load Times</h4>
            <ResponsiveContainer width="100%" height={250}>
              <BarChart data={pageSpeedMetricsData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="metric" />
                <YAxis label={{ value: 'Milliseconds', angle: -90, position: 'insideLeft' }} />
                <Tooltip
                  content={({ active, payload }) => {
                    if (active && payload && payload.length) {
                      return (
                        <div className="bg-white p-3 border border-gray-200 rounded shadow-lg">
                          <p className="font-semibold">{payload[0].payload.fullName}</p>
                          <p className="text-sm text-gray-600">
                            {payload[0].value}ms
                          </p>
                        </div>
                      );
                    }
                    return null;
                  }}
                />
                <Bar dataKey="value" fill="#3b82f6" />
              </BarChart>
            </ResponsiveContainer>
            {data.page_speed && (
              <div className="text-sm text-gray-600 mt-2 text-center">
                Based on {data.page_speed.pages_analyzed} pages analyzed
              </div>
            )}
          </div>
          <div>
            <h4 className="text-sm font-semibold text-gray-700 mb-3">Speed Distribution</h4>
            <ResponsiveContainer width="100%" height={250}>
              <BarChart data={speedDistributionData} layout="vertical">
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis type="number" />
                <YAxis dataKey="name" type="category" />
                <Tooltip formatter={(value: any) => `${value} pages`} />
                <Bar dataKey="value">
                  {speedDistributionData.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={entry.fill} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      </section>

      {/* Indexability Status */}
      <section className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
        <h3 className="text-xl font-bold text-gray-900 mb-4">Indexability Status</h3>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <div>
            <div className="text-center mb-4">
              <div className="text-5xl font-bold text-gray-900">
                {data.indexability?.indexability_rate
                  ? `${(data.indexability.indexability_rate * 100).toFixed(1)}%`
                  : 'N/A'}
              </div>
              <div className="text-sm text-gray-600 mt-1">Indexability Rate</div>
              {data.indexability && (
                <div className="text-sm text-gray-500 mt-2">
                  {data.indexability.indexed_pages.toLocaleString()} of{' '}
                  {data.indexability.total_pages_crawled.toLocaleString()} pages indexed
                </div>
              )}
            </div>
            <ResponsiveContainer width="100%" height={200}>
              <PieChart>
                <Pie
                  data={indexabilityData}
                  cx="50%"
                  cy="50%"
                  labelLine={false}
                  label={({ name, value }) => `${name}: ${value.toLocaleString()}`}
                  outerRadius={80}
                  fill="#8884d8"
                  dataKey="value"
                >
                  {indexabilityData.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={entry.fill} />
                  ))}
                </Pie>
                <Tooltip />
              </PieChart>
            </ResponsiveContainer>
          </div>
          <div>
            <h4 className="text-sm font-semibold text-gray-700 mb-3">Coverage Issues</h4>
            <div className="space-y-2">
              {data.indexability?.coverage_issues?.map((issue, index) => (
                <div
                  key={index}
                  className="flex items-center justify-between p-3 bg-gray-50 rounded-lg"
                >
                  <div className="flex items-center space-x-3">
                    <div
                      className="w-3 h-3 rounded-full"
                      style={{ backgroundColor: getSeverityColor(issue.severity) }}
                    />
                    <div>
                      <div className="text-sm font-medium text-gray-900">{issue.type}</div>
                      <div className="text-xs text-gray-500">{issue.severity}</div>
                    </div>
                  </div>
                  <div className="text-sm font-semibold text-gray-900">
                    {issue.count.toLocaleString()}
                  </div>
                </div>
              ))}
              {(!data.indexability?.coverage_issues ||
                data.indexability.coverage_issues.length === 0) && (
                <div className="text-sm text-gray-500 text-center py-4">
                  No coverage issues detected
                </div>
              )}
            </div>
          </div>
        </div>
      </section>

      {/* Critical Issues */}
      <section className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
        <h3 className="text-xl font-bold text-gray-900 mb-4">Critical Issues</h3>
        {data.critical_issues && data.critical_issues.length > 0 ? (
          <div className="space-y-4">
            {data.critical_issues.map((issue, index) => (
              <div
                key={index}
                className="border-l-4 p-4 bg-gray-50 rounded-r-lg"
                style={{ borderLeftColor: getSeverityColor(issue.severity) }}
              >
                <div className="flex items-start justify-between">
                  <div className="flex-1">
                    <div className="flex items-center space-x-2 mb-2">
                      <span
                        className="px-2 py-1 text-xs font-semibold rounded uppercase"
                        style={{
                          backgroundColor: `${getSeverityColor(issue.severity)}20`,
                          color: getSeverityColor(issue.severity),
                        }}
                      >
                        {issue.severity}
                      </span>
                      <span className="text-sm font-semibold text-gray-900">{issue.type}</span>
                    </div>
                    <p className="text-sm text-gray-700 mb-2">{issue.description}</p>
                    <div className="flex items-center space-x-4 text-xs text-gray-600">
                      <span>
                        <span className="font-semibold">
                          {issue.affected_pages.toLocaleString()}
                        </span>{' '}
                        pages affected
                      </span>
                      <span className="text-gray-400">•</span>
                      <span>
                        <span className="font-semibold">Impact:</span> {issue.impact}
                      </span>
                    </div>
                  </div>
                </div>
              </div>
            ))}
          </div>
        ) : (
          <div className="text-center py-8">
            <div className="text-5xl mb-3">✓</div>
            <div className="text-lg font-semibold text-green-600">No Critical Issues Detected</div>
            <div className="text-sm text-gray-600 mt-1">
              Your site's technical health is in good shape
            </div>
          </div>
        )}
      </section>

      {/* Summary Card */}
      <section className="bg-gradient-to-r from-blue-50 to-indigo-50 rounded-lg shadow-sm border border-blue-200 p-6">
        <h3 className="text-lg font-bold text-gray-900 mb-3">Technical Health Summary</h3>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <div className="bg-white rounded-lg p-4">
            <div className="text-sm text-gray-600 mb-1">Core Web Vitals</div>
            <div className="text-2xl font-bold">
              {data.core_web_vitals?.filter((v) => v.rating === 'good').length || 0}/
              {data.core_web_vitals?.length || 3}
            </div>
            <div className="text-xs text-gray-500">passing metrics</div>
          </div>
          <div className="bg-white rounded-lg p-4">
            <div className="text-sm text-gray-600 mb-1">Mobile Usability</div>
            <div className="text-2xl font-bold">{data.mobile_usability?.score || 0}/100</div>
            <div className="text-xs text-gray-500">
              {data.mobile_usability?.mobile_traffic_pct.toFixed(0)}% mobile traffic
            </div>
          </div>
          <div className="bg-white rounded-lg p-4">
            <div className="text-sm text-gray-600 mb-1">Indexability</div>
            <div className="text-2xl font-bold">
              {data.indexability?.indexability_rate
                ? `${(data.indexability.indexability_rate * 100).toFixed(0)}%`
                : 'N/A'}
            </div>
            <div className="text-xs text-gray-500">
              {data.critical_issues?.length || 0} critical issues
            </div>
          </div>
        </div>
      </section>
    </div>
  );
};

export default Module1TechnicalHealth;