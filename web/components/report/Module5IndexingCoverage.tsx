import React from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Progress } from '@/components/ui/progress';
import { TrendingUp, TrendingDown, CheckCircle2, XCircle, AlertTriangle, Activity, BarChart3, Clock } from 'lucide-react';
import {
  LineChart,
  Line,
  BarChart,
  Bar,
  PieChart,
  Pie,
  Cell,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  Area,
  AreaChart,
} from 'recharts';

interface IndexingStatus {
  indexed: number;
  excluded: number;
  errors: number;
  total: number;
}

interface IndexingTrend {
  date: string;
  indexed: number;
  excluded: number;
  errors: number;
}

interface ExclusionReason {
  reason: string;
  count: number;
  percentage: number;
  severity: 'low' | 'medium' | 'high' | 'critical';
}

interface CoreWebVital {
  metric: string;
  good: number;
  needs_improvement: number;
  poor: number;
  p75_value: number;
  threshold_good: number;
  threshold_poor: number;
  status: 'good' | 'needs_improvement' | 'poor';
}

interface CoverageTrend {
  date: string;
  coverage_rate: number;
  indexed_pages: number;
}

interface IndexingIssue {
  url: string;
  issue_type: string;
  severity: 'low' | 'medium' | 'high' | 'critical';
  detected_date: string;
  recommendation: string;
}

interface Module5Data {
  indexing_status: IndexingStatus;
  indexing_trends: IndexingTrend[];
  exclusion_breakdown: ExclusionReason[];
  core_web_vitals: CoreWebVital[];
  coverage_trends: CoverageTrend[];
  critical_issues: IndexingIssue[];
  coverage_score: number;
  health_status: 'excellent' | 'good' | 'fair' | 'poor' | 'critical';
  recommendations: string[];
}

interface Module5Props {
  data: Module5Data;
}

const Module5IndexingCoverage: React.FC<Module5Props> = ({ data }) => {
  const {
    indexing_status,
    indexing_trends,
    exclusion_breakdown,
    core_web_vitals,
    coverage_trends,
    critical_issues,
    coverage_score,
    health_status,
    recommendations,
  } = data;

  // Color schemes
  const statusColors = {
    indexed: '#10b981',
    excluded: '#f59e0b',
    errors: '#ef4444',
  };

  const severityColors = {
    low: '#3b82f6',
    medium: '#f59e0b',
    high: '#ef4444',
    critical: '#991b1b',
  };

  const healthStatusConfig = {
    excellent: { color: '#10b981', icon: CheckCircle2, label: 'Excellent' },
    good: { color: '#3b82f6', icon: CheckCircle2, label: 'Good' },
    fair: { color: '#f59e0b', icon: AlertTriangle, label: 'Fair' },
    poor: { color: '#ef4444', icon: XCircle, label: 'Poor' },
    critical: { color: '#991b1b', icon: XCircle, label: 'Critical' },
  };

  const HealthIcon = healthStatusConfig[health_status].icon;

  // Calculate percentages for indexing status
  const indexedPct = ((indexing_status.indexed / indexing_status.total) * 100).toFixed(1);
  const excludedPct = ((indexing_status.excluded / indexing_status.total) * 100).toFixed(1);
  const errorsPct = ((indexing_status.errors / indexing_status.total) * 100).toFixed(1);

  // Prepare pie chart data
  const pieChartData = [
    { name: 'Indexed', value: indexing_status.indexed, color: statusColors.indexed },
    { name: 'Excluded', value: indexing_status.excluded, color: statusColors.excluded },
    { name: 'Errors', value: indexing_status.errors, color: statusColors.errors },
  ];

  // Format number with commas
  const formatNumber = (num: number): string => {
    return num.toLocaleString();
  };

  // Custom tooltip for charts
  const CustomTooltip = ({ active, payload, label }: any) => {
    if (active && payload && payload.length) {
      return (
        <div className="bg-white p-3 border border-gray-200 rounded-lg shadow-lg">
          <p className="font-medium text-gray-900 mb-2">{label}</p>
          {payload.map((entry: any, index: number) => (
            <p key={index} className="text-sm" style={{ color: entry.color }}>
              {entry.name}: {formatNumber(entry.value)}
            </p>
          ))}
        </div>
      );
    }
    return null;
  };

  const CustomTooltipPercent = ({ active, payload, label }: any) => {
    if (active && payload && payload.length) {
      return (
        <div className="bg-white p-3 border border-gray-200 rounded-lg shadow-lg">
          <p className="font-medium text-gray-900 mb-2">{label}</p>
          {payload.map((entry: any, index: number) => (
            <p key={index} className="text-sm" style={{ color: entry.color }}>
              {entry.name}: {entry.value.toFixed(1)}%
            </p>
          ))}
        </div>
      );
    }
    return null;
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h2 className="text-3xl font-bold text-gray-900 mb-2">
          Module 5: Indexing Coverage & Core Web Vitals
        </h2>
        <p className="text-gray-600">
          Comprehensive analysis of indexing health, coverage trends, and page experience metrics
        </p>
      </div>

      {/* Overall Health Score */}
      <Card className="border-2" style={{ borderColor: healthStatusConfig[health_status].color }}>
        <CardHeader>
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <HealthIcon
                className="w-8 h-8"
                style={{ color: healthStatusConfig[health_status].color }}
              />
              <div>
                <CardTitle>Coverage Health Score</CardTitle>
                <CardDescription>
                  Overall indexing and page experience health
                </CardDescription>
              </div>
            </div>
            <div className="text-right">
              <div className="text-4xl font-bold" style={{ color: healthStatusConfig[health_status].color }}>
                {coverage_score}
              </div>
              <div className="text-sm text-gray-500 uppercase font-medium">
                {healthStatusConfig[health_status].label}
              </div>
            </div>
          </div>
        </CardHeader>
        <CardContent>
          <Progress
            value={coverage_score}
            className="h-3"
            style={{
              backgroundColor: '#e5e7eb',
            }}
          />
        </CardContent>
      </Card>

      {/* Indexing Status Overview */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <Card>
          <CardHeader>
            <CardTitle>Current Indexing Status</CardTitle>
            <CardDescription>
              Distribution of {formatNumber(indexing_status.total)} total pages
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="space-y-6">
              {/* Status Cards */}
              <div className="grid grid-cols-3 gap-4">
                <div className="bg-green-50 rounded-lg p-4 border border-green-200">
                  <div className="flex items-center gap-2 mb-2">
                    <CheckCircle2 className="w-5 h-5 text-green-600" />
                    <span className="text-sm font-medium text-green-900">Indexed</span>
                  </div>
                  <div className="text-2xl font-bold text-green-900">
                    {formatNumber(indexing_status.indexed)}
                  </div>
                  <div className="text-sm text-green-700">{indexedPct}%</div>
                </div>

                <div className="bg-amber-50 rounded-lg p-4 border border-amber-200">
                  <div className="flex items-center gap-2 mb-2">
                    <AlertTriangle className="w-5 h-5 text-amber-600" />
                    <span className="text-sm font-medium text-amber-900">Excluded</span>
                  </div>
                  <div className="text-2xl font-bold text-amber-900">
                    {formatNumber(indexing_status.excluded)}
                  </div>
                  <div className="text-sm text-amber-700">{excludedPct}%</div>
                </div>

                <div className="bg-red-50 rounded-lg p-4 border border-red-200">
                  <div className="flex items-center gap-2 mb-2">
                    <XCircle className="w-5 h-5 text-red-600" />
                    <span className="text-sm font-medium text-red-900">Errors</span>
                  </div>
                  <div className="text-2xl font-bold text-red-900">
                    {formatNumber(indexing_status.errors)}
                  </div>
                  <div className="text-sm text-red-700">{errorsPct}%</div>
                </div>
              </div>

              {/* Pie Chart */}
              <ResponsiveContainer width="100%" height={250}>
                <PieChart>
                  <Pie
                    data={pieChartData}
                    cx="50%"
                    cy="50%"
                    labelLine={false}
                    label={({ name, percent }) => `${name}: ${(percent * 100).toFixed(1)}%`}
                    outerRadius={80}
                    fill="#8884d8"
                    dataKey="value"
                  >
                    {pieChartData.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={entry.color} />
                    ))}
                  </Pie>
                  <Tooltip formatter={(value: number) => formatNumber(value)} />
                </PieChart>
              </ResponsiveContainer>
            </div>
          </CardContent>
        </Card>

        {/* Exclusion Breakdown */}
        <Card>
          <CardHeader>
            <CardTitle>Exclusion Reasons</CardTitle>
            <CardDescription>
              Why pages are excluded from indexing
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="space-y-3">
              {exclusion_breakdown.map((reason, index) => (
                <div key={index} className="space-y-2">
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-2">
                      <Badge
                        variant="outline"
                        style={{
                          backgroundColor: `${severityColors[reason.severity]}20`,
                          borderColor: severityColors[reason.severity],
                          color: severityColors[reason.severity],
                        }}
                      >
                        {reason.severity.toUpperCase()}
                      </Badge>
                      <span className="text-sm font-medium text-gray-900">
                        {reason.reason}
                      </span>
                    </div>
                    <span className="text-sm text-gray-600">
                      {formatNumber(reason.count)} ({reason.percentage.toFixed(1)}%)
                    </span>
                  </div>
                  <Progress
                    value={reason.percentage}
                    className="h-2"
                    style={{
                      backgroundColor: '#e5e7eb',
                    }}
                  />
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Indexing Trends Over Time */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Activity className="w-5 h-5" />
            Indexing Trends
          </CardTitle>
          <CardDescription>
            Historical indexing status over time
          </CardDescription>
        </CardHeader>
        <CardContent>
          <ResponsiveContainer width="100%" height={350}>
            <AreaChart data={indexing_trends}>
              <defs>
                <linearGradient id="colorIndexed" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor={statusColors.indexed} stopOpacity={0.8} />
                  <stop offset="95%" stopColor={statusColors.indexed} stopOpacity={0.1} />
                </linearGradient>
                <linearGradient id="colorExcluded" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor={statusColors.excluded} stopOpacity={0.8} />
                  <stop offset="95%" stopColor={statusColors.excluded} stopOpacity={0.1} />
                </linearGradient>
                <linearGradient id="colorErrors" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor={statusColors.errors} stopOpacity={0.8} />
                  <stop offset="95%" stopColor={statusColors.errors} stopOpacity={0.1} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
              <XAxis
                dataKey="date"
                tick={{ fontSize: 12 }}
                stroke="#6b7280"
              />
              <YAxis
                tick={{ fontSize: 12 }}
                stroke="#6b7280"
                tickFormatter={(value) => formatNumber(value)}
              />
              <Tooltip content={<CustomTooltip />} />
              <Legend />
              <Area
                type="monotone"
                dataKey="indexed"
                name="Indexed"
                stackId="1"
                stroke={statusColors.indexed}
                fill="url(#colorIndexed)"
              />
              <Area
                type="monotone"
                dataKey="excluded"
                name="Excluded"
                stackId="1"
                stroke={statusColors.excluded}
                fill="url(#colorExcluded)"
              />
              <Area
                type="monotone"
                dataKey="errors"
                name="Errors"
                stackId="1"
                stroke={statusColors.errors}
                fill="url(#colorErrors)"
              />
            </AreaChart>
          </ResponsiveContainer>
        </CardContent>
      </Card>

      {/* Coverage Rate Trend */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <TrendingUp className="w-5 h-5" />
            Coverage Rate Trend
          </CardTitle>
          <CardDescription>
            Percentage of pages successfully indexed over time
          </CardDescription>
        </CardHeader>
        <CardContent>
          <ResponsiveContainer width="100%" height={300}>
            <LineChart data={coverage_trends}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
              <XAxis
                dataKey="date"
                tick={{ fontSize: 12 }}
                stroke="#6b7280"
              />
              <YAxis
                tick={{ fontSize: 12 }}
                stroke="#6b7280"
                domain={[0, 100]}
                tickFormatter={(value) => `${value}%`}
              />
              <Tooltip content={<CustomTooltipPercent />} />
              <Legend />
              <Line
                type="monotone"
                dataKey="coverage_rate"
                name="Coverage Rate"
                stroke="#3b82f6"
                strokeWidth={3}
                dot={{ fill: '#3b82f6', r: 4 }}
                activeDot={{ r: 6 }}
              />
            </LineChart>
          </ResponsiveContainer>
        </CardContent>
      </Card>

      {/* Core Web Vitals */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <BarChart3 className="w-5 h-5" />
            Core Web Vitals
          </CardTitle>
          <CardDescription>
            Page experience metrics from real user data
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="space-y-6">
            {core_web_vitals.map((vital, index) => {
              const totalUrls = vital.good + vital.needs_improvement + vital.poor;
              const goodPct = (vital.good / totalUrls) * 100;
              const needsImprovementPct = (vital.needs_improvement / totalUrls) * 100;
              const poorPct = (vital.poor / totalUrls) * 100;

              const statusConfig = {
                good: { color: '#10b981', icon: CheckCircle2, label: 'Good' },
                needs_improvement: { color: '#f59e0b', icon: AlertTriangle, label: 'Needs Improvement' },
                poor: { color: '#ef4444', icon: XCircle, label: 'Poor' },
              };

              const StatusIcon = statusConfig[vital.status].icon;

              return (
                <div key={index} className="space-y-3">
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-3">
                      <StatusIcon
                        className="w-6 h-6"
                        style={{ color: statusConfig[vital.status].color }}
                      />
                      <div>
                        <div className="font-semibold text-gray-900">{vital.metric}</div>
                        <div className="text-sm text-gray-500">
                          P75: {vital.p75_value} (Threshold: ≤{vital.threshold_good})
                        </div>
                      </div>
                    </div>
                    <Badge
                      style={{
                        backgroundColor: `${statusConfig[vital.status].color}20`,
                        borderColor: statusConfig[vital.status].color,
                        color: statusConfig[vital.status].color,
                      }}
                    >
                      {statusConfig[vital.status].label}
                    </Badge>
                  </div>

                  <div className="flex gap-1 h-8 rounded-lg overflow-hidden">
                    <div
                      className="bg-green-500 flex items-center justify-center text-white text-xs font-medium"
                      style={{ width: `${goodPct}%` }}
                    >
                      {goodPct > 10 && `${goodPct.toFixed(0)}%`}
                    </div>
                    <div
                      className="bg-amber-500 flex items-center justify-center text-white text-xs font-medium"
                      style={{ width: `${needsImprovementPct}%` }}
                    >
                      {needsImprovementPct > 10 && `${needsImprovementPct.toFixed(0)}%`}
                    </div>
                    <div
                      className="bg-red-500 flex items-center justify-center text-white text-xs font-medium"
                      style={{ width: `${poorPct}%` }}
                    >
                      {poorPct > 10 && `${poorPct.toFixed(0)}%`}
                    </div>
                  </div>

                  <div className="flex justify-between text-sm text-gray-600">
                    <span>Good: {formatNumber(vital.good)}</span>
                    <span>Needs Work: {formatNumber(vital.needs_improvement)}</span>
                    <span>Poor: {formatNumber(vital.poor)}</span>
                  </div>
                </div>
              );
            })}
          </div>
        </CardContent>
      </Card>

      {/* Critical Issues */}
      {critical_issues.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <AlertTriangle className="w-5 h-5 text-red-600" />
              Critical Indexing Issues
            </CardTitle>
            <CardDescription>
              High-priority problems requiring immediate attention
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="space-y-3">
              {critical_issues.map((issue, index) => (
                <Alert
                  key={index}
                  className="border-l-4"
                  style={{ borderLeftColor: severityColors[issue.severity] }}
                >
                  <div className="flex items-start gap-3">
                    <Badge
                      style={{
                        backgroundColor: `${severityColors[issue.severity]}20`,
                        borderColor: severityColors[issue.severity],
                        color: severityColors[issue.severity],
                      }}
                    >
                      {issue.severity.toUpperCase()}
                    </Badge>
                    <div className="flex-1 space-y-2">
                      <div className="flex items-center justify-between">
                        <span className="font-semibold text-gray-900">{issue.issue_type}</span>
                        <div className="flex items-center gap-1 text-xs text-gray-500">
                          <Clock className="w-3 h-3" />
                          {issue.detected_date}
                        </div>
                      </div>
                      <div className="text-sm text-gray-600 font-mono bg-gray-50 p-2 rounded">
                        {issue.url}
                      </div>
                      <AlertDescription className="text-sm">
                        <strong>Recommendation:</strong> {issue.recommendation}
                      </AlertDescription>
                    </div>
                  </div>
                </Alert>
              ))}
            </div>
          </CardContent>
        </Card>
      )}

      {/* Recommendations */}
      <Card>
        <CardHeader>
          <CardTitle>Action Items</CardTitle>
          <CardDescription>
            Prioritized recommendations to improve indexing coverage and page experience
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="space-y-3">
            {recommendations.map((rec, index) => (
              <div
                key={index}
                className="flex items-start gap-3 p-4 bg-blue-50 rounded-lg border border-blue-200"
              >
                <div className="flex-shrink-0 w-6 h-6 bg-blue-600 text-white rounded-full flex items-center justify-center text-sm font-bold">
                  {index + 1}
                </div>
                <p className="text-sm text-gray-900 leading-relaxed">{rec}</p>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>

      {/* Summary Stats */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <Card>
          <CardContent className="pt-6">
            <div className="text-center">
              <div className="text-3xl font-bold text-green-600">
                {indexedPct}%
              </div>
              <div className="text-sm text-gray-600 mt-1">Coverage Rate</div>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="pt-6">
            <div className="text-center">
              <div className="text-3xl font-bold text-gray-900">
                {formatNumber(indexing_status.indexed)}
              </div>
              <div className="text-sm text-gray-600 mt-1">Pages Indexed</div>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="pt-6">
            <div className="text-center">
              <div className="text-3xl font-bold text-amber-600">
                {exclusion_breakdown.length}
              </div>
              <div className="text-sm text-gray-600 mt-1">Exclusion Types</div>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="pt-6">
            <div className="text-center">
              <div className="text-3xl font-bold text-red-600">
                {critical_issues.length}
              </div>
              <div className="text-sm text-gray-600 mt-1">Critical Issues</div>
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  );
};

export default Module5IndexingCoverage;
