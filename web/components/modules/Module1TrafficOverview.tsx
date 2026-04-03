import React, { useEffect, useState } from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Skeleton } from '@/components/ui/skeleton';
import { Badge } from '@/components/ui/badge';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, AreaChart, Area } from 'recharts';
import { TrendingUp, TrendingDown, Minus, Activity, Users, Eye, MousePointerClick, AlertCircle, Info, ArrowUp, ArrowDown } from 'lucide-react';
import { fetchModuleData } from '@/lib/api';

interface DailyDataPoint {
  date: string;
  clicks: number;
  impressions: number;
  users: number;
  sessions: number;
  pageviews: number;
  bounce_rate: number;
  avg_session_duration: number;
  ctr: number;
  avg_position: number;
}

interface MetricSummary {
  total: number;
  mom_change_pct: number;
  mom_change_absolute: number;
  wow_change_pct: number;
}

interface Forecast {
  clicks: number;
  ci_low: number;
  ci_high: number;
}

interface ChangePoint {
  date: string;
  magnitude: number;
  direction: 'drop' | 'spike' | 'shift';
  description?: string;
}

interface Seasonality {
  best_day: string;
  worst_day: string;
  monthly_cycle: boolean;
  cycle_description: string;
}

interface Anomaly {
  date: string;
  type: 'discord' | 'motif';
  magnitude: number;
  description?: string;
}

interface TopPage {
  url: string;
  clicks: number;
  impressions: number;
  ctr: number;
  avg_position: number;
}

interface Module1Data {
  overall_direction: 'strong_growth' | 'growth' | 'flat' | 'decline' | 'strong_decline';
  trend_slope_pct_per_month: number;
  traffic_health_score: number;
  change_points: ChangePoint[];
  seasonality: Seasonality;
  anomalies: Anomaly[];
  forecast: {
    '30d': Forecast;
    '60d': Forecast;
    '90d': Forecast;
  };
  daily_data: DailyDataPoint[];
  metrics_summary: {
    clicks: MetricSummary;
    impressions: MetricSummary;
    users: MetricSummary;
    sessions: MetricSummary;
    pageviews: MetricSummary;
    ctr: MetricSummary;
    avg_position: MetricSummary;
  };
  top_pages: TopPage[];
  data_range: {
    start_date: string;
    end_date: string;
    days_analyzed: number;
  };
}

interface Module1Props {
  reportId: string;
}

const Module1TrafficOverview: React.FC<Module1Props> = ({ reportId }) => {
  const [data, setData] = useState<Module1Data | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedMetrics, setSelectedMetrics] = useState<string[]>(['clicks', 'impressions']);

  useEffect(() => {
    const loadData = async () => {
      try {
        setLoading(true);
        setError(null);
        const moduleData = await fetchModuleData(reportId, 'module1');
        setData(moduleData);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load traffic overview data');
        console.error('Error loading Module 1 data:', err);
      } finally {
        setLoading(false);
      }
    };

    loadData();
  }, [reportId]);

  const getTrendIcon = (direction: string) => {
    switch (direction) {
      case 'strong_growth':
      case 'growth':
        return <TrendingUp className="h-5 w-5 text-green-600" />;
      case 'strong_decline':
      case 'decline':
        return <TrendingDown className="h-5 w-5 text-red-600" />;
      default:
        return <Minus className="h-5 w-5 text-gray-600" />;
    }
  };

  const getTrendColor = (direction: string) => {
    switch (direction) {
      case 'strong_growth':
        return 'bg-green-100 text-green-800 border-green-300';
      case 'growth':
        return 'bg-green-50 text-green-700 border-green-200';
      case 'flat':
        return 'bg-gray-100 text-gray-800 border-gray-300';
      case 'decline':
        return 'bg-orange-50 text-orange-700 border-orange-200';
      case 'strong_decline':
        return 'bg-red-100 text-red-800 border-red-300';
      default:
        return 'bg-gray-100 text-gray-800 border-gray-300';
    }
  };

  const formatNumber = (num: number): string => {
    if (num >= 1000000) {
      return (num / 1000000).toFixed(1) + 'M';
    }
    if (num >= 1000) {
      return (num / 1000).toFixed(1) + 'K';
    }
    return num.toFixed(0);
  };

  const formatPercent = (num: number): string => {
    return `${num > 0 ? '+' : ''}${num.toFixed(1)}%`;
  };

  const formatDate = (dateStr: string): string => {
    const date = new Date(dateStr);
    return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
  };

  const getChangeIcon = (change: number) => {
    if (change > 0) return <ArrowUp className="h-4 w-4 text-green-600" />;
    if (change < 0) return <ArrowDown className="h-4 w-4 text-red-600" />;
    return <Minus className="h-4 w-4 text-gray-600" />;
  };

  const getChangeColor = (change: number) => {
    if (change > 0) return 'text-green-600';
    if (change < 0) return 'text-red-600';
    return 'text-gray-600';
  };

  const metricConfig = {
    clicks: { label: 'Clicks', color: '#3b82f6', icon: MousePointerClick },
    impressions: { label: 'Impressions', color: '#8b5cf6', icon: Eye },
    users: { label: 'Users', color: '#10b981', icon: Users },
    ctr: { label: 'CTR', color: '#f59e0b', icon: Activity },
  };

  if (loading) {
    return (
      <div className="space-y-6">
        <Skeleton className="h-48 w-full" />
        <Skeleton className="h-96 w-full" />
        <Skeleton className="h-64 w-full" />
      </div>
    );
  }

  if (error) {
    return (
      <Alert variant="destructive">
        <AlertCircle className="h-4 w-4" />
        <AlertDescription>{error}</AlertDescription>
      </Alert>
    );
  }

  if (!data) {
    return (
      <Alert>
        <Info className="h-4 w-4" />
        <AlertDescription>No data available for this module.</AlertDescription>
      </Alert>
    );
  }

  const toggleMetric = (metric: string) => {
    setSelectedMetrics(prev => 
      prev.includes(metric) 
        ? prev.filter(m => m !== metric)
        : [...prev, metric]
    );
  };

  return (
    <div className="space-y-6">
      {/* Header Section */}
      <div>
        <h2 className="text-3xl font-bold mb-2">Traffic Health & Trajectory</h2>
        <p className="text-gray-600">
          Analyzing {data.data_range.days_analyzed} days of data from {formatDate(data.data_range.start_date)} to {formatDate(data.data_range.end_date)}
        </p>
      </div>

      {/* Overall Health Score */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center justify-between">
            <span>Overall Traffic Health</span>
            {getTrendIcon(data.overall_direction)}
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            <div className="flex items-center justify-between">
              <div>
                <div className="text-4xl font-bold">{data.traffic_health_score.toFixed(0)}/100</div>
                <div className="text-sm text-gray-600 mt-1">Health Score</div>
              </div>
              <Badge className={`${getTrendColor(data.overall_direction)} text-sm px-4 py-2`}>
                {data.overall_direction.replace('_', ' ').toUpperCase()}
              </Badge>
            </div>
            <div className="border-t pt-4">
              <div className="flex items-center gap-2">
                <span className="text-sm text-gray-600">Monthly Trend:</span>
                <span className={`text-sm font-semibold ${getChangeColor(data.trend_slope_pct_per_month)}`}>
                  {formatPercent(data.trend_slope_pct_per_month)} per month
                </span>
              </div>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Key Metrics Summary */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-gray-600 flex items-center gap-2">
              <MousePointerClick className="h-4 w-4" />
              Total Clicks
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-2">
              <div className="text-2xl font-bold">{formatNumber(data.metrics_summary.clicks.total)}</div>
              <div className="flex items-center gap-2 text-sm">
                {getChangeIcon(data.metrics_summary.clicks.mom_change_pct)}
                <span className={getChangeColor(data.metrics_summary.clicks.mom_change_pct)}>
                  {formatPercent(data.metrics_summary.clicks.mom_change_pct)} MoM
                </span>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-gray-600 flex items-center gap-2">
              <Eye className="h-4 w-4" />
              Total Impressions
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-2">
              <div className="text-2xl font-bold">{formatNumber(data.metrics_summary.impressions.total)}</div>
              <div className="flex items-center gap-2 text-sm">
                {getChangeIcon(data.metrics_summary.impressions.mom_change_pct)}
                <span className={getChangeColor(data.metrics_summary.impressions.mom_change_pct)}>
                  {formatPercent(data.metrics_summary.impressions.mom_change_pct)} MoM
                </span>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-gray-600 flex items-center gap-2">
              <Activity className="h-4 w-4" />
              Average CTR
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-2">
              <div className="text-2xl font-bold">{(data.metrics_summary.ctr.total * 100).toFixed(2)}%</div>
              <div className="flex items-center gap-2 text-sm">
                {getChangeIcon(data.metrics_summary.ctr.mom_change_pct)}
                <span className={getChangeColor(data.metrics_summary.ctr.mom_change_pct)}>
                  {formatPercent(data.metrics_summary.ctr.mom_change_pct)} MoM
                </span>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-gray-600 flex items-center gap-2">
              <Users className="h-4 w-4" />
              Total Users
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-2">
              <div className="text-2xl font-bold">{formatNumber(data.metrics_summary.users.total)}</div>
              <div className="flex items-center gap-2 text-sm">
                {getChangeIcon(data.metrics_summary.users.mom_change_pct)}
                <span className={getChangeColor(data.metrics_summary.users.mom_change_pct)}>
                  {formatPercent(data.metrics_summary.users.mom_change_pct)} MoM
                </span>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Traffic Trend Chart */}
      <Card>
        <CardHeader>
          <CardTitle>Traffic Trend</CardTitle>
          <CardDescription>
            Historical performance with forecasting
          </CardDescription>
          <div className="flex flex-wrap gap-2 mt-4">
            {Object.entries(metricConfig).map(([key, config]) => (
              <button
                key={key}
                onClick={() => toggleMetric(key)}
                className={`px-3 py-1 rounded-full text-sm font-medium transition-colors ${
                  selectedMetrics.includes(key)
                    ? 'bg-blue-600 text-white'
                    : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                }`}
              >
                {config.label}
              </button>
            ))}
          </div>
        </CardHeader>
        <CardContent>
          <ResponsiveContainer width="100%" height={400}>
            <LineChart data={data.daily_data}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
              <XAxis 
                dataKey="date" 
                tickFormatter={(value) => {
                  const date = new Date(value);
                  return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
                }}
                stroke="#6b7280"
              />
              <YAxis stroke="#6b7280" />
              <Tooltip 
                contentStyle={{ backgroundColor: '#fff', border: '1px solid #e5e7eb', borderRadius: '8px' }}
                labelFormatter={(value) => formatDate(value)}
                formatter={(value: number) => formatNumber(value)}
              />
              <Legend />
              {selectedMetrics.map(metric => {
                const config = metricConfig[metric as keyof typeof metricConfig];
                return (
                  <Line
                    key={metric}
                    type="monotone"
                    dataKey={metric}
                    stroke={config.color}
                    strokeWidth={2}
                    dot={false}
                    name={config.label}
                  />
                );
              })}
            </LineChart>
          </ResponsiveContainer>
        </CardContent>
      </Card>

      {/* Change Points */}
      {data.change_points && data.change_points.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle>Significant Changes Detected</CardTitle>
            <CardDescription>
              Algorithmic or external events that impacted traffic
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="space-y-3">
              {data.change_points.map((point, idx) => (
                <div key={idx} className="flex items-start gap-3 p-3 bg-gray-50 rounded-lg">
                  <div className={`p-2 rounded-full ${
                    point.direction === 'spike' ? 'bg-green-100' :
                    point.direction === 'drop' ? 'bg-red-100' :
                    'bg-blue-100'
                  }`}>
                    {point.direction === 'spike' ? <TrendingUp className="h-4 w-4 text-green-600" /> :
                     point.direction === 'drop' ? <TrendingDown className="h-4 w-4 text-red-600" /> :
                     <Activity className="h-4 w-4 text-blue-600" />}
                  </div>
                  <div className="flex-1">
                    <div className="flex items-center justify-between">
                      <span className="font-medium">{formatDate(point.date)}</span>
                      <Badge variant="outline" className="text-xs">
                        {(point.magnitude * 100).toFixed(1)}% change
                      </Badge>
                    </div>
                    {point.description && (
                      <p className="text-sm text-gray-600 mt-1">{point.description}</p>
                    )}
                  </div>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}

      {/* Seasonality Insights */}
      <Card>
        <CardHeader>
          <CardTitle>Seasonality Patterns</CardTitle>
          <CardDescription>
            Recurring traffic patterns identified in your data
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            <div className="grid grid-cols-2 gap-4">
              <div className="p-4 bg-green-50 rounded-lg border border-green-200">
                <div className="text-sm text-gray-600 mb-1">Best Day</div>
                <div className="text-lg font-semibold text-green-800">{data.seasonality.best_day}</div>
              </div>
              <div className="p-4 bg-orange-50 rounded-lg border border-orange-200">
                <div className="text-sm text-gray-600 mb-1">Worst Day</div>
                <div className="text-lg font-semibold text-orange-800">{data.seasonality.worst_day}</div>
              </div>
            </div>
            {data.seasonality.monthly_cycle && (
              <div className="p-4 bg-blue-50 rounded-lg border border-blue-200">
                <div className="text-sm text-gray-600 mb-1">Monthly Pattern</div>
                <div className="text-sm font-medium text-blue-800">{data.seasonality.cycle_description}</div>
              </div>
            )}
          </div>
        </CardContent>
      </Card>

      {/* Anomalies */}
      {data.anomalies && data.anomalies.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle>Anomalies Detected</CardTitle>
            <CardDescription>
              Unusual patterns that deviate from expected behavior
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="space-y-2">
              {data.anomalies.map((anomaly, idx) => (
                <div key={idx} className="flex items-center justify-between p-3 bg-amber-50 rounded-lg border border-amber-200">
                  <div className="flex items-center gap-3">
                    <AlertCircle className="h-4 w-4 text-amber-600" />
                    <div>
                      <div className="font-medium">{formatDate(anomaly.date)}</div>
                      {anomaly.description && (
                        <div className="text-sm text-gray-600">{anomaly.description}</div>
                      )}
                    </div>
                  </div>
                  <Badge variant="outline" className="text-xs">
                    {(anomaly.magnitude * 100).toFixed(1)}% deviation
                  </Badge>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}

      {/* Forecast */}
      <Card>
        <CardHeader>
          <CardTitle>Traffic Forecast</CardTitle>
          <CardDescription>
            Projected clicks based on historical trends
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            {Object.entries(data.forecast).map(([period, forecast]) => (
              <div key={period} className="p-4 border rounded-lg">
                <div className="text-sm text-gray-600 mb-2">{period.replace('d', ' Days')}</div>
                <div className="text-2xl font-bold mb-1">{formatNumber(forecast.clicks)}</div>
                <div className="text-xs text-gray-500">
                  Range: {formatNumber(forecast.ci_low)} - {formatNumber(forecast.ci_high)}
                </div>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>

      {/* Top Pages Table */}
      {data.top_pages && data.top_pages.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle>Top Performing Pages</CardTitle>
            <CardDescription>
              Pages with highest traffic in the analyzed period
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead>
                  <tr className="border-b">
                    <th className="text-left py-3 px-4 font-medium text-sm text-gray-600">Page</th>
                    <th className="text-right py-3 px-4 font-medium text-sm text-gray-600">Clicks</th>
                    <th className="text-right py-3 px-4 font-medium text-sm text-gray-600">Impressions</th>
                    <th className="text-right py-3 px-4 font-medium text-sm text-gray-600">CTR</th>
                    <th className="text-right py-3 px-4 font-medium text-sm text-gray-600">Avg Position</th>
                  </tr>
                </thead>
                <tbody>
                  {data.top_pages.map((page, idx) => (
                    <tr key={idx} className="border-b hover:bg-gray-50">
                      <td className="py-3 px-4 text-sm max-w-md truncate" title={page.url}>
                        {page.url}
                      </td>
                      <td className="text-right py-3 px-4 text-sm font-medium">
                        {formatNumber(page.clicks)}
                      </td>
                      <td className="text-right py-3 px-4 text-sm">
                        {formatNumber(page.impressions)}
                      </td>
                      <td className="text-right py-3 px-4 text-sm">
                        {(page.ctr * 100).toFixed(2)}%
                      </td>
                      <td className="text-right py-3 px-4 text-sm">
                        {page.avg_position.toFixed(1)}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
};

export default Module1TrafficOverview;
