import React, { useEffect, useState } from 'react';
import {
  LineChart,
  Line,
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  ComposedChart,
} from 'recharts';
import { TrendingUp, TrendingDown, Minus, AlertCircle, Calendar, Activity } from 'lucide-react';

interface TimeSeriesDataPoint {
  date: string;
  clicks: number;
  impressions: number;
  ctr: number;
  position: number;
}

interface Forecast {
  clicks: number;
  ci_low: number;
  ci_high: number;
}

interface ChangePoint {
  date: string;
  magnitude: number;
  direction: 'drop' | 'spike';
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
}

interface Summary {
  total_clicks: number;
  total_impressions: number;
  avg_ctr: number;
  avg_position: number;
  clicks_change_pct: number;
  impressions_change_pct: number;
  ctr_change_pct: number;
  position_change: number;
}

interface TrafficOverview {
  timeseries: TimeSeriesDataPoint[];
  summary: Summary;
}

interface Module1Data {
  overall_direction: 'strong_growth' | 'growth' | 'flat' | 'decline' | 'strong_decline';
  trend_slope_pct_per_month: number;
  change_points: ChangePoint[];
  seasonality: Seasonality;
  anomalies: Anomaly[];
  forecast: {
    '30d': Forecast;
    '60d': Forecast;
    '90d': Forecast;
  };
  traffic_overview: TrafficOverview;
}

interface Module1TrafficOverviewProps {
  reportId: string;
}

const Module1TrafficOverview: React.FC<Module1TrafficOverviewProps> = ({ reportId }) => {
  const [module1, setModule1] = useState<Module1Data | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true);
        setError(null);
        const response = await fetch(`/api/reports/${reportId}/modules/1`);
        
        if (!response.ok) {
          throw new Error(`Failed to fetch module data: ${response.statusText}`);
        }
        
        const data = await response.json();
        setModule1(data);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'An error occurred while loading data');
      } finally {
        setLoading(false);
      }
    };

    if (reportId) {
      fetchData();
    }
  }, [reportId]);

  if (loading) {
    return (
      <div className="bg-white rounded-lg shadow-md p-6">
        <div className="animate-pulse">
          <div className="h-8 bg-gray-200 rounded w-1/3 mb-6"></div>
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
            {[...Array(4)].map((_, i) => (
              <div key={i} className="h-32 bg-gray-200 rounded"></div>
            ))}
          </div>
          <div className="h-96 bg-gray-200 rounded mb-6"></div>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div className="h-64 bg-gray-200 rounded"></div>
            <div className="h-64 bg-gray-200 rounded"></div>
          </div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-white rounded-lg shadow-md p-6">
        <div className="flex items-center justify-center text-red-600 py-12">
          <AlertCircle className="w-6 h-6 mr-2" />
          <span className="text-lg">{error}</span>
        </div>
      </div>
    );
  }

  if (!module1 || !module1.traffic_overview) {
    return (
      <div className="bg-white rounded-lg shadow-md p-6">
        <div className="flex items-center justify-center text-gray-500 py-12">
          <AlertCircle className="w-6 h-6 mr-2" />
          <span className="text-lg">No data available</span>
        </div>
      </div>
    );
  }

  const { traffic_overview, overall_direction, trend_slope_pct_per_month, seasonality, forecast } = module1;
  const { timeseries, summary } = traffic_overview;

  const getTrendIcon = (direction: string) => {
    switch (direction) {
      case 'strong_growth':
      case 'growth':
        return <TrendingUp className="w-5 h-5 text-green-600" />;
      case 'strong_decline':
      case 'decline':
        return <TrendingDown className="w-5 h-5 text-red-600" />;
      default:
        return <Minus className="w-5 h-5 text-gray-600" />;
    }
  };

  const getTrendColor = (direction: string) => {
    switch (direction) {
      case 'strong_growth':
      case 'growth':
        return 'text-green-600';
      case 'strong_decline':
      case 'decline':
        return 'text-red-600';
      default:
        return 'text-gray-600';
    }
  };

  const formatNumber = (num: number): string => {
    if (num >= 1000000) {
      return (num / 1000000).toFixed(1) + 'M';
    } else if (num >= 1000) {
      return (num / 1000).toFixed(1) + 'K';
    }
    return num.toFixed(0);
  };

  const formatPercentage = (num: number): string => {
    const sign = num > 0 ? '+' : '';
    return `${sign}${num.toFixed(1)}%`;
  };

  const formatPosition = (pos: number): string => {
    return pos.toFixed(1);
  };

  const formatCTR = (ctr: number): string => {
    return (ctr * 100).toFixed(2) + '%';
  };

  const getChangeColor = (change: number): string => {
    if (change > 0) return 'text-green-600';
    if (change < 0) return 'text-red-600';
    return 'text-gray-600';
  };

  const getPositionChangeColor = (change: number): string => {
    // For position, negative is good (moving up), positive is bad (moving down)
    if (change < 0) return 'text-green-600';
    if (change > 0) return 'text-red-600';
    return 'text-gray-600';
  };

  const getDirectionLabel = (direction: string): string => {
    const labels: { [key: string]: string } = {
      strong_growth: 'Strong Growth',
      growth: 'Growth',
      flat: 'Stable',
      decline: 'Declining',
      strong_decline: 'Strong Decline',
    };
    return labels[direction] || direction;
  };

  // Prepare chart data with formatted dates
  const chartData = timeseries.map((point) => ({
    ...point,
    dateFormatted: new Date(point.date).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }),
    ctrPercent: point.ctr * 100,
  }));

  // Calculate date range
  const dateRange = timeseries.length > 0 ? {
    start: new Date(timeseries[0].date).toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric' }),
    end: new Date(timeseries[timeseries.length - 1].date).toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric' }),
  } : null;

  const CustomTooltip = ({ active, payload, label }: any) => {
    if (active && payload && payload.length) {
      return (
        <div className="bg-white p-4 border border-gray-200 rounded-lg shadow-lg">
          <p className="text-sm font-semibold text-gray-900 mb-2">{label}</p>
          {payload.map((entry: any, index: number) => (
            <p key={index} className="text-sm" style={{ color: entry.color }}>
              {entry.name}: {entry.name === 'CTR' ? entry.value.toFixed(2) + '%' : formatNumber(entry.value)}
            </p>
          ))}
        </div>
      );
    }
    return null;
  };

  return (
    <div className="bg-white rounded-lg shadow-md p-6">
      {/* Header */}
      <div className="mb-6">
        <div className="flex items-center justify-between mb-2">
          <h2 className="text-2xl font-bold text-gray-900">Traffic Overview</h2>
          <div className="flex items-center gap-2 text-sm text-gray-600">
            <Calendar className="w-4 h-4" />
            {dateRange && (
              <span>{dateRange.start} — {dateRange.end}</span>
            )}
          </div>
        </div>
        <div className="flex items-center gap-2">
          {getTrendIcon(overall_direction)}
          <span className={`text-lg font-semibold ${getTrendColor(overall_direction)}`}>
            {getDirectionLabel(overall_direction)}
          </span>
          <span className="text-gray-600">
            ({formatPercentage(trend_slope_pct_per_month)}/month)
          </span>
        </div>
      </div>

      {/* Key Metrics Cards */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-8">
        {/* Total Clicks */}
        <div className="bg-gradient-to-br from-blue-50 to-blue-100 rounded-lg p-4 border border-blue-200">
          <div className="flex items-center justify-between mb-2">
            <span className="text-sm font-medium text-blue-900">Total Clicks</span>
            <Activity className="w-4 h-4 text-blue-600" />
          </div>
          <div className="text-2xl font-bold text-blue-900 mb-1">
            {formatNumber(summary.total_clicks)}
          </div>
          <div className={`text-sm font-semibold flex items-center gap-1 ${getChangeColor(summary.clicks_change_pct)}`}>
            {summary.clicks_change_pct > 0 ? (
              <TrendingUp className="w-3 h-3" />
            ) : summary.clicks_change_pct < 0 ? (
              <TrendingDown className="w-3 h-3" />
            ) : (
              <Minus className="w-3 h-3" />
            )}
            <span>{formatPercentage(summary.clicks_change_pct)}</span>
          </div>
        </div>

        {/* Total Impressions */}
        <div className="bg-gradient-to-br from-purple-50 to-purple-100 rounded-lg p-4 border border-purple-200">
          <div className="flex items-center justify-between mb-2">
            <span className="text-sm font-medium text-purple-900">Total Impressions</span>
            <Activity className="w-4 h-4 text-purple-600" />
          </div>
          <div className="text-2xl font-bold text-purple-900 mb-1">
            {formatNumber(summary.total_impressions)}
          </div>
          <div className={`text-sm font-semibold flex items-center gap-1 ${getChangeColor(summary.impressions_change_pct)}`}>
            {summary.impressions_change_pct > 0 ? (
              <TrendingUp className="w-3 h-3" />
            ) : summary.impressions_change_pct < 0 ? (
              <TrendingDown className="w-3 h-3" />
            ) : (
              <Minus className="w-3 h-3" />
            )}
            <span>{formatPercentage(summary.impressions_change_pct)}</span>
          </div>
        </div>

        {/* Average CTR */}
        <div className="bg-gradient-to-br from-green-50 to-green-100 rounded-lg p-4 border border-green-200">
          <div className="flex items-center justify-between mb-2">
            <span className="text-sm font-medium text-green-900">Average CTR</span>
            <Activity className="w-4 h-4 text-green-600" />
          </div>
          <div className="text-2xl font-bold text-green-900 mb-1">
            {formatCTR(summary.avg_ctr)}
          </div>
          <div className={`text-sm font-semibold flex items-center gap-1 ${getChangeColor(summary.ctr_change_pct)}`}>
            {summary.ctr_change_pct > 0 ? (
              <TrendingUp className="w-3 h-3" />
            ) : summary.ctr_change_pct < 0 ? (
              <TrendingDown className="w-3 h-3" />
            ) : (
              <Minus className="w-3 h-3" />
            )}
            <span>{formatPercentage(summary.ctr_change_pct)}</span>
          </div>
        </div>

        {/* Average Position */}
        <div className="bg-gradient-to-br from-orange-50 to-orange-100 rounded-lg p-4 border border-orange-200">
          <div className="flex items-center justify-between mb-2">
            <span className="text-sm font-medium text-orange-900">Average Position</span>
            <Activity className="w-4 h-4 text-orange-600" />
          </div>
          <div className="text-2xl font-bold text-orange-900 mb-1">
            {formatPosition(summary.avg_position)}
          </div>
          <div className={`text-sm font-semibold flex items-center gap-1 ${getPositionChangeColor(summary.position_change)}`}>
            {summary.position_change < 0 ? (
              <TrendingUp className="w-3 h-3" />
            ) : summary.position_change > 0 ? (
              <TrendingDown className="w-3 h-3" />
            ) : (
              <Minus className="w-3 h-3" />
            )}
            <span>{summary.position_change > 0 ? '+' : ''}{summary.position_change.toFixed(1)}</span>
          </div>
        </div>
      </div>

      {/* Main Time Series Chart */}
      <div className="mb-8">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">Clicks & Impressions Over Time</h3>
        <ResponsiveContainer width="100%" height={400}>
          <ComposedChart data={chartData}>
            <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
            <XAxis 
              dataKey="dateFormatted" 
              tick={{ fontSize: 12 }}
              stroke="#6b7280"
            />
            <YAxis 
              yAxisId="left"
              tick={{ fontSize: 12 }}
              stroke="#6b7280"
              label={{ value: 'Clicks & Impressions', angle: -90, position: 'insideLeft', style: { fontSize: 12 } }}
            />
            <YAxis 
              yAxisId="right"
              orientation="right"
              tick={{ fontSize: 12 }}
              stroke="#6b7280"
              label={{ value: 'CTR (%)', angle: 90, position: 'insideRight', style: { fontSize: 12 } }}
            />
            <Tooltip content={<CustomTooltip />} />
            <Legend wrapperStyle={{ fontSize: 12 }} />
            <Area
              yAxisId="left"
              type="monotone"
              dataKey="impressions"
              fill="#c7d2fe"
              stroke="#818cf8"
              strokeWidth={2}
              name="Impressions"
              fillOpacity={0.6}
            />
            <Line
              yAxisId="left"
              type="monotone"
              dataKey="clicks"
              stroke="#3b82f6"
              strokeWidth={3}
              name="Clicks"
              dot={false}
            />
            <Line
              yAxisId="right"
              type="monotone"
              dataKey="ctrPercent"
              stroke="#10b981"
              strokeWidth={2}
              name="CTR"
              dot={false}
            />
          </ComposedChart>
        </ResponsiveContainer>
      </div>

      {/* Additional Insights Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        {/* Seasonality Insights */}
        <div className="bg-gray-50 rounded-lg p-5 border border-gray-200">
          <h4 className="text-md font-semibold text-gray-900 mb-3">Seasonality Patterns</h4>
          <div className="space-y-3">
            <div className="flex justify-between items-center">
              <span className="text-sm text-gray-600">Best Day:</span>
              <span className="text-sm font-semibold text-green-600">{seasonality.best_day}</span>
            </div>
            <div className="flex justify-between items-center">
              <span className="text-sm text-gray-600">Worst Day:</span>
              <span className="text-sm font-semibold text-red-600">{seasonality.worst_day}</span>
            </div>
            <div className="flex justify-between items-center">
              <span className="text-sm text-gray-600">Monthly Cycle:</span>
              <span className="text-sm font-semibold text-gray-900">
                {seasonality.monthly_cycle ? 'Yes' : 'No'}
              </span>
            </div>
            {seasonality.cycle_description && (
              <div className="pt-2 border-t border-gray-200">
                <p className="text-sm text-gray-700 italic">{seasonality.cycle_description}</p>
              </div>
            )}
          </div>
        </div>

        {/* Forecast */}
        <div className="bg-gray-50 rounded-lg p-5 border border-gray-200">
          <h4 className="text-md font-semibold text-gray-900 mb-3">Traffic Forecast</h4>
          <div className="space-y-3">
            <div>
              <div className="flex justify-between items-center mb-1">
                <span className="text-sm text-gray-600">30 Days:</span>
                <span className="text-sm font-semibold text-gray-900">
                  {formatNumber(forecast['30d'].clicks)} clicks
                </span>
              </div>
              <div className="text-xs text-gray-500">
                Range: {formatNumber(forecast['30d'].ci_low)} — {formatNumber(forecast['30d'].ci_high)}
              </div>
            </div>
            <div>
              <div className="flex justify-between items-center mb-1">
                <span className="text-sm text-gray-600">60 Days:</span>
                <span className="text-sm font-semibold text-gray-900">
                  {formatNumber(forecast['60d'].clicks)} clicks
                </span>
              </div>
              <div className="text-xs text-gray-500">
                Range: {formatNumber(forecast['60d'].ci_low)} — {formatNumber(forecast['60d'].ci_high)}
              </div>
            </div>
            <div>
              <div className="flex justify-between items-center mb-1">
                <span className="text-sm text-gray-600">90 Days:</span>
                <span className="text-sm font-semibold text-gray-900">
                  {formatNumber(forecast['90d'].clicks)} clicks
                </span>
              </div>
              <div className="text-xs text-gray-500">
                Range: {formatNumber(forecast['90d'].ci_low)} — {formatNumber(forecast['90d'].ci_high)}
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Position Chart */}
      <div className="mt-8">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">Average Position Trend</h3>
        <ResponsiveContainer width="100%" height={250}>
          <LineChart data={chartData}>
            <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
            <XAxis 
              dataKey="dateFormatted" 
              tick={{ fontSize: 12 }}
              stroke="#6b7280"
            />
            <YAxis 
              reversed
              tick={{ fontSize: 12 }}
              stroke="#6b7280"
              label={{ value: 'Position (lower is better)', angle: -90, position: 'insideLeft', style: { fontSize: 12 } }}
            />
            <Tooltip content={<CustomTooltip />} />
            <Legend wrapperStyle={{ fontSize: 12 }} />
            <Line
              type="monotone"
              dataKey="position"
              stroke="#f59e0b"
              strokeWidth={3}
              name="Position"
              dot={false}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
};

export default Module1TrafficOverview;