import React from 'react';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  Area,
  ReferenceLine,
  Dot,
} from 'recharts';
import { TrendingUp, TrendingDown, Minus, AlertTriangle } from 'lucide-react';

interface ChangePoint {
  date: string;
  magnitude: number;
  direction: 'drop' | 'spike';
}

interface ForecastPoint {
  clicks: number;
  ci_low: number;
  ci_high: number;
}

interface HealthTrajectoryData {
  overall_direction: 'strong_growth' | 'growth' | 'flat' | 'declining' | 'strong_decline';
  trend_slope_pct_per_month: number;
  change_points: ChangePoint[];
  seasonality: {
    best_day: string;
    worst_day: string;
    monthly_cycle: boolean;
    cycle_description: string;
  };
  anomalies: Array<{
    date: string;
    type: 'discord' | 'motif';
    magnitude: number;
  }>;
  forecast: {
    '30d': ForecastPoint;
    '60d': ForecastPoint;
    '90d': ForecastPoint;
  };
  daily_data: Array<{
    date: string;
    actual_clicks: number;
    trend: number;
    forecast?: number;
    forecast_ci_low?: number;
    forecast_ci_high?: number;
  }>;
}

interface HealthTrajectoryChartProps {
  data: HealthTrajectoryData;
}

const HealthTrajectoryChart: React.FC<HealthTrajectoryChartProps> = ({ data }) => {
  // Prepare chart data
  const chartData = data.daily_data.map((point) => ({
    date: point.date,
    actual: point.actual_clicks,
    trend: point.trend,
    forecast: point.forecast,
    forecastLow: point.forecast_ci_low,
    forecastHigh: point.forecast_ci_high,
  }));

  // Get direction indicator
  const getDirectionIcon = () => {
    switch (data.overall_direction) {
      case 'strong_growth':
      case 'growth':
        return <TrendingUp className="w-5 h-5 text-green-600" />;
      case 'strong_decline':
      case 'declining':
        return <TrendingDown className="w-5 h-5 text-red-600" />;
      case 'flat':
        return <Minus className="w-5 h-5 text-gray-600" />;
    }
  };

  const getDirectionColor = () => {
    switch (data.overall_direction) {
      case 'strong_growth':
      case 'growth':
        return 'text-green-600';
      case 'strong_decline':
      case 'declining':
        return 'text-red-600';
      case 'flat':
        return 'text-gray-600';
    }
  };

  const getDirectionLabel = () => {
    return data.overall_direction
      .split('_')
      .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
      .join(' ');
  };

  // Custom dot for change points
  const CustomDot = (props: any) => {
    const { cx, cy, payload } = props;
    const changePoint = data.change_points.find((cp) => cp.date === payload.date);

    if (!changePoint) return null;

    return (
      <g>
        <circle
          cx={cx}
          cy={cy}
          r={6}
          fill={changePoint.direction === 'drop' ? '#ef4444' : '#10b981'}
          stroke="#fff"
          strokeWidth={2}
        />
        <AlertTriangle
          x={cx - 8}
          y={cy - 24}
          className={`w-4 h-4 ${
            changePoint.direction === 'drop' ? 'text-red-600' : 'text-green-600'
          }`}
        />
      </g>
    );
  };

  // Custom tooltip
  const CustomTooltip = ({ active, payload, label }: any) => {
    if (!active || !payload || !payload.length) return null;

    const changePoint = data.change_points.find((cp) => cp.date === label);
    const anomaly = data.anomalies.find((a) => a.date === label);

    return (
      <div className="bg-white p-4 border border-gray-200 rounded-lg shadow-lg">
        <p className="font-semibold text-gray-900 mb-2">
          {new Date(label).toLocaleDateString('en-US', {
            year: 'numeric',
            month: 'short',
            day: 'numeric',
          })}
        </p>
        {payload.map((entry: any, index: number) => {
          if (!entry.value) return null;
          return (
            <p key={index} className="text-sm" style={{ color: entry.color }}>
              {entry.name}: {Math.round(entry.value).toLocaleString()}
              {entry.name === 'Forecast' && entry.payload.forecastLow && entry.payload.forecastHigh && (
                <span className="text-gray-500 ml-1">
                  (±{Math.round((entry.payload.forecastHigh - entry.payload.forecastLow) / 2).toLocaleString()})
                </span>
              )}
            </p>
          );
        })}
        {changePoint && (
          <div className="mt-2 pt-2 border-t border-gray-200">
            <p
              className={`text-sm font-semibold ${
                changePoint.direction === 'drop' ? 'text-red-600' : 'text-green-600'
              }`}
            >
              Change Point Detected
            </p>
            <p className="text-xs text-gray-600">
              {changePoint.direction === 'drop' ? 'Drop' : 'Spike'} of{' '}
              {Math.abs(changePoint.magnitude * 100).toFixed(1)}%
            </p>
          </div>
        )}
        {anomaly && (
          <div className="mt-2 pt-2 border-t border-gray-200">
            <p className="text-sm font-semibold text-orange-600">
              {anomaly.type === 'discord' ? 'Anomaly' : 'Recurring Pattern'}
            </p>
            <p className="text-xs text-gray-600">
              Magnitude: {Math.abs(anomaly.magnitude * 100).toFixed(1)}%
            </p>
          </div>
        )}
      </div>
    );
  };

  // Format date for x-axis
  const formatDate = (dateStr: string) => {
    const date = new Date(dateStr);
    return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
  };

  return (
    <div className="space-y-6">
      {/* Header with trend indicator */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          {getDirectionIcon()}
          <div>
            <h3 className="text-lg font-semibold text-gray-900">Traffic Trajectory</h3>
            <p className={`text-sm ${getDirectionColor()}`}>
              {getDirectionLabel()} — {data.trend_slope_pct_per_month > 0 ? '+' : ''}
              {data.trend_slope_pct_per_month.toFixed(1)}% per month
            </p>
          </div>
        </div>
        <div className="text-right">
          <p className="text-sm text-gray-600">90-day forecast</p>
          <p className="text-lg font-semibold text-gray-900">
            {Math.round(data.forecast['90d'].clicks).toLocaleString()} clicks
          </p>
          <p className="text-xs text-gray-500">
            {Math.round(data.forecast['90d'].ci_low).toLocaleString()} –{' '}
            {Math.round(data.forecast['90d'].ci_high).toLocaleString()}
          </p>
        </div>
      </div>

      {/* Chart */}
      <div className="w-full h-96">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart
            data={chartData}
            margin={{ top: 5, right: 30, left: 20, bottom: 5 }}
          >
            <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
            <XAxis
              dataKey="date"
              tickFormatter={formatDate}
              stroke="#6b7280"
              style={{ fontSize: '12px' }}
            />
            <YAxis
              stroke="#6b7280"
              style={{ fontSize: '12px' }}
              tickFormatter={(value) => value.toLocaleString()}
            />
            <Tooltip content={<CustomTooltip />} />
            <Legend wrapperStyle={{ paddingTop: '20px' }} />

            {/* Confidence interval area for forecast */}
            <Area
              type="monotone"
              dataKey="forecastHigh"
              stroke="none"
              fill="#3b82f6"
              fillOpacity={0.1}
              name="Confidence Interval"
            />
            <Area
              type="monotone"
              dataKey="forecastLow"
              stroke="none"
              fill="#3b82f6"
              fillOpacity={0.1}
            />

            {/* Trend line */}
            <Line
              type="monotone"
              dataKey="trend"
              stroke="#8b5cf6"
              strokeWidth={2}
              dot={false}
              name="Trend"
              strokeDasharray="5 5"
            />

            {/* Actual data */}
            <Line
              type="monotone"
              dataKey="actual"
              stroke="#10b981"
              strokeWidth={2}
              dot={<CustomDot />}
              name="Actual Clicks"
            />

            {/* Forecast line */}
            <Line
              type="monotone"
              dataKey="forecast"
              stroke="#3b82f6"
              strokeWidth={2}
              dot={false}
              name="Forecast"
              strokeDasharray="3 3"
            />

            {/* Reference line for today (where actual ends and forecast begins) */}
            {chartData.some((d) => d.forecast) && (
              <ReferenceLine
                x={chartData.find((d) => d.forecast)?.date}
                stroke="#9ca3af"
                strokeDasharray="3 3"
                label={{ value: 'Today', position: 'top', fill: '#6b7280' }}
              />
            )}
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Seasonality insights */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <div className="bg-blue-50 p-4 rounded-lg">
          <p className="text-sm font-semibold text-blue-900 mb-1">Seasonality Detected</p>
          <p className="text-xs text-blue-700">{data.seasonality.cycle_description}</p>
        </div>
        <div className="bg-green-50 p-4 rounded-lg">
          <p className="text-sm font-semibold text-green-900 mb-1">Best Performance</p>
          <p className="text-xs text-green-700">{data.seasonality.best_day}</p>
        </div>
        <div className="bg-orange-50 p-4 rounded-lg">
          <p className="text-sm font-semibold text-orange-900 mb-1">Lowest Performance</p>
          <p className="text-xs text-orange-700">{data.seasonality.worst_day}</p>
        </div>
      </div>

      {/* Change points summary */}
      {data.change_points.length > 0 && (
        <div className="border-t border-gray-200 pt-4">
          <h4 className="text-sm font-semibold text-gray-900 mb-3">
            Significant Changes Detected ({data.change_points.length})
          </h4>
          <div className="space-y-2">
            {data.change_points.slice(0, 3).map((cp, idx) => (
              <div
                key={idx}
                className="flex items-center justify-between py-2 px-3 bg-gray-50 rounded"
              >
                <div className="flex items-center gap-2">
                  {cp.direction === 'drop' ? (
                    <TrendingDown className="w-4 h-4 text-red-600" />
                  ) : (
                    <TrendingUp className="w-4 h-4 text-green-600" />
                  )}
                  <span className="text-sm text-gray-700">
                    {new Date(cp.date).toLocaleDateString('en-US', {
                      year: 'numeric',
                      month: 'short',
                      day: 'numeric',
                    })}
                  </span>
                </div>
                <span
                  className={`text-sm font-semibold ${
                    cp.direction === 'drop' ? 'text-red-600' : 'text-green-600'
                  }`}
                >
                  {cp.direction === 'drop' ? '' : '+'}
                  {(cp.magnitude * 100).toFixed(1)}%
                </span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Forecast summary */}
      <div className="border-t border-gray-200 pt-4">
        <h4 className="text-sm font-semibold text-gray-900 mb-3">Forecast Summary</h4>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <div className="bg-gray-50 p-4 rounded-lg">
            <p className="text-xs text-gray-600 mb-1">30 Days</p>
            <p className="text-lg font-semibold text-gray-900">
              {Math.round(data.forecast['30d'].clicks).toLocaleString()}
            </p>
            <p className="text-xs text-gray-500">
              ±{Math.round((data.forecast['30d'].ci_high - data.forecast['30d'].ci_low) / 2).toLocaleString()}
            </p>
          </div>
          <div className="bg-gray-50 p-4 rounded-lg">
            <p className="text-xs text-gray-600 mb-1">60 Days</p>
            <p className="text-lg font-semibold text-gray-900">
              {Math.round(data.forecast['60d'].clicks).toLocaleString()}
            </p>
            <p className="text-xs text-gray-500">
              ±{Math.round((data.forecast['60d'].ci_high - data.forecast['60d'].ci_low) / 2).toLocaleString()}
            </p>
          </div>
          <div className="bg-gray-50 p-4 rounded-lg">
            <p className="text-xs text-gray-600 mb-1">90 Days</p>
            <p className="text-lg font-semibold text-gray-900">
              {Math.round(data.forecast['90d'].clicks).toLocaleString()}
            </p>
            <p className="text-xs text-gray-500">
              ±{Math.round((data.forecast['90d'].ci_high - data.forecast['90d'].ci_low) / 2).toLocaleString()}
            </p>
          </div>
        </div>
      </div>
    </div>
  );
};

export default HealthTrajectoryChart;
