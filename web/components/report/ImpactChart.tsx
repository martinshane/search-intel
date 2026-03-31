import React from 'react';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, Cell } from 'recharts';

interface ImpactChartProps {
  recoveryClicks: number;
  growthClicks: number;
  className?: string;
}

const ImpactChart: React.FC<ImpactChartProps> = ({
  recoveryClicks,
  growthClicks,
  className = '',
}) => {
  const data = [
    {
      name: 'Recovery',
      value: recoveryClicks,
      label: 'Estimated Recovery',
      description: 'Monthly clicks recoverable from fixing declining pages',
    },
    {
      name: 'Growth',
      value: growthClicks,
      label: 'Growth Opportunity',
      description: 'Monthly clicks gainable from new opportunities',
    },
  ];

  const COLORS = {
    Recovery: '#f59e0b', // amber-500 for recovery (fixing problems)
    Growth: '#10b981', // emerald-500 for growth (new gains)
  };

  const formatNumber = (value: number): string => {
    if (value >= 1000) {
      return `${(value / 1000).toFixed(1)}k`;
    }
    return value.toFixed(0);
  };

  const CustomTooltip = ({ active, payload }: any) => {
    if (!active || !payload || !payload.length) {
      return null;
    }

    const data = payload[0].payload;

    return (
      <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg shadow-lg p-3">
        <p className="font-semibold text-gray-900 dark:text-gray-100 mb-1">
          {data.label}
        </p>
        <p className="text-sm text-gray-600 dark:text-gray-400 mb-2">
          {data.description}
        </p>
        <p className="text-lg font-bold text-gray-900 dark:text-gray-100">
          {data.value.toLocaleString()} clicks/month
        </p>
      </div>
    );
  };

  const totalImpact = recoveryClicks + growthClicks;

  return (
    <div className={`space-y-4 ${className}`}>
      {/* Summary cards */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <div className="bg-amber-50 dark:bg-amber-900/20 border border-amber-200 dark:border-amber-800 rounded-lg p-4">
          <div className="text-sm font-medium text-amber-800 dark:text-amber-300 mb-1">
            Recovery Potential
          </div>
          <div className="text-3xl font-bold text-amber-600 dark:text-amber-400">
            {recoveryClicks.toLocaleString()}
          </div>
          <div className="text-xs text-amber-700 dark:text-amber-500 mt-1">
            clicks/month
          </div>
        </div>

        <div className="bg-emerald-50 dark:bg-emerald-900/20 border border-emerald-200 dark:border-emerald-800 rounded-lg p-4">
          <div className="text-sm font-medium text-emerald-800 dark:text-emerald-300 mb-1">
            Growth Opportunity
          </div>
          <div className="text-3xl font-bold text-emerald-600 dark:text-emerald-400">
            {growthClicks.toLocaleString()}
          </div>
          <div className="text-xs text-emerald-700 dark:text-emerald-500 mt-1">
            clicks/month
          </div>
        </div>

        <div className="bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-lg p-4">
          <div className="text-sm font-medium text-blue-800 dark:text-blue-300 mb-1">
            Total Impact
          </div>
          <div className="text-3xl font-bold text-blue-600 dark:text-blue-400">
            {totalImpact.toLocaleString()}
          </div>
          <div className="text-xs text-blue-700 dark:text-blue-500 mt-1">
            clicks/month potential
          </div>
        </div>
      </div>

      {/* Bar chart */}
      <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg p-6">
        <h3 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-4">
          Estimated Traffic Impact
        </h3>
        
        <ResponsiveContainer width="100%" height={300}>
          <BarChart
            data={data}
            margin={{ top: 20, right: 30, left: 20, bottom: 5 }}
          >
            <CartesianGrid strokeDasharray="3 3" className="stroke-gray-200 dark:stroke-gray-700" />
            <XAxis
              dataKey="label"
              className="text-gray-600 dark:text-gray-400"
              tick={{ fill: 'currentColor' }}
            />
            <YAxis
              className="text-gray-600 dark:text-gray-400"
              tick={{ fill: 'currentColor' }}
              tickFormatter={formatNumber}
              label={{
                value: 'Clicks per Month',
                angle: -90,
                position: 'insideLeft',
                className: 'text-gray-600 dark:text-gray-400',
              }}
            />
            <Tooltip content={<CustomTooltip />} cursor={{ fill: 'rgba(0, 0, 0, 0.05)' }} />
            <Bar dataKey="value" radius={[8, 8, 0, 0]} maxBarSize={120}>
              {data.map((entry, index) => (
                <Cell key={`cell-${index}`} fill={COLORS[entry.name as keyof typeof COLORS]} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>

        <div className="mt-4 flex items-start space-x-6 text-sm text-gray-600 dark:text-gray-400">
          <div className="flex items-center">
            <div className="w-3 h-3 rounded bg-amber-500 mr-2"></div>
            <span>Recovery from fixing issues</span>
          </div>
          <div className="flex items-center">
            <div className="w-3 h-3 rounded bg-emerald-500 mr-2"></div>
            <span>Growth from new opportunities</span>
          </div>
        </div>
      </div>

      {/* Explanation */}
      <div className="bg-gray-50 dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-lg p-4">
        <p className="text-sm text-gray-700 dark:text-gray-300 leading-relaxed">
          <strong>Recovery</strong> represents traffic you're currently losing from declining pages,
          CTR anomalies, and technical issues — clicks you can win back by fixing known problems.{' '}
          <strong>Growth</strong> represents net-new traffic from striking distance keywords,
          content gaps, and untapped opportunities.
        </p>
      </div>
    </div>
  );
};

export default ImpactChart;
