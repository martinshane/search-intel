import React, { useState } from 'react';
import { ChevronDown, ChevronUp } from 'lucide-react';

interface ReportSectionProps {
  title: string;
  tldr: string;
  metric?: {
    value: string | number;
    label: string;
    trend?: 'up' | 'down' | 'neutral';
  };
  children: React.ReactNode;
  defaultExpanded?: boolean;
  badge?: string;
  badgeColor?: 'red' | 'yellow' | 'green' | 'blue' | 'gray';
}

const ReportSection: React.FC<ReportSectionProps> = ({
  title,
  tldr,
  metric,
  children,
  defaultExpanded = false,
  badge,
  badgeColor = 'blue',
}) => {
  const [isExpanded, setIsExpanded] = useState(defaultExpanded);

  const badgeColors = {
    red: 'bg-red-100 text-red-800 border-red-200',
    yellow: 'bg-yellow-100 text-yellow-800 border-yellow-200',
    green: 'bg-green-100 text-green-800 border-green-200',
    blue: 'bg-blue-100 text-blue-800 border-blue-200',
    gray: 'bg-gray-100 text-gray-800 border-gray-200',
  };

  const trendColors = {
    up: 'text-green-600',
    down: 'text-red-600',
    neutral: 'text-gray-600',
  };

  const trendIcons = {
    up: '↑',
    down: '↓',
    neutral: '→',
  };

  return (
    <div className="bg-white rounded-lg shadow-sm border border-gray-200 mb-4 overflow-hidden">
      {/* Header - Always visible, clickable */}
      <button
        onClick={() => setIsExpanded(!isExpanded)}
        className="w-full px-4 py-4 sm:px-6 sm:py-5 flex items-start justify-between hover:bg-gray-50 transition-colors text-left"
        aria-expanded={isExpanded}
      >
        <div className="flex-1 min-w-0 pr-4">
          {/* Title row with optional badge */}
          <div className="flex items-center gap-3 mb-2 flex-wrap">
            <h2 className="text-lg sm:text-xl font-semibold text-gray-900">
              {title}
            </h2>
            {badge && (
              <span
                className={`px-2 py-0.5 text-xs font-medium rounded border ${badgeColors[badgeColor]}`}
              >
                {badge}
              </span>
            )}
          </div>

          {/* TL;DR */}
          <p className="text-sm sm:text-base text-gray-600 mb-2">{tldr}</p>

          {/* Metric display (if provided) */}
          {metric && (
            <div className="flex items-baseline gap-2 mt-3">
              <span className="text-2xl sm:text-3xl font-bold text-gray-900">
                {metric.value}
              </span>
              <span className="text-sm text-gray-500">{metric.label}</span>
              {metric.trend && (
                <span
                  className={`text-sm font-medium ${trendColors[metric.trend]}`}
                >
                  {trendIcons[metric.trend]}
                </span>
              )}
            </div>
          )}
        </div>

        {/* Expand/collapse icon */}
        <div className="flex-shrink-0 ml-2 pt-1">
          {isExpanded ? (
            <ChevronUp className="w-5 h-5 text-gray-400" />
          ) : (
            <ChevronDown className="w-5 h-5 text-gray-400" />
          )}
        </div>
      </button>

      {/* Expandable content */}
      {isExpanded && (
        <div className="px-4 pb-4 sm:px-6 sm:pb-6 border-t border-gray-100">
          <div className="pt-4 sm:pt-5">{children}</div>
        </div>
      )}
    </div>
  );
};

export default ReportSection;
