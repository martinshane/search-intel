import React, { useState } from 'react';
import { ChevronDown, ChevronUp, TrendingUp, TrendingDown, Minus, AlertCircle, Target } from 'lucide-react';

interface TldrData {
  summary: string;
  direction?: 'up' | 'down' | 'neutral';
  metric?: string;
  metricValue?: string | number;
  severity?: 'critical' | 'warning' | 'info' | 'success';
}

interface VisualizationProps {
  type?: 'chart' | 'graph' | 'table' | 'matrix';
  data?: any;
}

interface DetailRow {
  [key: string]: any;
}

interface ActionItem {
  title: string;
  description: string;
  impact?: string;
  effort?: 'low' | 'medium' | 'high';
  priority?: number;
}

interface CollapsibleCardProps {
  title: string;
  subtitle?: string;
  tldr: TldrData;
  visualization?: VisualizationProps;
  details?: {
    columns: Array<{ key: string; label: string; sortable?: boolean }>;
    rows: DetailRow[];
  };
  actions?: ActionItem[];
  defaultExpanded?: boolean;
  onExpand?: (expanded: boolean) => void;
}

const TrendIcon: React.FC<{ direction?: 'up' | 'down' | 'neutral' }> = ({ direction }) => {
  switch (direction) {
    case 'up':
      return <TrendingUp className="w-5 h-5 text-green-500" />;
    case 'down':
      return <TrendingDown className="w-5 h-5 text-red-500" />;
    case 'neutral':
      return <Minus className="w-5 h-5 text-gray-500" />;
    default:
      return null;
  }
};

const SeverityBadge: React.FC<{ severity?: 'critical' | 'warning' | 'info' | 'success' }> = ({ severity }) => {
  if (!severity) return null;

  const colors = {
    critical: 'bg-red-100 text-red-800 border-red-200',
    warning: 'bg-yellow-100 text-yellow-800 border-yellow-200',
    info: 'bg-blue-100 text-blue-800 border-blue-200',
    success: 'bg-green-100 text-green-800 border-green-200',
  };

  const labels = {
    critical: 'Critical',
    warning: 'Warning',
    info: 'Info',
    success: 'Success',
  };

  return (
    <span className={`px-2 py-1 text-xs font-medium rounded-full border ${colors[severity]}`}>
      {labels[severity]}
    </span>
  );
};

const EffortBadge: React.FC<{ effort?: 'low' | 'medium' | 'high' }> = ({ effort }) => {
  if (!effort) return null;

  const colors = {
    low: 'bg-green-50 text-green-700 border-green-200',
    medium: 'bg-yellow-50 text-yellow-700 border-yellow-200',
    high: 'bg-red-50 text-red-700 border-red-200',
  };

  const labels = {
    low: 'Low Effort',
    medium: 'Medium Effort',
    high: 'High Effort',
  };

  return (
    <span className={`px-2 py-1 text-xs font-medium rounded border ${colors[effort]}`}>
      {labels[effort]}
    </span>
  );
};

export const CollapsibleCard: React.FC<CollapsibleCardProps> = ({
  title,
  subtitle,
  tldr,
  visualization,
  details,
  actions,
  defaultExpanded = false,
  onExpand,
}) => {
  const [isExpanded, setIsExpanded] = useState(defaultExpanded);

  const handleToggle = () => {
    const newState = !isExpanded;
    setIsExpanded(newState);
    onExpand?.(newState);
  };

  return (
    <div className="bg-white border border-gray-200 rounded-lg shadow-sm overflow-hidden mb-6">
      {/* Header */}
      <div className="border-b border-gray-200 bg-gray-50">
        <div className="p-6">
          <div className="flex items-start justify-between">
            <div className="flex-1">
              <div className="flex items-center gap-3 mb-2">
                <h2 className="text-2xl font-semibold text-gray-900">{title}</h2>
                <SeverityBadge severity={tldr.severity} />
              </div>
              {subtitle && (
                <p className="text-sm text-gray-600 mb-4">{subtitle}</p>
              )}
            </div>
            <button
              onClick={handleToggle}
              className="ml-4 p-2 hover:bg-gray-200 rounded-lg transition-colors focus:outline-none focus:ring-2 focus:ring-blue-500"
              aria-label={isExpanded ? 'Collapse section' : 'Expand section'}
            >
              {isExpanded ? (
                <ChevronUp className="w-6 h-6 text-gray-600" />
              ) : (
                <ChevronDown className="w-6 h-6 text-gray-600" />
              )}
            </button>
          </div>

          {/* TL;DR Section - Always visible */}
          <div className="bg-white border border-gray-200 rounded-lg p-4">
            <div className="flex items-start gap-3">
              <div className="flex-shrink-0 mt-1">
                <TrendIcon direction={tldr.direction} />
              </div>
              <div className="flex-1">
                <div className="flex items-center gap-2 mb-1">
                  <span className="text-xs font-semibold text-gray-500 uppercase tracking-wide">
                    TL;DR
                  </span>
                  {tldr.metric && (
                    <span className="text-xs text-gray-400">•</span>
                  )}
                  {tldr.metric && (
                    <span className="text-xs font-medium text-gray-600">
                      {tldr.metric}
                    </span>
                  )}
                </div>
                <p className="text-gray-900 leading-relaxed">{tldr.summary}</p>
                {tldr.metricValue && (
                  <div className="mt-2">
                    <span className="text-2xl font-bold text-gray-900">
                      {tldr.metricValue}
                    </span>
                  </div>
                )}
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Expandable Content */}
      {isExpanded && (
        <div className="p-6">
          {/* Visualization Section */}
          {visualization && (
            <div className="mb-6">
              <div className="bg-gray-50 border border-gray-200 rounded-lg p-6">
                <div className="flex items-center justify-center h-64 text-gray-400">
                  {/* Placeholder for visualization */}
                  <div className="text-center">
                    <div className="text-sm font-medium mb-2">
                      {visualization.type ? `${visualization.type.toUpperCase()} VISUALIZATION` : 'VISUALIZATION'}
                    </div>
                    <div className="text-xs text-gray-500">
                      Chart/graph will be rendered here
                    </div>
                  </div>
                </div>
              </div>
            </div>
          )}

          {/* Detail Table Section */}
          {details && details.rows.length > 0 && (
            <div className="mb-6">
              <h3 className="text-lg font-semibold text-gray-900 mb-3">Details</h3>
              <div className="border border-gray-200 rounded-lg overflow-hidden">
                <div className="overflow-x-auto">
                  <table className="min-w-full divide-y divide-gray-200">
                    <thead className="bg-gray-50">
                      <tr>
                        {details.columns.map((column) => (
                          <th
                            key={column.key}
                            className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider"
                          >
                            {column.label}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody className="bg-white divide-y divide-gray-200">
                      {details.rows.map((row, idx) => (
                        <tr key={idx} className="hover:bg-gray-50">
                          {details.columns.map((column) => (
                            <td
                              key={column.key}
                              className="px-4 py-3 text-sm text-gray-900 whitespace-nowrap"
                            >
                              {row[column.key] !== undefined ? String(row[column.key]) : '—'}
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          )}

          {/* Actions Section */}
          {actions && actions.length > 0 && (
            <div>
              <div className="flex items-center gap-2 mb-3">
                <Target className="w-5 h-5 text-blue-600" />
                <h3 className="text-lg font-semibold text-gray-900">Recommended Actions</h3>
              </div>
              <div className="space-y-3">
                {actions.map((action, idx) => (
                  <div
                    key={idx}
                    className="bg-blue-50 border border-blue-200 rounded-lg p-4"
                  >
                    <div className="flex items-start justify-between gap-4">
                      <div className="flex-1">
                        <div className="flex items-center gap-2 mb-1">
                          {action.priority && (
                            <span className="flex items-center justify-center w-6 h-6 rounded-full bg-blue-600 text-white text-xs font-bold">
                              {action.priority}
                            </span>
                          )}
                          <h4 className="font-semibold text-gray-900">{action.title}</h4>
                        </div>
                        <p className="text-sm text-gray-700 leading-relaxed mb-2">
                          {action.description}
                        </p>
                        {action.impact && (
                          <div className="flex items-center gap-2 text-xs text-gray-600">
                            <AlertCircle className="w-4 h-4" />
                            <span>Estimated impact: {action.impact}</span>
                          </div>
                        )}
                      </div>
                      <div className="flex-shrink-0">
                        <EffortBadge effort={action.effort} />
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
};

export default CollapsibleCard;
