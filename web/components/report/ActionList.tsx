import React from 'react';
import { AlertCircle, CheckCircle, Clock, TrendingUp, Zap, AlertTriangle } from 'lucide-react';

interface ActionItem {
  action: string;
  page?: string;
  keyword?: string;
  impact: number; // estimated monthly clicks
  effort: 'low' | 'medium' | 'high';
  priority_score?: number;
  dependencies?: string[];
  recommended_action?: string;
  details?: string;
}

interface ActionListProps {
  items: ActionItem[];
  title: string;
  variant?: 'critical' | 'quick-win' | 'strategic' | 'structural';
  className?: string;
}

const effortConfig = {
  low: {
    label: 'Low effort',
    color: 'text-emerald-700 bg-emerald-50 border-emerald-200',
    icon: Zap,
  },
  medium: {
    label: 'Medium effort',
    color: 'text-amber-700 bg-amber-50 border-amber-200',
    icon: Clock,
  },
  high: {
    label: 'High effort',
    color: 'text-red-700 bg-red-50 border-red-200',
    icon: AlertTriangle,
  },
};

const variantConfig = {
  critical: {
    bgColor: 'bg-red-50',
    borderColor: 'border-red-200',
    accentColor: 'bg-red-500',
    textColor: 'text-red-900',
    icon: AlertCircle,
    iconColor: 'text-red-600',
  },
  'quick-win': {
    bgColor: 'bg-emerald-50',
    borderColor: 'border-emerald-200',
    accentColor: 'bg-emerald-500',
    textColor: 'text-emerald-900',
    icon: CheckCircle,
    iconColor: 'text-emerald-600',
  },
  strategic: {
    bgColor: 'bg-blue-50',
    borderColor: 'border-blue-200',
    accentColor: 'bg-blue-500',
    textColor: 'text-blue-900',
    icon: TrendingUp,
    iconColor: 'text-blue-600',
  },
  structural: {
    bgColor: 'bg-purple-50',
    borderColor: 'border-purple-200',
    accentColor: 'bg-purple-500',
    textColor: 'text-purple-900',
    icon: Clock,
    iconColor: 'text-purple-600',
  },
};

export default function ActionList({ 
  items, 
  title, 
  variant = 'quick-win',
  className = '' 
}: ActionListProps) {
  const config = variantConfig[variant];
  const IconComponent = config.icon;

  if (!items || items.length === 0) {
    return null;
  }

  // Sort by impact (descending) and priority score if available
  const sortedItems = [...items].sort((a, b) => {
    if (a.priority_score && b.priority_score) {
      return b.priority_score - a.priority_score;
    }
    return b.impact - a.impact;
  });

  return (
    <div className={`space-y-3 ${className}`}>
      <div className="flex items-center gap-2 mb-4">
        <IconComponent className={`h-5 w-5 ${config.iconColor}`} />
        <h3 className={`text-lg font-semibold ${config.textColor}`}>
          {title}
        </h3>
        <span className="text-sm text-gray-500 ml-2">
          ({items.length} {items.length === 1 ? 'item' : 'items'})
        </span>
      </div>

      <div className="space-y-3">
        {sortedItems.map((item, index) => {
          const EffortIcon = effortConfig[item.effort].icon;
          
          return (
            <div
              key={index}
              className={`
                relative overflow-hidden rounded-lg border ${config.borderColor} ${config.bgColor}
                transition-all duration-200 hover:shadow-md
              `}
            >
              {/* Left accent bar */}
              <div className={`absolute left-0 top-0 bottom-0 w-1 ${config.accentColor}`} />

              <div className="p-4 pl-5">
                <div className="flex items-start justify-between gap-4">
                  <div className="flex-1 min-w-0">
                    {/* Action description */}
                    <p className="text-gray-900 font-medium mb-2 leading-relaxed">
                      {item.action}
                    </p>

                    {/* Page/Keyword details */}
                    {(item.page || item.keyword) && (
                      <div className="flex flex-wrap gap-2 mb-2">
                        {item.page && (
                          <span className="inline-flex items-center px-2 py-1 rounded text-xs font-mono bg-white border border-gray-200 text-gray-700">
                            <span className="text-gray-400 mr-1">📄</span>
                            {item.page.length > 50 ? `${item.page.substring(0, 50)}...` : item.page}
                          </span>
                        )}
                        {item.keyword && (
                          <span className="inline-flex items-center px-2 py-1 rounded text-xs font-mono bg-white border border-gray-200 text-gray-700">
                            <span className="text-gray-400 mr-1">🔍</span>
                            {item.keyword}
                          </span>
                        )}
                      </div>
                    )}

                    {/* Additional details */}
                    {item.details && (
                      <p className="text-sm text-gray-600 mb-2">
                        {item.details}
                      </p>
                    )}

                    {/* Dependencies */}
                    {item.dependencies && item.dependencies.length > 0 && (
                      <div className="mt-2 flex items-start gap-2">
                        <span className="text-xs text-amber-700 font-medium shrink-0 mt-0.5">
                          ⚠️ Depends on:
                        </span>
                        <div className="flex-1">
                          <ul className="text-xs text-amber-700 space-y-1">
                            {item.dependencies.map((dep, depIndex) => (
                              <li key={depIndex}>{dep}</li>
                            ))}
                          </ul>
                        </div>
                      </div>
                    )}
                  </div>

                  {/* Right column: Impact & Effort badges */}
                  <div className="flex flex-col items-end gap-2 shrink-0">
                    {/* Impact estimate */}
                    <div className="flex items-center gap-1.5 px-3 py-1.5 rounded-md bg-white border border-gray-200">
                      <TrendingUp className="h-4 w-4 text-emerald-600" />
                      <div className="text-right">
                        <div className="text-xs text-gray-500 leading-none mb-0.5">
                          Impact
                        </div>
                        <div className="text-sm font-semibold text-gray-900 leading-none">
                          +{item.impact.toLocaleString()}
                          <span className="text-xs text-gray-500 ml-0.5">
                            clicks/mo
                          </span>
                        </div>
                      </div>
                    </div>

                    {/* Effort badge */}
                    <div
                      className={`
                        flex items-center gap-1.5 px-3 py-1.5 rounded-md border
                        ${effortConfig[item.effort].color}
                      `}
                    >
                      <EffortIcon className="h-4 w-4" />
                      <span className="text-xs font-medium">
                        {effortConfig[item.effort].label}
                      </span>
                    </div>

                    {/* Priority score (if available) */}
                    {item.priority_score !== undefined && (
                      <div className="text-xs text-gray-500 mt-1">
                        Priority: {item.priority_score.toFixed(1)}
                      </div>
                    )}
                  </div>
                </div>
              </div>
            </div>
          );
        })}
      </div>

      {/* Summary footer */}
      <div className={`mt-4 p-3 rounded-lg border ${config.borderColor} bg-white`}>
        <div className="flex items-center justify-between text-sm">
          <span className="text-gray-600">
            Total estimated impact:
          </span>
          <span className="font-semibold text-gray-900">
            +{sortedItems.reduce((sum, item) => sum + item.impact, 0).toLocaleString()}{' '}
            <span className="text-gray-500 font-normal">clicks/month</span>
          </span>
        </div>
      </div>
    </div>
  );
}
