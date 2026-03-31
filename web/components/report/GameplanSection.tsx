import React from 'react';
import { ChevronDown, ChevronUp, TrendingUp, Zap, Target, Wrench, ExternalLink, Clock, DollarSign } from 'lucide-react';
import type { GameplanData } from '@/types/report';

interface GameplanSectionProps {
  data: GameplanData;
}

type TabType = 'critical' | 'quick_wins' | 'strategic' | 'structural';

const EFFORT_COLORS = {
  low: 'bg-green-100 text-green-800 border-green-200',
  medium: 'bg-yellow-100 text-yellow-800 border-yellow-200',
  high: 'bg-orange-100 text-orange-800 border-orange-200'
};

const EFFORT_LABELS = {
  low: 'Low effort',
  medium: 'Medium effort',
  high: 'High effort'
};

const TAB_CONFIG = {
  critical: {
    label: 'Critical Fixes',
    icon: Zap,
    color: 'text-red-600',
    bgColor: 'bg-red-50',
    borderColor: 'border-red-200',
    description: 'Do this week — immediate threats to current traffic'
  },
  quick_wins: {
    label: 'Quick Wins',
    icon: TrendingUp,
    color: 'text-blue-600',
    bgColor: 'bg-blue-50',
    borderColor: 'border-blue-200',
    description: 'Do this month — fast ROI with minimal effort'
  },
  strategic: {
    label: 'Strategic Plays',
    icon: Target,
    color: 'text-purple-600',
    bgColor: 'bg-purple-50',
    borderColor: 'border-purple-200',
    description: 'This quarter — high-impact content and optimization projects'
  },
  structural: {
    label: 'Structural Improvements',
    icon: Wrench,
    color: 'text-gray-600',
    bgColor: 'bg-gray-50',
    borderColor: 'border-gray-200',
    description: 'Ongoing — foundational changes for long-term growth'
  }
};

interface ActionItemProps {
  action: any;
  index: number;
  tabType: TabType;
}

const ActionItem: React.FC<ActionItemProps> = ({ action, index, tabType }) => {
  const [isExpanded, setIsExpanded] = React.useState(false);
  const effortColor = EFFORT_COLORS[action.effort as keyof typeof EFFORT_COLORS] || EFFORT_COLORS.medium;

  return (
    <div className="border border-gray-200 rounded-lg bg-white hover:shadow-md transition-shadow">
      {/* Header - Always visible */}
      <button
        onClick={() => setIsExpanded(!isExpanded)}
        className="w-full px-4 py-4 flex items-start justify-between text-left hover:bg-gray-50 transition-colors rounded-lg"
      >
        <div className="flex-1 min-w-0 pr-4">
          <div className="flex items-start gap-3">
            <span className="flex-shrink-0 w-6 h-6 rounded-full bg-gray-100 text-gray-700 text-sm font-medium flex items-center justify-center mt-0.5">
              {index + 1}
            </span>
            <div className="flex-1 min-w-0">
              <h4 className="font-medium text-gray-900 mb-1 leading-snug">
                {action.action}
              </h4>
              <div className="flex flex-wrap items-center gap-2 text-sm">
                {action.impact !== undefined && action.impact !== null && (
                  <div className="flex items-center gap-1 text-green-700">
                    <DollarSign className="w-3.5 h-3.5" />
                    <span className="font-medium">
                      {typeof action.impact === 'number' 
                        ? `+${action.impact.toLocaleString()} clicks/mo`
                        : action.impact}
                    </span>
                  </div>
                )}
                <span className={`px-2 py-0.5 rounded-full text-xs font-medium border ${effortColor}`}>
                  {EFFORT_LABELS[action.effort as keyof typeof EFFORT_LABELS] || action.effort}
                </span>
                {action.page && (
                  <span className="text-gray-500 text-xs truncate max-w-xs" title={action.page}>
                    {action.page}
                  </span>
                )}
              </div>
            </div>
          </div>
        </div>
        <div className="flex-shrink-0 ml-2">
          {isExpanded ? (
            <ChevronUp className="w-5 h-5 text-gray-400" />
          ) : (
            <ChevronDown className="w-5 h-5 text-gray-400" />
          )}
        </div>
      </button>

      {/* Expanded Details */}
      {isExpanded && (
        <div className="px-4 pb-4 pt-2 border-t border-gray-100 space-y-3">
          {action.details && (
            <div className="text-sm text-gray-700 leading-relaxed">
              {action.details}
            </div>
          )}
          
          {action.affected_items && action.affected_items.length > 0 && (
            <div>
              <div className="text-xs font-medium text-gray-500 uppercase tracking-wide mb-2">
                Affected {tabType === 'critical' ? 'Pages' : 'Items'}
              </div>
              <div className="space-y-1">
                {action.affected_items.map((item: string, idx: number) => (
                  <div key={idx} className="text-sm text-gray-600 flex items-start gap-2">
                    <span className="text-gray-400 flex-shrink-0">•</span>
                    <span className="break-all">{item}</span>
                  </div>
                ))}
              </div>
            </div>
          )}

          {action.keywords && action.keywords.length > 0 && (
            <div>
              <div className="text-xs font-medium text-gray-500 uppercase tracking-wide mb-2">
                Target Keywords
              </div>
              <div className="flex flex-wrap gap-1.5">
                {action.keywords.map((keyword: string, idx: number) => (
                  <span key={idx} className="px-2 py-1 bg-gray-100 text-gray-700 text-xs rounded">
                    {keyword}
                  </span>
                ))}
              </div>
            </div>
          )}

          {action.dependencies && action.dependencies.length > 0 && (
            <div className="pt-2 border-t border-gray-100">
              <div className="text-xs font-medium text-orange-600 uppercase tracking-wide mb-2 flex items-center gap-1">
                <Clock className="w-3.5 h-3.5" />
                Dependencies
              </div>
              <div className="text-sm text-gray-600 space-y-1">
                {action.dependencies.map((dep: string, idx: number) => (
                  <div key={idx} className="flex items-start gap-2">
                    <span className="text-orange-400 flex-shrink-0">→</span>
                    <span>{dep}</span>
                  </div>
                ))}
              </div>
            </div>
          )}

          {action.instructions && (
            <div className="pt-2 border-t border-gray-100">
              <div className="text-xs font-medium text-gray-500 uppercase tracking-wide mb-2">
                How to Execute
              </div>
              <div className="text-sm text-gray-700 leading-relaxed whitespace-pre-line">
                {action.instructions}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
};

export default function GameplanSection({ data }: GameplanSectionProps) {
  const [activeTab, setActiveTab] = React.useState<TabType>('critical');
  const [expandedCard, setExpandedCard] = React.useState(true);

  if (!data) {
    return (
      <div className="bg-white rounded-lg border border-gray-200 p-8">
        <div className="text-center text-gray-500">
          <Wrench className="w-12 h-12 mx-auto mb-3 text-gray-300" />
          <p>No gameplan data available</p>
        </div>
      </div>
    );
  }

  const currentActions = data[activeTab] || [];
  const hasActions = Object.values(data).some(actions => 
    Array.isArray(actions) && actions.length > 0
  );

  return (
    <div className="space-y-6">
      {/* Main Card */}
      <div className="bg-white rounded-lg border border-gray-200 shadow-sm">
        {/* Card Header */}
        <button
          onClick={() => setExpandedCard(!expandedCard)}
          className="w-full px-6 py-5 flex items-center justify-between hover:bg-gray-50 transition-colors rounded-t-lg"
        >
          <div className="flex items-center gap-3">
            <div className="p-2 bg-purple-100 rounded-lg">
              <Target className="w-6 h-6 text-purple-600" />
            </div>
            <div className="text-left">
              <h2 className="text-xl font-semibold text-gray-900">
                Your Prioritized Action Plan
              </h2>
              <p className="text-sm text-gray-600 mt-0.5">
                {data.total_estimated_monthly_click_recovery || 0} recoverable clicks + {data.total_estimated_monthly_click_growth || 0} growth opportunity
              </p>
            </div>
          </div>
          {expandedCard ? (
            <ChevronUp className="w-5 h-5 text-gray-400 flex-shrink-0" />
          ) : (
            <ChevronDown className="w-5 h-5 text-gray-400 flex-shrink-0" />
          )}
        </button>

        {/* Card Content */}
        {expandedCard && (
          <div className="px-6 pb-6">
            {/* Narrative Summary */}
            {data.narrative && (
              <div className="mb-6 p-4 bg-gray-50 rounded-lg border border-gray-200">
                <p className="text-sm text-gray-700 leading-relaxed whitespace-pre-line">
                  {data.narrative}
                </p>
              </div>
            )}

            {/* Impact Summary Cards */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-6">
              <div className="p-4 bg-gradient-to-br from-green-50 to-emerald-50 rounded-lg border border-green-200">
                <div className="text-sm font-medium text-green-900 mb-1">
                  Monthly Click Recovery Potential
                </div>
                <div className="text-2xl font-bold text-green-700">
                  {(data.total_estimated_monthly_click_recovery || 0).toLocaleString()}
                </div>
                <div className="text-xs text-green-600 mt-1">
                  From fixing current issues
                </div>
              </div>
              <div className="p-4 bg-gradient-to-br from-blue-50 to-indigo-50 rounded-lg border border-blue-200">
                <div className="text-sm font-medium text-blue-900 mb-1">
                  Monthly Click Growth Potential
                </div>
                <div className="text-2xl font-bold text-blue-700">
                  {(data.total_estimated_monthly_click_growth || 0).toLocaleString()}
                </div>
                <div className="text-xs text-blue-600 mt-1">
                  From new opportunities
                </div>
              </div>
            </div>

            {/* Tab Navigation */}
            <div className="border-b border-gray-200 mb-6">
              <div className="flex flex-wrap gap-2 -mb-px">
                {(Object.keys(TAB_CONFIG) as TabType[]).map((tabKey) => {
                  const tab = TAB_CONFIG[tabKey];
                  const Icon = tab.icon;
                  const count = (data[tabKey] || []).length;
                  const isActive = activeTab === tabKey;

                  return (
                    <button
                      key={tabKey}
                      onClick={() => setActiveTab(tabKey)}
                      className={`
                        flex items-center gap-2 px-4 py-3 border-b-2 font-medium text-sm transition-colors
                        ${isActive 
                          ? `${tab.color} border-current` 
                          : 'text-gray-500 border-transparent hover:text-gray-700 hover:border-gray-300'
                        }
                      `}
                    >
                      <Icon className="w-4 h-4" />
                      <span>{tab.label}</span>
                      {count > 0 && (
                        <span className={`
                          px-2 py-0.5 rounded-full text-xs font-semibold
                          ${isActive ? tab.bgColor : 'bg-gray-100 text-gray-600'}
                        `}>
                          {count}
                        </span>
                      )}
                    </button>
                  );
                })}
              </div>
            </div>

            {/* Active Tab Content */}
            <div>
              {/* Tab Description */}
              <div className={`
                mb-4 p-3 rounded-lg border
                ${TAB_CONFIG[activeTab].bgColor} ${TAB_CONFIG[activeTab].borderColor}
              `}>
                <p className="text-sm text-gray-700">
                  {TAB_CONFIG[activeTab].description}
                </p>
              </div>

              {/* Actions List */}
              {currentActions.length > 0 ? (
                <div className="space-y-3">
                  {currentActions.map((action, index) => (
                    <ActionItem
                      key={index}
                      action={action}
                      index={index}
                      tabType={activeTab}
                    />
                  ))}
                </div>
              ) : (
                <div className="text-center py-12 text-gray-500">
                  <div className={`
                    w-16 h-16 mx-auto mb-4 rounded-full flex items-center justify-center
                    ${TAB_CONFIG[activeTab].bgColor}
                  `}>
                    {React.createElement(TAB_CONFIG[activeTab].icon, {
                      className: `w-8 h-8 ${TAB_CONFIG[activeTab].color}`
                    })}
                  </div>
                  <p className="font-medium text-gray-700 mb-1">
                    No {TAB_CONFIG[activeTab].label.toLowerCase()} identified
                  </p>
                  <p className="text-sm">
                    {activeTab === 'critical' && "Great! No critical issues requiring immediate attention."}
                    {activeTab === 'quick_wins' && "Check back after implementing critical fixes."}
                    {activeTab === 'strategic' && "Focus on critical fixes and quick wins first."}
                    {activeTab === 'structural' && "Your current structure is performing well."}
                  </p>
                </div>
              )}
            </div>
          </div>
        )}
      </div>

      {/* Consulting CTA */}
      {hasActions && (
        <div className="bg-gradient-to-br from-purple-50 to-indigo-50 rounded-lg border border-purple-200 p-8 text-center shadow-sm">
          <div className="max-w-2xl mx-auto">
            <h3 className="text-2xl font-bold text-gray-900 mb-3">
              Want Help Executing This Plan?
            </h3>
            <p className="text-gray-700 mb-6 leading-relaxed">
              These opportunities represent{' '}
              <span className="font-semibold text-purple-700">
                {((data.total_estimated_monthly_click_recovery || 0) + 
                  (data.total_estimated_monthly_click_growth || 0)).toLocaleString()} clicks per month
              </span>
              {' '}in potential traffic. Let's capture them together.
            </p>
            <div className="flex flex-col sm:flex-row gap-3 justify-center items-center">
              <a
                href="/book-consultation"
                className="inline-flex items-center gap-2 px-6 py-3 bg-purple-600 text-white font-medium rounded-lg hover:bg-purple-700 transition-colors shadow-md hover:shadow-lg"
              >
                Book a Strategy Call
                <ExternalLink className="w-4 h-4" />
              </a>
              <a
                href="/services"
                className="inline-flex items-center gap-2 px-6 py-3 bg-white text-purple-600 font-medium rounded-lg border border-purple-200 hover:bg-purple-50 transition-colors"
              >
                View Services
              </a>
            </div>
            <p className="text-xs text-gray-500 mt-4">
              30-minute consultation • No obligation • Custom implementation roadmap
            </p>
          </div>
        </div>
      )}
    </div>
  );
}
