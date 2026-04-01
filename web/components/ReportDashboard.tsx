import React, { useState } from 'react';
import { FileText, TrendingUp, Search, FileCheck, Target, AlertTriangle, Clock, BarChart3, Network, Globe, Zap, Award, CheckCircle, Circle, Menu, X } from 'lucide-react';
import Module1TrafficOverview from './Module1TrafficOverview';

interface ModuleDefinition {
  id: number;
  name: string;
  shortName: string;
  icon: React.ElementType;
  component?: React.ComponentType<any>;
  description: string;
  status: 'complete' | 'in-progress' | 'pending';
}

interface ReportDashboardProps {
  siteName: string;
  generatedAt: string;
  reportData?: any;
}

const ReportDashboard: React.FC<ReportDashboardProps> = ({
  siteName,
  generatedAt,
  reportData
}) => {
  const [activeModuleId, setActiveModuleId] = useState<number>(1);
  const [sidebarOpen, setSidebarOpen] = useState<boolean>(true);

  const modules: ModuleDefinition[] = [
    {
      id: 1,
      name: 'Traffic Health & Trajectory',
      shortName: 'Health Overview',
      icon: TrendingUp,
      component: Module1TrafficOverview,
      description: 'Traffic trends, seasonality, and forecasting',
      status: 'complete'
    },
    {
      id: 2,
      name: 'Query Performance Analysis',
      shortName: 'Query Analysis',
      icon: Search,
      description: 'Keyword-level performance and opportunities',
      status: 'pending'
    },
    {
      id: 3,
      name: 'Page-Level Triage',
      shortName: 'Page Triage',
      icon: FileText,
      description: 'Per-page decay analysis and prioritization',
      status: 'pending'
    },
    {
      id: 4,
      name: 'SERP Landscape Analysis',
      shortName: 'SERP Analysis',
      icon: Globe,
      description: 'Competitor mapping and SERP feature impact',
      status: 'pending'
    },
    {
      id: 5,
      name: 'Indexability & Technical Health',
      shortName: 'Technical Health',
      icon: FileCheck,
      description: 'Crawl errors, indexing issues, and site health',
      status: 'pending'
    },
    {
      id: 6,
      name: 'Content Intelligence',
      shortName: 'Content Intel',
      icon: BarChart3,
      description: 'Cannibalization, thin content, and content gaps',
      status: 'pending'
    },
    {
      id: 7,
      name: 'Algorithm Update Impact',
      shortName: 'Algorithm Impact',
      icon: AlertTriangle,
      description: 'Correlation with Google updates',
      status: 'pending'
    },
    {
      id: 8,
      name: 'Query Intent Migration',
      shortName: 'Intent Migration',
      icon: Target,
      description: 'Search intent shifts over time',
      status: 'pending'
    },
    {
      id: 9,
      name: 'Internal Link Architecture',
      shortName: 'Link Architecture',
      icon: Network,
      description: 'PageRank flow and link equity optimization',
      status: 'pending'
    },
    {
      id: 10,
      name: 'Seasonal & Temporal Patterns',
      shortName: 'Seasonality',
      icon: Clock,
      description: 'Time-based trends and content calendar',
      status: 'pending'
    },
    {
      id: 11,
      name: 'Engagement & Conversion Correlation',
      shortName: 'Engagement',
      icon: Zap,
      description: 'Search traffic vs. business outcomes',
      status: 'pending'
    },
    {
      id: 12,
      name: 'The Gameplan',
      shortName: 'Action Plan',
      icon: Award,
      description: 'Prioritized action items and roadmap',
      status: 'pending'
    }
  ];

  const activeModule = modules.find(m => m.id === activeModuleId);
  const ActiveComponent = activeModule?.component;

  const getStatusIcon = (status: ModuleDefinition['status']) => {
    switch (status) {
      case 'complete':
        return <CheckCircle className="w-4 h-4 text-green-500" />;
      case 'in-progress':
        return <Circle className="w-4 h-4 text-blue-500 animate-pulse" />;
      case 'pending':
        return <Circle className="w-4 h-4 text-gray-300" />;
    }
  };

  const completedCount = modules.filter(m => m.status === 'complete').length;
  const progressPercentage = (completedCount / modules.length) * 100;

  return (
    <div className="min-h-screen bg-gray-50 flex flex-col">
      {/* Header */}
      <header className="bg-white border-b border-gray-200 sticky top-0 z-30 shadow-sm">
        <div className="px-4 sm:px-6 lg:px-8 py-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center space-x-4">
              <button
                onClick={() => setSidebarOpen(!sidebarOpen)}
                className="lg:hidden p-2 rounded-md hover:bg-gray-100 transition-colors"
              >
                {sidebarOpen ? (
                  <X className="w-5 h-5 text-gray-600" />
                ) : (
                  <Menu className="w-5 h-5 text-gray-600" />
                )}
              </button>
              <div>
                <h1 className="text-2xl font-bold text-gray-900">
                  Search Intelligence Report
                </h1>
                <div className="flex items-center space-x-4 mt-1">
                  <p className="text-sm text-gray-600">
                    <span className="font-medium">{siteName}</span>
                  </p>
                  <span className="text-gray-300">•</span>
                  <p className="text-sm text-gray-500">
                    Generated {new Date(generatedAt).toLocaleDateString('en-US', {
                      year: 'numeric',
                      month: 'long',
                      day: 'numeric'
                    })}
                  </p>
                </div>
              </div>
            </div>
            <div className="hidden md:flex items-center space-x-4">
              <div className="text-right">
                <p className="text-xs text-gray-500 uppercase tracking-wide">
                  Progress
                </p>
                <p className="text-sm font-semibold text-gray-900">
                  {completedCount} / {modules.length} modules
                </p>
              </div>
              <div className="w-32 bg-gray-200 rounded-full h-2">
                <div
                  className="bg-blue-600 h-2 rounded-full transition-all duration-500"
                  style={{ width: `${progressPercentage}%` }}
                />
              </div>
            </div>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <div className="flex-1 flex overflow-hidden">
        {/* Sidebar */}
        <aside
          className={`
            ${sidebarOpen ? 'translate-x-0' : '-translate-x-full'}
            lg:translate-x-0
            fixed lg:static
            inset-y-0 left-0
            z-20
            w-80
            bg-white
            border-r border-gray-200
            transition-transform duration-300
            ease-in-out
            flex flex-col
            overflow-hidden
          `}
        >
          <div className="flex-1 overflow-y-auto py-6">
            <nav className="space-y-1 px-3">
              {modules.map((module) => {
                const Icon = module.icon;
                const isActive = module.id === activeModuleId;

                return (
                  <button
                    key={module.id}
                    onClick={() => {
                      setActiveModuleId(module.id);
                      if (window.innerWidth < 1024) {
                        setSidebarOpen(false);
                      }
                    }}
                    className={`
                      w-full flex items-start space-x-3 px-3 py-3 rounded-lg
                      transition-all duration-150
                      ${isActive
                        ? 'bg-blue-50 text-blue-700 shadow-sm'
                        : 'text-gray-700 hover:bg-gray-50'
                      }
                    `}
                  >
                    <div className="flex-shrink-0 mt-0.5">
                      {getStatusIcon(module.status)}
                    </div>
                    <div className="flex-shrink-0">
                      <Icon
                        className={`w-5 h-5 ${
                          isActive ? 'text-blue-600' : 'text-gray-400'
                        }`}
                      />
                    </div>
                    <div className="flex-1 text-left min-w-0">
                      <div className="flex items-center justify-between">
                        <p
                          className={`text-sm font-medium truncate ${
                            isActive ? 'text-blue-700' : 'text-gray-900'
                          }`}
                        >
                          {module.shortName}
                        </p>
                        <span className="text-xs text-gray-500 ml-2 flex-shrink-0">
                          {module.id}
                        </span>
                      </div>
                      <p className="text-xs text-gray-500 mt-0.5 line-clamp-2">
                        {module.description}
                      </p>
                    </div>
                  </button>
                );
              })}
            </nav>
          </div>

          {/* Sidebar Footer */}
          <div className="border-t border-gray-200 p-4 bg-gray-50">
            <div className="flex items-center justify-between mb-2">
              <span className="text-xs font-medium text-gray-700">
                Overall Progress
              </span>
              <span className="text-xs font-semibold text-gray-900">
                {Math.round(progressPercentage)}%
              </span>
            </div>
            <div className="w-full bg-gray-200 rounded-full h-2">
              <div
                className="bg-blue-600 h-2 rounded-full transition-all duration-500"
                style={{ width: `${progressPercentage}%` }}
              />
            </div>
            <p className="text-xs text-gray-500 mt-2">
              {completedCount} of {modules.length} modules complete
            </p>
          </div>
        </aside>

        {/* Main Content Area */}
        <main className="flex-1 overflow-y-auto">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
            {/* Module Header */}
            {activeModule && (
              <div className="mb-8">
                <div className="flex items-center space-x-3 mb-2">
                  {React.createElement(activeModule.icon, {
                    className: "w-8 h-8 text-blue-600"
                  })}
                  <h2 className="text-3xl font-bold text-gray-900">
                    {activeModule.name}
                  </h2>
                </div>
                <p className="text-gray-600 ml-11">
                  {activeModule.description}
                </p>
              </div>
            )}

            {/* Dynamic Module Component */}
            <div className="bg-white rounded-lg shadow-sm border border-gray-200">
              {ActiveComponent ? (
                <ActiveComponent data={reportData?.modules?.[activeModuleId]} />
              ) : (
                <div className="p-12 text-center">
                  <div className="inline-flex items-center justify-center w-16 h-16 rounded-full bg-gray-100 mb-4">
                    {activeModule && React.createElement(activeModule.icon, {
                      className: "w-8 h-8 text-gray-400"
                    })}
                  </div>
                  <h3 className="text-lg font-semibold text-gray-900 mb-2">
                    Module In Development
                  </h3>
                  <p className="text-gray-600 max-w-md mx-auto">
                    {activeModule?.name} is currently being built. This module will provide {activeModule?.description.toLowerCase()}.
                  </p>
                  <div className="mt-6 inline-flex items-center space-x-2 text-sm text-gray-500">
                    <Circle className="w-4 h-4" />
                    <span>Coming soon</span>
                  </div>
                </div>
              )}
            </div>

            {/* Navigation Footer */}
            <div className="mt-8 flex items-center justify-between">
              <button
                onClick={() => {
                  if (activeModuleId > 1) {
                    setActiveModuleId(activeModuleId - 1);
                    window.scrollTo({ top: 0, behavior: 'smooth' });
                  }
                }}
                disabled={activeModuleId === 1}
                className={`
                  inline-flex items-center space-x-2 px-4 py-2 rounded-lg
                  font-medium text-sm transition-all
                  ${activeModuleId === 1
                    ? 'bg-gray-100 text-gray-400 cursor-not-allowed'
                    : 'bg-white text-gray-700 hover:bg-gray-50 border border-gray-300 shadow-sm'
                  }
                `}
              >
                <span>← Previous Module</span>
              </button>

              <button
                onClick={() => {
                  if (activeModuleId < modules.length) {
                    setActiveModuleId(activeModuleId + 1);
                    window.scrollTo({ top: 0, behavior: 'smooth' });
                  }
                }}
                disabled={activeModuleId === modules.length}
                className={`
                  inline-flex items-center space-x-2 px-4 py-2 rounded-lg
                  font-medium text-sm transition-all
                  ${activeModuleId === modules.length
                    ? 'bg-gray-100 text-gray-400 cursor-not-allowed'
                    : 'bg-blue-600 text-white hover:bg-blue-700 shadow-sm'
                  }
                `}
              >
                <span>Next Module →</span>
              </button>
            </div>
          </div>
        </main>
      </div>

      {/* Mobile Overlay */}
      {sidebarOpen && (
        <div
          className="fixed inset-0 bg-black bg-opacity-50 z-10 lg:hidden"
          onClick={() => setSidebarOpen(false)}
        />
      )}
    </div>
  );
};

export default ReportDashboard;
