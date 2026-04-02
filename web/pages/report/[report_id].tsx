import { useRouter } from 'next/router';
import { useState, useEffect } from 'react';
import Head from 'next/head';
import { Report, Module1TechnicalHealthData } from '../../types/report';
import Module1TechnicalHealth from '../../components/modules/Module1TechnicalHealth';

interface ReportPageProps {}

export default function ReportPage({}: ReportPageProps) {
  const router = useRouter();
  const { report_id } = router.query;
  const [report, setReport] = useState<Report | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [activeModule, setActiveModule] = useState<string>('module1');

  useEffect(() => {
    if (!report_id) return;

    const fetchReport = async () => {
      try {
        setLoading(true);
        setError(null);
        
        const response = await fetch(`/api/reports/${report_id}`);
        
        if (!response.ok) {
          if (response.status === 404) {
            throw new Error('Report not found');
          }
          throw new Error('Failed to load report');
        }
        
        const data = await response.json();
        setReport(data);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'An error occurred');
        console.error('Error fetching report:', err);
      } finally {
        setLoading(false);
      }
    };

    fetchReport();
  }, [report_id]);

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto"></div>
          <p className="mt-4 text-gray-600">Loading report...</p>
        </div>
      </div>
    );
  }

  if (error || !report) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center max-w-md mx-auto p-6">
          <div className="text-red-600 text-5xl mb-4">⚠️</div>
          <h1 className="text-2xl font-bold text-gray-900 mb-2">Error Loading Report</h1>
          <p className="text-gray-600 mb-6">{error || 'Report not found'}</p>
          <button
            onClick={() => router.push('/dashboard')}
            className="bg-blue-600 text-white px-6 py-2 rounded-lg hover:bg-blue-700 transition-colors"
          >
            Return to Dashboard
          </button>
        </div>
      </div>
    );
  }

  const modules = [
    { id: 'module1', name: 'Health & Trajectory', icon: '📊' },
    { id: 'module2', name: 'Page-Level Triage', icon: '🎯' },
    { id: 'module3', name: 'SERP Landscape', icon: '🔍' },
    { id: 'module4', name: 'Content Intelligence', icon: '📝' },
    { id: 'module5', name: 'The Gameplan', icon: '🎮' },
    { id: 'module6', name: 'Algorithm Impact', icon: '🤖' },
    { id: 'module7', name: 'Intent Migration', icon: '🎭' },
    { id: 'module8', name: 'Link Architecture', icon: '🕸️' },
    { id: 'module9', name: 'Topic Authority', icon: '👑' },
    { id: 'module10', name: 'Competitor Intelligence', icon: '🔬' },
    { id: 'module11', name: 'Predictive Modeling', icon: '🔮' },
    { id: 'module12', name: 'Executive Summary', icon: '📋' },
  ];

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'completed':
        return 'bg-green-100 text-green-800';
      case 'processing':
        return 'bg-yellow-100 text-yellow-800';
      case 'failed':
        return 'bg-red-100 text-red-800';
      default:
        return 'bg-gray-100 text-gray-800';
    }
  };

  const renderModuleContent = () => {
    if (!report.modules) {
      return (
        <div className="text-center py-12 text-gray-500">
          No module data available yet. The report is still processing.
        </div>
      );
    }

    switch (activeModule) {
      case 'module1':
        const module1Data = report.modules.module1 as Module1TechnicalHealthData | undefined;
        if (module1Data) {
          return <Module1TechnicalHealth data={module1Data} />;
        }
        return (
          <div className="text-center py-12 text-gray-500">
            Module 1 data not available yet.
          </div>
        );
      
      case 'module2':
        return (
          <div className="bg-white rounded-lg shadow-sm p-8">
            <h2 className="text-2xl font-bold text-gray-900 mb-4">Page-Level Triage</h2>
            <p className="text-gray-600">Coming soon...</p>
          </div>
        );
      
      case 'module3':
        return (
          <div className="bg-white rounded-lg shadow-sm p-8">
            <h2 className="text-2xl font-bold text-gray-900 mb-4">SERP Landscape Analysis</h2>
            <p className="text-gray-600">Coming soon...</p>
          </div>
        );
      
      case 'module4':
        return (
          <div className="bg-white rounded-lg shadow-sm p-8">
            <h2 className="text-2xl font-bold text-gray-900 mb-4">Content Intelligence</h2>
            <p className="text-gray-600">Coming soon...</p>
          </div>
        );
      
      case 'module5':
        return (
          <div className="bg-white rounded-lg shadow-sm p-8">
            <h2 className="text-2xl font-bold text-gray-900 mb-4">The Gameplan</h2>
            <p className="text-gray-600">Coming soon...</p>
          </div>
        );
      
      case 'module6':
        return (
          <div className="bg-white rounded-lg shadow-sm p-8">
            <h2 className="text-2xl font-bold text-gray-900 mb-4">Algorithm Update Impact</h2>
            <p className="text-gray-600">Coming soon...</p>
          </div>
        );
      
      case 'module7':
        return (
          <div className="bg-white rounded-lg shadow-sm p-8">
            <h2 className="text-2xl font-bold text-gray-900 mb-4">Query Intent Migration</h2>
            <p className="text-gray-600">Coming soon...</p>
          </div>
        );
      
      case 'module8':
        return (
          <div className="bg-white rounded-lg shadow-sm p-8">
            <h2 className="text-2xl font-bold text-gray-900 mb-4">Internal Link Architecture</h2>
            <p className="text-gray-600">Coming soon...</p>
          </div>
        );
      
      case 'module9':
        return (
          <div className="bg-white rounded-lg shadow-sm p-8">
            <h2 className="text-2xl font-bold text-gray-900 mb-4">Topic Authority Mapping</h2>
            <p className="text-gray-600">Coming soon...</p>
          </div>
        );
      
      case 'module10':
        return (
          <div className="bg-white rounded-lg shadow-sm p-8">
            <h2 className="text-2xl font-bold text-gray-900 mb-4">Competitor Intelligence</h2>
            <p className="text-gray-600">Coming soon...</p>
          </div>
        );
      
      case 'module11':
        return (
          <div className="bg-white rounded-lg shadow-sm p-8">
            <h2 className="text-2xl font-bold text-gray-900 mb-4">Predictive Modeling</h2>
            <p className="text-gray-600">Coming soon...</p>
          </div>
        );
      
      case 'module12':
        return (
          <div className="bg-white rounded-lg shadow-sm p-8">
            <h2 className="text-2xl font-bold text-gray-900 mb-4">Executive Summary</h2>
            <p className="text-gray-600">Coming soon...</p>
          </div>
        );
      
      default:
        return (
          <div className="text-center py-12 text-gray-500">
            Select a module to view details.
          </div>
        );
    }
  };

  return (
    <>
      <Head>
        <title>Search Intelligence Report - {report.domain}</title>
        <meta name="description" content={`Comprehensive search intelligence analysis for ${report.domain}`} />
      </Head>

      <div className="min-h-screen bg-gray-50">
        {/* Header */}
        <header className="bg-white border-b border-gray-200 sticky top-0 z-10">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-4">
            <div className="flex items-center justify-between">
              <div>
                <button
                  onClick={() => router.push('/dashboard')}
                  className="text-gray-600 hover:text-gray-900 mb-2 flex items-center text-sm"
                >
                  ← Back to Dashboard
                </button>
                <h1 className="text-2xl font-bold text-gray-900">{report.domain}</h1>
                <p className="text-sm text-gray-600 mt-1">
                  Report ID: {report.id} • Generated: {new Date(report.created_at).toLocaleDateString()}
                </p>
              </div>
              <div className="flex items-center space-x-4">
                <span className={`px-3 py-1 rounded-full text-sm font-medium ${getStatusColor(report.status)}`}>
                  {report.status.charAt(0).toUpperCase() + report.status.slice(1)}
                </span>
                <button
                  className="bg-blue-600 text-white px-4 py-2 rounded-lg hover:bg-blue-700 transition-colors text-sm font-medium"
                  onClick={() => {
                    window.print();
                  }}
                >
                  Export PDF
                </button>
              </div>
            </div>
          </div>
        </header>

        {/* Progress Bar */}
        {report.progress !== undefined && report.progress < 100 && (
          <div className="bg-white border-b border-gray-200">
            <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-3">
              <div className="flex items-center justify-between mb-2">
                <span className="text-sm font-medium text-gray-700">Processing Report</span>
                <span className="text-sm text-gray-600">{Math.round(report.progress)}%</span>
              </div>
              <div className="w-full bg-gray-200 rounded-full h-2">
                <div
                  className="bg-blue-600 h-2 rounded-full transition-all duration-500"
                  style={{ width: `${report.progress}%` }}
                ></div>
              </div>
            </div>
          </div>
        )}

        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
          <div className="grid grid-cols-12 gap-6">
            {/* Sidebar Navigation */}
            <div className="col-span-12 lg:col-span-3">
              <div className="bg-white rounded-lg shadow-sm p-4 sticky top-24">
                <h2 className="text-sm font-semibold text-gray-900 uppercase tracking-wide mb-4">
                  Analysis Modules
                </h2>
                <nav className="space-y-1">
                  {modules.map((module) => (
                    <button
                      key={module.id}
                      onClick={() => setActiveModule(module.id)}
                      className={`w-full text-left px-3 py-2 rounded-lg text-sm font-medium transition-colors ${
                        activeModule === module.id
                          ? 'bg-blue-50 text-blue-700'
                          : 'text-gray-700 hover:bg-gray-50'
                      }`}
                    >
                      <span className="mr-2">{module.icon}</span>
                      {module.name}
                    </button>
                  ))}
                </nav>
              </div>
            </div>

            {/* Main Content */}
            <div className="col-span-12 lg:col-span-9">
              {renderModuleContent()}
            </div>
          </div>
        </div>
      </div>
    </>
  );
}