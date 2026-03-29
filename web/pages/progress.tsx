import { useState, useEffect } from 'react';
import Head from 'next/head';

interface BuildLog {
  id: number;
  run_date: string;
  task: string;
  status: 'success' | 'failure' | 'running' | 'pending';
  notes: string | null;
  created_at: string;
}

interface GroupedLogs {
  [date: string]: BuildLog[];
}

export default function ProgressPage() {
  const [logs, setLogs] = useState<BuildLog[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [groupByDate, setGroupByDate] = useState(true);

  useEffect(() => {
    fetchLogs();
  }, []);

  const fetchLogs = async () => {
    try {
      setLoading(true);
      setError(null);

      const response = await fetch('/api/build-logs');
      
      if (!response.ok) {
        throw new Error(`Failed to fetch logs: ${response.statusText}`);
      }

      const data = await response.json();
      setLogs(data.logs || []);
    } catch (err) {
      console.error('Error fetching build logs:', err);
      setError(err instanceof Error ? err.message : 'Unknown error occurred');
    } finally {
      setLoading(false);
    }
  };

  const groupLogsByDate = (logs: BuildLog[]): GroupedLogs => {
    return logs.reduce((acc, log) => {
      const date = log.run_date;
      if (!acc[date]) {
        acc[date] = [];
      }
      acc[date].push(log);
      return acc;
    }, {} as GroupedLogs);
  };

  const getStatusColor = (status: BuildLog['status']) => {
    switch (status) {
      case 'success':
        return 'text-green-600 bg-green-50';
      case 'failure':
        return 'text-red-600 bg-red-50';
      case 'running':
        return 'text-blue-600 bg-blue-50';
      case 'pending':
        return 'text-gray-600 bg-gray-50';
      default:
        return 'text-gray-600 bg-gray-50';
    }
  };

  const getStatusIcon = (status: BuildLog['status']) => {
    switch (status) {
      case 'success':
        return '✓';
      case 'failure':
        return '✗';
      case 'running':
        return '↻';
      case 'pending':
        return '○';
      default:
        return '?';
    }
  };

  const formatDate = (dateStr: string) => {
    const date = new Date(dateStr);
    return date.toLocaleDateString('en-US', { 
      weekday: 'short', 
      year: 'numeric', 
      month: 'short', 
      day: 'numeric' 
    });
  };

  const formatTime = (dateStr: string) => {
    const date = new Date(dateStr);
    return date.toLocaleTimeString('en-US', { 
      hour: '2-digit', 
      minute: '2-digit'
    });
  };

  const renderLogRow = (log: BuildLog) => (
    <tr key={log.id} className="border-b border-gray-200 hover:bg-gray-50">
      {!groupByDate && (
        <td className="px-4 py-3 text-sm text-gray-700">
          {formatDate(log.run_date)}
        </td>
      )}
      <td className="px-4 py-3 text-sm text-gray-700">{log.task}</td>
      <td className="px-4 py-3">
        <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${getStatusColor(log.status)}`}>
          <span className="mr-1">{getStatusIcon(log.status)}</span>
          {log.status}
        </span>
      </td>
      <td className="px-4 py-3 text-sm text-gray-600">
        {log.notes || '—'}
      </td>
      <td className="px-4 py-3 text-xs text-gray-500">
        {formatTime(log.created_at)}
      </td>
    </tr>
  );

  const renderGroupedView = () => {
    const grouped = groupLogsByDate(logs);
    const dates = Object.keys(grouped).sort().reverse();

    return (
      <div className="space-y-6">
        {dates.map(date => (
          <div key={date} className="border border-gray-200 rounded-lg overflow-hidden">
            <div className="bg-gray-100 px-4 py-2 border-b border-gray-200">
              <h3 className="text-sm font-semibold text-gray-800">
                {formatDate(date)}
              </h3>
            </div>
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50">
                <tr>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Task
                  </th>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Status
                  </th>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Notes
                  </th>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Time
                  </th>
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-gray-200">
                {grouped[date].map(renderLogRow)}
              </tbody>
            </table>
          </div>
        ))}
      </div>
    );
  };

  const renderFlatView = () => (
    <div className="border border-gray-200 rounded-lg overflow-hidden">
      <table className="min-w-full divide-y divide-gray-200">
        <thead className="bg-gray-50">
          <tr>
            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
              Date
            </th>
            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
              Task
            </th>
            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
              Status
            </th>
            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
              Notes
            </th>
            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
              Time
            </th>
          </tr>
        </thead>
        <tbody className="bg-white divide-y divide-gray-200">
          {logs.map(renderLogRow)}
        </tbody>
      </table>
    </div>
  );

  const getStatusSummary = () => {
    const summary = logs.reduce((acc, log) => {
      acc[log.status] = (acc[log.status] || 0) + 1;
      return acc;
    }, {} as Record<string, number>);

    return summary;
  };

  const statusSummary = getStatusSummary();

  return (
    <>
      <Head>
        <title>Build Progress Dashboard | Search Intelligence</title>
        <meta name="description" content="Nightly build progress tracker for Search Intelligence Report" />
      </Head>

      <div className="min-h-screen bg-gray-50">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
          {/* Header */}
          <div className="mb-8">
            <h1 className="text-3xl font-bold text-gray-900 mb-2">
              Build Progress Dashboard
            </h1>
            <p className="text-gray-600">
              Tracking nightly build tasks and their status
            </p>
          </div>

          {/* Summary Cards */}
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
            <div className="bg-white rounded-lg shadow p-4 border-l-4 border-gray-400">
              <div className="text-sm text-gray-600 mb-1">Total Runs</div>
              <div className="text-2xl font-bold text-gray-900">{logs.length}</div>
            </div>
            <div className="bg-white rounded-lg shadow p-4 border-l-4 border-green-500">
              <div className="text-sm text-gray-600 mb-1">Success</div>
              <div className="text-2xl font-bold text-green-600">{statusSummary.success || 0}</div>
            </div>
            <div className="bg-white rounded-lg shadow p-4 border-l-4 border-red-500">
              <div className="text-sm text-gray-600 mb-1">Failures</div>
              <div className="text-2xl font-bold text-red-600">{statusSummary.failure || 0}</div>
            </div>
            <div className="bg-white rounded-lg shadow p-4 border-l-4 border-blue-500">
              <div className="text-sm text-gray-600 mb-1">Running</div>
              <div className="text-2xl font-bold text-blue-600">{statusSummary.running || 0}</div>
            </div>
          </div>

          {/* Controls */}
          <div className="flex justify-between items-center mb-6">
            <div className="flex items-center space-x-4">
              <button
                onClick={fetchLogs}
                disabled={loading}
                className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
              >
                {loading ? 'Refreshing...' : 'Refresh'}
              </button>
              <button
                onClick={() => setGroupByDate(!groupByDate)}
                className="px-4 py-2 bg-gray-200 text-gray-700 rounded-lg hover:bg-gray-300 transition-colors"
              >
                {groupByDate ? 'Show Flat View' : 'Group by Date'}
              </button>
            </div>
            <div className="text-sm text-gray-500">
              Last updated: {new Date().toLocaleTimeString()}
            </div>
          </div>

          {/* Content */}
          {loading && logs.length === 0 ? (
            <div className="bg-white rounded-lg shadow p-8 text-center">
              <div className="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600 mb-4"></div>
              <p className="text-gray-600">Loading build logs...</p>
            </div>
          ) : error ? (
            <div className="bg-red-50 border border-red-200 rounded-lg p-6">
              <div className="flex items-start">
                <div className="flex-shrink-0">
                  <span className="text-red-600 text-xl">⚠</span>
                </div>
                <div className="ml-3">
                  <h3 className="text-sm font-medium text-red-800 mb-2">
                    Error loading logs
                  </h3>
                  <p className="text-sm text-red-700">{error}</p>
                  <button
                    onClick={fetchLogs}
                    className="mt-3 text-sm text-red-600 hover:text-red-800 underline"
                  >
                    Try again
                  </button>
                </div>
              </div>
            </div>
          ) : logs.length === 0 ? (
            <div className="bg-white rounded-lg shadow p-8 text-center">
              <p className="text-gray-600 mb-4">No build logs found</p>
              <p className="text-sm text-gray-500">
                Build logs will appear here after the first nightly run
              </p>
            </div>
          ) : (
            <div className="bg-white rounded-lg shadow">
              {groupByDate ? renderGroupedView() : renderFlatView()}
            </div>
          )}

          {/* Footer */}
          <div className="mt-8 text-center text-sm text-gray-500">
            <p>This dashboard updates automatically with each nightly build run.</p>
            <p className="mt-1">Built for Shane's morning check-in ritual ☕</p>
          </div>
        </div>
      </div>
    </>
  );
}
