import { useState, useEffect } from 'react';
import { useRouter } from 'next/router';
import Head from 'next/head';

interface Property {
  id: string;
  name: string;
  type: 'gsc' | 'ga4';
}

interface ConnectionStatus {
  gsc: boolean;
  ga4: boolean;
}

export default function Home() {
  const router = useRouter();
  const [connectionStatus, setConnectionStatus] = useState<ConnectionStatus>({
    gsc: false,
    ga4: false,
  });
  const [gscProperties, setGscProperties] = useState<Property[]>([]);
  const [ga4Properties, setGa4Properties] = useState<Property[]>([]);
  const [selectedGscProperty, setSelectedGscProperty] = useState<string>('');
  const [selectedGa4Property, setSelectedGa4Property] = useState<string>('');
  const [isLoading, setIsLoading] = useState(true);
  const [isGenerating, setIsGenerating] = useState(false);
  const [error, setError] = useState<string>('');

  // Check auth status and load properties on mount
  useEffect(() => {
    checkAuthStatus();
  }, []);

  // Handle OAuth callback
  useEffect(() => {
    const urlParams = new URLSearchParams(window.location.search);
    const code = urlParams.get('code');
    const state = urlParams.get('state');
    const error = urlParams.get('error');

    if (error) {
      setError(`OAuth error: ${error}`);
      setIsLoading(false);
      return;
    }

    if (code && state) {
      handleOAuthCallback(code, state);
    }
  }, []);

  const checkAuthStatus = async () => {
    try {
      const response = await fetch('/api/auth/status', {
        credentials: 'include',
      });

      if (!response.ok) {
        throw new Error('Failed to check auth status');
      }

      const data = await response.json();
      setConnectionStatus({
        gsc: data.gsc_connected,
        ga4: data.ga4_connected,
      });

      if (data.gsc_connected) {
        await loadGscProperties();
      }
      if (data.ga4_connected) {
        await loadGa4Properties();
      }
    } catch (err) {
      console.error('Error checking auth status:', err);
      setError('Failed to check connection status');
    } finally {
      setIsLoading(false);
    }
  };

  const handleOAuthCallback = async (code: string, state: string) => {
    try {
      setIsLoading(true);
      const response = await fetch('/api/auth/callback', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        credentials: 'include',
        body: JSON.stringify({ code, state }),
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail || 'OAuth callback failed');
      }

      // Clean up URL
      window.history.replaceState({}, document.title, '/');

      // Refresh auth status
      await checkAuthStatus();
    } catch (err) {
      console.error('OAuth callback error:', err);
      setError(err instanceof Error ? err.message : 'OAuth authentication failed');
      setIsLoading(false);
    }
  };

  const loadGscProperties = async () => {
    try {
      const response = await fetch('/api/properties/gsc', {
        credentials: 'include',
      });

      if (!response.ok) {
        throw new Error('Failed to load GSC properties');
      }

      const data = await response.json();
      setGscProperties(
        data.properties.map((prop: any) => ({
          id: prop.siteUrl,
          name: prop.siteUrl,
          type: 'gsc' as const,
        }))
      );
    } catch (err) {
      console.error('Error loading GSC properties:', err);
      setError('Failed to load Search Console properties');
    }
  };

  const loadGa4Properties = async () => {
    try {
      const response = await fetch('/api/properties/ga4', {
        credentials: 'include',
      });

      if (!response.ok) {
        throw new Error('Failed to load GA4 properties');
      }

      const data = await response.json();
      setGa4Properties(
        data.properties.map((prop: any) => ({
          id: prop.property,
          name: prop.displayName,
          type: 'ga4' as const,
        }))
      );
    } catch (err) {
      console.error('Error loading GA4 properties:', err);
      setError('Failed to load GA4 properties');
    }
  };

  const initiateOAuth = async (provider: 'gsc' | 'ga4') => {
    try {
      setError('');
      const response = await fetch(`/api/auth/authorize/${provider}`, {
        credentials: 'include',
      });

      if (!response.ok) {
        throw new Error(`Failed to initiate ${provider.toUpperCase()} OAuth`);
      }

      const data = await response.json();
      window.location.href = data.auth_url;
    } catch (err) {
      console.error('OAuth initiation error:', err);
      setError(err instanceof Error ? err.message : 'Failed to start OAuth flow');
    }
  };

  const disconnectProvider = async (provider: 'gsc' | 'ga4') => {
    try {
      setError('');
      const response = await fetch(`/api/auth/disconnect/${provider}`, {
        method: 'POST',
        credentials: 'include',
      });

      if (!response.ok) {
        throw new Error(`Failed to disconnect ${provider.toUpperCase()}`);
      }

      setConnectionStatus({
        ...connectionStatus,
        [provider]: false,
      });

      if (provider === 'gsc') {
        setGscProperties([]);
        setSelectedGscProperty('');
      } else {
        setGa4Properties([]);
        setSelectedGa4Property('');
      }
    } catch (err) {
      console.error('Disconnect error:', err);
      setError(err instanceof Error ? err.message : 'Failed to disconnect');
    }
  };

  const generateReport = async () => {
    if (!selectedGscProperty) {
      setError('Please select a Search Console property');
      return;
    }

    try {
      setError('');
      setIsGenerating(true);

      const response = await fetch('/api/reports/generate', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        credentials: 'include',
        body: JSON.stringify({
          gsc_property: selectedGscProperty,
          ga4_property: selectedGa4Property || null,
        }),
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail || 'Failed to generate report');
      }

      const data = await response.json();
      
      // Redirect to report page
      router.push(`/report/${data.report_id}`);
    } catch (err) {
      console.error('Report generation error:', err);
      setError(err instanceof Error ? err.message : 'Failed to generate report');
      setIsGenerating(false);
    }
  };

  if (isLoading) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900 flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-500 mx-auto"></div>
          <p className="mt-4 text-slate-300">Loading...</p>
        </div>
      </div>
    );
  }

  return (
    <>
      <Head>
        <title>Search Intelligence Report - Free SEO Analysis Tool</title>
        <meta name="description" content="Generate a comprehensive Search Intelligence Report combining GSC, GA4, and SERP data with advanced statistical analysis" />
      </Head>

      <div className="min-h-screen bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900">
        {/* Hero Section */}
        <div className="container mx-auto px-4 py-16">
          <div className="max-w-4xl mx-auto text-center mb-16">
            <h1 className="text-5xl font-bold text-white mb-6">
              Search Intelligence Report
            </h1>
            <p className="text-xl text-slate-300 mb-4">
              Connect your Google Search Console and GA4 to generate a comprehensive analysis report
            </p>
            <p className="text-lg text-slate-400">
              12 integrated analysis modules • Statistical modeling • Predictive forecasting • Prioritized action plan
            </p>
          </div>

          {/* Error Display */}
          {error && (
            <div className="max-w-2xl mx-auto mb-8 bg-red-900/30 border border-red-500 rounded-lg p-4">
              <p className="text-red-300">{error}</p>
            </div>
          )}

          {/* Connection Cards */}
          <div className="max-w-4xl mx-auto grid md:grid-cols-2 gap-6 mb-12">
            {/* GSC Connection */}
            <div className="bg-slate-800/50 backdrop-blur rounded-xl p-6 border border-slate-700">
              <div className="flex items-start justify-between mb-4">
                <div>
                  <h3 className="text-xl font-semibold text-white mb-2">
                    Google Search Console
                  </h3>
                  <p className="text-sm text-slate-400">
                    Required for performance data
                  </p>
                </div>
                <div className={`px-3 py-1 rounded-full text-xs font-medium ${
                  connectionStatus.gsc
                    ? 'bg-green-500/20 text-green-300'
                    : 'bg-slate-700 text-slate-400'
                }`}>
                  {connectionStatus.gsc ? 'Connected' : 'Not connected'}
                </div>
              </div>

              {!connectionStatus.gsc ? (
                <button
                  onClick={() => initiateOAuth('gsc')}
                  className="w-full bg-blue-600 hover:bg-blue-700 text-white font-medium py-3 px-4 rounded-lg transition-colors"
                >
                  Connect Search Console
                </button>
              ) : (
                <div className="space-y-4">
                  <select
                    value={selectedGscProperty}
                    onChange={(e) => setSelectedGscProperty(e.target.value)}
                    className="w-full bg-slate-700 text-white border border-slate-600 rounded-lg py-2 px-3 focus:outline-none focus:ring-2 focus:ring-blue-500"
                  >
                    <option value="">Select a property...</option>
                    {gscProperties.map((prop) => (
                      <option key={prop.id} value={prop.id}>
                        {prop.name}
                      </option>
                    ))}
                  </select>
                  <button
                    onClick={() => disconnectProvider('gsc')}
                    className="w-full bg-slate-700 hover:bg-slate-600 text-slate-300 font-medium py-2 px-4 rounded-lg transition-colors text-sm"
                  >
                    Disconnect
                  </button>
                </div>
              )}
            </div>

            {/* GA4 Connection */}
            <div className="bg-slate-800/50 backdrop-blur rounded-xl p-6 border border-slate-700">
              <div className="flex items-start justify-between mb-4">
                <div>
                  <h3 className="text-xl font-semibold text-white mb-2">
                    Google Analytics 4
                  </h3>
                  <p className="text-sm text-slate-400">
                    Optional for engagement data
                  </p>
                </div>
                <div className={`px-3 py-1 rounded-full text-xs font-medium ${
                  connectionStatus.ga4
                    ? 'bg-green-500/20 text-green-300'
                    : 'bg-slate-700 text-slate-400'
                }`}>
                  {connectionStatus.ga4 ? 'Connected' : 'Not connected'}
                </div>
              </div>

              {!connectionStatus.ga4 ? (
                <button
                  onClick={() => initiateOAuth('ga4')}
                  className="w-full bg-blue-600 hover:bg-blue-700 text-white font-medium py-3 px-4 rounded-lg transition-colors"
                >
                  Connect GA4
                </button>
              ) : (
                <div className="space-y-4">
                  <select
                    value={selectedGa4Property}
                    onChange={(e) => setSelectedGa4Property(e.target.value)}
                    className="w-full bg-slate-700 text-white border border-slate-600 rounded-lg py-2 px-3 focus:outline-none focus:ring-2 focus:ring-blue-500"
                  >
                    <option value="">Select a property...</option>
                    {ga4Properties.map((prop) => (
                      <option key={prop.id} value={prop.id}>
                        {prop.name}
                      </option>
                    ))}
                  </select>
                  <button
                    onClick={() => disconnectProvider('ga4')}
                    className="w-full bg-slate-700 hover:bg-slate-600 text-slate-300 font-medium py-2 px-4 rounded-lg transition-colors text-sm"
                  >
                    Disconnect
                  </button>
                </div>
              )}
            </div>
          </div>

          {/* Generate Report Button */}
          <div className="max-w-2xl mx-auto">
            <button
              onClick={generateReport}
              disabled={!connectionStatus.gsc || !selectedGscProperty || isGenerating}
              className={`w-full py-4 px-6 rounded-xl font-semibold text-lg transition-all ${
                !connectionStatus.gsc || !selectedGscProperty || isGenerating
                  ? 'bg-slate-700 text-slate-500 cursor-not-allowed'
                  : 'bg-gradient-to-r from-blue-600 to-blue-700 hover:from-blue-700 hover:to-blue-800 text-white shadow-lg shadow-blue-500/50'
              }`}
            >
              {isGenerating ? (
                <span className="flex items-center justify-center">
                  <svg className="animate-spin -ml-1 mr-3 h-5 w-5 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                  </svg>
                  Generating Report...
                </span>
              ) : (
                'Generate Report'
              )}
            </button>
            
            {!connectionStatus.gsc && (
              <p className="text-center text-slate-400 mt-4 text-sm">
                Connect Search Console to get started
              </p>
            )}
            {connectionStatus.gsc && !selectedGscProperty && (
              <p className="text-center text-slate-400 mt-4 text-sm">
                Select a property to continue
              </p>
            )}
          </div>

          {/* Features Preview */}
          <div className="max-w-4xl mx-auto mt-20">
            <h2 className="text-3xl font-bold text-white text-center mb-12">
              What's in Your Report
            </h2>
            <div className="grid md:grid-cols-3 gap-6">
              {[
                {
                  title: 'Health & Trajectory',
                  description: 'MSTL decomposition, change point detection, 90-day forecast with confidence intervals',
                },
                {
                  title: 'Page Triage',
                  description: 'Trend analysis per page, CTR anomaly detection, priority scoring',
                },
                {
                  title: 'SERP Landscape',
                  description: 'Feature displacement analysis, competitor mapping, click share estimation',
                },
                {
                  title: 'Content Intelligence',
                  description: 'Cannibalization detection, striking distance opportunities, thin content flagging',
                },
                {
                  title: 'The Gameplan',
                  description: 'Prioritized action list with traffic impact estimates and effort levels',
                },
                {
                  title: 'Algorithm Impact',
                  description: 'Update correlation analysis, vulnerability assessment, recovery patterns',
                },
                {
                  title: 'Intent Migration',
                  description: 'Query intent tracking over time, AI Overview impact estimation',
                },
                {
                  title: 'CTR Modeling',
                  description: 'Context-aware CTR predictions, SERP feature opportunity scoring',
                },
                {
                  title: 'Site Architecture',
                  description: 'PageRank analysis, authority flow mapping, optimal link recommendations',
                },
                {
                  title: 'Branded Split',
                  description: 'Independent trend analysis, dependency risk scoring, growth projections',
                },
                {
                  title: 'Competitive Radar',
                  description: 'Emerging threat detection, competitor content velocity, vulnerability assessment',
                },
                {
                  title: 'Revenue Attribution',
                  description: 'Position-to-revenue modeling, ROI estimates for recommended actions',
                },
              ].map((feature, index) => (
                <div
                  key={index}
                  className="bg-slate-800/30 backdrop-blur rounded-lg p-6 border border-slate-700/50"
                >
                  <h3 className="text-lg font-semibold text-white mb-2">
                    {feature.title}
                  </h3>
                  <p className="text-sm text-slate-400">
                    {feature.description}
                  </p>
                </div>
              ))}
            </div>
          </div>

          {/* Footer */}
          <div className="max-w-4xl mx-auto mt-20 text-center text-slate-400 text-sm">
            <p>
              Report generation takes 2-5 minutes • All data processed securely • OAuth read-only access
            </p>
          </div>
        </div>
      </div>
    </>
  );
}