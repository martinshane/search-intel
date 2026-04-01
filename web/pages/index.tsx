import React, { useState, useEffect } from 'react';
import { useRouter } from 'next/router';
import Head from 'next/head';
import Link from 'next/link';

const API_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

interface Property {
  id: string;
  name: string;
  type: 'gsc' | 'ga4';
}

interface User {
  email: string;
  gsc_connected: boolean;
  ga4_connected: boolean;
  gsc_properties: Property[];
  ga4_properties: Property[];
}

export default function HomePage() {
  const router = useRouter();
  const [user, setUser] = useState<User | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedGscProperty, setSelectedGscProperty] = useState<string>('');
  const [selectedGa4Property, setSelectedGa4Property] = useState<string>('');
  const [generatingReport, setGeneratingReport] = useState(false);

  useEffect(() => {
    checkAuthStatus();
  }, []);

  const checkAuthStatus = async () => {
    try {
      const response = await fetch(`${API_URL}/api/auth/status`, {
        credentials: 'include',
      });
      
      if (response.ok) {
        const data = await response.json();
        setUser(data);
        
        // Auto-select first property if only one exists
        if (data.gsc_properties?.length === 1) {
          setSelectedGscProperty(data.gsc_properties[0].id);
        }
        if (data.ga4_properties?.length === 1) {
          setSelectedGa4Property(data.ga4_properties[0].id);
        }
      }
    } catch (err) {
      console.error('Auth check failed:', err);
    } finally {
      setLoading(false);
    }
  };

  const handleGscConnect = async () => {
    try {
      const response = await fetch(`${API_URL}/api/auth/gsc/authorize`, {
        credentials: 'include',
      });
      const data = await response.json();
      window.location.href = data.authorization_url;
    } catch (err) {
      setError('Failed to initiate GSC connection');
    }
  };

  const handleGa4Connect = async () => {
    try {
      const response = await fetch(`${API_URL}/api/auth/ga4/authorize`, {
        credentials: 'include',
      });
      const data = await response.json();
      window.location.href = data.authorization_url;
    } catch (err) {
      setError('Failed to initiate GA4 connection');
    }
  };

  const handleGenerateReport = async () => {
    if (!selectedGscProperty) {
      setError('Please select a Google Search Console property');
      return;
    }

    setGeneratingReport(true);
    setError(null);

    try {
      const response = await fetch(`${API_URL}/api/reports/generate`, {
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
      router.push(`/reports/${data.report_id}`);
    } catch (err: any) {
      setError(err.message || 'Failed to generate report');
      setGeneratingReport(false);
    }
  };

  if (loading) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900 flex items-center justify-center">
        <div className="text-white text-lg">Loading...</div>
      </div>
    );
  }

  return (
    <>
      <Head>
        <title>Search Intelligence Report — Free SEO Analysis Tool</title>
        <meta name="description" content="Generate a comprehensive SEO intelligence report for your website. Connect Google Search Console and GA4 for deep insights and actionable recommendations." />
        <meta name="viewport" content="width=device-width, initial-scale=1.0" />
      </Head>

      <div className="min-h-screen bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900">
        {/* Header */}
        <header className="border-b border-slate-700 bg-slate-900/50 backdrop-blur-sm">
          <div className="max-w-6xl mx-auto px-4 sm:px-6 lg:px-8 py-4 sm:py-6">
            <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
              <h1 className="text-xl sm:text-2xl font-bold text-white">
                Search Intelligence Report
              </h1>
              {user && (
                <div className="flex items-center gap-3 text-sm sm:text-base text-slate-300">
                  <span className="truncate max-w-[200px]">{user.email}</span>
                  <Link 
                    href="/dashboard"
                    className="px-3 py-1.5 sm:px-4 sm:py-2 bg-slate-700 hover:bg-slate-600 rounded-lg transition-colors whitespace-nowrap"
                  >
                    Dashboard
                  </Link>
                </div>
              )}
            </div>
          </div>
        </header>

        {/* Main Content */}
        <main className="max-w-6xl mx-auto px-4 sm:px-6 lg:px-8 py-8 sm:py-12 lg:py-16">
          {!user ? (
            /* Landing / Hero Section */
            <div className="text-center mb-12 sm:mb-16">
              <h2 className="text-3xl sm:text-4xl lg:text-5xl font-bold text-white mb-4 sm:mb-6 leading-tight px-2">
                Unlock Deep SEO Intelligence
                <br className="hidden sm:block" />
                <span className="bg-gradient-to-r from-blue-400 to-purple-400 bg-clip-text text-transparent">
                  {' '}In Minutes, Not Weeks
                </span>
              </h2>
              <p className="text-lg sm:text-xl text-slate-300 mb-8 sm:mb-12 max-w-2xl mx-auto px-4 leading-relaxed">
                Connect your Google Search Console and GA4 to generate a comprehensive analysis report with algorithmic insights, competitive intelligence, and prioritized action plans.
              </p>

              {/* Feature Grid */}
              <div className="grid sm:grid-cols-2 lg:grid-cols-3 gap-4 sm:gap-6 mb-8 sm:mb-12 px-2">
                {[
                  { icon: '📊', title: 'Health & Trajectory', desc: 'MSTL decomposition, change point detection, forecasting' },
                  { icon: '🎯', title: 'Page-Level Triage', desc: 'Decay detection, CTR anomalies, priority scoring' },
                  { icon: '🔍', title: 'SERP Intelligence', desc: 'Feature displacement, competitor mapping, intent analysis' },
                  { icon: '📝', title: 'Content Analysis', desc: 'Cannibalization detection, striking distance opportunities' },
                  { icon: '🏗️', title: 'Site Architecture', desc: 'PageRank flow, authority distribution, link recommendations' },
                  { icon: '💰', title: 'Revenue Attribution', desc: 'Position-to-revenue modeling, ROI estimates' },
                ].map((feature, idx) => (
                  <div key={idx} className="bg-slate-800/50 border border-slate-700 rounded-lg p-4 sm:p-6 hover:border-slate-600 transition-colors">
                    <div className="text-3xl sm:text-4xl mb-2 sm:mb-3">{feature.icon}</div>
                    <h3 className="text-base sm:text-lg font-semibold text-white mb-1 sm:mb-2">{feature.title}</h3>
                    <p className="text-xs sm:text-sm text-slate-400 leading-relaxed">{feature.desc}</p>
                  </div>
                ))}
              </div>

              {/* CTA Button */}
              <button
                onClick={handleGscConnect}
                className="w-full sm:w-auto px-8 sm:px-12 py-4 sm:py-5 bg-gradient-to-r from-blue-600 to-purple-600 hover:from-blue-700 hover:to-purple-700 text-white text-base sm:text-lg font-semibold rounded-lg shadow-lg hover:shadow-xl transition-all transform hover:scale-105 active:scale-100 touch-manipulation"
              >
                Connect Google Search Console — Free
              </button>
              <p className="text-xs sm:text-sm text-slate-400 mt-3 sm:mt-4 px-4">
                No credit card required • Report generated in 2-5 minutes
              </p>
            </div>
          ) : (
            /* Connected User — Property Selection */
            <div className="max-w-2xl mx-auto">
              <h2 className="text-2xl sm:text-3xl font-bold text-white mb-6 sm:mb-8 text-center sm:text-left">
                Generate Your Report
              </h2>

              {error && (
                <div className="mb-4 sm:mb-6 p-3 sm:p-4 bg-red-900/50 border border-red-700 rounded-lg text-red-200 text-sm sm:text-base">
                  {error}
                </div>
              )}

              {/* GSC Connection */}
              <div className="bg-slate-800/50 border border-slate-700 rounded-lg p-4 sm:p-6 mb-4 sm:mb-6">
                <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between mb-4 gap-3">
                  <div className="flex items-center gap-3">
                    <div className={`w-3 h-3 rounded-full ${user.gsc_connected ? 'bg-green-500' : 'bg-slate-600'}`} />
                    <h3 className="text-lg sm:text-xl font-semibold text-white">Google Search Console</h3>
                  </div>
                  {!user.gsc_connected && (
                    <button
                      onClick={handleGscConnect}
                      className="w-full sm:w-auto px-4 sm:px-6 py-2.5 sm:py-3 bg-blue-600 hover:bg-blue-700 text-white text-sm sm:text-base rounded-lg transition-colors touch-manipulation"
                    >
                      Connect GSC
                    </button>
                  )}
                </div>

                {user.gsc_connected && user.gsc_properties && user.gsc_properties.length > 0 && (
                  <div>
                    <label className="block text-sm font-medium text-slate-300 mb-2">
                      Select Property *
                    </label>
                    <select
                      value={selectedGscProperty}
                      onChange={(e) => setSelectedGscProperty(e.target.value)}
                      className="w-full px-3 sm:px-4 py-2.5 sm:py-3 bg-slate-900 border border-slate-600 rounded-lg text-white text-sm sm:text-base focus:outline-none focus:ring-2 focus:ring-blue-500 touch-manipulation"
                    >
                      <option value="">Choose a property...</option>
                      {user.gsc_properties.map((prop) => (
                        <option key={prop.id} value={prop.id}>
                          {prop.name}
                        </option>
                      ))}
                    </select>
                  </div>
                )}
              </div>

              {/* GA4 Connection */}
              <div className="bg-slate-800/50 border border-slate-700 rounded-lg p-4 sm:p-6 mb-6 sm:mb-8">
                <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between mb-4 gap-3">
                  <div className="flex items-center gap-3">
                    <div className={`w-3 h-3 rounded-full ${user.ga4_connected ? 'bg-green-500' : 'bg-slate-600'}`} />
                    <h3 className="text-lg sm:text-xl font-semibold text-white">Google Analytics 4</h3>
                  </div>
                  {!user.ga4_connected && (
                    <button
                      onClick={handleGa4Connect}
                      className="w-full sm:w-auto px-4 sm:px-6 py-2.5 sm:py-3 bg-purple-600 hover:bg-purple-700 text-white text-sm sm:text-base rounded-lg transition-colors touch-manipulation"
                    >
                      Connect GA4
                    </button>
                  )}
                </div>

                {user.ga4_connected && user.ga4_properties && user.ga4_properties.length > 0 && (
                  <div>
                    <label className="block text-sm font-medium text-slate-300 mb-2">
                      Select Property (Optional)
                    </label>
                    <select
                      value={selectedGa4Property}
                      onChange={(e) => setSelectedGa4Property(e.target.value)}
                      className="w-full px-3 sm:px-4 py-2.5 sm:py-3 bg-slate-900 border border-slate-600 rounded-lg text-white text-sm sm:text-base focus:outline-none focus:ring-2 focus:ring-purple-500 touch-manipulation"
                    >
                      <option value="">Skip for now (GSC-only report)</option>
                      {user.ga4_properties.map((prop) => (
                        <option key={prop.id} value={prop.id}>
                          {prop.name}
                        </option>
                      ))}
                    </select>
                    <p className="text-xs text-slate-400 mt-2">
                      GA4 enables engagement metrics, conversion tracking, and revenue attribution
                    </p>
                  </div>
                )}
              </div>

              {/* Generate Button */}
              <button
                onClick={handleGenerateReport}
                disabled={!selectedGscProperty || generatingReport}
                className="w-full px-6 sm:px-8 py-3.5 sm:py-4 bg-gradient-to-r from-blue-600 to-purple-600 hover:from-blue-700 hover:to-purple-700 disabled:from-slate-700 disabled:to-slate-700 disabled:cursor-not-allowed text-white text-base sm:text-lg font-semibold rounded-lg shadow-lg transition-all transform hover:scale-105 active:scale-100 disabled:transform-none disabled:shadow-none touch-manipulation"
              >
                {generatingReport ? (
                  <span className="flex items-center justify-center gap-3">
                    <svg className="animate-spin h-5 w-5" viewBox="0 0 24 24">
                      <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" fill="none" />
                      <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
                    </svg>
                    Generating Report...
                  </span>
                ) : (
                  'Generate Intelligence Report'
                )}
              </button>

              <p className="text-xs sm:text-sm text-slate-400 text-center mt-3 sm:mt-4 px-2">
                Report generation takes 2-5 minutes. You'll be redirected to the progress page.
              </p>
              <p className="text-xs text-slate-500 text-center mt-2">
                Already have reports?{' '}
                <Link href="/compare" className="text-blue-400 hover:text-blue-300 underline">
                  Compare previous reports →
                </Link>
                <span className="text-slate-600 mx-2">·</span>
                <Link href="/schedules" className="text-purple-400 hover:text-purple-300 underline">
                  Manage scheduled reports →
                </Link>
              </p>
            </div>
          )}
        </main>

        {/* Footer */}
        <footer className="border-t border-slate-700 mt-12 sm:mt-16 lg:mt-20">
          <div className="max-w-6xl mx-auto px-4 sm:px-6 lg:px-8 py-6 sm:py-8">
            <div className="flex flex-col sm:flex-row justify-between items-center gap-4 text-sm text-slate-400">
              <p className="text-center sm:text-left">
                © 2025 Search Intelligence Report. All rights reserved.
              </p>
              <div className="flex flex-wrap justify-center gap-4 sm:gap-6">
                <a href="/privacy" className="hover:text-white transition-colors">Privacy Policy</a>
                <a href="/terms" className="hover:text-white transition-colors">Terms of Service</a>
                <a href="mailto:support@searchintel.report" className="hover:text-white transition-colors">Contact</a>
              </div>
            </div>
          </div>
        </footer>
      </div>
    </>
  );
}
