'use client';

import { useState, useEffect } from 'react';
import { User, Database } from 'lucide-react';

interface GSCProperty {
  siteUrl: string;
  permissionLevel: string;
}

export default function HomePage() {
  const [isConnecting, setIsConnecting] = useState(false);
  const [isConnected, setIsConnected] = useState(false);
  const [properties, setProperties] = useState<GSCProperty[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [userEmail, setUserEmail] = useState<string | null>(null);

  useEffect(() => {
    checkAuthStatus();
  }, []);

  const checkAuthStatus = async () => {
    try {
      const response = await fetch('/api/auth/status');
      if (response.ok) {
        const data = await response.json();
        if (data.authenticated) {
          setIsConnected(true);
          setUserEmail(data.email);
          fetchProperties();
        }
      }
    } catch (err) {
      console.error('Failed to check auth status:', err);
    }
  };

  const fetchProperties = async () => {
    try {
      const response = await fetch('/api/gsc/properties');
      if (response.ok) {
        const data = await response.json();
        setProperties(data.properties || []);
      } else {
        const errorData = await response.json();
        setError(errorData.error || 'Failed to fetch properties');
      }
    } catch (err) {
      setError('Failed to fetch properties');
      console.error(err);
    }
  };

  const handleConnect = async () => {
    setIsConnecting(true);
    setError(null);

    try {
      const response = await fetch('/api/auth/google');
      if (response.ok) {
        const data = await response.json();
        if (data.authUrl) {
          window.location.href = data.authUrl;
        } else {
          setError('Failed to get authorization URL');
          setIsConnecting(false);
        }
      } else {
        const errorData = await response.json();
        setError(errorData.error || 'Failed to initiate OAuth');
        setIsConnecting(false);
      }
    } catch (err) {
      setError('Network error. Please try again.');
      setIsConnecting(false);
      console.error(err);
    }
  };

  const handleDisconnect = async () => {
    try {
      const response = await fetch('/api/auth/disconnect', {
        method: 'POST',
      });
      if (response.ok) {
        setIsConnected(false);
        setProperties([]);
        setUserEmail(null);
        setError(null);
      }
    } catch (err) {
      console.error('Failed to disconnect:', err);
    }
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-50 to-slate-100">
      <div className="container mx-auto px-4 py-16">
        {/* Header */}
        <div className="text-center mb-12">
          <div className="flex items-center justify-center mb-4">
            <Database className="w-12 h-12 text-blue-600" />
          </div>
          <h1 className="text-4xl font-bold text-slate-900 mb-3">
            Search Intelligence Report
          </h1>
          <p className="text-lg text-slate-600 max-w-2xl mx-auto">
            Generate a comprehensive analysis of your site's search performance.
            Connect your Google Search Console to get started.
          </p>
        </div>

        {/* Main Content Card */}
        <div className="max-w-3xl mx-auto bg-white rounded-lg shadow-lg p-8">
          {!isConnected ? (
            <div className="text-center">
              <div className="mb-6">
                <h2 className="text-2xl font-semibold text-slate-800 mb-3">
                  Connect Your Account
                </h2>
                <p className="text-slate-600">
                  We'll analyze 12+ months of search data to identify opportunities
                  and create a prioritized action plan.
                </p>
              </div>

              {error && (
                <div className="mb-6 p-4 bg-red-50 border border-red-200 rounded-lg">
                  <p className="text-red-700 text-sm">{error}</p>
                </div>
              )}

              <button
                onClick={handleConnect}
                disabled={isConnecting}
                className="inline-flex items-center px-8 py-4 bg-blue-600 text-white font-medium rounded-lg hover:bg-blue-700 disabled:bg-blue-300 disabled:cursor-not-allowed transition-colors"
              >
                {isConnecting ? (
                  <>
                    <svg
                      className="animate-spin -ml-1 mr-3 h-5 w-5 text-white"
                      xmlns="http://www.w3.org/2000/svg"
                      fill="none"
                      viewBox="0 0 24 24"
                    >
                      <circle
                        className="opacity-25"
                        cx="12"
                        cy="12"
                        r="10"
                        stroke="currentColor"
                        strokeWidth="4"
                      ></circle>
                      <path
                        className="opacity-75"
                        fill="currentColor"
                        d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
                      ></path>
                    </svg>
                    Connecting...
                  </>
                ) : (
                  <>
                    <User className="w-5 h-5 mr-2" />
                    Connect Google Search Console
                  </>
                )}
              </button>

              <div className="mt-8 pt-6 border-t border-slate-200">
                <h3 className="text-sm font-semibold text-slate-700 mb-3">
                  What you'll get:
                </h3>
                <ul className="text-left space-y-2 text-sm text-slate-600 max-w-md mx-auto">
                  <li className="flex items-start">
                    <span className="text-blue-600 mr-2">✓</span>
                    Health & trajectory analysis with 90-day forecast
                  </li>
                  <li className="flex items-start">
                    <span className="text-blue-600 mr-2">✓</span>
                    Page-level triage with decay detection
                  </li>
                  <li className="flex items-start">
                    <span className="text-blue-600 mr-2">✓</span>
                    SERP landscape & competitor analysis
                  </li>
                  <li className="flex items-start">
                    <span className="text-blue-600 mr-2">✓</span>
                    Content intelligence & cannibalization detection
                  </li>
                  <li className="flex items-start">
                    <span className="text-blue-600 mr-2">✓</span>
                    Prioritized action plan with impact estimates
                  </li>
                </ul>
              </div>
            </div>
          ) : (
            <div>
              <div className="flex items-center justify-between mb-6 pb-6 border-b border-slate-200">
                <div>
                  <h2 className="text-2xl font-semibold text-slate-800 mb-1">
                    Connected Properties
                  </h2>
                  {userEmail && (
                    <p className="text-sm text-slate-600">
                      Signed in as {userEmail}
                    </p>
                  )}
                </div>
                <button
                  onClick={handleDisconnect}
                  className="px-4 py-2 text-sm text-slate-600 hover:text-slate-800 border border-slate-300 rounded-lg hover:border-slate-400 transition-colors"
                >
                  Disconnect
                </button>
              </div>

              {error && (
                <div className="mb-6 p-4 bg-red-50 border border-red-200 rounded-lg">
                  <p className="text-red-700 text-sm">{error}</p>
                </div>
              )}

              {properties.length === 0 ? (
                <div className="text-center py-8">
                  <p className="text-slate-600">
                    No Search Console properties found. Make sure you have access
                    to at least one property in Google Search Console.
                  </p>
                </div>
              ) : (
                <div>
                  <p className="text-slate-600 mb-4">
                    Select a property to generate your Search Intelligence Report:
                  </p>
                  <div className="space-y-3">
                    {properties.map((property) => (
                      <div
                        key={property.siteUrl}
                        className="flex items-center justify-between p-4 border border-slate-200 rounded-lg hover:border-blue-300 hover:bg-blue-50 transition-colors cursor-pointer"
                      >
                        <div className="flex-1">
                          <p className="font-medium text-slate-800">
                            {property.siteUrl}
                          </p>
                          <p className="text-sm text-slate-500">
                            Permission: {property.permissionLevel}
                          </p>
                        </div>
                        <button
                          className="px-4 py-2 bg-blue-600 text-white text-sm font-medium rounded-lg hover:bg-blue-700 transition-colors"
                          onClick={() => {
                            // TODO: Navigate to report generation
                            console.log('Generate report for:', property.siteUrl);
                          }}
                        >
                          Generate Report
                        </button>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="text-center mt-12 text-sm text-slate-500">
          <p>
            Your data is secure. We only request read-only access to Google
            Search Console.
          </p>
        </div>
      </div>
    </div>
  );
}
