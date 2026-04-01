import React, { useState, useEffect, useCallback } from 'react';
import Head from 'next/head';
import Link from 'next/link';
import NavHeader from '../components/NavHeader';
import { useRouter } from 'next/router';

const API_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

/* ── Types ── */

interface Schedule {
  id: string;
  domain: string;
  gsc_property: string;
  ga4_property: string | null;
  frequency: 'weekly' | 'biweekly' | 'monthly';
  day_of_week: number;
  email_to: string;
  include_pdf: boolean;
  include_comparison: boolean;
  active: boolean;
  last_run_at: string | null;
  last_report_id: string | null;
  next_run_at: string | null;
  run_count: number;
  created_at: string;
}

interface Property {
  id: string;
  name: string;
  type: 'gsc' | 'ga4';
}

interface UserInfo {
  email: string;
  gsc_properties: Property[];
  ga4_properties: Property[];
}

const DAY_NAMES = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
const FREQUENCY_LABELS: Record<string, string> = {
  weekly: 'Every week',
  biweekly: 'Every 2 weeks',
  monthly: 'Every month',
};

/* ── Helper Components ── */

function Badge({ children, color }: { children: React.ReactNode; color: string }) {
  const colors: Record<string, string> = {
    green: 'bg-emerald-500/20 text-emerald-300 border-emerald-500/30',
    red: 'bg-red-500/20 text-red-300 border-red-500/30',
    blue: 'bg-blue-500/20 text-blue-300 border-blue-500/30',
    amber: 'bg-amber-500/20 text-amber-300 border-amber-500/30',
    gray: 'bg-slate-500/20 text-slate-300 border-slate-500/30',
  };
  return (
    <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium border ${colors[color] || colors.gray}`}>
      {children}
    </span>
  );
}

function formatDate(iso: string | null): string {
  if (!iso) return '—';
  const d = new Date(iso);
  return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric', hour: 'numeric', minute: '2-digit' });
}

/* ── Main Page ── */

export default function SchedulesPage() {
  const router = useRouter();
  const [schedules, setSchedules] = useState<Schedule[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [user, setUser] = useState<UserInfo | null>(null);

  // Create-form state
  const [showCreate, setShowCreate] = useState(false);
  const [creating, setCreating] = useState(false);
  const [formDomain, setFormDomain] = useState('');
  const [formGsc, setFormGsc] = useState('');
  const [formGa4, setFormGa4] = useState('');
  const [formFrequency, setFormFrequency] = useState<'weekly' | 'biweekly' | 'monthly'>('weekly');
  const [formDay, setFormDay] = useState(1); // Monday
  const [formEmail, setFormEmail] = useState('');
  const [formPdf, setFormPdf] = useState(true);
  const [formComparison, setFormComparison] = useState(true);

  // Edit state
  const [editingId, setEditingId] = useState<string | null>(null);
  const [editFrequency, setEditFrequency] = useState<'weekly' | 'biweekly' | 'monthly'>('weekly');
  const [editDay, setEditDay] = useState(1);
  const [editEmail, setEditEmail] = useState('');
  const [editPdf, setEditPdf] = useState(true);
  const [editComparison, setEditComparison] = useState(true);

  const fetchSchedules = useCallback(async () => {
    try {
      const res = await fetch(`${API_URL}/schedules/mine`, { credentials: 'include' });
      if (!res.ok) throw new Error('Failed to load schedules');
      const data = await res.json();
      setSchedules(data.schedules || []);
    } catch (err: any) {
      setError(err.message);
    }
  }, []);

  const fetchUser = useCallback(async () => {
    try {
      const res = await fetch(`${API_URL}/api/auth/status`, { credentials: 'include' });
      if (res.ok) {
        const data = await res.json();
        setUser(data);
        if (data.email) setFormEmail(data.email);
      }
    } catch {
      // Not logged in
    }
  }, []);

  useEffect(() => {
    Promise.all([fetchSchedules(), fetchUser()]).finally(() => setLoading(false));
  }, [fetchSchedules, fetchUser]);

  /* ── Handlers ── */

  const handleCreate = async (e: React.FormEvent) => {
    e.preventDefault();
    setCreating(true);
    setError(null);

    try {
      const res = await fetch(`${API_URL}/schedules/create`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({
          domain: formDomain,
          gsc_property: formGsc,
          ga4_property: formGa4 || null,
          frequency: formFrequency,
          day_of_week: formDay,
          email_to: formEmail,
          include_pdf: formPdf,
          include_comparison: formComparison,
        }),
      });

      if (!res.ok) {
        const errData = await res.json();
        throw new Error(errData.detail || 'Failed to create schedule');
      }

      setShowCreate(false);
      setFormDomain('');
      setFormGsc('');
      setFormGa4('');
      await fetchSchedules();
    } catch (err: any) {
      setError(err.message);
    } finally {
      setCreating(false);
    }
  };

  const handlePause = async (id: string) => {
    try {
      await fetch(`${API_URL}/schedules/${id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({ active: false }),
      });
      await fetchSchedules();
    } catch (err: any) {
      setError(err.message);
    }
  };

  const handleResume = async (id: string) => {
    try {
      await fetch(`${API_URL}/schedules/${id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({ active: true }),
      });
      await fetchSchedules();
    } catch (err: any) {
      setError(err.message);
    }
  };

  const handleDelete = async (id: string) => {
    if (!confirm('Remove this schedule? This cannot be undone.')) return;
    try {
      await fetch(`${API_URL}/schedules/${id}`, {
        method: 'DELETE',
        credentials: 'include',
      });
      await fetchSchedules();
    } catch (err: any) {
      setError(err.message);
    }
  };

  const startEdit = (s: Schedule) => {
    setEditingId(s.id);
    setEditFrequency(s.frequency);
    setEditDay(s.day_of_week);
    setEditEmail(s.email_to);
    setEditPdf(s.include_pdf);
    setEditComparison(s.include_comparison);
  };

  const handleSaveEdit = async (id: string) => {
    try {
      await fetch(`${API_URL}/schedules/${id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({
          frequency: editFrequency,
          day_of_week: editDay,
          email_to: editEmail,
          include_pdf: editPdf,
          include_comparison: editComparison,
        }),
      });
      setEditingId(null);
      await fetchSchedules();
    } catch (err: any) {
      setError(err.message);
    }
  };

  /* ── Render ── */

  if (loading) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900 flex items-center justify-center">
        <div className="animate-pulse text-slate-400 text-lg">Loading schedules...</div>
      </div>
    );
  }

  return (
    <>
      <Head>
        <title>Scheduled Reports — Search Intelligence</title>
        <meta name="description" content="Manage your automated SEO report schedules. Get weekly, biweekly, or monthly reports delivered to your inbox." />
      </Head>

      <div className="min-h-screen bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900">
        <NavHeader activePage="schedules" />

        <main className="max-w-5xl mx-auto px-4 py-8">
          {/* Header */}
          <div className="flex items-center justify-between mb-8">
            <div>
              <h1 className="text-3xl font-bold text-white">Scheduled Reports</h1>
              <p className="text-slate-400 mt-1">
                Automatically generate and deliver SEO reports on a recurring basis.
              </p>
            </div>
            <button
              onClick={() => setShowCreate(true)}
              className="px-5 py-2.5 bg-blue-600 hover:bg-blue-500 text-white rounded-lg font-medium transition-colors shadow-lg shadow-blue-600/20"
            >
              + New Schedule
            </button>
          </div>

          {/* Error banner */}
          {error && (
            <div className="mb-6 p-4 bg-red-500/10 border border-red-500/30 rounded-lg text-red-300 flex items-center justify-between">
              <span>{error}</span>
              <button onClick={() => setError(null)} className="text-red-400 hover:text-red-200 ml-4">✕</button>
            </div>
          )}

          {/* Create form */}
          {showCreate && (
            <div className="mb-8 p-6 bg-slate-800/80 border border-slate-700/50 rounded-xl">
              <h2 className="text-xl font-semibold text-white mb-4">Create New Schedule</h2>
              <form onSubmit={handleCreate} className="space-y-4">
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  {/* Domain */}
                  <div>
                    <label className="block text-sm font-medium text-slate-300 mb-1">Domain</label>
                    <input
                      type="text"
                      value={formDomain}
                      onChange={(e) => setFormDomain(e.target.value)}
                      placeholder="example.com"
                      required
                      className="w-full px-3 py-2 bg-slate-700/50 border border-slate-600 rounded-lg text-white placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                    />
                  </div>

                  {/* GSC Property */}
                  <div>
                    <label className="block text-sm font-medium text-slate-300 mb-1">GSC Property</label>
                    {user?.gsc_properties && user.gsc_properties.length > 0 ? (
                      <select
                        value={formGsc}
                        onChange={(e) => setFormGsc(e.target.value)}
                        required
                        className="w-full px-3 py-2 bg-slate-700/50 border border-slate-600 rounded-lg text-white focus:outline-none focus:ring-2 focus:ring-blue-500"
                      >
                        <option value="">Select property...</option>
                        {user.gsc_properties.map((p) => (
                          <option key={p.id} value={p.id}>{p.name}</option>
                        ))}
                      </select>
                    ) : (
                      <input
                        type="text"
                        value={formGsc}
                        onChange={(e) => setFormGsc(e.target.value)}
                        placeholder="sc-domain:example.com"
                        required
                        className="w-full px-3 py-2 bg-slate-700/50 border border-slate-600 rounded-lg text-white placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-blue-500"
                      />
                    )}
                  </div>

                  {/* GA4 Property */}
                  <div>
                    <label className="block text-sm font-medium text-slate-300 mb-1">GA4 Property <span className="text-slate-500">(optional)</span></label>
                    {user?.ga4_properties && user.ga4_properties.length > 0 ? (
                      <select
                        value={formGa4}
                        onChange={(e) => setFormGa4(e.target.value)}
                        className="w-full px-3 py-2 bg-slate-700/50 border border-slate-600 rounded-lg text-white focus:outline-none focus:ring-2 focus:ring-blue-500"
                      >
                        <option value="">None</option>
                        {user.ga4_properties.map((p) => (
                          <option key={p.id} value={p.id}>{p.name}</option>
                        ))}
                      </select>
                    ) : (
                      <input
                        type="text"
                        value={formGa4}
                        onChange={(e) => setFormGa4(e.target.value)}
                        placeholder="properties/123456789"
                        className="w-full px-3 py-2 bg-slate-700/50 border border-slate-600 rounded-lg text-white placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-blue-500"
                      />
                    )}
                  </div>

                  {/* Email */}
                  <div>
                    <label className="block text-sm font-medium text-slate-300 mb-1">Deliver to email</label>
                    <input
                      type="email"
                      value={formEmail}
                      onChange={(e) => setFormEmail(e.target.value)}
                      placeholder="you@example.com"
                      required
                      className="w-full px-3 py-2 bg-slate-700/50 border border-slate-600 rounded-lg text-white placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-blue-500"
                    />
                  </div>

                  {/* Frequency */}
                  <div>
                    <label className="block text-sm font-medium text-slate-300 mb-1">Frequency</label>
                    <select
                      value={formFrequency}
                      onChange={(e) => setFormFrequency(e.target.value as any)}
                      className="w-full px-3 py-2 bg-slate-700/50 border border-slate-600 rounded-lg text-white focus:outline-none focus:ring-2 focus:ring-blue-500"
                    >
                      <option value="weekly">Weekly</option>
                      <option value="biweekly">Every 2 weeks</option>
                      <option value="monthly">Monthly</option>
                    </select>
                  </div>

                  {/* Day of week */}
                  <div>
                    <label className="block text-sm font-medium text-slate-300 mb-1">Preferred day</label>
                    <select
                      value={formDay}
                      onChange={(e) => setFormDay(Number(e.target.value))}
                      className="w-full px-3 py-2 bg-slate-700/50 border border-slate-600 rounded-lg text-white focus:outline-none focus:ring-2 focus:ring-blue-500"
                    >
                      {DAY_NAMES.map((name, i) => (
                        <option key={i} value={i}>{name}</option>
                      ))}
                    </select>
                  </div>
                </div>

                {/* Toggles */}
                <div className="flex gap-6">
                  <label className="flex items-center gap-2 text-slate-300 cursor-pointer">
                    <input
                      type="checkbox"
                      checked={formPdf}
                      onChange={(e) => setFormPdf(e.target.checked)}
                      className="w-4 h-4 rounded bg-slate-700 border-slate-600 text-blue-500 focus:ring-blue-500 focus:ring-offset-0"
                    />
                    Attach PDF report
                  </label>
                  <label className="flex items-center gap-2 text-slate-300 cursor-pointer">
                    <input
                      type="checkbox"
                      checked={formComparison}
                      onChange={(e) => setFormComparison(e.target.checked)}
                      className="w-4 h-4 rounded bg-slate-700 border-slate-600 text-blue-500 focus:ring-blue-500 focus:ring-offset-0"
                    />
                    Include comparison to prior run
                  </label>
                </div>

                {/* Actions */}
                <div className="flex gap-3 pt-2">
                  <button
                    type="submit"
                    disabled={creating}
                    className="px-5 py-2 bg-blue-600 hover:bg-blue-500 disabled:bg-slate-600 disabled:cursor-not-allowed text-white rounded-lg font-medium transition-colors"
                  >
                    {creating ? 'Creating...' : 'Create Schedule'}
                  </button>
                  <button
                    type="button"
                    onClick={() => setShowCreate(false)}
                    className="px-5 py-2 bg-slate-700 hover:bg-slate-600 text-slate-300 rounded-lg font-medium transition-colors"
                  >
                    Cancel
                  </button>
                </div>
              </form>
            </div>
          )}

          {/* Schedules list */}
          {schedules.length === 0 && !showCreate ? (
            <div className="text-center py-20">
              <div className="text-6xl mb-4">📅</div>
              <h2 className="text-xl font-semibold text-white mb-2">No scheduled reports yet</h2>
              <p className="text-slate-400 mb-6 max-w-md mx-auto">
                Set up automatic SEO reports delivered to your inbox on a weekly, biweekly, or monthly basis.
                Each report includes a comparison to the previous run so you can track progress.
              </p>
              <button
                onClick={() => setShowCreate(true)}
                className="px-6 py-3 bg-blue-600 hover:bg-blue-500 text-white rounded-lg font-medium transition-colors shadow-lg shadow-blue-600/20"
              >
                Create your first schedule
              </button>
            </div>
          ) : (
            <div className="space-y-4">
              {schedules.map((s) => (
                <div
                  key={s.id}
                  className={`p-5 rounded-xl border transition-all ${
                    s.active
                      ? 'bg-slate-800/80 border-slate-700/50 hover:border-slate-600/50'
                      : 'bg-slate-800/40 border-slate-700/30 opacity-70'
                  }`}
                >
                  {editingId === s.id ? (
                    /* ── Edit Mode ── */
                    <div className="space-y-4">
                      <div className="flex items-center gap-3 mb-2">
                        <h3 className="text-lg font-semibold text-white">{s.domain}</h3>
                        <Badge color="blue">Editing</Badge>
                      </div>
                      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                        <div>
                          <label className="block text-xs font-medium text-slate-400 mb-1">Frequency</label>
                          <select
                            value={editFrequency}
                            onChange={(e) => setEditFrequency(e.target.value as any)}
                            className="w-full px-3 py-2 bg-slate-700/50 border border-slate-600 rounded-lg text-white text-sm focus:ring-2 focus:ring-blue-500"
                          >
                            <option value="weekly">Weekly</option>
                            <option value="biweekly">Every 2 weeks</option>
                            <option value="monthly">Monthly</option>
                          </select>
                        </div>
                        <div>
                          <label className="block text-xs font-medium text-slate-400 mb-1">Day</label>
                          <select
                            value={editDay}
                            onChange={(e) => setEditDay(Number(e.target.value))}
                            className="w-full px-3 py-2 bg-slate-700/50 border border-slate-600 rounded-lg text-white text-sm focus:ring-2 focus:ring-blue-500"
                          >
                            {DAY_NAMES.map((name, i) => (
                              <option key={i} value={i}>{name}</option>
                            ))}
                          </select>
                        </div>
                        <div>
                          <label className="block text-xs font-medium text-slate-400 mb-1">Email</label>
                          <input
                            type="email"
                            value={editEmail}
                            onChange={(e) => setEditEmail(e.target.value)}
                            className="w-full px-3 py-2 bg-slate-700/50 border border-slate-600 rounded-lg text-white text-sm focus:ring-2 focus:ring-blue-500"
                          />
                        </div>
                      </div>
                      <div className="flex gap-6">
                        <label className="flex items-center gap-2 text-slate-300 text-sm cursor-pointer">
                          <input type="checkbox" checked={editPdf} onChange={(e) => setEditPdf(e.target.checked)}
                            className="w-4 h-4 rounded bg-slate-700 border-slate-600 text-blue-500" />
                          PDF attachment
                        </label>
                        <label className="flex items-center gap-2 text-slate-300 text-sm cursor-pointer">
                          <input type="checkbox" checked={editComparison} onChange={(e) => setEditComparison(e.target.checked)}
                            className="w-4 h-4 rounded bg-slate-700 border-slate-600 text-blue-500" />
                          Include comparison
                        </label>
                      </div>
                      <div className="flex gap-2">
                        <button onClick={() => handleSaveEdit(s.id)}
                          className="px-4 py-1.5 bg-blue-600 hover:bg-blue-500 text-white rounded-lg text-sm font-medium transition-colors">
                          Save
                        </button>
                        <button onClick={() => setEditingId(null)}
                          className="px-4 py-1.5 bg-slate-700 hover:bg-slate-600 text-slate-300 rounded-lg text-sm font-medium transition-colors">
                          Cancel
                        </button>
                      </div>
                    </div>
                  ) : (
                    /* ── View Mode ── */
                    <div>
                      <div className="flex items-start justify-between">
                        <div className="flex items-center gap-3">
                          <h3 className="text-lg font-semibold text-white">{s.domain}</h3>
                          {s.active ? (
                            <Badge color="green">Active</Badge>
                          ) : (
                            <Badge color="gray">Paused</Badge>
                          )}
                          <Badge color="blue">{FREQUENCY_LABELS[s.frequency]}</Badge>
                        </div>
                        <div className="flex gap-2">
                          <button onClick={() => startEdit(s)}
                            className="px-3 py-1.5 text-slate-400 hover:text-white hover:bg-slate-700 rounded-lg text-sm transition-colors">
                            Edit
                          </button>
                          {s.active ? (
                            <button onClick={() => handlePause(s.id)}
                              className="px-3 py-1.5 text-amber-400 hover:text-amber-300 hover:bg-amber-500/10 rounded-lg text-sm transition-colors">
                              Pause
                            </button>
                          ) : (
                            <button onClick={() => handleResume(s.id)}
                              className="px-3 py-1.5 text-emerald-400 hover:text-emerald-300 hover:bg-emerald-500/10 rounded-lg text-sm transition-colors">
                              Resume
                            </button>
                          )}
                          <button onClick={() => handleDelete(s.id)}
                            className="px-3 py-1.5 text-red-400 hover:text-red-300 hover:bg-red-500/10 rounded-lg text-sm transition-colors">
                            Delete
                          </button>
                        </div>
                      </div>

                      {/* Details grid */}
                      <div className="mt-4 grid grid-cols-2 md:grid-cols-4 gap-4">
                        <div>
                          <div className="text-xs text-slate-500 uppercase tracking-wider">Delivers on</div>
                          <div className="text-sm text-slate-200 mt-0.5">{DAY_NAMES[s.day_of_week]}s</div>
                        </div>
                        <div>
                          <div className="text-xs text-slate-500 uppercase tracking-wider">Email</div>
                          <div className="text-sm text-slate-200 mt-0.5 truncate">{s.email_to}</div>
                        </div>
                        <div>
                          <div className="text-xs text-slate-500 uppercase tracking-wider">Next run</div>
                          <div className="text-sm text-slate-200 mt-0.5">{s.active ? formatDate(s.next_run_at) : '—'}</div>
                        </div>
                        <div>
                          <div className="text-xs text-slate-500 uppercase tracking-wider">Reports sent</div>
                          <div className="text-sm text-slate-200 mt-0.5">{s.run_count}</div>
                        </div>
                      </div>

                      {/* Options and last run */}
                      <div className="mt-3 flex flex-wrap items-center gap-4 text-xs text-slate-500">
                        <span>GSC: {s.gsc_property}</span>
                        {s.ga4_property && <span>GA4: {s.ga4_property}</span>}
                        {s.include_pdf && <span>📎 PDF</span>}
                        {s.include_comparison && <span>📊 Comparison</span>}
                        {s.last_run_at && (
                          <span>
                            Last run: {formatDate(s.last_run_at)}
                            {s.last_report_id && (
                              <> · <Link href={`/report/${s.last_report_id}`} className="text-blue-400 hover:text-blue-300">View report</Link></>
                            )}
                          </span>
                        )}
                      </div>
                    </div>
                  )}
                </div>
              ))}
            </div>
          )}

          {/* Consulting CTA */}
          <div className="mt-12 p-6 rounded-xl bg-gradient-to-r from-blue-600/20 via-indigo-600/20 to-purple-600/20 border border-blue-500/20">
            <div className="flex flex-col md:flex-row items-center justify-between gap-4">
              <div>
                <h3 className="text-lg font-semibold text-white">Need help acting on your reports?</h3>
                <p className="text-slate-400 text-sm mt-1">
                  Our search consultants can help you implement the recommendations from your intelligence reports.
                </p>
              </div>
              <a
                href="https://clankermarketing.com/contact"
                target="_blank"
                rel="noopener noreferrer"
                className="px-6 py-2.5 bg-white text-slate-900 rounded-lg font-medium hover:bg-slate-100 transition-colors whitespace-nowrap shadow-lg"
              >
                Book a consultation
              </a>
            </div>
          </div>
        </main>
      </div>
    </>
  );
}
