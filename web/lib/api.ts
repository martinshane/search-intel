/**
 * Centralised API client for the Search Intelligence frontend.
 *
 * All endpoint paths MUST match the backend router prefixes defined in
 * api/main.py:
 *   - /api/auth/*       → api.routers.auth
 *   - /api/reports/*    → api.routers.reports
 *   - /api/data/*       → api.routers.data_ingestion
 *   - /api/analysis/*   → api.routers.analysis
 *   - /api/modules/*    → api.routers.modules
 *   - /api/schedules/*  → api.routers.schedules
 *   - /health           → api.routers.health
 *
 * API_BASE is intentionally empty-string when NEXT_PUBLIC_API_URL is not
 * set so that requests become relative (e.g. /api/auth/status) and get
 * proxied by Next.js rewrites in next.config.js.  The old fallback of
 * "http://localhost:8000" was baked into production bundles and broke
 * every API call from real browsers.
 */

const API_BASE = process.env.NEXT_PUBLIC_API_URL || '';

// ---------------------------------------------------------------------------
// Fetch wrapper
// ---------------------------------------------------------------------------

export class APIError extends Error {
  status?: number;
  data?: any;
  constructor(message: string, status?: number, data?: any) {
    super(message);
    this.name = 'APIError';
    this.status = status;
    this.data = data;
  }
}

async function fetchAPI<T = any>(
  endpoint: string,
  options: RequestInit = {},
): Promise<T> {
  const response = await fetch(`${API_BASE}${endpoint}`, {
    ...options,
    credentials: 'include',
    headers: {
      'Content-Type': 'application/json',
      ...options.headers,
    },
  });

  if (!response.ok) {
    const errorData = await response.json().catch(() => ({}));
    throw new APIError(
      errorData.detail || errorData.message || `HTTP ${response.status}: ${response.statusText}`,
      response.status,
      errorData,
    );
  }

  return response.json();
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface User {
  id: string;
  email: string;
  gsc_token?: any;
  ga4_token?: any;
  created_at: string;
}

export interface Report {
  id: string;
  user_id: string;
  domain: string;
  gsc_property: string;
  ga4_property?: string;
  status: 'pending' | 'ingesting' | 'analyzing' | 'generating' | 'complete' | 'failed';
  progress: Record<string, string>;
  report_data?: any;
  created_at: string;
  completed_at?: string;
  current_module?: number;
  error_message?: string;
}

export interface GSCProperty {
  siteUrl: string;
  permissionLevel: string;
}

export interface GA4Property {
  name: string;
  displayName: string;
}

export interface AuthStatus {
  authenticated: boolean;
  user_id?: string;
  email?: string;
  gsc_connected?: boolean;
  ga4_connected?: boolean;
}

// ---------------------------------------------------------------------------
// Auth endpoints  (prefix: /api/auth)
// ---------------------------------------------------------------------------

export const auth = {
  /** Check current authentication status via JWT cookie. */
  async status(): Promise<AuthStatus> {
    return fetchAPI('/api/auth/status');
  },

  /** Get GSC OAuth authorize URL. */
  async gscAuthorize(): Promise<{ auth_url: string }> {
    return fetchAPI('/api/auth/gsc/authorize');
  },

  /** Get GA4 OAuth authorize URL. */
  async ga4Authorize(): Promise<{ auth_url: string }> {
    return fetchAPI('/api/auth/ga4/authorize');
  },

  /** List GSC properties for the authenticated user. */
  async gscProperties(): Promise<GSCProperty[]> {
    return fetchAPI('/api/auth/gsc/properties');
  },

  /** List GA4 properties for the authenticated user. */
  async ga4Properties(): Promise<GA4Property[]> {
    return fetchAPI('/api/auth/ga4/properties');
  },

  /** Revoke OAuth tokens. */
  async revoke(): Promise<{ success: boolean }> {
    return fetchAPI('/api/auth/revoke', { method: 'POST' });
  },

  /** Logout — clears JWT cookie. */
  async logout(): Promise<{ success: boolean }> {
    return fetchAPI('/api/auth/logout', { method: 'POST' });
  },
};

// ---------------------------------------------------------------------------
// Report endpoints  (prefix: /api/reports)
// ---------------------------------------------------------------------------

export const reports = {
  /** Generate (create) a new report. */
  async generate(
    gscProperty: string,
    ga4Property?: string | null,
    domain?: string,
  ): Promise<{ report_id: string }> {
    return fetchAPI('/api/reports/generate', {
      method: 'POST',
      body: JSON.stringify({
        gsc_property: gscProperty,
        ga4_property: ga4Property || null,
        domain: domain || undefined,
      }),
    });
  },

  /** Get a single report by ID. */
  async get(reportId: string): Promise<Report> {
    return fetchAPI(`/api/reports/${reportId}`);
  },

  /** Get report progress (for polling during generation). */
  async progress(reportId: string): Promise<any> {
    return fetchAPI(`/api/reports/${reportId}/progress`);
  },

  /** Get module results for progressive rendering. */
  async modules(reportId: string): Promise<any> {
    return fetchAPI(`/api/reports/${reportId}/modules`);
  },

  /** List current user's reports. */
  async mine(): Promise<Report[]> {
    return fetchAPI('/api/reports/user/me');
  },

  /** Get user's report history (for comparison picker). */
  async history(limit?: number): Promise<Report[]> {
    const qs = limit ? `?limit=${limit}` : '';
    return fetchAPI(`/api/reports/user/history${qs}`);
  },

  /** Retry a failed report. */
  async retry(reportId: string): Promise<{ success: boolean }> {
    return fetchAPI(`/api/reports/${reportId}/retry`, { method: 'POST' });
  },

  /** Get report PDF download URL (browser navigates to this). */
  pdfUrl(reportId: string): string {
    return `${API_BASE}/api/reports/${reportId}/pdf`;
  },

  /** Send report via email. */
  async email(reportId: string, to?: string): Promise<{ success: boolean }> {
    return fetchAPI(`/api/reports/${reportId}/email`, {
      method: 'POST',
      body: JSON.stringify({ to }),
    });
  },

  /** Get consulting CTAs for a report. */
  async ctas(reportId: string): Promise<any> {
    return fetchAPI(`/api/reports/${reportId}/ctas`);
  },

  /** Compare two reports. */
  async compare(reportId: string, baselineId: string): Promise<any> {
    return fetchAPI(`/api/reports/${reportId}/compare?baseline_id=${baselineId}`);
  },

  /** Get demo report (unauthenticated). */
  async demo(): Promise<any> {
    return fetchAPI('/api/reports/demo');
  },

  /** Get demo modules (unauthenticated). */
  async demoModules(): Promise<any> {
    return fetchAPI('/api/reports/demo/modules');
  },

  /** Get demo PDF URL (unauthenticated). */
  demoPdfUrl(): string {
    return `${API_BASE}/api/reports/demo/pdf`;
  },
};

// ---------------------------------------------------------------------------
// Schedule endpoints  (prefix: /api/schedules)
// ---------------------------------------------------------------------------

export const schedules = {
  /** List current user's schedules. */
  async mine(): Promise<any[]> {
    return fetchAPI('/api/schedules/mine');
  },

  /** Create a new schedule. */
  async create(data: {
    gsc_property: string;
    ga4_property?: string;
    frequency: string;
    email_to?: string;
  }): Promise<any> {
    return fetchAPI('/api/schedules/create', {
      method: 'POST',
      body: JSON.stringify(data),
    });
  },

  /** Update a schedule. */
  async update(id: string, data: any): Promise<any> {
    return fetchAPI(`/api/schedules/${id}`, {
      method: 'PUT',
      body: JSON.stringify(data),
    });
  },

  /** Delete a schedule. */
  async remove(id: string): Promise<{ success: boolean }> {
    return fetchAPI(`/api/schedules/${id}`, { method: 'DELETE' });
  },
};

// ---------------------------------------------------------------------------
// Health check
// ---------------------------------------------------------------------------

export const health = {
  async check(): Promise<{ status: string; timestamp?: string }> {
    return fetchAPI('/health');
  },
};

// ---------------------------------------------------------------------------
// Default export for convenience
// ---------------------------------------------------------------------------

const api = { auth, reports, schedules, health, APIError, fetchAPI };
export default api;
