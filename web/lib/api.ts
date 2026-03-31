import { createClient } from '@supabase/supabase-js';

const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';
const supabase = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL!,
  process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!
);

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
  gsc_property: string;
  ga4_property?: string;
  status: 'pending' | 'ingesting' | 'analyzing' | 'generating' | 'complete' | 'failed';
  progress: Record<string, string>;
  report_data?: any;
  created_at: string;
  completed_at?: string;
}

export interface GSCProperty {
  siteUrl: string;
  permissionLevel: string;
}

export interface GA4Property {
  name: string;
  displayName: string;
}

class APIError extends Error {
  constructor(
    message: string,
    public status?: number,
    public data?: any
  ) {
    super(message);
    this.name = 'APIError';
  }
}

async function fetchAPI(
  endpoint: string,
  options: RequestInit = {}
): Promise<any> {
  try {
    const response = await fetch(`${API_BASE_URL}${endpoint}`, {
      ...options,
      headers: {
        'Content-Type': 'application/json',
        ...options.headers,
      },
    });

    if (!response.ok) {
      const errorData = await response.json().catch(() => ({}));
      throw new APIError(
        errorData.detail || `HTTP ${response.status}: ${response.statusText}`,
        response.status,
        errorData
      );
    }

    return await response.json();
  } catch (error) {
    if (error instanceof APIError) {
      throw error;
    }
    throw new APIError(
      `Network error: ${error instanceof Error ? error.message : 'Unknown error'}`
    );
  }
}

// Auth & OAuth
export const auth = {
  async getGSCAuthUrl(): Promise<{ auth_url: string }> {
    return fetchAPI('/auth/gsc/authorize');
  },

  async handleGSCCallback(
    code: string,
    state: string
  ): Promise<{ user_id: string; email: string }> {
    return fetchAPI('/auth/gsc/callback', {
      method: 'POST',
      body: JSON.stringify({ code, state }),
    });
  },

  async getGA4AuthUrl(userId: string): Promise<{ auth_url: string }> {
    return fetchAPI('/auth/ga4/authorize', {
      method: 'POST',
      body: JSON.stringify({ user_id: userId }),
    });
  },

  async handleGA4Callback(
    code: string,
    state: string
  ): Promise<{ success: boolean }> {
    return fetchAPI('/auth/ga4/callback', {
      method: 'POST',
      body: JSON.stringify({ code, state }),
    });
  },

  async checkAuthStatus(userId: string): Promise<{
    gsc_connected: boolean;
    ga4_connected: boolean;
  }> {
    return fetchAPI(`/auth/status/${userId}`);
  },
};

// Properties
export const properties = {
  async getGSCProperties(userId: string): Promise<GSCProperty[]> {
    return fetchAPI(`/properties/gsc/${userId}`);
  },

  async getGA4Properties(userId: string): Promise<GA4Property[]> {
    return fetchAPI(`/properties/ga4/${userId}`);
  },
};

// Reports
export const reports = {
  async create(
    userId: string,
    gscProperty: string,
    ga4Property?: string
  ): Promise<Report> {
    return fetchAPI('/reports', {
      method: 'POST',
      body: JSON.stringify({
        user_id: userId,
        gsc_property: gscProperty,
        ga4_property: ga4Property,
      }),
    });
  },

  async get(reportId: string): Promise<Report> {
    return fetchAPI(`/reports/${reportId}`);
  },

  async list(userId: string): Promise<Report[]> {
    return fetchAPI(`/reports/user/${userId}`);
  },

  async getStatus(reportId: string): Promise<{
    status: Report['status'];
    progress: Record<string, string>;
    error?: string;
  }> {
    return fetchAPI(`/reports/${reportId}/status`);
  },

  async delete(reportId: string): Promise<{ success: boolean }> {
    return fetchAPI(`/reports/${reportId}`, {
      method: 'DELETE',
    });
  },
};

// Users
export const users = {
  async get(userId: string): Promise<User> {
    return fetchAPI(`/users/${userId}`);
  },

  async create(email: string): Promise<User> {
    return fetchAPI('/users', {
      method: 'POST',
      body: JSON.stringify({ email }),
    });
  },

  async update(userId: string, data: Partial<User>): Promise<User> {
    return fetchAPI(`/users/${userId}`, {
      method: 'PATCH',
      body: JSON.stringify(data),
    });
  },
};

// Supabase helpers
export const db = {
  async getUser(userId: string): Promise<User | null> {
    const { data, error } = await supabase
      .from('users')
      .select('*')
      .eq('id', userId)
      .single();

    if (error) {
      console.error('Error fetching user:', error);
      return null;
    }

    return data;
  },

  async getUserByEmail(email: string): Promise<User | null> {
    const { data, error } = await supabase
      .from('users')
      .select('*')
      .eq('email', email)
      .single();

    if (error) {
      if (error.code === 'PGRST116') {
        // Not found
        return null;
      }
      console.error('Error fetching user by email:', error);
      return null;
    }

    return data;
  },

  async createUser(email: string): Promise<User | null> {
    const { data, error } = await supabase
      .from('users')
      .insert({ email })
      .select()
      .single();

    if (error) {
      console.error('Error creating user:', error);
      return null;
    }

    return data;
  },

  async getReport(reportId: string): Promise<Report | null> {
    const { data, error } = await supabase
      .from('reports')
      .select('*')
      .eq('id', reportId)
      .single();

    if (error) {
      console.error('Error fetching report:', error);
      return null;
    }

    return data;
  },

  async getUserReports(userId: string): Promise<Report[]> {
    const { data, error } = await supabase
      .from('reports')
      .select('*')
      .eq('user_id', userId)
      .order('created_at', { ascending: false });

    if (error) {
      console.error('Error fetching user reports:', error);
      return [];
    }

    return data || [];
  },

  async subscribeToReport(
    reportId: string,
    callback: (report: Report) => void
  ): Promise<() => void> {
    const channel = supabase
      .channel(`report:${reportId}`)
      .on(
        'postgres_changes',
        {
          event: 'UPDATE',
          schema: 'public',
          table: 'reports',
          filter: `id=eq.${reportId}`,
        },
        (payload) => {
          callback(payload.new as Report);
        }
      )
      .subscribe();

    return () => {
      supabase.removeChannel(channel);
    };
  },
};

// Health check
export const health = {
  async check(): Promise<{ status: string; timestamp: string }> {
    return fetchAPI('/health');
  },
};

// Export everything as default for convenience
export default {
  auth,
  properties,
  reports,
  users,
  db,
  health,
  APIError,
};
