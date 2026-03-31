import { createClient } from '@supabase/supabase-js';
import { cookies } from 'next/headers';

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

/**
 * Get the current user from session cookie
 */
export async function getCurrentUser(): Promise<User | null> {
  try {
    const cookieStore = await cookies();
    const sessionCookie = cookieStore.get('session');

    if (!sessionCookie) {
      return null;
    }

    const { data, error } = await supabase
      .from('users')
      .select('*')
      .eq('id', sessionCookie.value)
      .single();

    if (error || !data) {
      return null;
    }

    return data as User;
  } catch (error) {
    console.error('Error getting current user:', error);
    return null;
  }
}

/**
 * Set session cookie for user
 */
export async function setUserSession(userId: string): Promise<void> {
  const cookieStore = await cookies();
  cookieStore.set('session', userId, {
    httpOnly: true,
    secure: process.env.NODE_ENV === 'production',
    sameSite: 'lax',
    maxAge: 60 * 60 * 24 * 30, // 30 days
    path: '/',
  });
}

/**
 * Clear user session
 */
export async function clearUserSession(): Promise<void> {
  const cookieStore = await cookies();
  cookieStore.delete('session');
}

/**
 * Get or create user by email
 */
export async function getOrCreateUser(email: string): Promise<User> {
  try {
    // Try to find existing user
    const { data: existingUser, error: fetchError } = await supabase
      .from('users')
      .select('*')
      .eq('email', email)
      .single();

    if (existingUser && !fetchError) {
      return existingUser as User;
    }

    // Create new user if not found
    const { data: newUser, error: createError } = await supabase
      .from('users')
      .insert({ email })
      .select()
      .single();

    if (createError) {
      throw createError;
    }

    return newUser as User;
  } catch (error) {
    console.error('Error in getOrCreateUser:', error);
    throw error;
  }
}

/**
 * Store OAuth tokens for a user
 */
export async function storeOAuthTokens(
  userId: string,
  gscToken?: any,
  ga4Token?: any
): Promise<void> {
  try {
    const updates: any = {};
    if (gscToken) {
      updates.gsc_token = gscToken;
    }
    if (ga4Token) {
      updates.ga4_token = ga4Token;
    }

    const { error } = await supabase
      .from('users')
      .update(updates)
      .eq('id', userId);

    if (error) {
      throw error;
    }
  } catch (error) {
    console.error('Error storing OAuth tokens:', error);
    throw error;
  }
}

/**
 * Get OAuth tokens for a user
 */
export async function getOAuthTokens(userId: string): Promise<{
  gscToken?: any;
  ga4Token?: any;
}> {
  try {
    const { data, error } = await supabase
      .from('users')
      .select('gsc_token, ga4_token')
      .eq('id', userId)
      .single();

    if (error) {
      throw error;
    }

    return {
      gscToken: data?.gsc_token,
      ga4Token: data?.ga4_token,
    };
  } catch (error) {
    console.error('Error getting OAuth tokens:', error);
    throw error;
  }
}

/**
 * Check if user has connected GSC
 */
export async function hasGSCConnected(userId: string): Promise<boolean> {
  try {
    const { data, error } = await supabase
      .from('users')
      .select('gsc_token')
      .eq('id', userId)
      .single();

    if (error) {
      return false;
    }

    return !!data?.gsc_token;
  } catch (error) {
    return false;
  }
}

/**
 * Check if user has connected GA4
 */
export async function hasGA4Connected(userId: string): Promise<boolean> {
  try {
    const { data, error } = await supabase
      .from('users')
      .select('ga4_token')
      .eq('id', userId)
      .single();

    if (error) {
      return false;
    }

    return !!data?.ga4_token;
  } catch (error) {
    return false;
  }
}

/**
 * Validate OAuth state parameter to prevent CSRF
 */
export function generateOAuthState(): string {
  return Math.random().toString(36).substring(2, 15) + 
         Math.random().toString(36).substring(2, 15);
}

/**
 * Store OAuth state in cookie for validation
 */
export async function storeOAuthState(state: string): Promise<void> {
  const cookieStore = await cookies();
  cookieStore.set('oauth_state', state, {
    httpOnly: true,
    secure: process.env.NODE_ENV === 'production',
    sameSite: 'lax',
    maxAge: 60 * 10, // 10 minutes
    path: '/',
  });
}

/**
 * Validate OAuth state matches stored value
 */
export async function validateOAuthState(state: string): Promise<boolean> {
  try {
    const cookieStore = await cookies();
    const storedState = cookieStore.get('oauth_state');

    if (!storedState || storedState.value !== state) {
      return false;
    }

    // Clear the state cookie after validation
    cookieStore.delete('oauth_state');
    return true;
  } catch (error) {
    console.error('Error validating OAuth state:', error);
    return false;
  }
}

/**
 * Check if user needs to reconnect OAuth (token expired)
 */
export async function needsOAuthReconnect(userId: string): Promise<{
  gsc: boolean;
  ga4: boolean;
}> {
  try {
    const { gscToken, ga4Token } = await getOAuthTokens(userId);

    // Check if tokens exist and are not expired
    const gscNeedsReconnect = !gscToken || isTokenExpired(gscToken);
    const ga4NeedsReconnect = !ga4Token || isTokenExpired(ga4Token);

    return {
      gsc: gscNeedsReconnect,
      ga4: ga4NeedsReconnect,
    };
  } catch (error) {
    console.error('Error checking OAuth reconnect status:', error);
    return { gsc: true, ga4: true };
  }
}

/**
 * Helper to check if OAuth token is expired
 */
function isTokenExpired(token: any): boolean {
  if (!token || !token.expiry_date) {
    return true;
  }

  // Consider token expired if it expires in the next 5 minutes
  const expiryTime = new Date(token.expiry_date).getTime();
  const now = Date.now();
  const bufferMs = 5 * 60 * 1000; // 5 minutes

  return expiryTime - now < bufferMs;
}

/**
 * Refresh OAuth token if needed
 */
export async function refreshOAuthTokenIfNeeded(
  userId: string,
  tokenType: 'gsc' | 'ga4'
): Promise<any> {
  try {
    const tokens = await getOAuthTokens(userId);
    const token = tokenType === 'gsc' ? tokens.gscToken : tokens.ga4Token;

    if (!token) {
      throw new Error(`No ${tokenType} token found`);
    }

    if (!isTokenExpired(token)) {
      return token;
    }

    // Token is expired, refresh it via backend API
    const response = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/auth/refresh`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        userId,
        tokenType,
        refreshToken: token.refresh_token,
      }),
    });

    if (!response.ok) {
      throw new Error('Failed to refresh token');
    }

    const { token: newToken } = await response.json();

    // Store the new token
    if (tokenType === 'gsc') {
      await storeOAuthTokens(userId, newToken);
    } else {
      await storeOAuthTokens(userId, undefined, newToken);
    }

    return newToken;
  } catch (error) {
    console.error('Error refreshing OAuth token:', error);
    throw error;
  }
}
