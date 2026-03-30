# Search Intelligence Report - Frontend

A Next.js-based frontend for generating comprehensive Search Intelligence Reports from Google Search Console and GA4 data.

## Prerequisites

- Node.js 18+ and npm
- A running backend API (see `../backend/README.md`)
- Google OAuth credentials configured in the backend

## Getting Started

### 1. Install Dependencies

```bash
npm install
```

### 2. Environment Variables

Create a `.env.local` file in the `web` directory:

```bash
# Backend API
NEXT_PUBLIC_API_URL=http://localhost:8000

# For production:
# NEXT_PUBLIC_API_URL=https://your-railway-app.railway.app
```

### 3. Run Development Server

```bash
npm run dev
```

Open [http://localhost:3000](http://localhost:3000) in your browser.

## Project Structure

```
web/
├── public/              # Static assets
├── src/
│   ├── app/             # Next.js 14 app directory
│   │   ├── page.tsx     # Home page with OAuth connect flow
│   │   ├── layout.tsx   # Root layout
│   │   └── globals.css  # Global styles
│   ├── components/      # React components
│   │   ├── ConnectButton.tsx      # Google OAuth trigger button
│   │   ├── PropertyList.tsx       # Connected properties display
│   │   └── OAuthCallback.tsx      # OAuth callback handler
│   └── lib/             # Utilities
│       ├── api.ts       # Backend API client
│       └── types.ts     # TypeScript types
├── package.json
├── tsconfig.json
├── next.config.js
└── tailwind.config.js
```

## Current Features (Day 5)

### OAuth Connection Flow

1. **Connect Button** — Triggers Google OAuth flow for GSC + GA4 access
2. **Backend OAuth Handler** — Backend generates authorization URL with required scopes
3. **User Authorization** — User grants permissions in Google's consent screen
4. **Callback Processing** — Backend exchanges code for tokens and stores them
5. **Property List** — Display connected GSC properties after successful authentication

### API Integration

The frontend communicates with the FastAPI backend:

```typescript
// Trigger OAuth flow
GET /api/auth/google/url

// Handle OAuth callback (backend route)
GET /api/auth/google/callback?code=...

// Get connected properties
GET /api/properties
```

### Component Structure

#### `ConnectButton.tsx`
- Fetches OAuth URL from backend
- Opens authorization URL in current window
- Handles loading and error states

#### `PropertyList.tsx`
- Displays list of connected GSC properties
- Shows property URL and verification status
- Allows property selection for report generation (future feature)

#### `OAuthCallback.tsx`
- Processes OAuth callback
- Extracts authorization code
- Sends to backend for token exchange
- Redirects to home page with success/error state

## Styling

Using **Tailwind CSS** for basic styling:

- Clean, minimal design
- Responsive layout
- Focus on functionality over aesthetics (per spec: "Basic styling only")

## Development Workflow

### Adding New Components

```bash
# Create component file
touch src/components/MyComponent.tsx
```

```typescript
// src/components/MyComponent.tsx
export default function MyComponent() {
  return <div>My Component</div>;
}
```

### Making API Calls

Use the centralized API client:

```typescript
// src/lib/api.ts
import { apiClient } from '@/lib/api';

// In your component
const data = await apiClient.get('/endpoint');
```

### Type Safety

Define types in `src/lib/types.ts`:

```typescript
export interface Property {
  property_url: string;
  permission_level: string;
}

export interface AuthState {
  isAuthenticated: boolean;
  properties: Property[];
}
```

## Building for Production

```bash
npm run build
npm run start
```

## Deployment

### Railway Deployment

1. Connect your repository to Railway
2. Set environment variables:
   - `NEXT_PUBLIC_API_URL=https://your-backend.railway.app`
3. Railway will auto-detect Next.js and deploy

### Environment-Specific Configuration

```bash
# Development
NEXT_PUBLIC_API_URL=http://localhost:8000

# Production
NEXT_PUBLIC_API_URL=https://search-intel-api.railway.app
```

## Next Steps (Phase 1 Continuation)

### Immediate (Days 6-7)
- [ ] Property selection UI
- [ ] Report generation trigger button
- [ ] Loading state for report generation (2-5 min process)
- [ ] Basic error handling and user feedback

### Week 2
- [ ] Report viewing interface
- [ ] Basic charts (Recharts integration)
- [ ] Module 1 visualization (Health & Trajectory)
- [ ] Module 2 visualization (Page Triage)

### Week 3-4
- [ ] All Phase 1 module visualizations
- [ ] Collapsible section cards
- [ ] Download/export functionality
- [ ] Consulting CTA placement

## Common Issues

### CORS Errors

If you see CORS errors in the browser console:
- Ensure backend has proper CORS middleware configured
- Check that `NEXT_PUBLIC_API_URL` matches your backend URL
- Backend must include frontend origin in allowed origins

### OAuth Redirect Issues

- Ensure redirect URI in Google Cloud Console matches backend callback URL
- Format: `https://your-backend.railway.app/api/auth/google/callback`
- Must be exact match (including trailing slash if present)

### Environment Variables Not Loading

- Variables must be prefixed with `NEXT_PUBLIC_` to be exposed to browser
- Restart dev server after changing `.env.local`
- Variables are read at build time for production

## Tech Stack

- **Framework:** Next.js 14 (App Router)
- **Language:** TypeScript
- **Styling:** Tailwind CSS
- **Charts:** Recharts (to be added)
- **HTTP Client:** fetch API (native)
- **State Management:** React hooks (useState, useEffect)

## API Client Documentation

### `src/lib/api.ts`

Centralized API client with error handling:

```typescript
class APIClient {
  async get(endpoint: string): Promise<any>
  async post(endpoint: string, data: any): Promise<any>
  async put(endpoint: string, data: any): Promise<any>
  async delete(endpoint: string): Promise<any>
}

export const apiClient = new APIClient();
```

All methods:
- Automatically prefix with `NEXT_PUBLIC_API_URL`
- Include credentials for cookie-based auth
- Parse JSON responses
- Throw descriptive errors

## Testing

```bash
# Run type checking
npm run type-check

# Run linting
npm run lint

# Run build (catches build-time errors)
npm run build
```

## Contributing

This is a single-developer project (you). Key principles:

1. **Keep it simple** — Don't over-engineer
2. **Follow the spec** — Each feature maps to a module
3. **Production quality** — Proper error handling, no shortcuts
4. **Type safety** — Use TypeScript properly

## Resources

- [Next.js Documentation](https://nextjs.org/docs)
- [Tailwind CSS Documentation](https://tailwindcss.com/docs)
- [Recharts Documentation](https://recharts.org/)
- [Technical Spec](../docs/SPEC.md)

---

**Current Status:** Day 5 complete — OAuth flow working, property list displaying
**Next Milestone:** Report generation trigger (Day 6-7)