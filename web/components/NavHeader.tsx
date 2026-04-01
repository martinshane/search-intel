import { useRouter } from 'next/router';
import Link from 'next/link';
import { useState } from 'react';

/**
 * Shared navigation header for all authenticated pages.
 * Dark theme matching the app design.
 * 
 * Props:
 *   email – optional user email to display
 *   activePage – which nav link to highlight ('reports' | 'compare' | 'schedules' | 'home')
 */
interface NavHeaderProps {
  email?: string | null;
  activePage?: 'home' | 'reports' | 'compare' | 'schedules';
}

export default function NavHeader({ email, activePage }: NavHeaderProps) {
  const router = useRouter();
  const [mobileOpen, setMobileOpen] = useState(false);

  const links = [
    { href: '/reports', label: 'My Reports', key: 'reports' as const },
    { href: '/compare', label: 'Compare', key: 'compare' as const },
    { href: '/schedules', label: 'Schedules', key: 'schedules' as const },
  ];

  const linkClass = (key: string) =>
    key === activePage
      ? 'text-blue-400 font-medium'
      : 'text-slate-400 hover:text-white transition';

  return (
    <header className="border-b border-slate-800 bg-slate-950/80 backdrop-blur-sm sticky top-0 z-40">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 py-3 flex items-center justify-between">
        {/* Brand */}
        <Link href="/" className="flex items-center gap-2 text-white font-bold text-lg shrink-0">
          <svg className="w-6 h-6 text-blue-400" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
            <path strokeLinecap="round" strokeLinejoin="round" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
          </svg>
          <span className="hidden sm:inline">Search Intelligence</span>
          <span className="sm:hidden">Search Intel</span>
        </Link>

        {/* Desktop nav */}
        <nav className="hidden md:flex items-center gap-6 text-sm">
          {links.map((l) => (
            <Link key={l.key} href={l.href} className={linkClass(l.key)}>
              {l.label}
            </Link>
          ))}
          <Link
            href="/"
            className="flex items-center gap-1 px-3 py-1.5 bg-blue-600 hover:bg-blue-500 text-white rounded-lg text-sm font-medium transition"
          >
            <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M12 4v16m8-8H4" />
            </svg>
            New Report
          </Link>
        </nav>

        {/* User email + mobile toggle */}
        <div className="flex items-center gap-3">
          {email && (
            <span className="hidden lg:block text-xs text-slate-500 truncate max-w-[180px]">{email}</span>
          )}
          {/* Mobile hamburger */}
          <button
            onClick={() => setMobileOpen(!mobileOpen)}
            className="md:hidden p-2 text-slate-400 hover:text-white"
            aria-label="Toggle navigation"
          >
            {mobileOpen ? (
              <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
              </svg>
            ) : (
              <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M4 6h16M4 12h16M4 18h16" />
              </svg>
            )}
          </button>
        </div>
      </div>

      {/* Mobile menu */}
      {mobileOpen && (
        <div className="md:hidden border-t border-slate-800 bg-slate-950 px-4 pb-4 pt-2 space-y-1">
          {links.map((l) => (
            <Link
              key={l.key}
              href={l.href}
              onClick={() => setMobileOpen(false)}
              className={`block px-3 py-2 rounded-lg text-sm ${
                l.key === activePage
                  ? 'bg-slate-800 text-blue-400 font-medium'
                  : 'text-slate-400 hover:bg-slate-800 hover:text-white'
              }`}
            >
              {l.label}
            </Link>
          ))}
          <Link
            href="/"
            onClick={() => setMobileOpen(false)}
            className="block px-3 py-2 rounded-lg text-sm bg-blue-600 text-white font-medium text-center mt-2"
          >
            + New Report
          </Link>
          {email && (
            <p className="px-3 pt-2 text-xs text-slate-600 truncate">{email}</p>
          )}
        </div>
      )}
    </header>
  );
}
