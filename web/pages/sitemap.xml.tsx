import { GetServerSideProps } from "next";

/**
 * Dynamic XML sitemap for Search Intelligence Report.
 *
 * Renders as /sitemap.xml via Next.js server-side rendering.
 * Lists all public pages with proper lastmod, changefreq, and priority.
 *
 * For an SEO tool targeting SEO professionals, having a proper sitemap
 * is table-stakes credibility.
 */

const SITE_URL = "https://clankermarketing.com";

interface SitemapEntry {
  loc: string;
  lastmod: string;
  changefreq: string;
  priority: string;
}

function generateSitemapXml(entries: SitemapEntry[]): string {
  return `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xsi:schemaLocation="http://www.sitemaps.org/schemas/sitemap/0.9
        http://www.sitemaps.org/schemas/sitemap/0.9/sitemap.xsd">
${entries
  .map(
    (e) => `  <url>
    <loc>${e.loc}</loc>
    <lastmod>${e.lastmod}</lastmod>
    <changefreq>${e.changefreq}</changefreq>
    <priority>${e.priority}</priority>
  </url>`
  )
  .join("\n")}
</urlset>`;
}

export const getServerSideProps: GetServerSideProps = async ({ res }) => {
  const today = new Date().toISOString().split("T")[0];

  const staticPages: SitemapEntry[] = [
    {
      loc: SITE_URL,
      lastmod: today,
      changefreq: "weekly",
      priority: "1.0",
    },
    {
      loc: `${SITE_URL}/report/demo`,
      lastmod: today,
      changefreq: "monthly",
      priority: "0.9",
    },
    {
      loc: `${SITE_URL}/privacy`,
      lastmod: "2026-03-29",
      changefreq: "yearly",
      priority: "0.3",
    },
    {
      loc: `${SITE_URL}/terms`,
      lastmod: "2026-03-29",
      changefreq: "yearly",
      priority: "0.3",
    },
  ];

  // Future enhancement: fetch published report IDs from Supabase
  // and add them as dynamic URLs with their creation dates.
  // For now, the demo report is the primary public-facing page.

  const sitemap = generateSitemapXml(staticPages);

  res.setHeader("Content-Type", "application/xml; charset=utf-8");
  res.setHeader("Cache-Control", "public, s-maxage=86400, stale-while-revalidate=43200");
  res.write(sitemap);
  res.end();

  return { props: {} };
};

// The page component is never rendered (we write directly to res above)
export default function Sitemap() {
  return null;
}