import Head from 'next/head';
import Link from 'next/link';
import NavHeader from '../components/NavHeader';

export default function PrivacyPolicy() {
  return (
    <>
      <Head>
        <title>Privacy Policy — Search Intelligence Report</title>
        <meta name="description" content="Privacy policy for the Search Intelligence Report tool by Clanker Marketing." />
        <meta name="viewport" content="width=device-width, initial-scale=1.0" />
      </Head>

      <div className="min-h-screen bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900">
        <NavHeader />

        <main className="max-w-3xl mx-auto px-4 sm:px-6 lg:px-8 py-8 sm:py-12">
          <h1 className="text-3xl sm:text-4xl font-bold text-white mb-8">Privacy Policy</h1>
          <p className="text-sm text-slate-400 mb-8">Last updated: April 1, 2026</p>

          <div className="prose prose-invert max-w-none space-y-6 text-slate-300 leading-relaxed">

            <section>
              <h2 className="text-xl font-semibold text-white mt-8 mb-3">1. Who We Are</h2>
              <p>
                Search Intelligence Report is operated by Clanker Marketing (&ldquo;we&rdquo;,
                &ldquo;us&rdquo;, &ldquo;our&rdquo;). This privacy policy explains how we collect,
                use, and protect information when you use our free SEO analysis tool at
                this website (&ldquo;the Service&rdquo;).
              </p>
            </section>

            <section>
              <h2 className="text-xl font-semibold text-white mt-8 mb-3">2. Information We Collect</h2>

              <h3 className="text-lg font-medium text-slate-200 mt-4 mb-2">Account Information</h3>
              <p>
                When you sign in with Google, we receive your email address and basic
                profile information from Google. We use this solely to identify your
                account and associate reports with you.
              </p>

              <h3 className="text-lg font-medium text-slate-200 mt-4 mb-2">Google Search Console &amp; Analytics Data</h3>
              <p>
                With your explicit permission via Google OAuth, we access read-only data
                from your Google Search Console and Google Analytics 4 properties. This
                data is used exclusively to generate your Search Intelligence Report. We
                request only the minimum scopes necessary:
              </p>
              <ul className="list-disc list-inside space-y-1 ml-4 text-slate-400">
                <li>Google Search Console — read-only access to search performance data</li>
                <li>Google Analytics 4 — read-only access to traffic and engagement data</li>
              </ul>
              <p className="mt-3">
                We never modify, delete, or write to your Google Search Console or
                Google Analytics properties. Our access is strictly read-only.
              </p>

              <h3 className="text-lg font-medium text-slate-200 mt-4 mb-2">Report Data</h3>
              <p>
                The analysis results generated from your data are stored in our database
                so you can access your reports at any time. Report data includes
                aggregated statistics, trend analyses, and recommendations derived from
                your search performance data.
              </p>

              <h3 className="text-lg font-medium text-slate-200 mt-4 mb-2">Technical Data</h3>
              <p>
                We automatically collect standard technical information including IP
                address, browser type, and pages visited. This is used for security,
                debugging, and service improvement. We do not use tracking cookies for
                advertising purposes.
              </p>
            </section>

            <section>
              <h2 className="text-xl font-semibold text-white mt-8 mb-3">3. How We Use Your Information</h2>
              <p>We use the information we collect to:</p>
              <ul className="list-disc list-inside space-y-1 ml-4 text-slate-400">
                <li>Generate your Search Intelligence Reports</li>
                <li>Authenticate your account and protect against unauthorized access</li>
                <li>Send report results and scheduled report emails you have opted into</li>
                <li>Improve the accuracy and usefulness of our analysis</li>
                <li>Diagnose technical issues and maintain service reliability</li>
              </ul>
              <p className="mt-3">
                We do <strong className="text-white">not</strong> sell, rent, or share
                your personal information or search data with third parties for marketing
                purposes.
              </p>
            </section>

            <section>
              <h2 className="text-xl font-semibold text-white mt-8 mb-3">4. Third-Party Services</h2>
              <p>We use the following third-party services to operate the tool:</p>
              <ul className="list-disc list-inside space-y-1 ml-4 text-slate-400">
                <li><strong className="text-slate-200">Google APIs</strong> — to access your GSC and GA4 data with your authorization</li>
                <li><strong className="text-slate-200">DataForSEO</strong> — to retrieve public SERP data for competitive analysis (no personal data is shared)</li>
                <li><strong className="text-slate-200">Supabase</strong> — database hosting for storing accounts and reports</li>
                <li><strong className="text-slate-200">Railway</strong> — application hosting infrastructure</li>
                <li><strong className="text-slate-200">Anthropic (Claude API)</strong> — to generate narrative summaries in reports (only aggregated, anonymized data is sent)</li>
              </ul>
              <p className="mt-3">
                Each third-party service is bound by their own privacy policies. We only
                share the minimum data necessary for each service to function.
              </p>
            </section>

            <section>
              <h2 className="text-xl font-semibold text-white mt-8 mb-3">5. Data Storage &amp; Security</h2>
              <p>
                Your OAuth tokens are encrypted at rest. Report data is stored in a
                secured PostgreSQL database. All data transmission uses HTTPS/TLS
                encryption. We implement rate limiting and authentication checks on all
                API endpoints.
              </p>
              <p className="mt-3">
                Cached API responses (raw GSC/GA4 data) are automatically deleted after
                24 hours. Generated reports are retained until you delete them or close
                your account.
              </p>
            </section>

            <section>
              <h2 className="text-xl font-semibold text-white mt-8 mb-3">6. Your Rights &amp; Choices</h2>
              <p>You can at any time:</p>
              <ul className="list-disc list-inside space-y-1 ml-4 text-slate-400">
                <li><strong className="text-slate-200">Revoke access</strong> — disconnect your Google accounts from your profile page or directly from your Google account security settings</li>
                <li><strong className="text-slate-200">Delete reports</strong> — remove any generated report from your account</li>
                <li><strong className="text-slate-200">Delete your account</strong> — contact us to have all your data permanently removed</li>
                <li><strong className="text-slate-200">Export data</strong> — download your reports as PDF</li>
              </ul>
              <p className="mt-3">
                When you revoke Google access, we immediately stop accessing your
                GSC/GA4 data. Previously generated reports remain available until you
                delete them.
              </p>
            </section>

            <section>
              <h2 className="text-xl font-semibold text-white mt-8 mb-3">7. Data Retention</h2>
              <p>
                We retain your account information and reports for as long as your
                account is active. If you request account deletion, we will permanently
                remove all associated data within 30 days. Cached API responses are
                automatically purged after 24 hours regardless.
              </p>
            </section>

            <section>
              <h2 className="text-xl font-semibold text-white mt-8 mb-3">8. Children&rsquo;s Privacy</h2>
              <p>
                The Service is not directed at individuals under 18 years of age. We do
                not knowingly collect personal information from children. If you believe
                a child has provided us with personal data, please contact us and we
                will promptly delete it.
              </p>
            </section>

            <section>
              <h2 className="text-xl font-semibold text-white mt-8 mb-3">9. Changes to This Policy</h2>
              <p>
                We may update this privacy policy from time to time. When we make
                material changes, we will update the &ldquo;Last updated&rdquo; date at
                the top of this page. Continued use of the Service after changes
                constitutes acceptance of the updated policy.
              </p>
            </section>

            <section>
              <h2 className="text-xl font-semibold text-white mt-8 mb-3">10. Contact</h2>
              <p>
                If you have questions about this privacy policy or your data, contact us at:{' '}
                <a href="mailto:privacy@clankermarketing.com" className="text-blue-400 hover:text-blue-300 underline">
                  privacy@clankermarketing.com
                </a>
              </p>
            </section>

          </div>

          <div className="mt-12 pt-6 border-t border-slate-700">
            <Link href="/" className="text-blue-400 hover:text-blue-300 transition-colors">
              &larr; Back to Home
            </Link>
          </div>
        </main>

        <footer className="border-t border-slate-700 mt-12">
          <div className="max-w-3xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
            <p className="text-sm text-slate-400 text-center">
              &copy; 2026 Clanker Marketing. All rights reserved.
            </p>
          </div>
        </footer>
      </div>
    </>
  );
}
