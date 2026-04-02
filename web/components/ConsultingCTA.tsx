import React, { useState } from 'react';
import { AlertCircle, CheckCircle, Mail, Sparkles } from 'lucide-react';

interface ConsultingCTAProps {
  reportId?: string;
}

export const ConsultingCTA: React.FC<ConsultingCTAProps> = ({ reportId }) => {
  const [email, setEmail] = useState('');
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [submitState, setSubmitState] = useState<'idle' | 'success' | 'error'>('idle');
  const [errorMessage, setErrorMessage] = useState('');

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    
    if (!email || !email.includes('@')) {
      setSubmitState('error');
      setErrorMessage('Please enter a valid email address');
      return;
    }

    setIsSubmitting(true);
    setSubmitState('idle');
    setErrorMessage('');

    try {
      const response = await fetch('/api/consulting-leads', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          email,
          report_id: reportId || null,
        }),
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.error || 'Failed to submit');
      }

      setSubmitState('success');
      setEmail('');
      
      // Reset success message after 5 seconds
      setTimeout(() => {
        setSubmitState('idle');
      }, 5000);
    } catch (error) {
      setSubmitState('error');
      setErrorMessage(
        error instanceof Error 
          ? error.message 
          : 'Something went wrong. Please try again.'
      );
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="consulting-cta">
      <div className="consulting-cta__container">
        <div className="consulting-cta__header">
          <div className="consulting-cta__icon">
            <Sparkles size={32} />
          </div>
          <h2 className="consulting-cta__title">
            Need Expert Help Implementing These Recommendations?
          </h2>
          <p className="consulting-cta__subtitle">
            This report gives you the roadmap. We can help you execute it.
          </p>
        </div>

        <div className="consulting-cta__content">
          <div className="consulting-cta__services">
            <h3 className="consulting-cta__services-title">Our Search Consulting Services:</h3>
            <ul className="consulting-cta__services-list">
              <li className="consulting-cta__service-item">
                <div className="consulting-cta__service-icon">
                  <svg width="20" height="20" viewBox="0 0 20 20" fill="none">
                    <path d="M16.7 4.3L7.5 13.5L3.3 9.3" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"/>
                  </svg>
                </div>
                <div className="consulting-cta__service-text">
                  <strong>Technical SEO Audit & Implementation</strong>
                  <span>Deep-dive analysis of indexing, crawlability, site architecture, and Core Web Vitals with hands-on fixes</span>
                </div>
              </li>
              <li className="consulting-cta__service-item">
                <div className="consulting-cta__service-icon">
                  <svg width="20" height="20" viewBox="0 0 20 20" fill="none">
                    <path d="M16.7 4.3L7.5 13.5L3.3 9.3" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"/>
                  </svg>
                </div>
                <div className="consulting-cta__service-text">
                  <strong>Content Strategy & Execution</strong>
                  <span>Data-driven content planning, cannibalization resolution, and striking-distance keyword optimization</span>
                </div>
              </li>
              <li className="consulting-cta__service-item">
                <div className="consulting-cta__service-icon">
                  <svg width="20" height="20" viewBox="0 0 20 20" fill="none">
                    <path d="M16.7 4.3L7.5 13.5L3.3 9.3" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"/>
                  </svg>
                </div>
                <div className="consulting-cta__service-text">
                  <strong>Link Building & Digital PR</strong>
                  <span>White-hat link acquisition strategies, competitor backlink gap analysis, and authority building campaigns</span>
                </div>
              </li>
              <li className="consulting-cta__service-item">
                <div className="consulting-cta__service-icon">
                  <svg width="20" height="20" viewBox="0 0 20 20" fill="none">
                    <path d="M16.7 4.3L7.5 13.5L3.3 9.3" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"/>
                  </svg>
                </div>
                <div className="consulting-cta__service-text">
                  <strong>Algorithm Recovery & Monitoring</strong>
                  <span>Diagnose algorithm update impacts, create recovery roadmaps, and implement ongoing vulnerability protection</span>
                </div>
              </li>
            </ul>
          </div>

          <div className="consulting-cta__form-wrapper">
            <form onSubmit={handleSubmit} className="consulting-cta__form">
              <p className="consulting-cta__form-label">
                Get a custom proposal based on your report:
              </p>
              
              <div className="consulting-cta__input-group">
                <div className="consulting-cta__input-wrapper">
                  <Mail className="consulting-cta__input-icon" size={20} />
                  <input
                    type="email"
                    value={email}
                    onChange={(e) => setEmail(e.target.value)}
                    placeholder="your.email@company.com"
                    className="consulting-cta__input"
                    disabled={isSubmitting || submitState === 'success'}
                    required
                  />
                </div>
                <button
                  type="submit"
                  className="consulting-cta__submit"
                  disabled={isSubmitting || submitState === 'success'}
                >
                  {isSubmitting ? 'Submitting...' : submitState === 'success' ? 'Sent!' : 'Get Proposal'}
                </button>
              </div>

              {submitState === 'success' && (
                <div className="consulting-cta__message consulting-cta__message--success">
                  <CheckCircle size={18} />
                  <span>Thanks! We'll send you a custom proposal within 24 hours.</span>
                </div>
              )}

              {submitState === 'error' && (
                <div className="consulting-cta__message consulting-cta__message--error">
                  <AlertCircle size={18} />
                  <span>{errorMessage}</span>
                </div>
              )}

              <p className="consulting-cta__privacy">
                No spam, ever. We'll send you a personalized consulting proposal based on your report data.
              </p>
            </form>
          </div>
        </div>
      </div>

      <style jsx>{`
        .consulting-cta {
          margin: 4rem 0;
          padding: 3rem;
          background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
          border-radius: 16px;
          box-shadow: 0 20px 60px rgba(102, 126, 234, 0.3);
          position: relative;
          overflow: hidden;
        }

        .consulting-cta::before {
          content: '';
          position: absolute;
          top: -50%;
          right: -50%;
          width: 100%;
          height: 100%;
          background: radial-gradient(circle, rgba(255, 255, 255, 0.1) 0%, transparent 70%);
          pointer-events: none;
        }

        .consulting-cta__container {
          position: relative;
          z-index: 1;
        }

        .consulting-cta__header {
          text-align: center;
          margin-bottom: 3rem;
        }

        .consulting-cta__icon {
          display: inline-flex;
          align-items: center;
          justify-content: center;
          width: 64px;
          height: 64px;
          background: rgba(255, 255, 255, 0.2);
          backdrop-filter: blur(10px);
          border-radius: 16px;
          margin-bottom: 1.5rem;
          color: white;
        }

        .consulting-cta__title {
          font-size: 2rem;
          font-weight: 700;
          color: white;
          margin: 0 0 1rem 0;
          line-height: 1.2;
        }

        .consulting-cta__subtitle {
          font-size: 1.125rem;
          color: rgba(255, 255, 255, 0.9);
          margin: 0;
          font-weight: 400;
        }

        .consulting-cta__content {
          display: grid;
          grid-template-columns: 1fr 1fr;
          gap: 3rem;
          align-items: start;
        }

        .consulting-cta__services {
          background: rgba(255, 255, 255, 0.15);
          backdrop-filter: blur(10px);
          padding: 2rem;
          border-radius: 12px;
          border: 1px solid rgba(255, 255, 255, 0.2);
        }

        .consulting-cta__services-title {
          font-size: 1.125rem;
          font-weight: 600;
          color: white;
          margin: 0 0 1.5rem 0;
        }

        .consulting-cta__services-list {
          list-style: none;
          padding: 0;
          margin: 0;
          display: flex;
          flex-direction: column;
          gap: 1.5rem;
        }

        .consulting-cta__service-item {
          display: flex;
          gap: 1rem;
          align-items: start;
        }

        .consulting-cta__service-icon {
          flex-shrink: 0;
          width: 24px;
          height: 24px;
          background: rgba(255, 255, 255, 0.25);
          border-radius: 6px;
          display: flex;
          align-items: center;
          justify-content: center;
          color: white;
        }

        .consulting-cta__service-text {
          display: flex;
          flex-direction: column;
          gap: 0.25rem;
        }

        .consulting-cta__service-text strong {
          font-size: 1rem;
          font-weight: 600;
          color: white;
          display: block;
        }

        .consulting-cta__service-text span {
          font-size: 0.875rem;
          color: rgba(255, 255, 255, 0.85);
          line-height: 1.5;
        }

        .consulting-cta__form-wrapper {
          background: white;
          padding: 2rem;
          border-radius: 12px;
          box-shadow: 0 10px 40px rgba(0, 0, 0, 0.15);
        }

        .consulting-cta__form {
          display: flex;
          flex-direction: column;
          gap: 1.5rem;
        }

        .consulting-cta__form-label {
          font-size: 1rem;
          font-weight: 600;
          color: #1f2937;
          margin: 0;
        }

        .consulting-cta__input-group {
          display: flex;
          flex-direction: column;
          gap: 0.75rem;
        }

        .consulting-cta__input-wrapper {
          position: relative;
          display: flex;
          align-items: center;
        }

        .consulting-cta__input-icon {
          position: absolute;
          left: 1rem;
          color: #9ca3af;
          pointer-events: none;
        }

        .consulting-cta__input {
          width: 100%;
          padding: 0.875rem 1rem 0.875rem 3rem;
          border: 2px solid #e5e7eb;
          border-radius: 8px;
          font-size: 1rem;
          transition: all 0.2s;
          background: white;
        }

        .consulting-cta__input:focus {
          outline: none;
          border-color: #667eea;
          box-shadow: 0 0 0 3px rgba(102, 126, 234, 0.1);
        }

        .consulting-cta__input:disabled {
          background: #f9fafb;
          cursor: not-allowed;
        }

        .consulting-cta__submit {
          padding: 0.875rem 2rem;
          background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
          color: white;
          border: none;
          border-radius: 8px;
          font-size: 1rem;
          font-weight: 600;
          cursor: pointer;
          transition: all 0.2s;
          box-shadow: 0 4px 12px rgba(102, 126, 234, 0.3);
        }

        .consulting-cta__submit:hover:not(:disabled) {
          transform: translateY(-2px);
          box-shadow: 0 6px 20px rgba(102, 126, 234, 0.4);
        }

        .consulting-cta__submit:active:not(:disabled) {
          transform: translateY(0);
        }

        .consulting-cta__submit:disabled {
          opacity: 0.6;
          cursor: not-allowed;
          transform: none;
        }

        .consulting-cta__message {
          display: flex;
          align-items: center;
          gap: 0.5rem;
          padding: 0.75rem 1rem;
          border-radius: 8px;
          font-size: 0.875rem;
          font-weight: 500;
        }

        .consulting-cta__message--success {
          background: #d1fae5;
          color: #065f46;
        }

        .consulting-cta__message--error {
          background: #fee2e2;
          color: #991b1b;
        }

        .consulting-cta__privacy {
          font-size: 0.8125rem;
          color: #6b7280;
          margin: 0;
          text-align: center;
          line-height: 1.5;
        }

        @media (max-width: 1024px) {
          .consulting-cta__content {
            grid-template-columns: 1fr;
            gap: 2rem;
          }
        }

        @media (max-width: 768px) {
          .consulting-cta {
            padding: 2rem 1.5rem;
            margin: 3rem 0;
          }

          .consulting-cta__title {
            font-size: 1.5rem;
          }

          .consulting-cta__subtitle {
            font-size: 1rem;
          }

          .consulting-cta__services,
          .consulting-cta__form-wrapper {
            padding: 1.5rem;
          }

          .consulting-cta__input-group {
            gap: 0.75rem;
          }

          .consulting-cta__submit {
            width: 100%;
          }

          .consulting-cta__service-text strong {
            font-size: 0.9375rem;
          }

          .consulting-cta__service-text span {
            font-size: 0.8125rem;
          }
        }

        @media (max-width: 480px) {
          .consulting-cta {
            padding: 1.5rem 1rem;
          }

          .consulting-cta__title {
            font-size: 1.25rem;
          }

          .consulting-cta__icon {
            width: 48px;
            height: 48px;
          }
        }
      `}</style>
    </div>
  );
};

export default ConsultingCTA;