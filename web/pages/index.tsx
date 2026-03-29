import React from 'react';
import Head from 'next/head';

export default function Home() {
  return (
    <>
      <Head>
        <title>Search Intelligence Report</title>
        <meta name="description" content="Generate comprehensive search intelligence reports for your site" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <link rel="icon" href="/favicon.ico" />
      </Head>
      <main style={{
        minHeight: '100vh',
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        justifyContent: 'center',
        padding: '2rem',
        fontFamily: 'system-ui, -apple-system, sans-serif'
      }}>
        <h1 style={{
          fontSize: '3rem',
          fontWeight: 'bold',
          marginBottom: '1rem',
          textAlign: 'center'
        }}>
          Search Intelligence Report
        </h1>
        <p style={{
          fontSize: '1.25rem',
          color: '#666',
          textAlign: 'center',
          maxWidth: '600px',
          marginBottom: '2rem'
        }}>
          A comprehensive search intelligence platform combining GSC, GA4, and SERP data
          to deliver actionable insights for your organic search strategy.
        </p>
        <div style={{
          padding: '1rem 2rem',
          backgroundColor: '#f0f0f0',
          borderRadius: '8px',
          color: '#333'
        }}>
          Coming Soon
        </div>
      </main>
    </>
  );
}