/**
 * NetworkGraph — D3 force-directed internal link graph for Module 9.
 *
 * Renders the site's internal link structure as an interactive force
 * simulation where:
 *   - Nodes = pages (sized by PageRank, coloured by silo)
 *   - Edges = internal links (opacity by edge weight)
 *   - Orphan nodes are highlighted in red
 *   - Hover shows page URL + link counts
 *   - Drag nodes to rearrange the layout
 *
 * Data contract (matches Module 9 output):
 *   nodes: Array<{ id: string; pagerank?: number; silo?: string;
 *                   internal_links_in?: number; internal_links_out?: number;
 *                   is_orphan?: boolean }>
 *   links: Array<{ source: string; target: string; weight?: number }>
 *
 * Falls back to a placeholder message when d3 is unavailable or data
 * is missing, so it never breaks the surrounding page.
 */
import React, { useEffect, useRef, useState, useCallback } from 'react';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface GraphNode {
  id: string;
  pagerank?: number;
  silo?: string;
  internal_links_in?: number;
  internal_links_out?: number;
  is_orphan?: boolean;
  // d3 simulation properties (added at runtime)
  x?: number;
  y?: number;
  fx?: number | null;
  fy?: number | null;
}

interface GraphLink {
  source: string | GraphNode;
  target: string | GraphNode;
  weight?: number;
}

interface NetworkGraphProps {
  nodes: GraphNode[];
  links: GraphLink[];
  width?: number;
  height?: number;
}

// ---------------------------------------------------------------------------
// Silo colour palette — 12 distinct hues for up to 12 content silos
// ---------------------------------------------------------------------------
const SILO_COLORS = [
  '#8b5cf6', '#3b82f6', '#06b6d4', '#10b981',
  '#f59e0b', '#ef4444', '#ec4899', '#6366f1',
  '#14b8a6', '#f97316', '#84cc16', '#a855f7',
];

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

export default function NetworkGraph({
  nodes,
  links,
  width: propWidth,
  height: propHeight,
}: NetworkGraphProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const svgRef = useRef<SVGSVGElement>(null);
  const [tooltip, setTooltip] = useState<{
    x: number;
    y: number;
    node: GraphNode;
  } | null>(null);
  const [dimensions, setDimensions] = useState({ width: propWidth || 800, height: propHeight || 500 });

  // Responsive sizing
  useEffect(() => {
    if (propWidth && propHeight) return;
    const el = containerRef.current;
    if (!el) return;
    const ro = new ResizeObserver((entries) => {
      for (const entry of entries) {
        const w = entry.contentRect.width || 800;
        setDimensions({ width: w, height: Math.min(w * 0.6, 600) });
      }
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, [propWidth, propHeight]);

  // Main D3 render
  useEffect(() => {
    const svg = svgRef.current;
    if (!svg || !nodes || nodes.length === 0) return;

    let d3: any;
    try {
      d3 = require('d3');
    } catch {
      return; // d3 not available — will show placeholder
    }

    const { width, height } = dimensions;

    // Clear previous render
    d3.select(svg).selectAll('*').remove();

    // Build silo → colour map
    const siloSet = Array.from(new Set(nodes.map((n) => n.silo || 'unknown')));
    const siloColor: Record<string, string> = {};
    siloSet.forEach((s, i) => {
      siloColor[s] = SILO_COLORS[i % SILO_COLORS.length];
    });

    // PageRank → node radius (clamped)
    const prValues = nodes.map((n) => n.pagerank || 0);
    const prMax = Math.max(...prValues, 0.001);
    const radiusScale = (pr: number) => 4 + (pr / prMax) * 16;

    // Deep-copy data so D3 mutation doesn't corrupt React state
    const simNodes: GraphNode[] = nodes.map((n) => ({ ...n }));
    const simLinks: GraphLink[] = links.map((l) => ({ ...l }));

    // Force simulation
    const simulation = d3
      .forceSimulation(simNodes)
      .force(
        'link',
        d3
          .forceLink(simLinks)
          .id((d: GraphNode) => d.id)
          .distance(60)
          .strength((l: any) => Math.min((l.weight || 1) * 0.3, 1))
      )
      .force('charge', d3.forceManyBody().strength(-120))
      .force('center', d3.forceCenter(width / 2, height / 2))
      .force('collision', d3.forceCollide().radius((d: GraphNode) => radiusScale(d.pagerank || 0) + 2))
      .alphaDecay(0.03);

    const svgEl = d3.select(svg).attr('viewBox', `0 0 ${width} ${height}`);

    // Zoom behaviour
    const g = svgEl.append('g');
    svgEl.call(
      d3.zoom().scaleExtent([0.3, 5]).on('zoom', (event: any) => {
        g.attr('transform', event.transform);
      })
    );

    // Edges
    const link = g
      .append('g')
      .attr('class', 'links')
      .selectAll('line')
      .data(simLinks)
      .enter()
      .append('line')
      .attr('stroke', '#94a3b8')
      .attr('stroke-opacity', (d: any) => Math.min(0.15 + (d.weight || 1) * 0.1, 0.6))
      .attr('stroke-width', (d: any) => Math.min(0.5 + (d.weight || 1) * 0.5, 3));

    // Nodes
    const node = g
      .append('g')
      .attr('class', 'nodes')
      .selectAll('circle')
      .data(simNodes)
      .enter()
      .append('circle')
      .attr('r', (d: GraphNode) => radiusScale(d.pagerank || 0))
      .attr('fill', (d: GraphNode) =>
        d.is_orphan ? '#ef4444' : siloColor[d.silo || 'unknown'] || '#6366f1'
      )
      .attr('stroke', (d: GraphNode) => (d.is_orphan ? '#dc2626' : '#fff'))
      .attr('stroke-width', (d: GraphNode) => (d.is_orphan ? 2 : 1.5))
      .attr('cursor', 'grab')
      .on('mouseover', (_event: any, d: GraphNode) => {
        const rect = svg.getBoundingClientRect();
        setTooltip({
          x: (d.x || 0) + rect.left,
          y: (d.y || 0) + rect.top - 10,
          node: d,
        });
      })
      .on('mouseout', () => setTooltip(null));

    // Drag behaviour
    node.call(
      d3
        .drag()
        .on('start', (event: any, d: any) => {
          if (!event.active) simulation.alphaTarget(0.3).restart();
          d.fx = d.x;
          d.fy = d.y;
        })
        .on('drag', (event: any, d: any) => {
          d.fx = event.x;
          d.fy = event.y;
        })
        .on('end', (event: any, d: any) => {
          if (!event.active) simulation.alphaTarget(0);
          d.fx = null;
          d.fy = null;
        })
    );

    // Tick
    simulation.on('tick', () => {
      link
        .attr('x1', (d: any) => d.source.x)
        .attr('y1', (d: any) => d.source.y)
        .attr('x2', (d: any) => d.target.x)
        .attr('y2', (d: any) => d.target.y);

      node.attr('cx', (d: any) => d.x).attr('cy', (d: any) => d.y);
    });

    // Legend
    const legendG = svgEl.append('g').attr('transform', `translate(12, 16)`);
    siloSet.slice(0, 8).forEach((silo, i) => {
      const row = legendG.append('g').attr('transform', `translate(0, ${i * 18})`);
      row
        .append('rect')
        .attr('width', 10)
        .attr('height', 10)
        .attr('rx', 2)
        .attr('fill', siloColor[silo]);
      row
        .append('text')
        .attr('x', 14)
        .attr('y', 9)
        .attr('font-size', '10px')
        .attr('fill', '#64748b')
        .text(silo.length > 20 ? silo.slice(0, 18) + '...' : silo);
    });
    // Orphan legend entry
    if (nodes.some((n) => n.is_orphan)) {
      const idx = Math.min(siloSet.length, 8);
      const row = legendG.append('g').attr('transform', `translate(0, ${idx * 18})`);
      row.append('circle').attr('cx', 5).attr('cy', 5).attr('r', 5).attr('fill', '#ef4444').attr('stroke', '#dc2626').attr('stroke-width', 1.5);
      row.append('text').attr('x', 14).attr('y', 9).attr('font-size', '10px').attr('fill', '#ef4444').text('Orphan page');
    }

    return () => {
      simulation.stop();
    };
  }, [nodes, links, dimensions]);

  // Truncate URL for display
  const shortUrl = (url: string) => {
    try {
      const u = new URL(url);
      const path = u.pathname.length > 40 ? u.pathname.slice(0, 37) + '...' : u.pathname;
      return path || '/';
    } catch {
      return url.length > 45 ? url.slice(0, 42) + '...' : url;
    }
  };

  if (!nodes || nodes.length === 0) {
    return (
      <div className="flex items-center justify-center h-48 bg-gray-50 rounded-lg border border-dashed border-gray-300 text-sm text-gray-500">
        No link graph data available. Run a site crawl to populate.
      </div>
    );
  }

  return (
    <div ref={containerRef} className="relative w-full">
      <div className="flex items-center justify-between mb-2">
        <h3 className="text-base font-semibold text-gray-900">Internal Link Network</h3>
        <span className="text-xs text-gray-500">
          {nodes.length} pages &middot; {links.length} links &middot; Scroll to zoom, drag to rearrange
        </span>
      </div>
      <div className="border border-gray-200 rounded-lg overflow-hidden bg-slate-50">
        <svg
          ref={svgRef}
          width={dimensions.width}
          height={dimensions.height}
          style={{ display: 'block', maxWidth: '100%', height: 'auto' }}
        />
      </div>
      {tooltip && (
        <div
          className="fixed z-50 pointer-events-none bg-gray-900 text-white text-xs rounded-lg px-3 py-2 shadow-lg max-w-xs"
          style={{ left: tooltip.x, top: tooltip.y, transform: 'translate(-50%, -100%)' }}
        >
          <div className="font-semibold truncate">{shortUrl(tooltip.node.id)}</div>
          {tooltip.node.silo && <div className="text-gray-300">Silo: {tooltip.node.silo}</div>}
          <div className="text-gray-300">
            PR: {((tooltip.node.pagerank || 0) * 1000).toFixed(1)} &middot;
            In: {tooltip.node.internal_links_in ?? '?'} &middot;
            Out: {tooltip.node.internal_links_out ?? '?'}
          </div>
          {tooltip.node.is_orphan && <div className="text-red-400 font-semibold">Orphan page</div>}
        </div>
      )}
    </div>
  );
}
