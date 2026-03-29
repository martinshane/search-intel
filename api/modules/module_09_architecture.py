"""
Module 9: Site Architecture & Authority Flow Analysis

Analyzes internal linking structure and authority distribution using graph analysis.
Identifies orphaned pages, authority sinks, and optimal link insertion opportunities.
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
import pandas as pd
import networkx as nx
from collections import defaultdict

logger = logging.getLogger(__name__)


@dataclass
class PageNode:
    """Represents a page in the site graph"""
    url: str
    title: Optional[str]
    clicks: int
    impressions: int
    avg_position: Optional[float]
    word_count: Optional[int]
    pagerank: float = 0.0


@dataclass
class LinkRecommendation:
    """Represents a recommended internal link"""
    target_page: str
    link_from: str
    suggested_anchor: str
    estimated_pagerank_boost: float
    reason: str


class SiteArchitectureAnalyzer:
    """Analyzes site architecture and authority flow"""
    
    def __init__(self):
        self.graph: Optional[nx.DiGraph] = None
        self.pagerank_scores: Dict[str, float] = {}
        
    def build_graph(
        self,
        link_data: List[Dict[str, str]],
        page_performance: pd.DataFrame
    ) -> nx.DiGraph:
        """
        Build directed graph from internal link data
        
        Args:
            link_data: List of dicts with 'from_url', 'to_url', 'anchor_text'
            page_performance: DataFrame with GSC performance data per page
            
        Returns:
            NetworkX directed graph
        """
        try:
            G = nx.DiGraph()
            
            # Add nodes with performance data
            for _, row in page_performance.iterrows():
                url = row['page']
                G.add_node(
                    url,
                    clicks=row.get('clicks', 0),
                    impressions=row.get('impressions', 0),
                    avg_position=row.get('position'),
                    title=row.get('title'),
                    word_count=row.get('word_count')
                )
            
            # Add edges (internal links)
            for link in link_data:
                from_url = link['from_url']
                to_url = link['to_url']
                anchor = link.get('anchor_text', '')
                
                if from_url in G and to_url in G:
                    G.add_edge(from_url, to_url, anchor_text=anchor)
            
            self.graph = G
            logger.info(f"Built graph with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges")
            
            return G
            
        except Exception as e:
            logger.error(f"Error building graph: {e}")
            raise
    
    def calculate_pagerank(
        self,
        alpha: float = 0.85,
        max_iter: int = 100
    ) -> Dict[str, float]:
        """
        Calculate PageRank for all pages
        
        Args:
            alpha: Damping factor (default 0.85)
            max_iter: Maximum iterations
            
        Returns:
            Dict mapping URL to PageRank score
        """
        try:
            if self.graph is None:
                raise ValueError("Graph not built yet. Call build_graph() first.")
            
            pagerank = nx.pagerank(
                self.graph,
                alpha=alpha,
                max_iter=max_iter
            )
            
            self.pagerank_scores = pagerank
            logger.info(f"Calculated PageRank for {len(pagerank)} pages")
            
            return pagerank
            
        except Exception as e:
            logger.error(f"Error calculating PageRank: {e}")
            raise
    
    def identify_authority_distribution(
        self,
        top_n: int = 20
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Identify pages by authority distribution
        
        Args:
            top_n: Number of top pages to return
            
        Returns:
            Dict with top_authority_pages, starved_pages, authority_sinks
        """
        try:
            if not self.pagerank_scores:
                raise ValueError("PageRank not calculated. Call calculate_pagerank() first.")
            
            # Sort pages by PageRank
            sorted_pages = sorted(
                self.pagerank_scores.items(),
                key=lambda x: x[1],
                reverse=True
            )
            
            # Top authority pages
            top_authority = []
            for url, pr in sorted_pages[:top_n]:
                node_data = self.graph.nodes[url]
                top_authority.append({
                    'url': url,
                    'pagerank': pr,
                    'clicks': node_data.get('clicks', 0),
                    'impressions': node_data.get('impressions', 0)
                })
            
            # Starved pages: high potential (impressions) but low PageRank
            starved_pages = []
            for url, pr in self.pagerank_scores.items():
                node_data = self.graph.nodes[url]
                impressions = node_data.get('impressions', 0)
                clicks = node_data.get('clicks', 0)
                
                # High impressions but low PageRank = starved
                if impressions > 1000 and pr < 0.001:
                    starved_pages.append({
                        'url': url,
                        'pagerank': pr,
                        'clicks': clicks,
                        'impressions': impressions,
                        'ctr': clicks / impressions if impressions > 0 else 0
                    })
            
            starved_pages.sort(key=lambda x: x['impressions'], reverse=True)
            
            # Authority sinks: high PageRank but low traffic value
            authority_sinks = []
            for url, pr in sorted_pages[:50]:  # Check top 50 by PageRank
                node_data = self.graph.nodes[url]
                clicks = node_data.get('clicks', 0)
                
                # High PageRank but low clicks = authority sink
                if pr > 0.002 and clicks < 100:
                    authority_sinks.append({
                        'url': url,
                        'pagerank': pr,
                        'clicks': clicks,
                        'wasted_authority': pr * 1000  # Relative metric
                    })
            
            return {
                'top_authority_pages': top_authority,
                'starved_pages': starved_pages[:top_n],
                'authority_sinks': authority_sinks[:top_n]
            }
            
        except Exception as e:
            logger.error(f"Error identifying authority distribution: {e}")
            raise
    
    def calculate_authority_flow_to_conversion(
        self,
        conversion_urls: List[str]
    ) -> float:
        """
        Calculate what percentage of authority flows to conversion pages
        
        Args:
            conversion_urls: List of money/conversion page URLs
            
        Returns:
            Float between 0 and 1 representing authority flow percentage
        """
        try:
            total_pr = sum(self.pagerank_scores.values())
            conversion_pr = sum(
                self.pagerank_scores.get(url, 0)
                for url in conversion_urls
                if url in self.pagerank_scores
            )
            
            return conversion_pr / total_pr if total_pr > 0 else 0.0
            
        except Exception as e:
            logger.error(f"Error calculating authority flow: {e}")
            raise
    
    def find_orphan_pages(
        self,
        sitemap_urls: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Identify orphan pages (no internal links pointing to them)
        
        Args:
            sitemap_urls: Optional list of URLs from sitemap
            
        Returns:
            List of orphan page data
        """
        try:
            orphans = []
            
            for node in self.graph.nodes():
                in_degree = self.graph.in_degree(node)
                
                if in_degree == 0:
                    node_data = self.graph.nodes[node]
                    orphans.append({
                        'url': node,
                        'clicks': node_data.get('clicks', 0),
                        'impressions': node_data.get('impressions', 0),
                        'in_sitemap': sitemap_urls is None or node in sitemap_urls
                    })
            
            # Sort by impressions (most impactful orphans first)
            orphans.sort(key=lambda x: x['impressions'], reverse=True)
            
            logger.info(f"Found {len(orphans)} orphan pages")
            
            return orphans
            
        except Exception as e:
            logger.error(f"Error finding orphan pages: {e}")
            raise
    
    def detect_content_silos(self) -> List[Dict[str, Any]]:
        """
        Use community detection to identify content silos
        
        Returns:
            List of content silo data
        """
        try:
            import community as community_louvain
            
            # Convert to undirected for community detection
            undirected = self.graph.to_undirected()
            
            # Detect communities
            partition = community_louvain.best_partition(undirected)
            
            # Group pages by community
            communities = defaultdict(list)
            for node, comm_id in partition.items():
                communities[comm_id].append(node)
            
            # Calculate metrics per silo
            silos = []
            total_pr = sum(self.pagerank_scores.values())
            
            for comm_id, pages in communities.items():
                silo_pr = sum(self.pagerank_scores.get(p, 0) for p in pages)
                
                # Try to infer silo name from URL patterns
                url_segments = defaultdict(int)
                for page in pages:
                    parts = page.strip('/').split('/')
                    if len(parts) > 1:
                        url_segments[parts[0]] += 1
                
                silo_name = max(url_segments, key=url_segments.get) if url_segments else f"silo_{comm_id}"
                
                silos.append({
                    'name': silo_name,
                    'pages': len(pages),
                    'internal_pagerank_share': silo_pr / total_pr if total_pr > 0 else 0,
                    'sample_pages': pages[:5]  # First 5 pages as examples
                })
            
            # Sort by PageRank share
            silos.sort(key=lambda x: x['internal_pagerank_share'], reverse=True)
            
            logger.info(f"Detected {len(silos)} content silos")
            
            return silos
            
        except ImportError:
            logger.warning("python-louvain not installed. Skipping community detection.")
            return []
        except Exception as e:
            logger.error(f"Error detecting content silos: {e}")
            raise
    
    def generate_link_recommendations(
        self,
        starved_pages: List[Dict[str, Any]],
        query_data: Optional[pd.DataFrame] = None,
        top_n: int = 10
    ) -> List[LinkRecommendation]:
        """
        Generate optimal link insertion recommendations
        
        Args:
            starved_pages: List of pages needing authority
            query_data: Optional GSC query data for topical matching
            top_n: Number of recommendations to return
            
        Returns:
            List of LinkRecommendation objects
        """
        try:
            recommendations = []
            
            # Get high-authority pages
            sorted_by_pr = sorted(
                self.pagerank_scores.items(),
                key=lambda x: x[1],
                reverse=True
            )
            high_authority_pages = [url for url, _ in sorted_by_pr[:50]]
            
            for starved in starved_pages[:top_n]:
                target_url = starved['url']
                
                # Find potential linking pages
                for source_url in high_authority_pages:
                    # Skip if already links
                    if self.graph.has_edge(source_url, target_url):
                        continue
                    
                    # Calculate potential PageRank boost (simplified)
                    source_pr = self.pagerank_scores.get(source_url, 0)
                    source_out_degree = self.graph.out_degree(source_url)
                    
                    # PageRank contribution = source_PR * damping / out_degree
                    potential_boost = source_pr * 0.85 / max(source_out_degree, 1)
                    
                    # Generate anchor text suggestion
                    target_data = self.graph.nodes[target_url]
                    suggested_anchor = target_data.get('title', target_url.split('/')[-1])
                    
                    recommendations.append(
                        LinkRecommendation(
                            target_page=target_url,
                            link_from=source_url,
                            suggested_anchor=suggested_anchor,
                            estimated_pagerank_boost=potential_boost,
                            reason="high_authority_source"
                        )
                    )
            
            # Sort by estimated boost
            recommendations.sort(key=lambda x: x.estimated_pagerank_boost, reverse=True)
            
            return recommendations[:top_n]
            
        except Exception as e:
            logger.error(f"Error generating link recommendations: {e}")
            raise
    
    def export_network_graph_data(self) -> Dict[str, Any]:
        """
        Export graph data for visualization (D3.js format)
        
        Returns:
            Dict with nodes and links arrays
        """
        try:
            nodes = []
            for node in self.graph.nodes():
                node_data = self.graph.nodes[node]
                nodes.append({
                    'id': node,
                    'pagerank': self.pagerank_scores.get(node, 0),
                    'clicks': node_data.get('clicks', 0),
                    'impressions': node_data.get('impressions', 0),
                    'title': node_data.get('title', '')
                })
            
            links = []
            for source, target in self.graph.edges():
                edge_data = self.graph[source][target]
                links.append({
                    'source': source,
                    'target': target,
                    'anchor_text': edge_data.get('anchor_text', '')
                })
            
            return {
                'nodes': nodes,
                'links': links
            }
            
        except Exception as e:
            logger.error(f"Error exporting network graph data: {e}")
            raise


def analyze_site_architecture(
    link_graph: List[Dict[str, str]],
    page_performance: pd.DataFrame,
    conversion_urls: Optional[List[str]] = None,
    sitemap_urls: Optional[List[str]] = None,
    query_data: Optional[pd.DataFrame] = None
) -> Dict[str, Any]:
    """
    Main analysis function for Module 9
    
    Args:
        link_graph: List of internal links (from_url, to_url, anchor_text)
        page_performance: DataFrame with GSC performance data
        conversion_urls: Optional list of conversion/money page URLs
        sitemap_urls: Optional list of URLs from sitemap
        query_data: Optional GSC query data for topical matching
        
    Returns:
        Complete architecture analysis results
    """
    try:
        logger.info("Starting site architecture analysis")
        
        analyzer = SiteArchitectureAnalyzer()
        
        # Build graph
        graph = analyzer.build_graph(link_graph, page_performance)
        
        # Calculate PageRank
        pagerank_scores = analyzer.calculate_pagerank()
        
        # Identify authority distribution
        authority_dist = analyzer.identify_authority_distribution()
        
        # Calculate authority flow to conversion pages
        authority_flow = 0.0
        if conversion_urls:
            authority_flow = analyzer.calculate_authority_flow_to_conversion(conversion_urls)
        
        # Find orphan pages
        orphan_pages = analyzer.find_orphan_pages(sitemap_urls)
        
        # Detect content silos
        content_silos = analyzer.detect_content_silos()
        
        # Generate link recommendations
        link_recommendations = analyzer.generate_link_recommendations(
            authority_dist['starved_pages'],
            query_data
        )
        
        # Export network graph data for visualization
        network_graph_data = analyzer.export_network_graph_data()
        
        results = {
            'pagerank_distribution': authority_dist,
            'authority_flow_to_conversion': authority_flow,
            'orphan_pages': orphan_pages,
            'content_silos': content_silos,
            'link_recommendations': [
                {
                    'target_page': rec.target_page,
                    'link_from': rec.link_from,
                    'suggested_anchor': rec.suggested_anchor,
                    'estimated_pagerank_boost': rec.estimated_pagerank_boost,
                    'reason': rec.reason
                }
                for rec in link_recommendations
            ],
            'network_graph_data': network_graph_data,
            'summary': {
                'total_pages': graph.number_of_nodes(),
                'total_internal_links': graph.number_of_edges(),
                'avg_internal_links_per_page': graph.number_of_edges() / graph.number_of_nodes() if graph.number_of_nodes() > 0 else 0,
                'orphan_pages_count': len(orphan_pages),
                'content_silos_detected': len(content_silos)
            }
        }
        
        logger.info("Site architecture analysis complete")
        
        return results
        
    except Exception as e:
        logger.error(f"Error in site architecture analysis: {e}")
        raise


# Example usage for testing
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Mock data for testing
    mock_links = [
        {'from_url': 'https://example.com/', 'to_url': 'https://example.com/about', 'anchor_text': 'About Us'},
        {'from_url': 'https://example.com/', 'to_url': 'https://example.com/blog', 'anchor_text': 'Blog'},
        {'from_url': 'https://example.com/blog', 'to_url': 'https://example.com/blog/post-1', 'anchor_text': 'Read More'},
    ]
    
    mock_performance = pd.DataFrame({
        'page': ['https://example.com/', 'https://example.com/about', 'https://example.com/blog', 'https://example.com/blog/post-1'],
        'clicks': [1000, 50, 200, 30],
        'impressions': [10000, 500, 3000, 2000],
        'position': [3.2, 12.5, 8.1, 15.3]
    })
    
    result = analyze_site_architecture(
        link_graph=mock_links,
        page_performance=mock_performance
    )
    
    print("Analysis complete:")
    print(f"Total pages: {result['summary']['total_pages']}")
    print(f"Total internal links: {result['summary']['total_internal_links']}")
    print(f"Orphan pages: {result['summary']['orphan_pages_count']}")
