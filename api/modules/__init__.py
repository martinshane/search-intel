"""
Modules package for the Search Intelligence Report.

This package contains all analysis modules that process GSC, GA4, and SERP data
to generate the comprehensive Search Intelligence Report.

Each module is a self-contained analysis unit that:
- Reads from the shared data store
- Applies specific statistical/ML techniques
- Returns structured results for report generation

Modules run sequentially as some depend on outputs from earlier modules.
"""

from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)

__version__ = "0.1.0"

# Module execution order (some modules depend on outputs from earlier modules)
MODULE_EXECUTION_ORDER = [
    "health_trajectory",      # Module 1: Foundation - trend analysis
    "page_triage",            # Module 2: Per-page analysis
    "serp_landscape",         # Module 3: SERP feature analysis
    "content_intelligence",   # Module 4: Content analysis
    "gameplan",               # Module 5: Synthesis of modules 1-4
    "algorithm_impact",       # Module 6: Algorithm update correlation
    "intent_migration",       # Module 7: Query intent shifts
    "ctr_modeling",           # Module 8: Context-aware CTR
    "site_architecture",      # Module 9: Link graph analysis
    "branded_split",          # Module 10: Branded vs non-branded
    "competitive_radar",      # Module 11: Competitor analysis
    "revenue_attribution",    # Module 12: Revenue modeling
]

# Module metadata
MODULE_INFO = {
    "health_trajectory": {
        "name": "Health & Trajectory",
        "description": "Time series decomposition, trend analysis, and forecasting",
        "dependencies": [],
    },
    "page_triage": {
        "name": "Page-Level Triage",
        "description": "Per-page trend analysis and CTR anomaly detection",
        "dependencies": [],
    },
    "serp_landscape": {
        "name": "SERP Landscape Analysis",
        "description": "SERP feature analysis and competitor mapping",
        "dependencies": [],
    },
    "content_intelligence": {
        "name": "Content Intelligence",
        "description": "Cannibalization detection and content gap analysis",
        "dependencies": [],
    },
    "gameplan": {
        "name": "The Gameplan",
        "description": "Prioritized action plan synthesis",
        "dependencies": ["health_trajectory", "page_triage", "serp_landscape", "content_intelligence"],
    },
    "algorithm_impact": {
        "name": "Algorithm Update Impact",
        "description": "Correlation with known algorithm updates",
        "dependencies": ["health_trajectory"],
    },
    "intent_migration": {
        "name": "Query Intent Migration",
        "description": "Intent classification and distribution over time",
        "dependencies": [],
    },
    "ctr_modeling": {
        "name": "CTR Modeling by SERP Context",
        "description": "Context-aware CTR prediction and optimization",
        "dependencies": ["serp_landscape"],
    },
    "site_architecture": {
        "name": "Site Architecture & Authority Flow",
        "description": "Internal link graph analysis and PageRank simulation",
        "dependencies": ["page_triage"],
    },
    "branded_split": {
        "name": "Branded vs Non-Branded Health",
        "description": "Separate analysis of branded and non-branded traffic",
        "dependencies": ["health_trajectory"],
    },
    "competitive_radar": {
        "name": "Competitive Threat Radar",
        "description": "Competitor tracking and emerging threat detection",
        "dependencies": ["serp_landscape"],
    },
    "revenue_attribution": {
        "name": "Revenue Attribution",
        "description": "Revenue modeling and ROI estimation",
        "dependencies": ["page_triage", "ctr_modeling", "gameplan"],
    },
}


def get_module_dependencies(module_name: str) -> list[str]:
    """
    Get the list of module dependencies for a given module.
    
    Args:
        module_name: Name of the module
        
    Returns:
        List of module names that must complete before this module runs
    """
    if module_name not in MODULE_INFO:
        logger.warning(f"Unknown module: {module_name}")
        return []
    
    return MODULE_INFO[module_name]["dependencies"]


def validate_module_order(modules: list[str]) -> bool:
    """
    Validate that modules are ordered correctly based on dependencies.
    
    Args:
        modules: List of module names in proposed execution order
        
    Returns:
        True if order is valid, False otherwise
    """
    completed = set()
    
    for module in modules:
        dependencies = get_module_dependencies(module)
        
        # Check if all dependencies have been completed
        for dep in dependencies:
            if dep not in completed:
                logger.error(
                    f"Module {module} depends on {dep}, "
                    f"but {dep} has not been executed yet"
                )
                return False
        
        completed.add(module)
    
    return True


__all__ = [
    "MODULE_EXECUTION_ORDER",
    "MODULE_INFO",
    "get_module_dependencies",
    "validate_module_order",
]