"""
Report Generator
Orchestrates the execution of all analysis modules and compiles the final report.
"""

import logging
from typing import Dict, Any, Optional
from datetime import datetime
import traceback
import json

from api.modules import (
    module_1_health_trajectory,
    module_2_page_triage,
    module_3_serp_landscape,
    module_4_content_intelligence,
    module_5_gameplan,
    module_6_algorithm_updates,
    module_7_query_intent,
    module_8_link_architecture,
    module_9_seasonality,
    module_10_revenue_impact,
    module_11_channel_analysis,
    module_12_predictive_model
)

logger = logging.getLogger(__name__)

# Define the complete module pipeline
MODULES = [
    {
        'id': 'module_1',
        'name': 'Health & Trajectory',
        'function': module_1_health_trajectory.analyze,
        'dependencies': []
    },
    {
        'id': 'module_2',
        'name': 'Page-Level Triage',
        'function': module_2_page_triage.analyze,
        'dependencies': ['module_1']
    },
    {
        'id': 'module_3',
        'name': 'SERP Landscape Analysis',
        'function': module_3_serp_landscape.analyze,
        'dependencies': []
    },
    {
        'id': 'module_4',
        'name': 'Content Intelligence',
        'function': module_4_content_intelligence.analyze,
        'dependencies': ['module_2']
    },
    {
        'id': 'module_5',
        'name': 'The Gameplan',
        'function': module_5_gameplan.analyze,
        'dependencies': ['module_1', 'module_2', 'module_3', 'module_4']
    },
    {
        'id': 'module_6',
        'name': 'Algorithm Update Impact',
        'function': module_6_algorithm_updates.analyze,
        'dependencies': ['module_1']
    },
    {
        'id': 'module_7',
        'name': 'Query Intent Migration',
        'function': module_7_query_intent.analyze,
        'dependencies': []
    },
    {
        'id': 'module_8',
        'name': 'Internal Link Architecture',
        'function': module_8_link_architecture.analyze,
        'dependencies': ['module_2']
    },
    {
        'id': 'module_9',
        'name': 'Seasonality Intelligence',
        'function': module_9_seasonality.analyze,
        'dependencies': ['module_1']
    },
    {
        'id': 'module_10',
        'name': 'Revenue Impact Analysis',
        'function': module_10_revenue_impact.analyze,
        'dependencies': ['module_1', 'module_2']
    },
    {
        'id': 'module_11',
        'name': 'Channel Synergy Analysis',
        'function': module_11_channel_analysis.analyze,
        'dependencies': []
    },
    {
        'id': 'module_12',
        'name': 'Predictive Intelligence',
        'function': module_12_predictive_model.analyze,
        'dependencies': ['module_1', 'module_2', 'module_3', 'module_4', 'module_9']
    }
]


class ReportGenerator:
    """
    Manages the execution of all analysis modules and compiles the final report.
    """
    
    def __init__(self, data: Dict[str, Any]):
        """
        Initialize the report generator with fetched data.
        
        Args:
            data: Dictionary containing all fetched data:
                - gsc_data: Google Search Console data
                - ga4_data: Google Analytics 4 data
                - serp_data: SERP data from DataForSEO
                - crawl_data: Site crawl data
                - site_url: The site URL being analyzed
        """
        self.data = data
        self.results = {}
        self.errors = {}
        self.execution_log = []
        
    def _log_execution(self, module_id: str, status: str, message: str = "", duration: float = 0):
        """Log module execution details."""
        entry = {
            'module_id': module_id,
            'status': status,
            'message': message,
            'duration_seconds': duration,
            'timestamp': datetime.utcnow().isoformat()
        }
        self.execution_log.append(entry)
        logger.info(f"Module {module_id}: {status} - {message} ({duration:.2f}s)")
        
    def _check_dependencies(self, module: Dict[str, Any]) -> bool:
        """
        Check if all dependencies for a module have been successfully executed.
        
        Args:
            module: Module configuration dictionary
            
        Returns:
            bool: True if all dependencies are satisfied
        """
        for dep_id in module['dependencies']:
            if dep_id not in self.results:
                logger.warning(f"Module {module['id']} dependency {dep_id} not satisfied")
                return False
        return True
        
    def _prepare_module_input(self, module: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepare input data for a specific module based on its dependencies.
        
        Args:
            module: Module configuration dictionary
            
        Returns:
            Dictionary of input data for the module
        """
        module_input = {
            'gsc_data': self.data.get('gsc_data'),
            'ga4_data': self.data.get('ga4_data'),
            'serp_data': self.data.get('serp_data'),
            'crawl_data': self.data.get('crawl_data'),
            'site_url': self.data.get('site_url')
        }
        
        # Add outputs from dependency modules
        for dep_id in module['dependencies']:
            if dep_id in self.results:
                module_input[dep_id] = self.results[dep_id]
                
        return module_input
        
    def _execute_module(self, module: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Execute a single analysis module.
        
        Args:
            module: Module configuration dictionary
            
        Returns:
            Module results or None if execution failed
        """
        module_id = module['id']
        module_name = module['name']
        
        start_time = datetime.utcnow()
        
        try:
            logger.info(f"Starting {module_name} ({module_id})...")
            
            # Check dependencies
            if not self._check_dependencies(module):
                error_msg = f"Dependencies not satisfied: {module['dependencies']}"
                self._log_execution(module_id, 'SKIPPED', error_msg, 0)
                self.errors[module_id] = {
                    'error': 'DependencyError',
                    'message': error_msg
                }
                return None
                
            # Prepare input data
            module_input = self._prepare_module_input(module)
            
            # Execute the module
            result = module['function'](module_input)
            
            # Calculate duration
            duration = (datetime.utcnow() - start_time).total_seconds()
            
            # Log success
            self._log_execution(module_id, 'SUCCESS', '', duration)
            
            return result
            
        except Exception as e:
            duration = (datetime.utcnow() - start_time).total_seconds()
            error_msg = str(e)
            error_trace = traceback.format_exc()
            
            logger.error(f"Error in {module_name} ({module_id}): {error_msg}\n{error_trace}")
            
            self._log_execution(module_id, 'ERROR', error_msg, duration)
            
            self.errors[module_id] = {
                'error': type(e).__name__,
                'message': error_msg,
                'traceback': error_trace
            }
            
            return None
            
    def generate_report(self) -> Dict[str, Any]:
        """
        Execute all modules in sequence and compile the final report.
        
        Returns:
            Complete report dictionary with all module results
        """
        logger.info("Starting report generation...")
        start_time = datetime.utcnow()
        
        # Execute each module in sequence
        for module in MODULES:
            result = self._execute_module(module)
            if result is not None:
                self.results[module['id']] = result
                
        # Calculate total execution time
        total_duration = (datetime.utcnow() - start_time).total_seconds()
        
        # Compile final report
        report = {
            'metadata': {
                'site_url': self.data.get('site_url'),
                'generated_at': datetime.utcnow().isoformat(),
                'total_execution_time_seconds': total_duration,
                'modules_executed': len(self.results),
                'modules_failed': len(self.errors),
                'execution_log': self.execution_log
            },
            'results': self.results,
            'errors': self.errors if self.errors else None,
            'summary': self._generate_summary()
        }
        
        logger.info(f"Report generation completed in {total_duration:.2f}s")
        logger.info(f"Modules executed: {len(self.results)}/{len(MODULES)}")
        
        if self.errors:
            logger.warning(f"Modules with errors: {list(self.errors.keys())}")
            
        return report
        
    def _generate_summary(self) -> Dict[str, Any]:
        """
        Generate a high-level summary of the report.
        
        Returns:
            Summary dictionary with key metrics and findings
        """
        summary = {
            'status': 'partial' if self.errors else 'complete',
            'modules_completed': len(self.results),
            'modules_total': len(MODULES)
        }
        
        # Extract key metrics from module results
        try:
            # Health & Trajectory summary
            if 'module_1' in self.results:
                health = self.results['module_1']
                summary['overall_direction'] = health.get('overall_direction')
                summary['trend_slope_pct_per_month'] = health.get('trend_slope_pct_per_month')
                if 'forecast' in health and '30d' in health['forecast']:
                    summary['projected_30d_clicks'] = health['forecast']['30d'].get('clicks')
                    
            # Page Triage summary
            if 'module_2' in self.results:
                triage = self.results['module_2']
                if 'summary' in triage:
                    summary['total_pages_analyzed'] = triage['summary'].get('total_pages_analyzed')
                    summary['pages_critical'] = triage['summary'].get('critical', 0)
                    summary['pages_decaying'] = triage['summary'].get('decaying', 0)
                    summary['recoverable_clicks_monthly'] = triage['summary'].get('total_recoverable_clicks_monthly')
                    
            # SERP Landscape summary
            if 'module_3' in self.results:
                serp = self.results['module_3']
                summary['keywords_analyzed'] = serp.get('keywords_analyzed')
                summary['total_click_share'] = serp.get('total_click_share')
                summary['click_share_opportunity'] = serp.get('click_share_opportunity')
                
            # Content Intelligence summary
            if 'module_4' in self.results:
                content = self.results['module_4']
                if 'cannibalization_clusters' in content:
                    summary['cannibalization_issues'] = len(content['cannibalization_clusters'])
                if 'striking_distance' in content:
                    summary['striking_distance_opportunities'] = len(content['striking_distance'])
                    
            # Gameplan summary
            if 'module_5' in self.results:
                gameplan = self.results['module_5']
                summary['total_action_items'] = (
                    len(gameplan.get('critical', [])) +
                    len(gameplan.get('quick_wins', [])) +
                    len(gameplan.get('strategic', [])) +
                    len(gameplan.get('structural', []))
                )
                summary['critical_actions'] = len(gameplan.get('critical', []))
                summary['estimated_monthly_click_recovery'] = gameplan.get('total_estimated_monthly_click_recovery')
                summary['estimated_monthly_click_growth'] = gameplan.get('total_estimated_monthly_click_growth')
                
            # Algorithm Update summary
            if 'module_6' in self.results:
                algo = self.results['module_6']
                if 'updates_impacting_site' in algo:
                    summary['algorithm_updates_detected'] = len(algo['updates_impacting_site'])
                summary['vulnerability_score'] = algo.get('vulnerability_score')
                
            # Revenue Impact summary
            if 'module_10' in self.results:
                revenue = self.results['module_10']
                if 'revenue_attribution' in revenue:
                    summary['total_search_revenue'] = revenue['revenue_attribution'].get('total_search_revenue')
                    summary['search_revenue_trend_pct'] = revenue['revenue_attribution'].get('trend_pct_change')
                if 'forecast' in revenue and '30d' in revenue['forecast']:
                    summary['projected_30d_revenue'] = revenue['forecast']['30d'].get('revenue')
                if 'recoverable_revenue' in revenue:
                    summary['recoverable_revenue_monthly'] = revenue['recoverable_revenue'].get('total_monthly')
                    
            # Seasonality summary
            if 'module_9' in self.results:
                seasonality = self.results['module_9']
                if 'detected_patterns' in seasonality:
                    summary['seasonal_patterns_detected'] = len(seasonality['detected_patterns'])
                if 'next_peak' in seasonality:
                    summary['next_seasonal_peak'] = seasonality['next_peak'].get('date')
                    
            # Predictive Model summary
            if 'module_12' in self.results:
                predictive = self.results['module_12']
                if 'risk_assessment' in predictive:
                    summary['overall_risk_score'] = predictive['risk_assessment'].get('overall_risk_score')
                if 'opportunity_score' in predictive:
                    summary['opportunity_score'] = predictive['opportunity_score']
                    
        except Exception as e:
            logger.error(f"Error generating summary: {e}")
            summary['summary_error'] = str(e)
            
        return summary
        
    def save_report(self, output_path: str):
        """
        Save the generated report to a JSON file.
        
        Args:
            output_path: Path where the report should be saved
        """
        try:
            report = self.generate_report()
            
            with open(output_path, 'w') as f:
                json.dump(report, f, indent=2, default=str)
                
            logger.info(f"Report saved to {output_path}")
            
        except Exception as e:
            logger.error(f"Error saving report: {e}")
            raise


def generate_report(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Main entry point for report generation.
    
    Args:
        data: Dictionary containing all fetched data
        
    Returns:
        Complete report dictionary
    """
    generator = ReportGenerator(data)
    return generator.generate_report()


def validate_input_data(data: Dict[str, Any]) -> tuple[bool, list[str]]:
    """
    Validate that input data contains all required components.
    
    Args:
        data: Input data dictionary
        
    Returns:
        Tuple of (is_valid, list of error messages)
    """
    errors = []
    
    # Check required data sources
    if not data.get('gsc_data'):
        errors.append("Missing GSC data")
        
    if not data.get('ga4_data'):
        errors.append("Missing GA4 data")
        
    if not data.get('site_url'):
        errors.append("Missing site URL")
        
    # Validate GSC data structure
    if data.get('gsc_data'):
        gsc = data['gsc_data']
        required_gsc_keys = ['daily_data', 'page_data', 'query_data']
        for key in required_gsc_keys:
            if key not in gsc:
                errors.append(f"GSC data missing '{key}'")
                
    # Validate GA4 data structure
    if data.get('ga4_data'):
        ga4 = data['ga4_data']
        required_ga4_keys = ['landing_pages', 'traffic_overview']
        for key in required_ga4_keys:
            if key not in ga4:
                errors.append(f"GA4 data missing '{key}'")
                
    is_valid = len(errors) == 0
    return is_valid, errors


def get_module_status(module_id: str, results: Dict[str, Any], errors: Dict[str, Any]) -> str:
    """
    Get the execution status of a specific module.
    
    Args:
        module_id: Module identifier
        results: Results dictionary
        errors: Errors dictionary
        
    Returns:
        Status string: 'success', 'error', or 'not_executed'
    """
    if module_id in results:
        return 'success'
    elif module_id in errors:
        return 'error'
    else:
        return 'not_executed'
