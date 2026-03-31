"""
Parallel execution pipeline for analysis modules.

Implements dependency-aware parallel execution:
- Independent modules run concurrently using asyncio
- Modules with dependencies wait for their prerequisites
- Progress tracking and error handling for each module
"""

import asyncio
import time
from typing import Dict, List, Callable, Any, Optional
from dataclasses import dataclass
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class ModuleStatus(Enum):
    """Execution status for a module."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETE = "complete"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class ModuleResult:
    """Result from a module execution."""
    module_name: str
    status: ModuleStatus
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    duration_seconds: Optional[float] = None
    started_at: Optional[float] = None
    completed_at: Optional[float] = None


@dataclass
class ModuleDefinition:
    """Definition of an analysis module."""
    name: str
    func: Callable
    dependencies: List[str]
    timeout_seconds: int = 300  # 5 minutes default


class PipelineExecutor:
    """
    Executes analysis modules in parallel where possible.
    
    Analyzes dependency graph and runs independent modules concurrently.
    Tracks progress and handles failures gracefully.
    """
    
    def __init__(self, modules: List[ModuleDefinition]):
        """
        Initialize pipeline with module definitions.
        
        Args:
            modules: List of module definitions with dependencies
        """
        self.modules = {m.name: m for m in modules}
        self.results: Dict[str, ModuleResult] = {}
        self.progress_callback: Optional[Callable] = None
        
        # Validate dependency graph
        self._validate_dependencies()
    
    def _validate_dependencies(self) -> None:
        """Validate that all dependencies exist and there are no cycles."""
        for module in self.modules.values():
            for dep in module.dependencies:
                if dep not in self.modules:
                    raise ValueError(
                        f"Module '{module.name}' depends on unknown module '{dep}'"
                    )
        
        # Check for cycles using DFS
        visited = set()
        rec_stack = set()
        
        def has_cycle(name: str) -> bool:
            visited.add(name)
            rec_stack.add(name)
            
            for dep in self.modules[name].dependencies:
                if dep not in visited:
                    if has_cycle(dep):
                        return True
                elif dep in rec_stack:
                    return True
            
            rec_stack.remove(name)
            return False
        
        for module_name in self.modules:
            if module_name not in visited:
                if has_cycle(module_name):
                    raise ValueError("Circular dependency detected in module graph")
    
    def set_progress_callback(self, callback: Callable[[str, ModuleStatus, Optional[float]], None]) -> None:
        """
        Set callback for progress updates.
        
        Args:
            callback: Function called with (module_name, status, progress_pct)
        """
        self.progress_callback = callback
    
    def _update_progress(self, module_name: str, status: ModuleStatus, progress_pct: Optional[float] = None) -> None:
        """Update progress via callback if set."""
        if self.progress_callback:
            try:
                self.progress_callback(module_name, status, progress_pct)
            except Exception as e:
                logger.warning(f"Progress callback failed: {e}")
    
    def _get_ready_modules(self) -> List[str]:
        """
        Get list of modules ready to execute.
        
        A module is ready if:
        - It hasn't been started yet
        - All its dependencies are complete
        """
        ready = []
        
        for name, module in self.modules.items():
            # Skip if already processed
            if name in self.results:
                continue
            
            # Check if all dependencies are complete
            deps_complete = all(
                dep in self.results and 
                self.results[dep].status == ModuleStatus.COMPLETE
                for dep in module.dependencies
            )
            
            if deps_complete:
                ready.append(name)
        
        return ready
    
    async def _execute_module(
        self,
        module_name: str,
        shared_data: Dict[str, Any]
    ) -> ModuleResult:
        """
        Execute a single module.
        
        Args:
            module_name: Name of module to execute
            shared_data: Shared data context passed to all modules
        
        Returns:
            ModuleResult with execution outcome
        """
        module = self.modules[module_name]
        result = ModuleResult(
            module_name=module_name,
            status=ModuleStatus.RUNNING,
            started_at=time.time()
        )
        
        logger.info(f"Starting module: {module_name}")
        self._update_progress(module_name, ModuleStatus.RUNNING, 0.0)
        
        try:
            # Gather dependency results
            dep_results = {
                dep: self.results[dep].data
                for dep in module.dependencies
                if dep in self.results and self.results[dep].data is not None
            }
            
            # Execute module function with timeout
            module_data = await asyncio.wait_for(
                self._run_module_func(module.func, shared_data, dep_results),
                timeout=module.timeout_seconds
            )
            
            result.status = ModuleStatus.COMPLETE
            result.data = module_data
            result.completed_at = time.time()
            result.duration_seconds = result.completed_at - result.started_at
            
            logger.info(
                f"Completed module: {module_name} "
                f"in {result.duration_seconds:.2f}s"
            )
            self._update_progress(module_name, ModuleStatus.COMPLETE, 100.0)
            
        except asyncio.TimeoutError:
            result.status = ModuleStatus.FAILED
            result.error = f"Module timed out after {module.timeout_seconds}s"
            result.completed_at = time.time()
            result.duration_seconds = result.completed_at - result.started_at
            logger.error(f"Module {module_name} timed out")
            self._update_progress(module_name, ModuleStatus.FAILED, None)
            
        except Exception as e:
            result.status = ModuleStatus.FAILED
            result.error = str(e)
            result.completed_at = time.time()
            result.duration_seconds = result.completed_at - result.started_at
            logger.error(f"Module {module_name} failed: {e}", exc_info=True)
            self._update_progress(module_name, ModuleStatus.FAILED, None)
        
        return result
    
    async def _run_module_func(
        self,
        func: Callable,
        shared_data: Dict[str, Any],
        dep_results: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Run module function, handling both sync and async functions.
        
        Args:
            func: Module function to execute
            shared_data: Shared data context
            dep_results: Results from dependency modules
        
        Returns:
            Module result data
        """
        # Check if function is async
        if asyncio.iscoroutinefunction(func):
            return await func(shared_data, dep_results)
        else:
            # Run sync function in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(
                None,
                func,
                shared_data,
                dep_results
            )
    
    async def execute(
        self,
        shared_data: Dict[str, Any],
        max_concurrent: int = 4
    ) -> Dict[str, ModuleResult]:
        """
        Execute all modules with dependency-aware parallelization.
        
        Args:
            shared_data: Shared data context available to all modules
            max_concurrent: Maximum number of modules to run concurrently
        
        Returns:
            Dict mapping module names to their results
        """
        start_time = time.time()
        logger.info(
            f"Starting pipeline execution with {len(self.modules)} modules, "
            f"max_concurrent={max_concurrent}"
        )
        
        # Semaphore to limit concurrent executions
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def run_with_semaphore(module_name: str) -> ModuleResult:
            async with semaphore:
                return await self._execute_module(module_name, shared_data)
        
        # Execute modules in waves based on dependencies
        while len(self.results) < len(self.modules):
            ready_modules = self._get_ready_modules()
            
            if not ready_modules:
                # Check if we're stuck due to failures
                incomplete = set(self.modules.keys()) - set(self.results.keys())
                if incomplete:
                    # Mark remaining modules as skipped
                    for module_name in incomplete:
                        self.results[module_name] = ModuleResult(
                            module_name=module_name,
                            status=ModuleStatus.SKIPPED,
                            error="Skipped due to dependency failures"
                        )
                        logger.warning(f"Skipping module {module_name} due to dependency failures")
                break
            
            # Execute ready modules concurrently
            tasks = [
                run_with_semaphore(module_name)
                for module_name in ready_modules
            ]
            
            # Wait for this wave to complete
            wave_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Store results
            for result in wave_results:
                if isinstance(result, Exception):
                    logger.error(f"Unexpected error in module execution: {result}")
                    continue
                self.results[result.module_name] = result
        
        total_duration = time.time() - start_time
        
        # Log summary
        complete_count = sum(
            1 for r in self.results.values()
            if r.status == ModuleStatus.COMPLETE
        )
        failed_count = sum(
            1 for r in self.results.values()
            if r.status == ModuleStatus.FAILED
        )
        skipped_count = sum(
            1 for r in self.results.values()
            if r.status == ModuleStatus.SKIPPED
        )
        
        logger.info(
            f"Pipeline execution complete in {total_duration:.2f}s: "
            f"{complete_count} complete, {failed_count} failed, {skipped_count} skipped"
        )
        
        return self.results
    
    def get_execution_graph(self) -> Dict[str, Any]:
        """
        Get execution graph for visualization.
        
        Returns:
            Graph structure with nodes and edges
        """
        nodes = []
        edges = []
        
        for name, module in self.modules.items():
            result = self.results.get(name)
            
            node = {
                "id": name,
                "status": result.status.value if result else "pending",
                "duration": result.duration_seconds if result else None,
            }
            nodes.append(node)
            
            for dep in module.dependencies:
                edges.append({
                    "from": dep,
                    "to": name
                })
        
        return {
            "nodes": nodes,
            "edges": edges
        }


def create_standard_pipeline() -> PipelineExecutor:
    """
    Create the standard 12-module analysis pipeline.
    
    Returns:
        Configured PipelineExecutor
    """
    from . import modules
    
    module_defs = [
        # Module 1: Health & Trajectory (no dependencies)
        ModuleDefinition(
            name="health_trajectory",
            func=modules.analyze_health_trajectory,
            dependencies=[],
            timeout_seconds=180
        ),
        
        # Module 2: Page Triage (no dependencies)
        ModuleDefinition(
            name="page_triage",
            func=modules.analyze_page_triage,
            dependencies=[],
            timeout_seconds=240
        ),
        
        # Module 6: Algorithm Impact (depends on Module 1 for change points)
        ModuleDefinition(
            name="algorithm_impact",
            func=modules.analyze_algorithm_impacts,
            dependencies=["health_trajectory"],
            timeout_seconds=120
        ),
        
        # Module 10: Branded Split (no dependencies)
        ModuleDefinition(
            name="branded_split",
            func=modules.analyze_branded_split,
            dependencies=[],
            timeout_seconds=150
        ),
        
        # Module 3: SERP Landscape (no dependencies - uses SERP data)
        ModuleDefinition(
            name="serp_landscape",
            func=modules.analyze_serp_landscape,
            dependencies=[],
            timeout_seconds=180
        ),
        
        # Module 4: Content Intelligence (depends on Module 2)
        ModuleDefinition(
            name="content_intelligence",
            func=modules.analyze_content_intelligence,
            dependencies=["page_triage"],
            timeout_seconds=200
        ),
        
        # Module 7: Intent Migration (no dependencies)
        ModuleDefinition(
            name="intent_migration",
            func=modules.analyze_intent_migration,
            dependencies=[],
            timeout_seconds=240  # LLM calls can be slow
        ),
        
        # Module 8: CTR Modeling (depends on Module 3 for SERP context)
        ModuleDefinition(
            name="ctr_modeling",
            func=modules.model_contextual_ctr,
            dependencies=["serp_landscape"],
            timeout_seconds=180
        ),
        
        # Module 9: Site Architecture (no dependencies)
        ModuleDefinition(
            name="site_architecture",
            func=modules.analyze_site_architecture,
            dependencies=[],
            timeout_seconds=300  # Graph analysis can be slow
        ),
        
        # Module 11: Competitive Radar (depends on Module 3)
        ModuleDefinition(
            name="competitive_radar",
            func=modules.analyze_competitive_threats,
            dependencies=["serp_landscape"],
            timeout_seconds=120
        ),
        
        # Module 12: Revenue Attribution (depends on Module 2, 8)
        ModuleDefinition(
            name="revenue_attribution",
            func=modules.estimate_revenue_attribution,
            dependencies=["page_triage", "ctr_modeling"],
            timeout_seconds=150
        ),
        
        # Module 5: Gameplan (depends on modules 1-4, plus others)
        ModuleDefinition(
            name="gameplan",
            func=modules.generate_gameplan,
            dependencies=[
                "health_trajectory",
                "page_triage",
                "serp_landscape",
                "content_intelligence"
            ],
            timeout_seconds=300  # LLM synthesis can be slow
        ),
    ]
    
    return PipelineExecutor(module_defs)


async def execute_analysis_pipeline(
    shared_data: Dict[str, Any],
    progress_callback: Optional[Callable] = None,
    max_concurrent: int = 4
) -> Dict[str, ModuleResult]:
    """
    Execute the complete analysis pipeline.
    
    Args:
        shared_data: Shared data context with ingested data
        progress_callback: Optional callback for progress updates
        max_concurrent: Maximum concurrent module executions
    
    Returns:
        Dict mapping module names to results
    """
    pipeline = create_standard_pipeline()
    
    if progress_callback:
        pipeline.set_progress_callback(progress_callback)
    
    return await pipeline.execute(shared_data, max_concurrent=max_concurrent)
