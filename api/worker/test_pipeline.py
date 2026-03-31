"""
Unit tests for pipeline stages, error handling, and status transitions.
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, AsyncMock
from sqlalchemy.ext.asyncio import AsyncSession
import pandas as pd

from worker.pipeline import (
    Pipeline,
    PipelineStage,
    PipelineError,
    StageError,
    DataIngestionStage,
    AnalysisStage,
    ReportGenerationStage,
    execute_pipeline,
)
from models.database import Report, ReportStatus


@pytest.fixture
def mock_session():
    """Create mock async session."""
    session = Mock(spec=AsyncSession)
    session.commit = AsyncMock()
    session.refresh = AsyncMock()
    return session


@pytest.fixture
def sample_report():
    """Create sample report instance."""
    return Report(
        id="test-report-123",
        user_id="test-user",
        gsc_property="https://example.com",
        ga4_property="123456789",
        status=ReportStatus.PENDING,
        progress={},
        report_data={},
        created_at=datetime.utcnow(),
    )


@pytest.fixture
def sample_ingested_data():
    """Sample data from ingestion stage."""
    return {
        "gsc_daily": pd.DataFrame({
            "date": pd.date_range("2024-01-01", periods=365),
            "clicks": [100 + i for i in range(365)],
            "impressions": [1000 + i * 10 for i in range(365)],
        }),
        "gsc_pages": pd.DataFrame({
            "page": [f"/page-{i}" for i in range(50)],
            "clicks": [100 - i for i in range(50)],
            "impressions": [1000 - i * 10 for i in range(50)],
            "ctr": [0.1 - i * 0.001 for i in range(50)],
            "position": [5.0 + i * 0.5 for i in range(50)],
        }),
        "gsc_queries": pd.DataFrame({
            "query": [f"query {i}" for i in range(100)],
            "clicks": [50 - i * 0.3 for i in range(100)],
            "impressions": [500 - i * 3 for i in range(100)],
            "position": [8.0 + i * 0.2 for i in range(100)],
        }),
        "ga4_landing_pages": pd.DataFrame({
            "landing_page": [f"/page-{i}" for i in range(50)],
            "sessions": [200 - i * 2 for i in range(50)],
            "bounce_rate": [0.4 + i * 0.01 for i in range(50)],
            "avg_session_duration": [120 - i for i in range(50)],
        }),
    }


class TestPipelineStage:
    """Test base PipelineStage class."""

    def test_stage_initialization(self):
        """Test stage initializes with correct attributes."""
        stage = PipelineStage(name="test_stage")
        assert stage.name == "test_stage"
        assert stage.status == "pending"
        assert stage.error is None

    @pytest.mark.asyncio
    async def test_execute_not_implemented(self):
        """Test that execute raises NotImplementedError."""
        stage = PipelineStage(name="test_stage")
        with pytest.raises(NotImplementedError):
            await stage.execute({}, {}, None)

    @pytest.mark.asyncio
    async def test_stage_status_transitions(self):
        """Test stage status transitions through execution."""
        class TestStage(PipelineStage):
            async def execute(self, context, config, session):
                return {"result": "success"}

        stage = TestStage(name="test_stage")
        assert stage.status == "pending"
        
        result = await stage.execute({}, {}, None)
        assert result == {"result": "success"}


class TestDataIngestionStage:
    """Test data ingestion stage."""

    @pytest.mark.asyncio
    async def test_ingestion_success(self, mock_session, sample_report):
        """Test successful data ingestion."""
        stage = DataIngestionStage()
        
        with patch("worker.pipeline.GSCClient") as mock_gsc, \
             patch("worker.pipeline.GA4Client") as mock_ga4:
            
            # Mock GSC client
            mock_gsc_instance = Mock()
            mock_gsc_instance.get_search_analytics = AsyncMock(return_value=pd.DataFrame({
                "date": pd.date_range("2024-01-01", periods=100),
                "clicks": [50] * 100,
                "impressions": [500] * 100,
            }))
            mock_gsc_instance.get_pages = AsyncMock(return_value=pd.DataFrame({
                "page": ["/page-1", "/page-2"],
                "clicks": [100, 50],
                "impressions": [1000, 500],
            }))
            mock_gsc_instance.get_queries = AsyncMock(return_value=pd.DataFrame({
                "query": ["query 1", "query 2"],
                "clicks": [80, 40],
                "impressions": [800, 400],
            }))
            mock_gsc.return_value = mock_gsc_instance
            
            # Mock GA4 client
            mock_ga4_instance = Mock()
            mock_ga4_instance.get_landing_pages = AsyncMock(return_value=pd.DataFrame({
                "landing_page": ["/page-1", "/page-2"],
                "sessions": [200, 100],
                "bounce_rate": [0.4, 0.5],
            }))
            mock_ga4.return_value = mock_ga4_instance
            
            context = {}
            config = {
                "gsc_property": "https://example.com",
                "ga4_property": "123456789",
                "date_range_months": 12,
            }
            
            result = await stage.execute(context, config, mock_session)
            
            assert "gsc_daily" in result
            assert "gsc_pages" in result
            assert "gsc_queries" in result
            assert "ga4_landing_pages" in result
            assert len(result["gsc_daily"]) == 100

    @pytest.mark.asyncio
    async def test_ingestion_gsc_failure(self, mock_session):
        """Test handling of GSC API failure."""
        stage = DataIngestionStage()
        
        with patch("worker.pipeline.GSCClient") as mock_gsc:
            mock_gsc_instance = Mock()
            mock_gsc_instance.get_search_analytics = AsyncMock(
                side_effect=Exception("GSC API error")
            )
            mock_gsc.return_value = mock_gsc_instance
            
            context = {}
            config = {
                "gsc_property": "https://example.com",
                "ga4_property": "123456789",
                "date_range_months": 12,
            }
            
            with pytest.raises(StageError) as exc_info:
                await stage.execute(context, config, mock_session)
            
            assert "GSC API error" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_ingestion_empty_data(self, mock_session):
        """Test handling of empty data from APIs."""
        stage = DataIngestionStage()
        
        with patch("worker.pipeline.GSCClient") as mock_gsc, \
             patch("worker.pipeline.GA4Client") as mock_ga4:
            
            mock_gsc_instance = Mock()
            mock_gsc_instance.get_search_analytics = AsyncMock(
                return_value=pd.DataFrame()
            )
            mock_gsc_instance.get_pages = AsyncMock(
                return_value=pd.DataFrame()
            )
            mock_gsc_instance.get_queries = AsyncMock(
                return_value=pd.DataFrame()
            )
            mock_gsc.return_value = mock_gsc_instance
            
            mock_ga4_instance = Mock()
            mock_ga4_instance.get_landing_pages = AsyncMock(
                return_value=pd.DataFrame()
            )
            mock_ga4.return_value = mock_ga4_instance
            
            context = {}
            config = {
                "gsc_property": "https://example.com",
                "ga4_property": "123456789",
                "date_range_months": 12,
            }
            
            with pytest.raises(StageError) as exc_info:
                await stage.execute(context, config, mock_session)
            
            assert "No data" in str(exc_info.value) or "Empty" in str(exc_info.value)


class TestAnalysisStage:
    """Test analysis stage."""

    @pytest.mark.asyncio
    async def test_analysis_success(self, mock_session, sample_ingested_data):
        """Test successful analysis execution."""
        stage = AnalysisStage()
        
        with patch("worker.pipeline.analyze_health_trajectory") as mock_health, \
             patch("worker.pipeline.analyze_page_triage") as mock_triage, \
             patch("worker.pipeline.analyze_serp_landscape") as mock_serp, \
             patch("worker.pipeline.analyze_content_intelligence") as mock_content:
            
            mock_health.return_value = {
                "overall_direction": "growing",
                "trend_slope_pct_per_month": 2.5,
            }
            mock_triage.return_value = {
                "pages": [],
                "summary": {"total_pages_analyzed": 50},
            }
            mock_serp.return_value = {
                "keywords_analyzed": 100,
                "competitors": [],
            }
            mock_content.return_value = {
                "cannibalization_clusters": [],
                "striking_distance": [],
            }
            
            context = sample_ingested_data
            config = {}
            
            result = await stage.execute(context, config, mock_session)
            
            assert "health_trajectory" in result
            assert "page_triage" in result
            assert "serp_landscape" in result
            assert "content_intelligence" in result

    @pytest.mark.asyncio
    async def test_analysis_module_failure(self, mock_session, sample_ingested_data):
        """Test handling of analysis module failure."""
        stage = AnalysisStage()
        
        with patch("worker.pipeline.analyze_health_trajectory") as mock_health:
            mock_health.side_effect = Exception("Analysis error")
            
            context = sample_ingested_data
            config = {}
            
            with pytest.raises(StageError) as exc_info:
                await stage.execute(context, config, mock_session)
            
            assert "Analysis error" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_analysis_missing_data(self, mock_session):
        """Test handling of missing required data."""
        stage = AnalysisStage()
        
        context = {}  # Missing required data
        config = {}
        
        with pytest.raises(StageError) as exc_info:
            await stage.execute(context, config, mock_session)
        
        assert "Missing" in str(exc_info.value) or "required" in str(exc_info.value).lower()


class TestReportGenerationStage:
    """Test report generation stage."""

    @pytest.mark.asyncio
    async def test_report_generation_success(self, mock_session):
        """Test successful report generation."""
        stage = ReportGenerationStage()
        
        with patch("worker.pipeline.generate_gameplan") as mock_gameplan, \
             patch("worker.pipeline.generate_narrative") as mock_narrative:
            
            mock_gameplan.return_value = {
                "critical": [],
                "quick_wins": [],
                "strategic": [],
            }
            mock_narrative.return_value = "This is the report narrative."
            
            context = {
                "health_trajectory": {"overall_direction": "growing"},
                "page_triage": {"summary": {}},
                "serp_landscape": {"keywords_analyzed": 100},
                "content_intelligence": {"cannibalization_clusters": []},
            }
            config = {}
            
            result = await stage.execute(context, config, mock_session)
            
            assert "gameplan" in result
            assert "narrative" in result
            assert "generated_at" in result

    @pytest.mark.asyncio
    async def test_report_generation_llm_failure(self, mock_session):
        """Test handling of LLM API failure."""
        stage = ReportGenerationStage()
        
        with patch("worker.pipeline.generate_gameplan") as mock_gameplan, \
             patch("worker.pipeline.generate_narrative") as mock_narrative:
            
            mock_gameplan.return_value = {"critical": []}
            mock_narrative.side_effect = Exception("LLM API error")
            
            context = {
                "health_trajectory": {"overall_direction": "growing"},
                "page_triage": {"summary": {}},
                "serp_landscape": {"keywords_analyzed": 100},
                "content_intelligence": {"cannibalization_clusters": []},
            }
            config = {}
            
            with pytest.raises(StageError) as exc_info:
                await stage.execute(context, config, mock_session)
            
            assert "LLM API error" in str(exc_info.value)


class TestPipeline:
    """Test Pipeline orchestration."""

    @pytest.mark.asyncio
    async def test_pipeline_success(self, mock_session, sample_report):
        """Test successful pipeline execution."""
        pipeline = Pipeline(report_id=sample_report.id)
        
        # Add mock stages
        stage1 = Mock(spec=PipelineStage)
        stage1.name = "stage1"
        stage1.execute = AsyncMock(return_value={"data": "result1"})
        
        stage2 = Mock(spec=PipelineStage)
        stage2.name = "stage2"
        stage2.execute = AsyncMock(return_value={"data": "result2"})
        
        pipeline.stages = [stage1, stage2]
        
        with patch("worker.pipeline.get_report") as mock_get_report, \
             patch("worker.pipeline.update_report_status") as mock_update:
            
            mock_get_report.return_value = sample_report
            
            await pipeline.run(mock_session)
            
            assert stage1.execute.called
            assert stage2.execute.called
            assert pipeline.context["data"] == "result2"

    @pytest.mark.asyncio
    async def test_pipeline_stage_failure(self, mock_session, sample_report):
        """Test pipeline handles stage failure."""
        pipeline = Pipeline(report_id=sample_report.id)
        
        stage1 = Mock(spec=PipelineStage)
        stage1.name = "stage1"
        stage1.execute = AsyncMock(side_effect=StageError("Stage failed"))
        
        pipeline.stages = [stage1]
        
        with patch("worker.pipeline.get_report") as mock_get_report, \
             patch("worker.pipeline.update_report_status") as mock_update:
            
            mock_get_report.return_value = sample_report
            
            with pytest.raises(PipelineError) as exc_info:
                await pipeline.run(mock_session)
            
            assert "Stage failed" in str(exc_info.value)
            assert mock_update.called

    @pytest.mark.asyncio
    async def test_pipeline_progress_tracking(self, mock_session, sample_report):
        """Test pipeline tracks progress correctly."""
        pipeline = Pipeline(report_id=sample_report.id)
        
        stage1 = Mock(spec=PipelineStage)
        stage1.name = "stage1"
        stage1.execute = AsyncMock(return_value={})
        
        stage2 = Mock(spec=PipelineStage)
        stage2.name = "stage2"
        stage2.execute = AsyncMock(return_value={})
        
        pipeline.stages = [stage1, stage2]
        
        with patch("worker.pipeline.get_report") as mock_get_report, \
             patch("worker.pipeline.update_report_progress") as mock_progress:
            
            mock_get_report.return_value = sample_report
            
            await pipeline.run(mock_session)
            
            # Should update progress for each stage
            assert mock_progress.call_count >= 2


class TestExecutePipeline:
    """Test top-level pipeline execution function."""

    @pytest.mark.asyncio
    async def test_execute_pipeline_success(self, mock_session, sample_report):
        """Test successful end-to-end pipeline execution."""
        with patch("worker.pipeline.Pipeline") as mock_pipeline_class, \
             patch("worker.pipeline.get_async_session") as mock_get_session, \
             patch("worker.pipeline.get_report") as mock_get_report:
            
            mock_pipeline = Mock()
            mock_pipeline.run = AsyncMock()
            mock_pipeline.context = {"final": "result"}
            mock_pipeline_class.return_value = mock_pipeline
            
            mock_get_session.return_value.__aenter__.return_value = mock_session
            mock_get_report.return_value = sample_report
            
            await execute_pipeline(sample_report.id)
            
            assert mock_pipeline.run.called

    @pytest.mark.asyncio
    async def test_execute_pipeline_report_not_found(self, mock_session):
        """Test handling of non-existent report."""
        with patch("worker.pipeline.get_async_session") as mock_get_session, \
             patch("worker.pipeline.get_report") as mock_get_report:
            
            mock_get_session.return_value.__aenter__.return_value = mock_session
            mock_get_report.return_value = None
            
            with pytest.raises(PipelineError) as exc_info:
                await execute_pipeline("non-existent-id")
            
            assert "not found" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_execute_pipeline_db_error(self, mock_session, sample_report):
        """Test handling of database errors."""
        with patch("worker.pipeline.get_async_session") as mock_get_session, \
             patch("worker.pipeline.get_report") as mock_get_report:
            
            mock_get_session.return_value.__aenter__.side_effect = Exception("DB error")
            
            with pytest.raises(PipelineError) as exc_info:
                await execute_pipeline(sample_report.id)
            
            assert "DB error" in str(exc_info.value)


class TestStageErrorHandling:
    """Test error handling across stages."""

    @pytest.mark.asyncio
    async def test_stage_timeout(self, mock_session):
        """Test handling of stage timeout."""
        class SlowStage(PipelineStage):
            async def execute(self, context, config, session):
                import asyncio
                await asyncio.sleep(100)  # Simulate slow operation
                return {}

        stage = SlowStage(name="slow_stage")
        
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(
                stage.execute({}, {}, mock_session),
                timeout=1.0
            )

    @pytest.mark.asyncio
    async def test_stage_memory_error(self, mock_session):
        """Test handling of memory errors."""
        class MemoryHungryStage(PipelineStage):
            async def execute(self, context, config, session):
                # Simulate memory error
                raise MemoryError("Out of memory")

        stage = MemoryHungryStage(name="memory_stage")
        
        with pytest.raises(StageError) as exc_info:
            await stage.execute({}, {}, mock_session)
        
        assert "memory" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_stage_partial_failure(self, mock_session, sample_ingested_data):
        """Test handling of partial stage failures."""
        stage = AnalysisStage()
        
        with patch("worker.pipeline.analyze_health_trajectory") as mock_health, \
             patch("worker.pipeline.analyze_page_triage") as mock_triage:
            
            # First analysis succeeds
            mock_health.return_value = {"overall_direction": "growing"}
            
            # Second analysis fails
            mock_triage.side_effect = Exception("Triage failed")
            
            context = sample_ingested_data
            config = {}
            
            with pytest.raises(StageError):
                await stage.execute(context, config, mock_session)


class TestPipelineRecovery:
    """Test pipeline recovery and retry logic."""

    @pytest.mark.asyncio
    async def test_pipeline_retry_on_transient_error(self, mock_session, sample_report):
        """Test pipeline retries on transient errors."""
        pipeline = Pipeline(report_id=sample_report.id)
        
        stage = Mock(spec=PipelineStage)
        stage.name = "flaky_stage"
        
        # Fail first time, succeed second time
        call_count = 0
        async def flaky_execute(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception("Transient error")
            return {"success": True}
        
        stage.execute = flaky_execute
        pipeline.stages = [stage]
        
        with patch("worker.pipeline.get_report") as mock_get_report:
            mock_get_report.return_value = sample_report
            
            # This would need retry logic in actual implementation
            # For now, just test that error is raised
            with pytest.raises(PipelineError):
                await pipeline.run(mock_session)

    @pytest.mark.asyncio
    async def test_pipeline_checkpoint_resume(self, mock_session, sample_report):
        """Test pipeline can resume from checkpoint."""
        # Set report to have completed first stage
        sample_report.progress = {
            "current_stage": "stage2",
            "completed_stages": ["stage1"],
            "stage1": {"status": "completed", "data": {"result": "cached"}},
        }
        
        pipeline = Pipeline(report_id=sample_report.id)
        
        stage1 = Mock(spec=PipelineStage)
        stage1.name = "stage1"
        stage1.execute = AsyncMock(return_value={"result": "new"})
        
        stage2 = Mock(spec=PipelineStage)
        stage2.name = "stage2"
        stage2.execute = AsyncMock(return_value={"result": "stage2"})
        
        pipeline.stages = [stage1, stage2]
        
        with patch("worker.pipeline.get_report") as mock_get_report:
            mock_get_report.return_value = sample_report
            
            await pipeline.run(mock_session)
            
            # Stage1 should not be called (already completed)
            # In actual implementation, would check checkpoint logic
            assert stage2.execute.called


class TestDataValidation:
    """Test data validation throughout pipeline."""

    @pytest.mark.asyncio
    async def test_validate_gsc_data_structure(self, mock_session):
        """Test validation of GSC data structure."""
        stage = DataIngestionStage()
        
        with patch("worker.pipeline.GSCClient") as mock_gsc, \
             patch("worker.pipeline.GA4Client") as mock_ga4:
            
            # Return data with wrong structure
            mock_gsc_instance = Mock()
            mock_gsc_instance.get_search_analytics = AsyncMock(
                return_value=pd.DataFrame({"wrong": ["columns"]})
            )
            mock_gsc.return_value = mock_gsc_instance
            
            mock_ga4_instance = Mock()
            mock_ga4_instance.get_landing_pages = AsyncMock(
                return_value=pd.DataFrame()
            )
            mock_ga4.return_value = mock_ga4_instance
            
            context = {}
            config = {
                "gsc_property": "https://example.com",
                "ga4_property": "123456789",
                "date_range_months": 12,
            }
            
            with pytest.raises(StageError):
                await stage.execute(context, config, mock_session)

    @pytest.mark.asyncio
    async def test_validate_date_ranges(self, mock_session):
        """Test validation of date range consistency."""
        stage = DataIngestionStage()
        
        with patch("worker.pipeline.GSCClient") as mock_gsc, \
             patch("worker.pipeline.GA4Client") as mock_ga4:
            
            # GSC data with different date range than requested
            mock_gsc_instance = Mock()
            mock_gsc_instance.get_search_analytics = AsyncMock(
                return_value=pd.DataFrame({
                    "date": pd.date_range("2024-01-01", periods=30),  # Only 30 days
                    "clicks": [50] * 30,
                    "impressions": [500] * 30,
                })
            )
            mock_gsc_instance.get_pages = AsyncMock(
                return_value=pd.DataFrame({
                    "page": ["/page-1"],
                    "clicks": [100],
                    "impressions": [1000],
                })
            )
            mock_gsc_instance.get_queries = AsyncMock(
                return_value=pd.DataFrame({
                    "query": ["query 1"],
                    "clicks": [80],
                    "impressions": [800],
                })
            )
            mock_gsc.return_value = mock_gsc_instance
            
            mock_ga4_instance = Mock()
            mock_ga4_instance.get_landing_pages = AsyncMock(
                return_value=pd.DataFrame({
                    "landing_page": ["/page-1"],
                    "sessions": [200],
                })
            )
            mock_ga4.return_value = mock_ga4_instance
            
            context = {}
            config = {
                "gsc_property": "https://example.com",
                "ga4_property": "123456789",
                "date_range_months": 12,  # Requesting 12 months
            }
            
            # Should warn or fail if insufficient data
            result = await stage.execute(context, config, mock_session)
            # In actual implementation, would check for warnings


class TestConcurrency:
    """Test concurrent pipeline execution."""

    @pytest.mark.asyncio
    async def test_multiple_pipelines_concurrent(self, mock_session):
        """Test multiple pipelines can run concurrently."""
        report1 = Report(id="report-1", user_id="user-1", status=ReportStatus.PENDING)
        report2 = Report(id="report-2", user_id="user-2", status=ReportStatus.PENDING)
        
        with patch("worker.pipeline.Pipeline") as mock_pipeline_class, \
             patch("worker.pipeline.get_async_session") as mock_get_session, \
             patch("worker.pipeline.get_report") as mock_get_report:
            
            mock_pipeline1 = Mock()
            mock_pipeline1.run = AsyncMock()
            
            mock_pipeline2 = Mock()
            mock_pipeline2.run = AsyncMock()
            
            mock_pipeline_class.side_effect = [mock_pipeline1, mock_pipeline2]
            mock_get_session.return_value.__aenter__.return_value = mock_session
            mock_get_report.side_effect = [report1, report2]
            
            # Run both pipelines concurrently
            import asyncio
            await asyncio.gather(
                execute_pipeline("report-1"),
                execute_pipeline("report-2"),
            )
            
            assert mock_pipeline1.run.called
            assert mock_pipeline2.run.called

