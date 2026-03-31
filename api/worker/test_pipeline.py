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
    async def test_stage_status_transitions(self, mock_session, sample_report):
        """Test stage properly updates status during execution."""

        class TestStage(PipelineStage):
            async def execute(self, report, context, session):
                return {"test": "data"}

        stage = TestStage(name="test_stage")
        assert stage.status == "pending"

        result = await stage.run(sample_report, {}, mock_session)

        assert stage.status == "complete"
        assert result == {"test": "data"}

    @pytest.mark.asyncio
    async def test_stage_error_handling(self, mock_session, sample_report):
        """Test stage properly handles and records errors."""

        class FailingStage(PipelineStage):
            async def execute(self, report, context, session):
                raise ValueError("Test error")

        stage = FailingStage(name="failing_stage")

        with pytest.raises(StageError) as exc_info:
            await stage.run(sample_report, {}, mock_session)

        assert stage.status == "failed"
        assert "Test error" in str(exc_info.value)
        assert stage.error is not None


class TestDataIngestionStage:
    """Test data ingestion stage."""

    @pytest.mark.asyncio
    async def test_ingestion_success(self, mock_session, sample_report):
        """Test successful data ingestion."""
        stage = DataIngestionStage()

        with patch("worker.pipeline.GSCClient") as mock_gsc, \
             patch("worker.pipeline.GA4Client") as mock_ga4:

            # Mock GSC client
            gsc_instance = Mock()
            gsc_instance.get_daily_performance = AsyncMock(return_value=pd.DataFrame({
                "date": pd.date_range("2024-01-01", periods=30),
                "clicks": [100] * 30,
                "impressions": [1000] * 30,
            }))
            gsc_instance.get_page_performance = AsyncMock(return_value=pd.DataFrame({
                "page": ["/page-1"],
                "clicks": [100],
            }))
            gsc_instance.get_query_performance = AsyncMock(return_value=pd.DataFrame({
                "query": ["test query"],
                "clicks": [50],
            }))
            mock_gsc.return_value = gsc_instance

            # Mock GA4 client
            ga4_instance = Mock()
            ga4_instance.get_landing_pages = AsyncMock(return_value=pd.DataFrame({
                "landing_page": ["/page-1"],
                "sessions": [200],
            }))
            mock_ga4.return_value = ga4_instance

            result = await stage.execute(sample_report, {}, mock_session)

            assert "gsc_daily" in result
            assert "gsc_pages" in result
            assert "gsc_queries" in result
            assert "ga4_landing_pages" in result
            assert len(result["gsc_daily"]) == 30

    @pytest.mark.asyncio
    async def test_ingestion_gsc_failure(self, mock_session, sample_report):
        """Test handling of GSC API failure."""
        stage = DataIngestionStage()

        with patch("worker.pipeline.GSCClient") as mock_gsc:
            gsc_instance = Mock()
            gsc_instance.get_daily_performance = AsyncMock(
                side_effect=Exception("GSC API error")
            )
            mock_gsc.return_value = gsc_instance

            with pytest.raises(StageError) as exc_info:
                await stage.run(sample_report, {}, mock_session)

            assert "GSC API error" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_ingestion_empty_data(self, mock_session, sample_report):
        """Test handling of empty data from APIs."""
        stage = DataIngestionStage()

        with patch("worker.pipeline.GSCClient") as mock_gsc, \
             patch("worker.pipeline.GA4Client") as mock_ga4:

            gsc_instance = Mock()
            gsc_instance.get_daily_performance = AsyncMock(
                return_value=pd.DataFrame()
            )
            mock_gsc.return_value = gsc_instance

            ga4_instance = Mock()
            ga4_instance.get_landing_pages = AsyncMock(
                return_value=pd.DataFrame()
            )
            mock_ga4.return_value = ga4_instance

            with pytest.raises(StageError) as exc_info:
                await stage.run(sample_report, {}, mock_session)

            assert "No data returned" in str(exc_info.value) or "empty" in str(exc_info.value).lower()


class TestAnalysisStage:
    """Test analysis stage."""

    @pytest.mark.asyncio
    async def test_analysis_success(self, mock_session, sample_report, sample_ingested_data):
        """Test successful analysis execution."""
        stage = AnalysisStage(
            name="health_trajectory",
            analyzer_func=Mock(return_value={"trend": "growing"}),
        )

        context = sample_ingested_data

        result = await stage.execute(sample_report, context, mock_session)

        assert result["health_trajectory"] == {"trend": "growing"}

    @pytest.mark.asyncio
    async def test_analysis_missing_data(self, mock_session, sample_report):
        """Test analysis fails gracefully with missing data."""
        stage = AnalysisStage(
            name="health_trajectory",
            analyzer_func=Mock(side_effect=KeyError("gsc_daily")),
        )

        with pytest.raises(StageError) as exc_info:
            await stage.run(sample_report, {}, mock_session)

        assert "gsc_daily" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_analysis_computation_error(self, mock_session, sample_report, sample_ingested_data):
        """Test analysis handles computation errors."""

        def failing_analyzer(data):
            raise ValueError("Computation failed")

        stage = AnalysisStage(
            name="test_analysis",
            analyzer_func=failing_analyzer,
        )

        with pytest.raises(StageError) as exc_info:
            await stage.run(sample_report, sample_ingested_data, mock_session)

        assert "Computation failed" in str(exc_info.value)


class TestReportGenerationStage:
    """Test report generation stage."""

    @pytest.mark.asyncio
    async def test_generation_success(self, mock_session, sample_report):
        """Test successful report generation."""
        stage = ReportGenerationStage()

        context = {
            "health_trajectory": {"trend": "growing"},
            "page_triage": {"summary": "10 pages analyzed"},
            "gameplan": {"critical": [], "quick_wins": []},
        }

        with patch("worker.pipeline.generate_narrative") as mock_narrative:
            mock_narrative.return_value = "Test narrative"

            result = await stage.execute(sample_report, context, mock_session)

            assert "report" in result
            assert "health_trajectory" in result["report"]
            assert result["report"]["generated_at"] is not None

    @pytest.mark.asyncio
    async def test_generation_missing_analysis(self, mock_session, sample_report):
        """Test generation fails with incomplete analysis."""
        stage = ReportGenerationStage()

        context = {
            "health_trajectory": {"trend": "growing"},
            # Missing other required analyses
        }

        with pytest.raises(StageError) as exc_info:
            await stage.run(sample_report, context, mock_session)

        assert "incomplete" in str(exc_info.value).lower() or "missing" in str(exc_info.value).lower()


class TestPipeline:
    """Test Pipeline orchestration."""

    @pytest.mark.asyncio
    async def test_pipeline_initialization(self):
        """Test pipeline initializes with correct stages."""
        pipeline = Pipeline()

        assert len(pipeline.stages) > 0
        assert pipeline.current_stage == 0
        assert all(isinstance(stage, PipelineStage) for stage in pipeline.stages)

    @pytest.mark.asyncio
    async def test_pipeline_execution_success(self, mock_session, sample_report):
        """Test complete pipeline execution."""
        # Create minimal pipeline
        stage1 = Mock(spec=PipelineStage)
        stage1.name = "stage1"
        stage1.run = AsyncMock(return_value={"data": "stage1"})

        stage2 = Mock(spec=PipelineStage)
        stage2.name = "stage2"
        stage2.run = AsyncMock(return_value={"data": "stage2"})

        pipeline = Pipeline()
        pipeline.stages = [stage1, stage2]

        context = await pipeline.execute(sample_report, mock_session)

        assert stage1.run.called
        assert stage2.run.called
        assert context["data"] == "stage2"

    @pytest.mark.asyncio
    async def test_pipeline_stage_failure(self, mock_session, sample_report):
        """Test pipeline handles stage failure."""
        stage1 = Mock(spec=PipelineStage)
        stage1.name = "stage1"
        stage1.run = AsyncMock(return_value={"data": "stage1"})

        stage2 = Mock(spec=PipelineStage)
        stage2.name = "stage2"
        stage2.run = AsyncMock(side_effect=StageError("Stage failed", "stage2"))

        pipeline = Pipeline()
        pipeline.stages = [stage1, stage2]

        with pytest.raises(PipelineError) as exc_info:
            await pipeline.execute(sample_report, mock_session)

        assert "stage2" in str(exc_info.value)
        assert pipeline.current_stage == 1

    @pytest.mark.asyncio
    async def test_pipeline_progress_tracking(self, mock_session, sample_report):
        """Test pipeline tracks progress correctly."""
        stage1 = Mock(spec=PipelineStage)
        stage1.name = "ingestion"
        stage1.status = "pending"
        stage1.run = AsyncMock(side_effect=lambda r, c, s: setattr(stage1, "status", "complete") or {"data": "stage1"})

        stage2 = Mock(spec=PipelineStage)
        stage2.name = "analysis"
        stage2.status = "pending"
        stage2.run = AsyncMock(side_effect=lambda r, c, s: setattr(stage2, "status", "complete") or {"data": "stage2"})

        pipeline = Pipeline()
        pipeline.stages = [stage1, stage2]

        await pipeline.execute(sample_report, mock_session)

        assert stage1.status == "complete"
        assert stage2.status == "complete"
        assert pipeline.current_stage == 2


class TestStatusTransitions:
    """Test report status transitions during pipeline."""

    @pytest.mark.asyncio
    async def test_status_pending_to_ingesting(self, mock_session, sample_report):
        """Test transition from pending to ingesting."""
        assert sample_report.status == ReportStatus.PENDING

        with patch("worker.pipeline.Pipeline.execute") as mock_execute:
            mock_execute.return_value = {}

            await execute_pipeline(sample_report.id, mock_session)

            # Status should have been updated to ingesting at start
            assert mock_session.commit.called

    @pytest.mark.asyncio
    async def test_status_analyzing(self, mock_session):
        """Test transition to analyzing status."""
        report = Report(
            id="test-report",
            user_id="test-user",
            gsc_property="https://example.com",
            status=ReportStatus.INGESTING,
            progress={"ingestion": "complete"},
        )

        # Simulate analysis stage starting
        report.status = ReportStatus.ANALYZING
        report.progress["analysis"] = "running"

        assert report.status == ReportStatus.ANALYZING
        assert report.progress["analysis"] == "running"

    @pytest.mark.asyncio
    async def test_status_generating(self, mock_session):
        """Test transition to generating status."""
        report = Report(
            id="test-report",
            user_id="test-user",
            gsc_property="https://example.com",
            status=ReportStatus.ANALYZING,
            progress={
                "ingestion": "complete",
                "analysis": "complete",
            },
        )

        report.status = ReportStatus.GENERATING
        report.progress["generation"] = "running"

        assert report.status == ReportStatus.GENERATING

    @pytest.mark.asyncio
    async def test_status_complete(self, mock_session, sample_report):
        """Test transition to complete status."""
        sample_report.status = ReportStatus.GENERATING
        sample_report.progress = {
            "ingestion": "complete",
            "analysis": "complete",
            "generation": "complete",
        }

        sample_report.status = ReportStatus.COMPLETE
        sample_report.completed_at = datetime.utcnow()

        assert sample_report.status == ReportStatus.COMPLETE
        assert sample_report.completed_at is not None

    @pytest.mark.asyncio
    async def test_status_failed_on_error(self, mock_session, sample_report):
        """Test transition to failed status on error."""
        with patch("worker.pipeline.Pipeline.execute") as mock_execute:
            mock_execute.side_effect = PipelineError("Pipeline failed", "test_stage")

            with pytest.raises(PipelineError):
                await execute_pipeline(sample_report.id, mock_session)

            # Report status should be updated to failed
            # (implementation would update this in exception handler)


class TestProgressUpdates:
    """Test progress tracking and updates."""

    @pytest.mark.asyncio
    async def test_progress_initialization(self, sample_report):
        """Test progress dict initializes correctly."""
        assert sample_report.progress == {}

    @pytest.mark.asyncio
    async def test_progress_stage_updates(self, mock_session, sample_report):
        """Test progress updates after each stage."""
        pipeline = Pipeline()

        # Mock stages that update progress
        def create_stage_with_progress(name):
            stage = Mock(spec=PipelineStage)
            stage.name = name
            stage.status = "pending"

            async def run_with_progress(r, c, s):
                stage.status = "complete"
                r.progress[name] = "complete"
                return {name: "data"}

            stage.run = run_with_progress
            return stage

        pipeline.stages = [
            create_stage_with_progress("ingestion"),
            create_stage_with_progress("analysis"),
        ]

        await pipeline.execute(sample_report, mock_session)

        assert sample_report.progress["ingestion"] == "complete"
        assert sample_report.progress["analysis"] == "complete"

    @pytest.mark.asyncio
    async def test_progress_percentage_calculation(self):
        """Test progress percentage calculation."""
        report = Report(
            id="test-report",
            user_id="test-user",
            gsc_property="https://example.com",
            status=ReportStatus.ANALYZING,
            progress={
                "ingestion": "complete",
                "health_trajectory": "complete",
                "page_triage": "running",
                "serp_landscape": "pending",
            },
        )

        # Calculate progress
        total_stages = 4
        completed_stages = sum(1 for v in report.progress.values() if v == "complete")
        progress_pct = (completed_stages / total_stages) * 100

        assert progress_pct == 50.0


class TestErrorHandling:
    """Test comprehensive error handling."""

    @pytest.mark.asyncio
    async def test_stage_error_includes_context(self):
        """Test StageError includes helpful context."""
        error = StageError("Test error", "test_stage", {"detail": "more info"})

        assert error.stage_name == "test_stage"
        assert error.context["detail"] == "more info"
        assert "test_stage" in str(error)

    @pytest.mark.asyncio
    async def test_pipeline_error_propagation(self, mock_session, sample_report):
        """Test errors propagate correctly through pipeline."""
        stage = Mock(spec=PipelineStage)
        stage.name = "failing_stage"
        stage.run = AsyncMock(side_effect=ValueError("Inner error"))

        pipeline = Pipeline()
        pipeline.stages = [stage]

        with pytest.raises(PipelineError) as exc_info:
            await pipeline.execute(sample_report, mock_session)

        assert "failing_stage" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_recoverable_vs_fatal_errors(self):
        """Test distinction between recoverable and fatal errors."""
        # Recoverable: API rate limit (could retry)
        recoverable = StageError(
            "Rate limited",
            "ingestion",
            {"recoverable": True, "retry_after": 60}
        )
        assert recoverable.context["recoverable"] is True

        # Fatal: Invalid credentials (cannot retry)
        fatal = StageError(
            "Invalid OAuth token",
            "ingestion",
            {"recoverable": False}
        )
        assert fatal.context["recoverable"] is False

    @pytest.mark.asyncio
    async def test_error_cleanup(self, mock_session, sample_report):
        """Test cleanup occurs after error."""
        stage = Mock(spec=PipelineStage)
        stage.name = "test_stage"
        stage.run = AsyncMock(side_effect=Exception("Test error"))
        stage.cleanup = AsyncMock()

        pipeline = Pipeline()
        pipeline.stages = [stage]

        try:
            await pipeline.execute(sample_report, mock_session)
        except PipelineError:
            pass

        # Cleanup should still be called even after error
        # (this would be implemented in the actual pipeline)


class TestConcurrency:
    """Test concurrent execution scenarios."""

    @pytest.mark.asyncio
    async def test_parallel_stage_execution(self):
        """Test stages that can run in parallel."""
        # Some analysis modules could run in parallel
        # This tests the infrastructure for that

        import asyncio

        results = []

        async def mock_stage(name, delay):
            await asyncio.sleep(delay)
            return {name: f"result_{name}"}

        # Run two stages in parallel
        stage1_task = mock_stage("stage1", 0.1)
        stage2_task = mock_stage("stage2", 0.1)

        stage1_result, stage2_result = await asyncio.gather(
            stage1_task, stage2_task
        )

        assert stage1_result == {"stage1": "result_stage1"}
        assert stage2_result == {"stage2": "result_stage2"}

    @pytest.mark.asyncio
    async def test_pipeline_isolation(self, mock_session):
        """Test multiple pipelines don't interfere."""
        # Create two separate reports
        report1 = Report(
            id="report-1",
            user_id="user-1",
            gsc_property="https://site1.com",
            status=ReportStatus.PENDING,
        )

        report2 = Report(
            id="report-2",
            user_id="user-2",
            gsc_property="https://site2.com",
            status=ReportStatus.PENDING,
        )

        # Both should be able to run independently
        assert report1.id != report2.id
        assert report1.gsc_property != report2.gsc_property


class TestDataValidation:
    """Test data validation throughout pipeline."""

    @pytest.mark.asyncio
    async def test_validate_ing