import pytest
import numpy as np
from unittest.mock import Mock, patch

from five_safes_tes_analytics.auth.submission_api_session import SubmissionAPISession 
from five_safes_tes_analytics.runners.analysis_runner import AnalysisRunner


class TestAnalysisRunner:
    """Test cases for AnalysisRunner class (entrypoint for analytics workflows)."""
    
    @pytest.fixture 
    def mock_tes_client(self): 
        mock_tes_client = Mock()
        mock_tes_client.submit_task.return_value = {"id": "123"}
        mock_tes_client.get_task_status.return_value = {"status": 11, "description": "Completed"}
        return mock_tes_client

    @pytest.fixture 
    def mock_submission_api_session(self): 
        session = Mock(spec=SubmissionAPISession)
        session.__enter__ = Mock(return_value=session) 
        session.__exit__ = Mock(return_value=False) 
        return session

    @pytest.fixture
    def runner(self, mock_tes_client, mock_submission_api_session):
        """Set up AnalysisRunner with mocked TES client (runner expects tes_client, token, project)."""
        with patch(
            "five_safes_tes_analytics.runners.analysis_runner.SubmissionAPISession", 
            return_value=mock_submission_api_session 
        ): 
            runner = AnalysisRunner(tes_client=mock_tes_client, project="test_project")
            runner.statistical_analyzer.analyze_data = Mock(return_value=10.0)
            runner.data_processor = Mock() 
            runner.data_processor.aggregate_data.return_value = np.array([[10, 100]])
            yield runner 

    @patch('five_safes_tes_analytics.runners.analysis_orchestrator.MinIOClient')
    def test_run_analysis(self, mock_minio, runner):
        """Test running a complete analysis workflow."""
        mock_minio_instance = Mock()
        mock_minio_instance.get_object.return_value = "n,total\n10,100\n"
        mock_minio.return_value = mock_minio_instance
        
        mock_orchestrator = Mock()
        mock_orchestrator.setup_analysis.return_value = ("metadata_task", None, "test-bucket", ['TRE1', 'TRE2'])
        mock_orchestrator._submit_and_collect_results.return_value = ("123", ["n,total\n10,100\n", "n,total\n15,150\n"])
        mock_orchestrator.tres = ['TRE1', 'TRE2']

        with patch("five_safes_tes_analytics.runners.analysis_orchestrator", return_value=mock_orchestrator): 
            user_query = "SELECT value_as_number FROM measurement WHERE concept_id = 123"
            result = runner.run_analysis(
                "mean",
                user_query,
                ["TRE1", "TRE2"]
            )
        
        assert "analysis_type" in result
        assert "result" in result
        assert "task_id" in result
        assert "tres_used" in result
        assert "data_sources" in result
        assert "complete_query" in result
        assert result["analysis_type"] == "mean"
        assert result["result"] == 10.0
        assert result["task_id"] == "123"
        assert result["tres_used"] == ["TRE1", "TRE2"]
        assert result["data_sources"] == 2
    
    def test_get_analysis_requirements(self, runner):
        """Test getting analysis requirements from AnalysisRunner."""
        requirements = runner.get_analysis_requirements("mean")
        
        assert "return_format" in requirements
        assert "aggregation_function" in requirements
        assert "analysis_function" in requirements
        assert callable(requirements["aggregation_function"])
        assert callable(requirements["analysis_function"])
    
    def test_get_supported_analysis_types(self, runner):
        """Test getting supported analysis types from AnalysisRunner."""
        types = runner.get_supported_analysis_types()
        
        assert "mean" in types
        assert "variance" in types
        assert "pmcc" in types
        assert "contingencytable" in types
        assert "percentilesketch" in types
