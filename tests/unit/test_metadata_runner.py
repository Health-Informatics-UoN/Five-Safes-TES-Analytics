import pytest
import os
from unittest.mock import Mock, patch

from five_safes_tes_analytics.clients.bunny_tes_client import BunnyTES
from five_safes_tes_analytics.runners.metadata_runner import MetadataRunner
from five_safes_tes_analytics.auth.submission_api_session import SubmissionAPISession


class TestMetadataRunner:
    """Test cases for MetadataRunner class."""
    
    @pytest.fixture
    def mock_tes_client(self):
        """Set up mock TES client for MetadataRunner (runner expects tes_client, token, project)."""
        client = Mock(spec=BunnyTES)
        client.task = Mock()
        return client
    
    @pytest.fixture 
    def mock_submission_api_session(self): 
        session = Mock(spec=SubmissionAPISession)
        session.__enter__ = Mock(return_value=session) 
        session.__exit__ = Mock(return_value=False) 
        return session
    
    @pytest.fixture
    def metadata_runner(self, mock_tes_client, mock_submission_api_session):
        """Set up MetadataRunner instance with mock TES client."""
        with patch(
            "five_safes_tes_analytics.runners.metadata_runner.SubmissionAPISession", 
            return_value=mock_submission_api_session
        ): 
            yield MetadataRunner(tes_client=mock_tes_client, project="test_project")

    def test_metadata_initialization(self, metadata_runner, mock_tes_client):
        """Test that MetadataRunner initializes correctly."""
        assert metadata_runner.tes_client == mock_tes_client
        assert metadata_runner.analysis_orchestrator is None
        assert metadata_runner.data_processor is not None
        assert isinstance(metadata_runner.aggregated_data, dict)
    
    def test_postprocess_metadata(self, metadata_runner):
        """Test postprocess_metadata returns raw data unchanged (placeholder)."""
        raw_data = {"test_key": "test_value", "count": 42}
        result = metadata_runner.postprocess_metadata(raw_data)
        
        # Placeholder: just returns the raw data unchanged
        assert result == raw_data
        
    def test_postprocess_metadata_list(self, metadata_runner):
        """Test postprocess_metadata handles list data (placeholder)."""
        raw_data = [{"count": 100}, {"count": 150}]
        result = metadata_runner.postprocess_metadata(raw_data)
        
        # Placeholder: just returns the raw data unchanged
        assert result == raw_data
    
    def test_get_metadata(self, metadata_runner):
        """Test get_metadata workflow with placeholder aggregation."""
        # Configure mock engine methods
        mock_orchestrator = Mock()
        mock_orchestrator.setup_analysis.return_value = ("metadata_task", None, "test-bucket", ['TRE1', 'TRE2'])
        mock_orchestrator._submit_and_collect_results.return_value = ("task-123", [{"metadata": "test_data"}])
        mock_orchestrator.tres = ['TRE1', 'TRE2']

        with patch("five_safes_tes_analytics.runners.metadata_runner.AnalysisOrchestrator", return_value=mock_orchestrator):
            result = metadata_runner.get_metadata(
                tres=['TRE1', 'TRE2'],
                task_name="test_metadata",
                bucket="test-bucket"
            )
        
        # Verify result structure
        assert isinstance(result, dict)
        assert result['analysis_type'] == "metadata"
        assert result['task_id'] == "task-123"
        assert result['tres_used'] == ['TRE1', 'TRE2']
        assert result['data_sources'] == 1
        assert 'result' in result
        # Placeholder: result should be the raw data passed through
        assert result['result'] == [{"metadata": "test_data"}]
    
    def test_get_metadata_calls_tes_methods(self, metadata_runner):
        """Test that get_metadata calls the correct TES methods."""
        mock_orchestrator = Mock()
        mock_orchestrator.setup_analysis.return_value = ("metadata_task", None, "test-bucket", ['TRE1'])
        mock_orchestrator._submit_and_collect_results.return_value = ("task-456", [{"data": "test"}])
        mock_orchestrator.tres = ['TRE1']

        with patch("five_safes_tes_analytics.runners.metadata_runner.AnalysisOrchestrator", return_value=mock_orchestrator):
            metadata_runner.get_metadata(
                tres=['TRE1'],
            )
        
        # Verify TES client methods were called
        metadata_runner.tes_client.set_tes_messages.assert_called_once()
        metadata_runner.tes_client.set_tags.assert_called_once()
        metadata_runner.tes_client.create_FiveSAFES_TES_message.assert_called_once()
    
    def test_get_metadata_stores_raw_data(self, metadata_runner):
        """Test that get_metadata stores raw data (placeholder - no aggregation yet)."""
        mock_orchestrator = Mock()
        mock_orchestrator.setup_analysis.return_value = ("metadata_task", None, "test-bucket", ['TRE1', 'TRE2'])
        # Mock data from two TREs
        mock_data = [
            {"count": 100, "mean": 25.5},
            {"count": 150, "mean": 30.2}
        ]
        mock_orchestrator._submit_and_collect_results.return_value = ("task-789", mock_data)
        mock_orchestrator.tres = ['TRE1', 'TRE2']

        with patch("five_safes_tes_analytics.runners.metadata_runner.AnalysisOrchestrator", return_value=mock_orchestrator):
            result = metadata_runner.get_metadata(tres=['TRE1', 'TRE2'])
        
        # TODO: Once aggregation is implemented, this test should verify proper aggregation
        # For now, verify that raw data is passed through unchanged
        assert result['result'] == mock_data
        
        # Verify raw data was stored in aggregated_data
        assert 'raw_data' in metadata_runner.aggregated_data
        assert metadata_runner.aggregated_data['raw_data'] == mock_data
    
    def test_get_metadata_error_handling(self, metadata_runner):
        """Test that get_metadata handles errors properly."""
        mock_orchestrator = Mock()
        mock_orchestrator.setup_analysis.return_value = ("metadata_task", None, "test-bucket", ['TRE1'])
        mock_orchestrator._submit_and_collect_results.side_effect=Exception("TES submission failed")
        mock_orchestrator.tres = ['TRE1']

        # Should raise the exception
        with patch("five_safes_tes_analytics.runners.metadata_runner.AnalysisOrchestrator", return_value=mock_orchestrator):
            with pytest.raises(Exception, match="TES submission failed"):
                metadata_runner.get_metadata(tres=['TRE1'])


class TestBunnyTESMessageConstruction:
    """Tests for the full BunnyTES message construction workflow."""
    
    @pytest.fixture
    def bunny_tes(self):
        """Set up BunnyTES instance."""
        with patch.dict(os.environ, {
            'TES_BASE_URL': 'http://bunny.example.com',
            'TES_DOCKER_IMAGE': 'bunny:v1.0',
            'postgresServer': 'metadata-db',      
            'postgresPort': '5432',           
            'postgresUsername': 'metadata_user', 
            'postgresPassword': 'metadata_pass', 
            'postgresDatabase': 'metadata_db',    
            'postgresSchema': 'public',              
            '5STES_TRES': 'TRE1,TRE2,TRE3',
            'COLLECTION_ID': 'metadata-collection',
            'BUNNY_LOGGER_LEVEL': 'INFO',
            'TASK_API_BASE_URL': 'http://api.bunny.com',
            'TASK_API_USERNAME': 'bunny_api',
            'TASK_API_PASSWORD': 'bunny_secret'
        }):
            yield BunnyTES()
    
    def test_complete_metadata_workflow(self, bunny_tes):
        """Test complete workflow for metadata TES task creation."""
        code = "DEMOGRAPHICS"
        
        # Create complete TES message
        bunny_tes.set_tes_messages(task_name="integration_test_metadata", analysis=code)
        
        task = bunny_tes.task
        
        # Verify all components are present
        assert task is not None
        assert task.name == "integration_test_metadata"
        assert task.inputs == []
        assert task.outputs is not None and len(task.outputs) > 0
        assert task.executors is not None and len(task.executors) > 0
        
        # Verify executor configuration
        executor = task.executors[0]
        assert executor.image == "bunny:v1.0"
        assert executor.workdir == "/app"
        assert executor.command is not None
        assert executor.env is not None
        
        # Verify postgres* vars in executor (wrapper converts to DATASOURCE_DB_*)
        assert executor.env["postgresSchema"] == 'public'
        assert executor.env["postgresDatabase"] == 'metadata_db'
        assert "postgresDatabase" in executor.env
        assert "postgresServer" in executor.env
        assert "postgresPort" in executor.env
        assert "postgresUsername" in executor.env
        assert "postgresPassword" in executor.env

        # Verify tags for FiveSAFES
        assert task.tags is not None
        assert "tres" in task.tags
        assert "Project" in task.tags
        assert "TRE1|TRE2|TRE3" in task.tags["tres"]
    
    def test_metadata_command_structure(self, bunny_tes):
        """Test that metadata command has correct structure."""
        bunny_tes.set_tes_messages(task_name="command_test", analysis="DISTRIBUTION")
        
        executor = bunny_tes.task.executors[0]
        command = executor.command
        
        # Verify command structure (args only; entrypoint runs bunny)
        assert "--body-json" in command
        assert "--output" in command
        
        # Find and verify JSON body
        json_args = [arg for arg in command if "code" in arg]
        assert len(json_args) > 0
        
        # Verify code is in JSON
        assert "GENERIC" in json_args[0]
