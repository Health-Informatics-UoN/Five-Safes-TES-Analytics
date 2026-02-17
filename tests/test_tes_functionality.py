#!/usr/bin/env python3
"""
Unit tests for TES (Task Execution Service) functionality.
Tests the TES message creation, data validation, and client operations.
"""

import pytest
import os
import json
from unittest.mock import Mock, patch, MagicMock
import sys
from urllib.parse import urlparse

# Add the current directory to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from analytics_tes import AnalyticsTES
from bunny_tes import BunnyTES
from metadata_runner import MetadataRunner
from tes_client import BaseTESClient
import tes


class TestAnalyticsTES:
    """Test cases for AnalyticsTES class methods."""
    
    @pytest.fixture
    def analytics_tes(self):
        """Set up AnalyticsTES instance with mock environment variables."""
        with patch.dict(os.environ, {
            'TES_BASE_URL': 'http://test-tes-url.com',
            'TES_DOCKER_IMAGE': 'test-image:latest',
            'DB_HOST': 'test-db-host',
            'DB_PORT': '5432',
            'DB_USERNAME': 'test-user',
            'DB_PASSWORD': 'test-password',
            'DB_NAME': 'test-db',
            '5STES_TRES': 'TRE1,TRE2'
        }):
            return AnalyticsTES()
    
    def test_set_inputs(self, analytics_tes):
        """Test that set_inputs returns None for analytics tasks."""
        analytics_tes.set_inputs()
        assert analytics_tes.inputs is None
    
    def test_set_outputs(self, analytics_tes):
        """Test set_outputs creates correct output structure."""
        analytics_tes.set_outputs(
            name="test_analysis",
            output_path="/outputs",
            output_type="DIRECTORY",
            description="Test output",
            url=""
        )
        
        assert isinstance(analytics_tes.outputs, list)
        assert len(analytics_tes.outputs) == 1
        
        output = analytics_tes.outputs[0]
        assert isinstance(output, tes.Output)
        assert output.name == "test_analysis"
        assert output.path == "/outputs"
        assert output.type == "DIRECTORY"
        assert output.description == "Test output"
    
    def test_set_env(self, analytics_tes):
        """Test set_env creates correct environment variables."""
        analytics_tes._set_env()
        
        assert isinstance(analytics_tes.env, dict)
        assert "postgresDatabase" in analytics_tes.env
        assert "postgresServer" in analytics_tes.env
        assert "postgresPassword" in analytics_tes.env
        assert "postgresUsername" in analytics_tes.env
        
        # Verify values match the default_db_config
        assert analytics_tes.env["postgresDatabase"] == analytics_tes.default_db_config['name']
        assert analytics_tes.env["postgresServer"] == analytics_tes.default_db_config['host']
        assert analytics_tes.env["postgresPassword"] == analytics_tes.default_db_config['password']
        assert analytics_tes.env["postgresUsername"] == analytics_tes.default_db_config['username']
    
    def test_set_command(self, analytics_tes):
        """Test set_command creates correct command array."""
     
        query = "SELECT value_as_number FROM measurement WHERE concept_id = 123"
        analysis_type = "mean"
        output_path = "/outputs"
        output_format = "json"
        
        analytics_tes._set_command(query, analysis_type, output_path, output_format)
        
        assert isinstance(analytics_tes.command, list)
        assert len(analytics_tes.command) == 4
        
        # Check each command argument
        assert f"--user-query={query}" in analytics_tes.command
        assert f"--analysis={analysis_type}" in analytics_tes.command
        assert f"--output-filename={output_path}/output" in analytics_tes.command
        assert f"--output-format={output_format}" in analytics_tes.command
        
        
    
    def test_set_executors(self, analytics_tes):
        """Test set_executors creates correct executor structure."""
        query = "SELECT * FROM measurement"
        analysis_type = "variance"
        
        analytics_tes.set_executors(
            query=query,
            analysis_type=analysis_type,
            workdir="/app",
            output_path="/outputs",
            output_format="json"
        )
        
        assert isinstance(analytics_tes.executors, list)
        assert len(analytics_tes.executors) == 1
        
        executor = analytics_tes.executors[0]
        assert isinstance(executor, tes.Executor)
        assert executor.image == analytics_tes.default_image
        assert executor.workdir == "/app"
        
        # Verify command was set correctly
        assert isinstance(executor.command, list)
        assert any("--user-query=" in cmd for cmd in executor.command)
        assert any("--analysis=" in cmd for cmd in executor.command)
        
        # Verify environment was set correctly
        assert isinstance(executor.env, dict)
        assert "postgresDatabase" in executor.env
    
    def test_set_tes_messages(self, analytics_tes):
        """Test set_tes_messages creates complete TES task."""
        query = "SELECT value_as_number FROM measurement"
        analysis_type = "mean"
        name = "test_analysis"
        
        analytics_tes.set_tes_messages(
            query=query,
            analysis_type=analysis_type,
            task_name=name,
            output_format="json"
        )
        
        # Verify task was created
        assert analytics_tes.task is not None
        assert isinstance(analytics_tes.task, tes.Task)
        
        # Verify task components
        assert analytics_tes.task.name == name
        assert analytics_tes.task.inputs is None  # Analytics tasks have no inputs
        assert analytics_tes.task.outputs is not None
        assert analytics_tes.task.executors is not None
        
        # Verify tags were set (FiveSAFES)
        assert analytics_tes.task.tags is not None
        assert "Project" in analytics_tes.task.tags
        assert "tres" in analytics_tes.task.tags


class TestTESMessageStructure:
    """Test cases to validate TES message data structure."""
    
    @pytest.fixture
    def analytics_tes(self):
        """Set up AnalyticsTES instance with mock environment variables (postgres* names used by tes_client)."""
        with patch.dict(os.environ, {
            'TES_BASE_URL': 'http://test-tes-url.com',
            'TES_DOCKER_IMAGE': 'analytics:v1.0',
            'postgresServer': 'postgres.example.com',
            'postgresPort': '5432',
            'postgresUsername': 'analytics_user',
            'postgresPassword': 'secure_password',
            'postgresDatabase': 'omop_cdm',
            '5STES_TRES': 'TRE1,TRE2,TRE3'
        }):
            return AnalyticsTES()
    
    def test_tes_message_mean_analysis(self, analytics_tes):
        """Test TES message for mean analysis has correct structure."""
        query = "SELECT value_as_number FROM measurement WHERE concept_id = 123"
        
        analytics_tes.set_tes_messages(
            query=query,
            analysis_type="mean",
            task_name="mean_analysis_test"
        )
        
        task = analytics_tes.task
        
        # Verify basic structure
        assert task.name == "mean_analysis_test"
        assert task.inputs is None
        assert len(task.outputs) == 1
        assert len(task.executors) == 1
        
        # Verify outputs
        output = task.outputs[0]
        assert output.path == "/outputs"
        assert output.type == "DIRECTORY"
        
        # Verify executor
        executor = task.executors[0]
        assert executor.image == "analytics:v1.0"
        assert executor.workdir == "/app"
        
        # Verify command contains analysis type
        command_str = " ".join(executor.command)
        assert "--analysis=mean" in command_str
        assert "--user-query=" in command_str
        
        # Verify tags
        assert "tres" in task.tags
        assert "TRE1|TRE2|TRE3" in task.tags["tres"]
    
    def test_tes_message_variance_analysis(self, analytics_tes):
        """Test TES message for variance analysis."""
        query = "SELECT value_as_number FROM measurement"
        
        analytics_tes.set_tes_messages(
            query=query,
            analysis_type="variance",
            task_name="variance_test"
        )
        
        executor = analytics_tes.task.executors[0]
        command_str = " ".join(executor.command)
        
        assert "--analysis=variance" in command_str
    
    def test_tes_message_pmcc_analysis(self, analytics_tes):
        """Test TES message for PMCC analysis."""
        query = "SELECT x, y FROM measurements"
        
        analytics_tes.set_tes_messages(
            query=query,
            analysis_type="pmcc",
            task_name="pmcc_test"
        )
        
        executor = analytics_tes.task.executors[0]
        command_str = " ".join(executor.command)
        
        assert "--analysis=pmcc" in command_str
    
    def test_tes_message_environment_variables(self, analytics_tes):
        """Test that environment variables are correctly set in TES message."""
        analytics_tes.set_tes_messages(
            query="SELECT * FROM test",
            analysis_type="mean",
            task_name="env_test"
        )
        
        executor = analytics_tes.task.executors[0]
        env = executor.env
        
        assert env["postgresDatabase"] == "omop_cdm"
        assert env["postgresServer"] == "postgres.example.com"
        assert env["postgresUsername"] == "analytics_user"
        assert env["postgresPassword"] == "secure_password"
    
    def test_tes_message_output_format(self, analytics_tes):
        """Test that output format is correctly set."""
        analytics_tes.set_tes_messages(
            query="SELECT * FROM test",
            analysis_type="mean",
            task_name="output_format_test",
            output_format="csv"
        )
        
        executor = analytics_tes.task.executors[0]
        command_str = " ".join(executor.command)
        
        assert "--output-format=csv" in command_str


class TestBaseTESClientURLConstruction:
    """Test cases for TES Client URL construction and configuration."""
    
    def test_tes_url_construction(self):
        """Test TES URL construction from base URL."""
        # NOTE: There's a bug in the URL construction when base_url has no path
        # The Path object doesn't add a leading slash, resulting in URLs like
        # "http://example.comv1" instead of "http://example.com/v1"
        # This test documents the current (buggy) behavior
        with patch.dict(os.environ, {
            'TES_BASE_URL': 'http://example.com',
            'TES_DOCKER_IMAGE': 'test:latest',
            'DB_HOST': 'db',
            'DB_PORT': '5432',
            'DB_USERNAME': 'user',
            'DB_PASSWORD': 'pass',
            'DB_NAME': 'db',
            '5STES_TRES': 'TRE1'
        }):
            client = AnalyticsTES()
            
            assert client.TES_url == "http://example.com/v1"
    
    def test_submission_url_construction(self):
        """Test submission URL construction."""
    
        with patch.dict(os.environ, {
            'TES_BASE_URL': 'http://example.com',
            'TES_DOCKER_IMAGE': 'test:latest',
            'DB_HOST': 'db',
            'DB_PORT': '5432',
            'DB_USERNAME': 'user',
            'DB_PASSWORD': 'pass',
            'DB_NAME': 'db',
            '5STES_TRES': 'TRE1'
        }):
            client = AnalyticsTES()
            
            assert client.submission_url == "http://example.com/api/Submission"
    
    def test_tes_url_with_path_in_base(self):
        """Test TES URL construction when base URL has a path."""
        
        with patch.dict(os.environ, {
            'TES_BASE_URL': 'http://example.com/api/tes',
            'TES_DOCKER_IMAGE': 'test:latest',
            'DB_HOST': 'db',
            'DB_PORT': '5432',
            'DB_USERNAME': 'user',
            'DB_PASSWORD': 'pass',
            'DB_NAME': 'db',
            '5STES_TRES': 'TRE1'
        }):
            client = AnalyticsTES()
            
            # Should append /v1 to the path
            parsed = urlparse(client.TES_url)
            assert parsed.path.endswith("/v1")
    
    def test_required_env_variables(self):
        """Test that missing required environment variables raise errors."""
        # Missing TES_BASE_URL
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="TES_BASE_URL"):
                AnalyticsTES()
        
        # Missing TES_DOCKER_IMAGE
        with patch.dict(os.environ, {'TES_BASE_URL': 'http://test.com'}, clear=True):
            with pytest.raises(ValueError, match="TES_DOCKER_IMAGE"):
                AnalyticsTES()
    
    def test_tags_configuration(self):
        """Test that tags are correctly configured."""
        with patch.dict(os.environ, {
            'TES_BASE_URL': 'http://example.com',
            'TES_DOCKER_IMAGE': 'test:latest',
            'DB_HOST': 'db',
            'DB_PORT': '5432',
            'DB_USERNAME': 'user',
            'DB_PASSWORD': 'pass',
            'DB_NAME': 'db',
            '5STES_TRES': 'TRE1,TRE2,TRE3'
        }):
            client = AnalyticsTES()
            
            assert "tres" in client.tags
            assert isinstance(client.tags["tres"], list)
            assert client.tags["tres"] == ['TRE1', 'TRE2', 'TRE3']


class TestTESTaskIntegration:
    """Integration tests for complete TES task creation workflow."""
    
    @pytest.fixture
    def analytics_tes(self):
        """Set up AnalyticsTES instance."""
        with patch.dict(os.environ, {
            'TES_BASE_URL': 'http://test-tes-url.com',
            'TES_DOCKER_IMAGE': 'analytics:latest',
            'DB_HOST': 'postgres-server',
            'DB_PORT': '5432',
            'DB_USERNAME': 'test_user',
            'DB_PASSWORD': 'test_pass',
            'DB_NAME': 'test_db',
            '5STES_TRES': 'TRE1,TRE2'
        }):
            return AnalyticsTES()
    
    def test_complete_mean_analysis_workflow(self, analytics_tes):
        """Test complete workflow for mean analysis TES task creation."""
        query = "SELECT value_as_number FROM measurement WHERE concept_id = 123"
        
        # Create complete TES message
        analytics_tes.set_tes_messages(
            query=query,
            analysis_type="mean",
            task_name="integration_test_mean",
            output_format="json"
        )
        
        task = analytics_tes.task
        
        # Verify all components are present
        assert task is not None
        assert task.name == "integration_test_mean"
        assert task.outputs is not None and len(task.outputs) > 0
        assert task.executors is not None and len(task.executors) > 0
        
        # Verify executor configuration
        executor = task.executors[0]
        assert executor.image == "analytics:latest"
        assert executor.workdir == "/app"
        assert executor.command is not None
        assert executor.env is not None
        
        # Verify tags for FiveSAFES
        assert task.tags is not None
        assert "tres" in task.tags
        assert "Project" in task.tags
    
    def test_task_serialization(self, analytics_tes):
        """Test that TES task can be serialized to JSON."""
        analytics_tes.set_tes_messages(
            query="SELECT * FROM test",
            analysis_type="variance",
            task_name="serialization_test"
        )
        
        task = analytics_tes.task
        
        # Convert to dict (py-tes objects should be serializable)
        try:
            task_dict = task.as_dict()
            assert isinstance(task_dict, dict)
            assert "name" in task_dict
            assert "executors" in task_dict
            assert "outputs" in task_dict
        except AttributeError:
            # Some versions might not have as_dict, try manual conversion
            assert task.name is not None
            assert task.executors is not None


class TestBunnyTES:
    """Test cases for BunnyTES class methods."""
    
    @pytest.fixture
    def bunny_tes(self):
        """Set up BunnyTES instance with mock environment variables."""
        with patch.dict(os.environ, {
            'TES_BASE_URL': 'http://test-bunny-url.com',
            'TES_DOCKER_IMAGE': 'bunny-image:latest',
            'DB_HOST': 'bunny-db-host',
            'DB_PORT': '5432',
            'DB_USERNAME': 'bunny-user',
            'DB_PASSWORD': 'bunny-password',
            'DB_NAME': 'bunny-db',
            'postgresSchema': 'public',
            '5STES_TRES': 'TRE1,TRE2',
            'COLLECTION_ID': 'test-collection-123',
            'BUNNY_LOGGER_LEVEL': 'DEBUG',
            'TASK_API_BASE_URL': 'http://task-api.example.com',
            'TASK_API_USERNAME': 'api_user',
            'TASK_API_PASSWORD': 'api_pass'
        }):
            return BunnyTES()
    
    def test_bunny_specific_env_vars(self, bunny_tes):
        """Test that BunnyTES reads bunny-specific environment variables."""
        assert bunny_tes.collection_id == 'test-collection-123'
        assert bunny_tes.bunny_logger_level == 'DEBUG'
        assert bunny_tes.task_api_base_url == 'http://task-api.example.com'
        assert bunny_tes.task_api_username == 'api_user'
        assert bunny_tes.task_api_password == 'api_pass'
    
    def test_set_inputs(self, bunny_tes):
        """Test that set_inputs sets empty list for Bunny tasks."""
        bunny_tes.set_inputs()
        assert isinstance(bunny_tes.inputs, list)
        assert len(bunny_tes.inputs) == 0
    
    def test_set_outputs(self, bunny_tes):
        """Test set_outputs creates correct output structure."""
        bunny_tes.set_outputs(
            name="bunny_output",
            output_path="/outputs",
            output_type="DIRECTORY",
            url="",
            description="Bunny output"
        )
        
        assert isinstance(bunny_tes.outputs, list)
        assert len(bunny_tes.outputs) == 1
        
        output = bunny_tes.outputs[0]
        assert isinstance(output, tes.Output)
        assert output.name == "bunny_output"
        assert output.path == "/outputs"
        assert output.type == "DIRECTORY"
        assert output.description == "Bunny output"
    
    def test_set_env(self, bunny_tes):
        """Test set_env creates correct environment variables including bunny-specific vars."""
        bunny_tes._set_env()
        
        assert isinstance(bunny_tes.env, dict)
        
        # Postgres-style vars (wrapper reads these and exports as DATASOURCE_DB_*)
        assert "postgresDatabase" in bunny_tes.env
        assert "postgresServer" in bunny_tes.env
        assert "postgresPassword" in bunny_tes.env
        assert "postgresUsername" in bunny_tes.env
        assert "postgresPort" in bunny_tes.env
        assert "postgresSchema" in bunny_tes.env

        assert bunny_tes.env["postgresSchema"] == 'public'
    
    def test_set_command(self, bunny_tes):
        """Test set_command creates correct command array for Bunny."""
        output_path = "/outputs"
        code = "DEMOGRAPHICS"
        
        bunny_tes._set_command(output_path, code)
        
        assert isinstance(bunny_tes.command, list)
        assert "--body-json" in bunny_tes.command
        assert "--output" in bunny_tes.command
        
        # Check output path in command
        assert f"{output_path}/output.json" in bunny_tes.command
        
        # Check that code is in the JSON body
        json_arg = [arg for arg in bunny_tes.command if "code" in arg][0]
        assert code in json_arg
    
    def test_set_executors(self, bunny_tes):
        """Test set_executors creates correct executor structure for Bunny."""
        code = "DEMOGRAPHICS"
        
        bunny_tes.set_executors(
            workdir="/app",
            output_path="/outputs",
            analysis=code
        )
        
        assert isinstance(bunny_tes.executors, list)
        assert len(bunny_tes.executors) == 1
        
        executor = bunny_tes.executors[0]
        assert isinstance(executor, tes.Executor)
        assert executor.image == bunny_tes.default_image
        assert executor.workdir == "/app"
        
        # Verify command was set correctly (args only; entrypoint runs bunny)
        assert isinstance(executor.command, list)
        assert "--body-json" in executor.command

        # Verify environment includes bunny-specific vars
        assert isinstance(executor.env, dict)
    
    def test_set_tes_messages(self, bunny_tes):
        """Test set_tes_messages creates complete Bunny TES task."""
        name = "test_bunny_task"
        code = "DEMOGRAPHICS"
        
        bunny_tes.set_tes_messages(task_name=name, analysis=code)
        
        # Verify task was created
        assert bunny_tes.task is not None
        assert isinstance(bunny_tes.task, tes.Task)
        
        # Verify task components
        assert bunny_tes.task.name == name
        assert bunny_tes.task.inputs == []  # Bunny tasks have empty input list
        assert bunny_tes.task.outputs is not None
        assert bunny_tes.task.executors is not None
        
        # Verify tags were set (FiveSAFES)
        assert bunny_tes.task.tags is not None
        assert "Project" in bunny_tes.task.tags
        assert "tres" in bunny_tes.task.tags
    
    def test_bunny_message_structure(self, bunny_tes):
        """Test that Bunny TES message has correct structure for metadata."""
        bunny_tes.set_tes_messages(task_name="metadata_test", analysis="DEMOGRAPHICS")
        
        task = bunny_tes.task
        
        # Verify basic structure
        assert task.name == "metadata_test"
        assert task.inputs == []
        assert len(task.outputs) == 1
        assert len(task.executors) == 1
        
        # Verify executor
        executor = task.executors[0]
        assert executor.workdir == "/app"
        
        # Verify command has bunny args (entrypoint runs bunny)
        assert "--body-json" in executor.command

        # Verify environment has postgres* vars for wrapper
        env = executor.env
        assert "postgresSchema" in env
        assert "postgresDatabase" in env


class TestMetadataRunner:
    """Test cases for MetadataRunner class."""
    
    @pytest.fixture
    def mock_tes_client(self):
        """Set up mock TES client for MetadataRunner (runner expects tes_client, token, project)."""
        client = Mock(spec=BunnyTES)
        client.task = Mock()
        return client
    
    @pytest.fixture
    def metadata_runner(self, mock_tes_client):
        """Set up MetadataRunner instance with mock TES client."""
        return MetadataRunner(tes_client=mock_tes_client, token="test_token", project="test_project")
    
    def test_metadata_initialization(self, metadata_runner, mock_tes_client):
        """Test that MetadataRunner initializes correctly."""
        assert metadata_runner.tes_client == mock_tes_client
        assert metadata_runner.analysis_orchestrator is not None
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
        metadata_runner.analysis_orchestrator.setup_analysis = Mock(return_value=("metadata_task", None, "test-bucket", ['TRE1', 'TRE2']))
        #metadata_runner.analysis_orchestrator.setup_analysis.return_value = ("metadata_task", "test-bucket", ['TRE1', 'TRE2'])
        test_data = [{"metadata": "test_data"}]
        metadata_runner.analysis_orchestrator._submit_and_collect_results = Mock(return_value=("task-123", test_data))
        #metadata_runner.analysis_orchestrator._submit_and_collect_results.return_value = ("task-123", test_data)
        
        # Call get_metadata
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
        assert result['result'] == test_data
    
    def test_get_metadata_calls_tes_methods(self, metadata_runner):
        """Test that get_metadata calls the correct TES methods."""
        metadata_runner.analysis_orchestrator.setup_analysis = Mock(return_value=("metadata_task", None, "test-bucket", ['TRE1']))
        #metadata_runner.analysis_orchestrator.setup_analysis.return_value = ("metadata_task", "test-bucket", ['TRE1', 'TRE2'])
        metadata_runner.analysis_orchestrator._submit_and_collect_results = Mock(return_value=("task-456", [{"data": "test"}]))
        
        #metadata_runner.analysis_orchestrator.setup_analysis.return_value = ("metadata_task", "test-bucket", ['TRE1'])
        #metadata_runner.analysis_orchestrator._submit_and_collect_results.return_value = ("task-456", [{"data": "test"}])
        
        # Call get_metadata
        metadata_runner.get_metadata(tres=['TRE1'])
        
        # Verify TES client methods were called
        metadata_runner.analysis_orchestrator.tes_client.set_tes_messages.assert_called_once()
        metadata_runner.analysis_orchestrator.tes_client.set_tags.assert_called_once()
        metadata_runner.analysis_orchestrator.tes_client.create_FiveSAFES_TES_message.assert_called_once()
    
    def test_get_metadata_stores_raw_data(self, metadata_runner):
        """Test that get_metadata stores raw data (placeholder - no aggregation yet)."""
        metadata_runner.analysis_orchestrator.setup_analysis = Mock(return_value=("metadata_task", None, "test-bucket", ['TRE1', 'TRE2']))
        #metadata_runner.analysis_orchestrator.setup_analysis.return_value = ("metadata_task", "test-bucket", ['TRE1', 'TRE2'])
        
        # Mock data from two TREs
        mock_data = [
            {"count": 100, "mean": 25.5},
            {"count": 150, "mean": 30.2}
        ]
        metadata_runner.analysis_orchestrator._submit_and_collect_results = Mock(return_value=("task-789", mock_data))
        #metadata_runner.analysis_orchestrator._submit_and_collect_results.return_value = ("task-789", mock_data)
        
        result = metadata_runner.get_metadata(tres=['TRE1', 'TRE2'])
        
        # TODO: Once aggregation is implemented, this test should verify proper aggregation
        # For now, verify that raw data is passed through unchanged
        assert result['result'] == mock_data
        
        # Verify raw data was stored in aggregated_data
        assert 'raw_data' in metadata_runner.aggregated_data
        assert metadata_runner.aggregated_data['raw_data'] == mock_data
    
    def test_get_metadata_error_handling(self, metadata_runner):
        """Test that get_metadata handles errors properly."""
        metadata_runner.analysis_orchestrator.setup_analysis = Mock(return_value=("metadata_task", None, "test-bucket", ['TRE1']))
        #metadata_runner.analysis_orchestrator.setup_analysis.return_value = ("metadata_task", "test-bucket", ['TRE1'])
        metadata_runner.analysis_orchestrator._submit_and_collect_results = Mock(side_effect=Exception("TES submission failed"))
        #metadata_runner.analysis_orchestrator._submit_and_collect_results.side_effect = Exception("TES submission failed")
        
        # Should raise the exception
        with pytest.raises(Exception, match="TES submission failed"):
            metadata_runner.get_metadata(tres=['TRE1'])


class TestMetadataRunnerTESIntegration:
    """Integration tests for metadata TES message creation."""
    
    @pytest.fixture
    def bunny_tes(self):
        """Set up BunnyTES instance."""
        with patch.dict(os.environ, {
            'TES_BASE_URL': 'http://bunny.example.com',
            'TES_DOCKER_IMAGE': 'bunny:v1.0',
            'DB_HOST': 'metadata-db',
            'DB_PORT': '5432',
            'DB_USERNAME': 'metadata_user',
            'DB_PASSWORD': 'metadata_pass',
            'DB_NAME': 'metadata_db',
            'SQL_SCHEMA': 'cdm',
            '5STES_TRES': 'TRE1,TRE2,TRE3',
            'COLLECTION_ID': 'metadata-collection',
            'BUNNY_LOGGER_LEVEL': 'INFO',
            'TASK_API_BASE_URL': 'http://api.bunny.com',
            'TASK_API_USERNAME': 'bunny_api',
            'TASK_API_PASSWORD': 'bunny_secret'
        }):
            return BunnyTES()
    
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
