import os
from unittest.mock import patch

import pytest
import tes

from five_safes_tes_analytics.clients.analytics_tes_client import AnalyticsTES


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