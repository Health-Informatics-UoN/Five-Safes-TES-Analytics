"""
Unit tests for BunnyTES task construction.

Verifies that BunnyTES.set_tes_messages() builds a well-formed TES task: the executor
environment contains the expected postgres* variables, the command is structured as
args-only (no leading 'bunny' prefix), and the workdir is set correctly.
"""
import os
from unittest.mock import patch

import pytest
import tes

from five_safes_tes_analytics.clients.bunny_tes_client import BunnyTES


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


class TestBunnyTESTaskConstruction:
    """Task built by BunnyTES must match what the bunny-wrapper entrypoint expects."""

    @pytest.fixture
    def bunny_tes(self):
        """BunnyTES with mocked env so default_db_config is set."""
        with patch.dict(os.environ, {
            'TES_BASE_URL': 'http://tes',
            'TES_DOCKER_IMAGE': 'bunny-wrapper:test',
            'postgresServer': 'db.example.com',
            'postgresPort': '5432',
            'postgresUsername': 'bunny_user',
            'postgresPassword': 'bunny_pass',
            'postgresDatabase': 'bunny_db',
            'postgresSchema': 'public',
            '5STES_TRES': 'TRE1',
            'COLLECTION_ID': 'col1',
            'TASK_API_BASE_URL': 'http://api',
            'TASK_API_USERNAME': 'u',
            'TASK_API_PASSWORD': 'p',
        }, clear=False):
            yield BunnyTES()

    def test_executor_env_has_postgres_vars_for_entrypoint(self, bunny_tes):
        """Entrypoint reads postgres* and exports DATASOURCE_DB_*; task must provide postgres*."""
        bunny_tes.set_tes_messages(analysis='DISTRIBUTION', task_name='t', task_description='')
        executor = bunny_tes.task.executors[0]
        env = executor.env
        assert env.get('postgresDatabase') == 'bunny_db'
        assert env.get('postgresServer') == 'db.example.com'
        assert env.get('postgresPort') == '5432'
        assert env.get('postgresSchema') == 'public'
        assert env.get('postgresUsername') == 'bunny_user'
        assert env.get('postgresPassword') == 'bunny_pass'

    def test_command_is_args_only_no_bunny_prefix(self, bunny_tes):
        """Entrypoint runs 'bunny $@'; command must be args only (no leading 'bunny')."""
        bunny_tes.set_tes_messages(analysis='DISTRIBUTION', task_name='t', task_description='')
        executor = bunny_tes.task.executors[0]
        command = executor.command
        assert isinstance(command, list)
        assert len(command) >= 1
        assert command[0] != 'bunny'
        assert '--body-json' in command
        assert '--output' in command

    def test_executor_has_workdir(self, bunny_tes):
        """Executor workdir is set (wrapper may use it)."""
        bunny_tes.set_tes_messages(analysis='DISTRIBUTION', task_name='t', task_description='')
        executor = bunny_tes.task.executors[0]
        assert executor.workdir is not None
        assert executor.workdir in ('/', '/app')
