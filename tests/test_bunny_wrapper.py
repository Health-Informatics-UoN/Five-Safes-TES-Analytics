"""
Unit tests for the bunny wrapper: task structure and entrypoint contract.

The wrapper runs in the container: it reads postgres* env vars and exports DATASOURCE_DB_*
for bunny. These tests verify (1) BunnyTES builds the task with the env/command the
wrapper expects, and (2) the entrypoint script has the expected shape.
"""
import os
import sys
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

pytest.importorskip("tes")

# Add project root for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import tes
from bunny_tes import BunnyTES


class TestBunnyWrapperTaskStructure:
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


class TestBunnyWrapperEntrypointScript:
    """Entrypoint script must read postgres* and export DATASOURCE_DB_*."""

    def test_entrypoint_exists(self):
        """Entrypoint script file exists."""
        path = Path(__file__).parent.parent / 'docker' / 'bunny-wrapper' / 'entrypoint.sh'
        assert path.exists(), f"Expected entrypoint at {path}"

    def test_entrypoint_exports_datasource_from_postgres_vars(self):
        """Entrypoint should export DATASOURCE_DB_* from postgres* vars (so task must set postgres*)."""
        path = Path(__file__).parent.parent / 'docker' / 'bunny-wrapper' / 'entrypoint.sh'
        if not path.exists():
            pytest.skip("entrypoint.sh not found")
        content = path.read_text()
        assert 'postgresDatabase' in content or 'postgresServer' in content
        assert 'DATASOURCE_DB' in content
        assert 'bunny' in content

    def test_entrypoint_passes_args_to_bunny(self):
        """Entrypoint must run bunny with task command as args ($@)."""
        path = Path(__file__).parent.parent / 'docker' / 'bunny-wrapper' / 'entrypoint.sh'
        if not path.exists():
            pytest.skip("entrypoint.sh not found")
        content = path.read_text()
        assert 'bunny' in content
        assert '$@' in content or '"$@"' in content
