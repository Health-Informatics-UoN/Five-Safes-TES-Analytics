"""
Unit tests for BunnyTES task construction.

Verifies that BunnyTES.set_tes_messages() builds a well-formed TES task: the executor
environment contains the expected postgres* variables, the command is structured as
args-only (no leading 'bunny' prefix), and the workdir is set correctly.
"""
import os
from unittest.mock import patch

import pytest

pytest.importorskip("tes")

from five_safes_tes_analytics.clients.bunny_tes_client import BunnyTES


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
