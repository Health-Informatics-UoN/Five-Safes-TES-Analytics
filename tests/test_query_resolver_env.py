"""
Unit tests for query_resolver building the connection string from environment variables
when --db-connection is not provided (e.g. when run in the container with postgres* env set).
"""
import os
import sys
from unittest.mock import patch
from urllib.parse import urlparse

import pytest

pytest.importorskip("sqlalchemy")

# Add docker directory so we can import query_resolver at runtime
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'docker')))

import query_resolver  # type: ignore[import-untyped]


class TestParseConnectionStringFromEnv:
    """Tests for parse_connection_string(None) building URL from postgres* env vars."""

    @patch.dict(os.environ, {
        'postgresUsername': 'myuser',
        'postgresPassword': 'mypass',
        'postgresServer': 'db.example.com',
        'postgresPort': '5432',
        'postgresDatabase': 'omop',
    }, clear=False)
    def test_builds_url_from_env_when_connection_string_is_none(self):
        """When connection_string is None, URL is built from postgres* env vars."""
        result = query_resolver.parse_connection_string(None)
        parsed = urlparse(result)
        assert parsed.scheme == 'postgresql'
        assert parsed.hostname == 'db.example.com'
        assert parsed.port == 5432
        assert parsed.path == '/omop' or parsed.path.rstrip('/') == 'omop'
        assert parsed.username == 'myuser'
        assert parsed.password == 'mypass'

    @patch.dict(os.environ, {
        'postgresUsername': 'user',
        'postgresPassword': 'pass',
        'postgresServer': 'host',
        'postgresDatabase': 'dbname',
    }, clear=False)
    def test_port_defaults_to_5432_when_postgres_port_missing(self):
        """When postgresPort is not set, port in URL is 5432."""
        if 'postgresPort' in os.environ:
            del os.environ['postgresPort']
        result = query_resolver.parse_connection_string(None)
        parsed = urlparse(result)
        assert parsed.port == 5432

    @patch.dict(os.environ, {
        'postgresUsername': 'user@domain',
        'postgresPassword': 'p@ss',
        'postgresServer': 'host',
        'postgresPort': '5432',
        'postgresDatabase': 'db',
    }, clear=False)
    def test_special_chars_in_credentials_are_encoded(self):
        """Username and password from env are URL-encoded in the connection string."""
        result = query_resolver.parse_connection_string(None)
        parsed = urlparse(result)
        assert parsed.scheme == 'postgresql'
        # @ in password is encoded in URL; urlparse decodes so we get the intended value
        assert parsed.password == 'p@ss'
        assert parsed.username == 'user@domain'

    def test_missing_required_env_raises(self):
        """When required postgres* env vars are missing/empty, validate_environment raises ValueError."""
        with patch.dict(os.environ, {
            'postgresUsername': '',
            'postgresPassword': '',
            'postgresServer': '',
            'postgresDatabase': '',
        }, clear=False):
            with pytest.raises(ValueError) as exc_info:
                query_resolver.validate_environment()
        assert "Missing required env var" in str(exc_info.value)
        assert "postgres" in str(exc_info.value)
