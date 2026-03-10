"""
Unit tests for query_resolver building the connection string from environment variables
when --db-connection is not provided (e.g. when run in the container with postgres* env set).
"""
import json 
import os
import tempfile 
from unittest.mock import patch, Mock 
from urllib.parse import urlparse

import pytest

pytest.importorskip("sqlalchemy")

from five_safes_tes_analytics.node import query_resolver  


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
        """Username and password from env are URL-encoded in the connection string.
        
        If a special character (in this case @) appears in the user/password then this should be encoded in the url
        via percent-encoding. 
        """
        result = query_resolver.parse_connection_string(None)
        parsed = urlparse(result)
        assert parsed.scheme == 'postgresql'
        assert parsed.password == 'p%40ss'
        assert parsed.username == 'user%40domain'

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


class TestValidateEnvironment: 
    DB_CONN = "postgresql://user:pass@localhost:5432/db"
    USER_QUERY = "SELECT * FROM users"

    @patch('five_safes_tes_analytics.node.query_resolver.create_engine')
    def test_process_query_with_unsupported_analysis_type(self, mock_create_engine):
        """Unsupported analysis type should cause sys.exit(1)."""
        with pytest.raises(SystemExit) as exc_info:
            query_resolver.process_query(self.USER_QUERY, "unsupported", self.DB_CONN, "output", "json")
        assert exc_info.value.code == 1

    @patch('five_safes_tes_analytics.node.query_resolver.create_engine')
    def test_process_query_with_none_analysis_type(self, mock_create_engine):
        """None analysis type should cause sys.exit(1)."""
        with pytest.raises(SystemExit) as exc_info:
            query_resolver.process_query(self.USER_QUERY, None, self.DB_CONN, "output", "json")
        assert exc_info.value.code == 1

    @patch('five_safes_tes_analytics.node.query_resolver.create_engine')
    def test_process_query_unsupported_analysis_type_error_message(self, mock_create_engine, capsys):
        """Error message should mention the unsupported analysis type."""
        with pytest.raises(SystemExit):
            query_resolver.process_query(self.USER_QUERY, "unsupported", self.DB_CONN, "output", "json")
        captured = capsys.readouterr()
        assert "Unsupported analysis type" in captured.err


class TestProcessQuery: 
    def test_decimal_encoder(self):
        """Test DecimalEncoder for JSON serialization."""
        from decimal import Decimal
        import json
        
        encoder = query_resolver.DecimalEncoder()
        
        # Test with Decimal - should convert to float
        decimal_value = Decimal('123.45')
        result = encoder.default(decimal_value)
        assert result == 123.45
        assert isinstance(result, float)
        
        # Test with mixed data using json.dumps (proper usage)
        test_data = {
            "decimal_value": Decimal('123.45'),
            "string_value": "test",
            "number_value": 42
        }
        
        json_str = json.dumps(test_data, cls=query_resolver.DecimalEncoder)
        parsed_data = json.loads(json_str)
        
        assert parsed_data["decimal_value"] == 123.45
        assert parsed_data["string_value"] == "test"
        assert parsed_data["number_value"] == 42
    
    def test_main_function_with_valid_inputs(self):
        """Test main function with valid inputs."""
        user_query = "SELECT value_as_number FROM measurements WHERE value_as_number IS NOT NULL"
        analysis = "mean"
        db_connection = "sqlite:///:memory:"  # Use in-memory SQLite for testing
        output_filename = "test_output"
        output_format = "json"
        
        # Mock the database engine and connection
        with patch('five_safes_tes_analytics.node.query_resolver.create_engine') as mock_create_engine:
            mock_engine = Mock()
            mock_conn = Mock()
            
            # Create a proper mock that behaves like engine.Result
            from sqlalchemy.engine import Result
            mock_result = Mock(spec=Result)
            mock_result.keys.return_value = ["n", "total"]
            mock_result.fetchall.return_value = [(100, 1500.5)]
            
            # Set up context manager properly
            mock_connection_context = Mock()
            mock_connection_context.__enter__ = Mock(return_value=mock_conn)
            mock_connection_context.__exit__ = Mock(return_value=None)
            
            mock_create_engine.return_value = mock_engine
            mock_engine.connect.return_value = mock_connection_context
            mock_conn.execute.return_value = mock_result
            
            # Create temporary file for output
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
                temp_filename = temp_file.name
            
            try:
                # Call process_query function directly
                query_resolver.process_query(user_query, analysis, db_connection, output_filename, output_format)
                
                # Verify engine was created with parsed URL (already SQLAlchemy format)
                mock_create_engine.assert_called_once_with(db_connection)
                
                # Verify query was executed
                mock_conn.execute.assert_called()
                
                # Check output file was created
                output_file = f"{output_filename}.{output_format}"
                assert os.path.exists(output_file)
                
                # Verify output content
                with open(output_file, 'r') as f:
                    result = json.load(f)
                
                assert "n" in result
                assert "total" in result
                assert result["n"] == 100
                assert result["total"] == 1500.5
                
            finally:
                # Clean up
                if os.path.exists(output_file):
                    os.remove(output_file)
                if os.path.exists(temp_filename):
                    os.remove(temp_filename)


class TestConnectionStringParsing:
    """Tests for converting semicolon-style connection strings to SQLAlchemy URLs."""

    def test_parse_semicolon_format(self):
        cs = "Host=localhost:5432;Username=user;Password=pass;Database=db"
        result = query_resolver.parse_connection_string(cs)
        assert result == "postgresql://user:pass@localhost:5432/db"

    def test_parse_with_prefixes(self):
        """Parse semicolon format (same as test_parse_semicolon_format, different values)."""
        cs = "Host=db:5432;Username=postgres;Password=password;Database=omop"
        result = query_resolver.parse_connection_string(cs)
        assert result == "postgresql://postgres:password@db:5432/omop"

    def test_parse_special_chars_in_credentials(self):
        cs = "Host=mydb:5432;Username=user+name;Password=p@ss word;Database=d_b"
        result = query_resolver.parse_connection_string(cs)
        # user+name -> user%2Bname, p@ss word -> p%40ss+word
        assert result == "postgresql://user%2Bname:p%40ss+word@mydb:5432/d_b"

    def test_process_query_uses_converted_url(self):
        user_query = "SELECT value_as_number FROM measurements"
        analysis = "mean"
        semicolon_cs = "Host=db:5432;Username=postgres;Password=secret;Database=omop"
        output_filename = "conn_parse_it"
        output_format = "json"

        with patch('five_safes_tes_analytics.node.query_resolver.create_engine') as mock_create_engine:
            mock_engine = Mock()
            mock_conn = Mock()

            from sqlalchemy.engine import Result
            mock_result = Mock(spec=Result)
            mock_result.keys.return_value = ["n", "total"]
            mock_result.fetchall.return_value = [(1, 2.0)]

            mock_connection_context = Mock()
            mock_connection_context.__enter__ = Mock(return_value=mock_conn)
            mock_connection_context.__exit__ = Mock(return_value=None)

            mock_create_engine.return_value = mock_engine
            mock_engine.connect.return_value = mock_connection_context
            mock_conn.execute.return_value = mock_result

            try:
                query_resolver.process_query(user_query, analysis, semicolon_cs, output_filename, output_format)

                expected_url = "postgresql://postgres:secret@db:5432/omop"
                mock_create_engine.assert_called_once_with(expected_url)
            finally:
                out = f"{output_filename}.{output_format}"
                if os.path.exists(out):
                    os.remove(out)
