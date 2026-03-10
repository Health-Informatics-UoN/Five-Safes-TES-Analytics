"""
Unit tests for five_safes_tes_analytics.node.query_resolver.

Covers:
- Building a PostgreSQL connection URL from environment variables. 
- process_query() exiting with code 1 and writing an appropriate error message
  for unsupported analysis types and database errors.
- process_query() correctly writing JSON output for valid analysis types.
- parse_connection_string() converting semicolon-format connection strings to
  SQLAlchemy URLs, including special character encoding.
- The Click CLI interface: expected options, their names, and defaults.

All tests are unit tests — external dependencies (SQLAlchemy engine, database
connections) are mocked. No real database or container is required.
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
    
    def test_process_query_writes_json_output(self):
        user_query = "SELECT value_as_number FROM measurements"
        analysis = "mean"
        db_connection = "sqlite:///:memory:"
        output_filename = "test_output"
        
        # Test JSON output
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
            
            output_file = f"{output_filename}.json"
            try:
                query_resolver.process_query(user_query, analysis, db_connection, output_filename, "json")
                
                assert os.path.exists(output_file)
                
                with open(output_file, 'r') as f:
                    result = json.load(f)
                
                assert isinstance(result, dict)
                
            finally:
                if os.path.exists(output_file):
                    os.remove(output_file)
    
    def test_process_query_exits_on_database_error(self):
        user_query = "SELECT * FROM nonexistent_table"
        analysis = "mean"
        db_connection = "sqlite:///:memory:"
        output_filename = "test_output"
        output_format = "json"
        
        # Mock database error
        with patch('five_safes_tes_analytics.node.query_resolver.create_engine') as mock_create_engine, \
             patch('five_safes_tes_analytics.node.query_resolver.click.echo') as mock_echo:
            mock_engine = Mock()
            mock_engine.connect.side_effect = Exception("Database connection failed")
            mock_create_engine.return_value = mock_engine
            
            # process_query catches all exceptions and calls sys.exit(1)
            with pytest.raises(SystemExit):
                query_resolver.process_query(user_query, analysis, db_connection, output_filename, output_format)
            
            # Verify the error message was printed
            mock_echo.assert_called_once()
            error_call = mock_echo.call_args
            assert "Database connection failed" in str(error_call)


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


class TestClickCLI:
    """Validate Click command options and defaults."""

    def test_click_options_exist(self):
        """Test that all expected Click options exist.
        
        Note: Click converts dashes to underscores in parameter names.
        So --user-query becomes user_query, --db-connection becomes db_connection, etc.
        """
        cmd = query_resolver.main
        option_names = {opt.name for opt in cmd.params}
        # Expected parameter names (with underscores, not dashes)
        expected_params = {'user_query', 'analysis', 'db_connection', 'output_filename', 'output_format'}
        assert expected_params <= option_names, f"Missing options. Found: {option_names}, Expected: {expected_params}"

    def test_click_cli_option_names(self):
        """Test that CLI option names (with dashes) are correct.
        
        opt.opts contains the CLI option names (e.g., ['--user-query']),
        while opt.name contains the Python parameter name (e.g., 'user_query').
        """
        cmd = query_resolver.main
        cli_options = set()
        for opt in cmd.params:
            # opt.opts is a list like ['--user-query'] or ['-u', '--user-query']
            for cli_opt in opt.opts:
                if cli_opt.startswith('--'):
                    cli_options.add(cli_opt[2:])  # Remove '--' prefix
        
        expected_cli_options = {'user-query', 'analysis', 'db-connection', 'output-filename', 'output-format'}
        assert expected_cli_options <= cli_options, f"Missing CLI options. Found: {cli_options}, Expected: {expected_cli_options}"

    def test_output_format_default_json(self):
        """Test that output-format option defaults to 'json'."""
        cmd = query_resolver.main
        # Click converts --output-format to output_format as the parameter name
        out_opt = next(opt for opt in cmd.params if opt.name == 'output_format')
        assert out_opt.default == 'json'
    
    def test_main_function_with_unsupported_analysis(self):
        """Test main function with unsupported analysis type."""
        user_query = "SELECT * FROM users"
        analysis = "unsupported_analysis"
        db_connection = "sqlite:///:memory:"
        output_filename = "test_output"
        output_format = "json"
        
        # process_query catches all exceptions and calls sys.exit(1)
        with patch('five_safes_tes_analytics.node.query_resolver.click.echo') as mock_echo:
            with pytest.raises(SystemExit):
                query_resolver.process_query(user_query, analysis, db_connection, output_filename, output_format)
            
            # Verify the error message was printed
            mock_echo.assert_called_once()
            error_call = mock_echo.call_args
            assert "Unsupported analysis type" in str(error_call)
    
    def test_main_function_with_percentile_sketch(self):
        """Test main function with percentile sketch analysis."""
        user_query = "SELECT value_as_number FROM measurements"
        analysis = "percentile_sketch"
        db_connection = "sqlite:///:memory:"
        output_filename = "test_output"
        output_format = "json"
        
        # Mock the database engine and connection
        with patch('five_safes_tes_analytics.node.query_resolver.create_engine') as mock_create_engine:
            mock_engine = Mock()
            mock_conn = Mock()
            
            # Create a proper mock that behaves like engine.Result
            from sqlalchemy.engine import Result
            mock_result = Mock(spec=Result)
            mock_result.keys.return_value = ["value_as_number"]
            mock_result.fetchall.return_value = [
                (10.5,),
                (20.3,),
                (15.7,),
                (None,),  # Should be filtered out
            ]
            
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
                
                # Check output file was created
                output_file = f"{output_filename}.{output_format}"
                assert os.path.exists(output_file)
                
                # Verify output content (should be JSON string from TDigest)
                with open(output_file, 'r') as f:
                    result = json.load(f)
                
                # Should be a JSON string containing TDigest data
                assert isinstance(result, dict)
                
                assert "centroids" in result
                assert "n" in result
                
            finally:
                # Clean up
                if os.path.exists(output_file):
                    os.remove(output_file)
                if os.path.exists(temp_filename):
                    os.remove(temp_filename)
    
    @pytest.mark.integration
    def test_main_function_with_contingency_table(self):
        """Test main function with contingency table analysis."""
        user_query = "SELECT gender, race FROM patients"
        analysis = "contingency_table"
        db_connection = "sqlite:///:memory:"
        output_filename = "test_output"
        output_format = "json"
        
        # Create a real SQLite database with test data
        import sqlite3
        
        # Create temporary database
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as temp_db:
            temp_db_path = temp_db.name
        
        try:
            # Create test data
            conn = sqlite3.connect(temp_db_path)
            cursor = conn.cursor()
            
            # Create test table
            cursor.execute("""
                CREATE TABLE patients (
                    id INTEGER PRIMARY KEY,
                    gender TEXT,
                    race TEXT
                )
            """)
            
            # Insert test data
            test_data = [
                (1, "Male", "White"),
                (2, "Male", "Black"),
                (3, "Female", "White"),
                (4, "Female", "Black"),
                (5, "Male", "White"),
                (6, "Female", "White"),
                (7, "Male", "Black"),
                (8, "Female", "Black"),
            ]
            cursor.executemany("INSERT INTO patients (id, gender, race) VALUES (?, ?, ?)", test_data)
            conn.commit()
            conn.close()
            
            # Update connection string to use our test database
            db_connection = f"sqlite:///{temp_db_path}"
            
            # Create temporary file for output
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
                temp_filename = temp_file.name
            
            output_file = f"{output_filename}.{output_format}"
            try:
                # Call process_query function directly
                query_resolver.process_query(user_query, analysis, db_connection, output_filename, output_format)
                
                # Check output file was created
                assert os.path.exists(output_file)
                
                # Verify output content
                with open(output_file, 'r') as f:
                    result = json.load(f)
                
                # Should be a list of dictionaries with grouped contingency table data
                assert isinstance(result, list)
                assert len(result) == 4  # Should have 4 combinations
                
                # Check that all rows have the expected structure
                for row in result:
                    assert "gender" in row
                    assert "race" in row
                    assert "n" in row
                    assert isinstance(row["n"], int)
                    assert row["n"] > 0
                
                # Check that all expected combinations are present
                combinations = set()
                for row in result:
                    combinations.add((row["gender"], row["race"]))
                
                expected_combinations = {("Male", "White"), ("Male", "Black"), ("Female", "White"), ("Female", "Black")}
                assert combinations == expected_combinations
                
                # Verify counts are reasonable (should sum to 8 total patients)
                total_count = sum(row["n"] for row in result)
                assert total_count == 8
                
                # Verify specific counts (each combination should have 2 patients)
                for row in result:
                    assert row["n"] == 2
                
            finally:
                # Clean up
                if os.path.exists(output_file):
                    os.remove(output_file)
                if os.path.exists(temp_filename):
                    os.remove(temp_filename)
                    
        finally:
            # Clean up database
            if os.path.exists(temp_db_path):
                os.remove(temp_db_path)

