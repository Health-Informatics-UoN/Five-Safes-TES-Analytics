import pytest
import json
import tempfile
import os
import sys
from unittest.mock import Mock, patch

# Add the parent directory to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import the query_resolver module (docker package)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'docker')))
from five_safes_tes_analytics.node import query_resolver


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
        with patch('query_resolver.click.echo') as mock_echo:
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
        with patch('query_resolver.create_engine') as mock_create_engine:
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
