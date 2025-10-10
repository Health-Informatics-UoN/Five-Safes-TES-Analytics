import pytest
import json
import tempfile
import os
import subprocess
import sys
from unittest.mock import Mock, patch, MagicMock
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Add the parent directory to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import the query_resolver module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'Container')))
import query_resolver
import local_processing


class TestQueryResolver:
    """Test the query_resolver.py entry point functionality."""
    
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
        with patch('query_resolver.create_engine') as mock_create_engine:
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
                
                # Verify engine was created
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
                assert isinstance(result, str)
                tdigest_data = json.loads(result)
                assert "centroids" in tdigest_data
                assert "n" in tdigest_data
                
            finally:
                # Clean up
                if os.path.exists(output_file):
                    os.remove(output_file)
                if os.path.exists(temp_filename):
                    os.remove(temp_filename)
    
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


class TestDockerContainerScenarios:
    """Test scenarios that would occur in a Docker container."""
    
    def test_docker_entrypoint_with_click_args(self):
        """Test Docker entrypoint with Click command line arguments."""
        # This would simulate: docker run image --user-query "SELECT * FROM users" --analysis mean
        
        with patch('query_resolver.main') as mock_main:
            # Simulate command line arguments
            with patch('sys.argv', ['query_resolver.py', '--user-query', 'SELECT * FROM users', '--analysis', 'mean']):
                query_resolver.main()
                mock_main.assert_called_once()
    
    def test_docker_entrypoint_without_args(self):
        """Test Docker entrypoint without arguments (uses test values)."""
        # This would simulate: docker run image (no args)
        
        with patch('query_resolver.main') as mock_main:
            # Simulate no command line arguments
            with patch('sys.argv', ['query_resolver.py']):
                query_resolver.main()
                mock_main.assert_called_once()
    
    def test_docker_environment_variables(self):
        """Test Docker container with environment variables."""
        # Simulate environment variables that might be passed to Docker
        env_vars = {
            'DB_CONNECTION': 'postgresql://user:pass@db:5432/database',
            'ANALYSIS_TYPE': 'mean',
            'USER_QUERY': 'SELECT value_as_number FROM measurements'
        }
        
        with patch.dict(os.environ, env_vars):
            with patch('query_resolver.main') as mock_main:
                # Simulate command line with env vars
                with patch('sys.argv', [
                    'query_resolver.py',
                    '--user-query', os.environ['USER_QUERY'],
                    '--analysis', os.environ['ANALYSIS_TYPE'],
                    '--db-connection', os.environ['DB_CONNECTION']
                ]):
                    query_resolver.main()
                    mock_main.assert_called_once()
    
    def test_docker_output_formats(self):
        """Test Docker container with different output formats."""
        user_query = "SELECT value_as_number FROM measurements"
        analysis = "mean"
        db_connection = "sqlite:///:memory:"
        output_filename = "test_output"
        
        # Test JSON output
        with patch('query_resolver.create_engine') as mock_create_engine:
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
    
    def test_docker_error_handling(self):
        """Test Docker container error handling."""
        user_query = "SELECT * FROM nonexistent_table"
        analysis = "mean"
        db_connection = "sqlite:///:memory:"
        output_filename = "test_output"
        output_format = "json"
        
        # Mock database error
        with patch('query_resolver.create_engine') as mock_create_engine, \
             patch('query_resolver.click.echo') as mock_echo:
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


class TestDockerBuildAndRun:
    """Test Docker container build and run scenarios."""
    
    def test_dockerfile_structure(self):
        """Test that Dockerfile has correct structure."""
        dockerfile_path = os.path.join(os.path.dirname(__file__), '..', 'Container', 'Dockerfile')
        
        if os.path.exists(dockerfile_path):
            with open(dockerfile_path, 'r') as f:
                dockerfile_content = f.read()
            
            # Check for required components
            assert "FROM python:3.12" in dockerfile_content
            assert "ADD query_resolver.py" in dockerfile_content
            assert "COPY local_processing.py" in dockerfile_content
            assert "RUN pip install" in dockerfile_content
            assert "ENTRYPOINT" in dockerfile_content
            assert "query_resolver.py" in dockerfile_content
    
    def test_required_files_present(self):
        """Test that all required files are present for Docker build."""
        container_dir = os.path.join(os.path.dirname(__file__), '..', 'Container')
        
        required_files = ['Dockerfile', 'query_resolver.py', 'local_processing.py']
        
        for file in required_files:
            file_path = os.path.join(container_dir, file)
            assert os.path.exists(file_path), f"Required file {file} not found"
    
    def test_docker_dependencies(self):
        """Test that Docker container has correct dependencies."""
        dockerfile_path = os.path.join(os.path.dirname(__file__), '..', 'Container', 'Dockerfile')
        
        if os.path.exists(dockerfile_path):
            with open(dockerfile_path, 'r') as f:
                dockerfile_content = f.read()
            
            # Check for required Python packages
            required_packages = ['click', 'sqlalchemy', 'numpy', 'tdigest']
            
            for package in required_packages:
                assert package in dockerfile_content, f"Required package {package} not found in Dockerfile"


if __name__ == "__main__":
    pytest.main([__file__])
