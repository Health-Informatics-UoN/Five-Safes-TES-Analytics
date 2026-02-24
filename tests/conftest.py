import os
import pytest
import sys

# Add the parent directory to the path so we can import our modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

@pytest.fixture(autouse=True)
def setup_test_environment(request, monkeypatch):
    """Set up test environment variables for unit style  tests."""
    
    if request.node.get_closest_marker("integration"):  # disables mocking of env vars for integration tests 
        return    

    monkeypatch.setenv('TES_BASE_URL', 'http://test-tes-url.com')
    monkeypatch.setenv('TES_DOCKER_IMAGE', 'test-docker-image:latest')
    monkeypatch.setenv('DB_HOST', 'test-db-host')
    monkeypatch.setenv('DB_PORT', '5432')
    monkeypatch.setenv('DB_USERNAME', 'test-user')
    monkeypatch.setenv('DB_PASSWORD', 'test-password')
    monkeypatch.setenv('DB_NAME', 'test-db')
    monkeypatch.setenv('MINIO_ENDPOINT', 'test-minio-endpoint')
    monkeypatch.setenv('MINIO_ACCESS_KEY', 'test-access-key')
    monkeypatch.setenv('MINIO_SECRET_KEY', 'test-secret-key')
    monkeypatch.setenv('MINIO_OUTPUT_BUCKET', 'test-output-bucket')
    monkeypatch.setenv('MINIO_STS_ENDPOINT', 'http://test-sts-endpoint.com')
    monkeypatch.setenv('5STES_TOKEN', 'test-token')
    monkeypatch.setenv('5STES_PROJECT', 'test-project')
    