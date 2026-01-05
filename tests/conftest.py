import os
import pytest
import sys

# Add the parent directory to the path so we can import our modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """Set up test environment variables for all tests."""
    # Set required environment variables for testing
    os.environ['TES_BASE_URL'] = 'http://test-tes-url.com'
    os.environ['TES_DOCKER_IMAGE'] = 'test-docker-image:latest'
    os.environ['DB_HOST'] = 'test-db-host'
    os.environ['DB_PORT'] = '5432'
    os.environ['DB_USERNAME'] = 'test-user'
    os.environ['DB_PASSWORD'] = 'test-password'
    os.environ['DB_NAME'] = 'test-db'
    os.environ['MINIO_ENDPOINT'] = 'test-minio-endpoint'
    os.environ['MINIO_ACCESS_KEY'] = 'test-access-key'
    os.environ['MINIO_SECRET_KEY'] = 'test-secret-key'
    os.environ['MINIO_OUTPUT_BUCKET'] = 'test-output-bucket'
    os.environ['MINIO_STS_ENDPOINT'] = 'http://test-sts-endpoint.com'
    os.environ['5STES_TOKEN'] = 'test-token'
    os.environ['5STES_PROJECT'] = 'test-project'
    
    yield
    
    # Clean up environment variables after tests
    test_vars = [
        'TES_BASE_URL', 'TES_DOCKER_IMAGE', 'DB_HOST', 'DB_PORT', 
        'DB_USERNAME', 'DB_PASSWORD', 'DB_NAME', 'MINIO_ENDPOINT',
        'MINIO_ACCESS_KEY', 'MINIO_SECRET_KEY', 'MINIO_OUTPUT_BUCKET',
        'MINIO_STS_ENDPOINT', '5STES_TOKEN', '5STES_PROJECT'
    ]
    for var in test_vars:
        if var in os.environ:
            del os.environ[var] 