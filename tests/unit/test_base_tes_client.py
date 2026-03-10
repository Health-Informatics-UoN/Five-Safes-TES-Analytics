import os
from unittest.mock import patch
from urllib.parse import urlparse

import pytest

from five_safes_tes_analytics.clients.analytics_tes_client import AnalyticsTES


class TestBaseTESClientURLConstruction:
    """Test cases for TES Client URL construction and configuration."""
    
    def test_tes_url_construction(self):
        """Test TES URL construction from base URL."""
        # NOTE: There's a bug in the URL construction when base_url has no path
        # The Path object doesn't add a leading slash, resulting in URLs like
        # "http://example.comv1" instead of "http://example.com/v1"
        # This test documents the current (buggy) behavior
        with patch.dict(os.environ, {
            'TES_BASE_URL': 'http://example.com',
            'TES_DOCKER_IMAGE': 'test:latest',
            'DB_HOST': 'db',
            'DB_PORT': '5432',
            'DB_USERNAME': 'user',
            'DB_PASSWORD': 'pass',
            'DB_NAME': 'db',
            '5STES_TRES': 'TRE1'
        }):
            client = AnalyticsTES()
            
            assert client.TES_url == "http://example.com/v1"
    
    def test_submission_url_construction(self):
        """Test submission URL construction."""
    
        with patch.dict(os.environ, {
            'TES_BASE_URL': 'http://example.com',
            'TES_DOCKER_IMAGE': 'test:latest',
            'DB_HOST': 'db',
            'DB_PORT': '5432',
            'DB_USERNAME': 'user',
            'DB_PASSWORD': 'pass',
            'DB_NAME': 'db',
            '5STES_TRES': 'TRE1'
        }):
            client = AnalyticsTES()
            
            assert client.submission_url == "http://example.com/api/Submission"
    
    def test_tes_url_with_path_in_base(self):
        """Test TES URL construction when base URL has a path."""
        
        with patch.dict(os.environ, {
            'TES_BASE_URL': 'http://example.com/api/tes',
            'TES_DOCKER_IMAGE': 'test:latest',
            'DB_HOST': 'db',
            'DB_PORT': '5432',
            'DB_USERNAME': 'user',
            'DB_PASSWORD': 'pass',
            'DB_NAME': 'db',
            '5STES_TRES': 'TRE1'
        }):
            client = AnalyticsTES()
            
            # Should append /v1 to the path
            parsed = urlparse(client.TES_url)
            assert parsed.path.endswith("/v1")
    
    def test_required_env_variables(self):
        """Test that missing required environment variables raise errors."""
        # Missing TES_BASE_URL
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="TES_BASE_URL"):
                AnalyticsTES()
        
        # Missing TES_DOCKER_IMAGE
        with patch.dict(os.environ, {'TES_BASE_URL': 'http://test.com'}, clear=True):
            with pytest.raises(ValueError, match="TES_DOCKER_IMAGE"):
                AnalyticsTES()
    
    def test_tags_configuration(self):
        """Test that tags are correctly configured."""
        with patch.dict(os.environ, {
            'TES_BASE_URL': 'http://example.com',
            'TES_DOCKER_IMAGE': 'test:latest',
            'DB_HOST': 'db',
            'DB_PORT': '5432',
            'DB_USERNAME': 'user',
            'DB_PASSWORD': 'pass',
            'DB_NAME': 'db',
            '5STES_TRES': 'TRE1,TRE2,TRE3'
        }):
            client = AnalyticsTES()
            
            assert "tres" in client.tags
            assert isinstance(client.tags["tres"], list)
            assert client.tags["tres"] == ['TRE1', 'TRE2', 'TRE3']