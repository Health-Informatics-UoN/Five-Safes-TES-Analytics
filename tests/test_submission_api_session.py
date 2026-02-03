from unittest.mock import patch, Mock 

import pytest 

from submission_api_session import SubmissionAPISession 


CLIENT_ID = "Dare-Control-API"
CLIENT_SECRET = "2e60b956-16bc-4dea-8b49-118a8baac5e5"
USERNAME = "globaladminuser"
PASSWORD = "password123"
TOKEN_URL = "http://localhost:8085/realms/Dare-Control/protocol/openid-connect/token"
LOGOUT_URL = "http://localhost:8085/realms/Dare-Control/protocol/openid-connect/logout"


@pytest.fixture
def mock_response(): 
    pass 


def test_login_successful():
    with patch("submission_api_session.requests") as mock_requests: 
        mock_response = Mock() 
        mock_response.json.return_value = {
            "access_token": "abc", 
            "refresh_token": "xyz"
        }
        mock_response.raise_for_status.return_value = None
        mock_requests.post.return_value = mock_response

        session = SubmissionAPISession(
            client_id="fake_client", 
            client_secret="fake_secret", 
            username="username", 
            password="password", 
            token_url="token_url", 
            logout_url="logout_url"
        ) 

        session._login()
        
        assert session.access_token == "abc"
        assert session.refresh_token == "xyz"


def test_refresh_replaces_tokens(): 
    with patch("submission_api_session.requests") as mock_requests: 
        session = SubmissionAPISession(
            client_id="fake_client", 
            client_secret="fake_secret", 
            username="username", 
            password="password", 
            token_url="token_url", 
            logout_url="logout_url"
        ) 
        session._access_token = "abc"
        session._refresh_token = "xyz"

        mock_response = Mock() 
        mock_response.json.return_value = {
            "access_token": "123", 
            "refresh_token": "456"
        }
        mock_response.raise_for_status.return_value = None
        mock_requests.post.return_value = mock_response

        session._refresh()

        assert session.access_token == "123"
        assert session.refresh_token == "456"


def test_request_retries_on_401(): 
    pass 


def test_request_adds_authorisation_header(): 
    pass 


def test_logout_successful(): 
    pass 


def test_submission_api_session_context_manager(): 
    pass 


