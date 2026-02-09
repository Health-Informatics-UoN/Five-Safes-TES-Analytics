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


def test_request_successful_on_200(): 
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

        mock_response = Mock(status_code=200)
        mock_requests.request.return_value = mock_response 

        mock_refresh_response = Mock() 
        mock_refresh_response.json.return_value = {
            "access_token": "123", 
            "refresh_token": "456"
        }
        mock_refresh_response.raise_for_status.return_value = None
        mock_requests.post.return_value = mock_refresh_response

        headers = {
            'accept': 'text/plain',
            'Content-Type': 'application/json'
        }

        response = session.request(
            "POST",
            "fake_url",
            headers=headers 
        )

        assert mock_requests.request.call_count == 1
        assert response.status_code == 200
        assert session.access_token == "abc"
        assert session.refresh_token == "xyz"


def test_request_retries_on_401(): 
    """
    To test that this retries on a 401 we need to: 
        - Check that self._refresh is called once and only once. 
        - Check token is properly refreshed. 
        - Check requests method is called twice. 
    """
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

        mock_response_401 = Mock(status_code=401)
        mock_response_200 = Mock(status_code=200)
        mock_requests.request.side_effect = [mock_response_401, mock_response_200]

        mock_refresh_response = Mock() 
        mock_refresh_response.json.return_value = {
            "access_token": "123", 
            "refresh_token": "456"
        }
        mock_refresh_response.raise_for_status.return_value = None
        mock_requests.post.return_value = mock_refresh_response

        data = {
            "Action": "AssumeRoleWithWebIdentity",
            "Version": "2011-06-15",
            "DurationSeconds": "3600"
        }

        response = session.request(
            "POST",
            "fake_url",
            token_in="body",
            token_field="WebIdentityToken",
            data=data
        )
        
        assert mock_requests.request.call_count == 2
        assert response.status_code == 200
        assert session.access_token == "123"
        assert session.refresh_token == "456"


def test_request_adds_authorisation_header(): 
    pass 


def test_logout_successful(): 
    pass 


def test_submission_api_session_context_manager(): 
    pass 


