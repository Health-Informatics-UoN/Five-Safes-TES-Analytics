from unittest.mock import patch, Mock 
import pytest 

from submission_api_session import SubmissionAPISession 


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


def test_logout_successful(): 
    with patch("submission_api_session.requests") as _: 
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

        session._logout()

        assert session.access_token is None 
        assert session.refresh_token is None


def test_context_manager_calls_login_and_logout():
    with patch.object(SubmissionAPISession, "_login") as login, \
         patch.object(SubmissionAPISession, "_logout") as logout:

        with SubmissionAPISession(
            "id", "secret", "user", "pass", "token", "logout"
        ):
            pass

        login.assert_called_once()
        logout.assert_called_once()


class TestSubmissionAPISessionIntegration: 
    """
    These tests are disabled by default as they require a deployed version of 5STES. However, if ran 
    they test the interaction of the SubmissionAPISession with an actual instance of a 5STES submission 
    layer. 
    """
    @pytest.mark.integration
    def test_login_on_real_submission_api_endpoint(self): 
        with SubmissionAPISession() as session: 
            assert isinstance(session.access_token, str) 
            assert isinstance(session.refresh_token, str)
            assert session.access_token.count(".") == 2
            assert session.refresh_token.count(".") == 2

    @pytest.mark.integration 
    def test_refresh_on_real_submission_api_endpoint(self): 
        with SubmissionAPISession() as session: 
            access_token_before_refresh = session.access_token
            refresh_token_before_refresh = session.refresh_token 

            session._refresh()

            access_token_post_refresh = session.access_token
            refresh_token_post_refresh = session.refresh_token 

            assert isinstance(session.access_token, str) 
            assert isinstance(session.refresh_token, str)
            assert session.access_token.count(".") == 2
            assert session.refresh_token.count(".") == 2
            assert access_token_before_refresh != access_token_post_refresh 
            assert refresh_token_before_refresh != refresh_token_post_refresh 

    @pytest.mark.integration 
    def test_token_fetching_and_refreshing_with_mean_analysis(self): 
        pass 
