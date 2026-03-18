import base64
import json 
import time 
import pytest 

from five_safes_tes_analytics.auth.submission_api_session import SubmissionAPISession 
from five_safes_tes_analytics.clients.minio_client import MinIOClient 


class TestSubmissionAPISessionIntegration: 
    """
    These tests are disabled by default as they require a deployed version of 5STES. 
    If ran with: 
    
    poetry run pytest -s tests/test_submission_api_session.py -m integration

    they test the interaction of the SubmissionAPISession with an actual instance of a 5STES submission layer. 
    Make sure .env variables are correctly configured to point to a deployed version
    of 5STES. 
    """
    @staticmethod
    def validate_jwt_token(session_token: str): 
        """
        Helper function for integration tests to validate a JWT token returned by submission API.

        JWT payloads are base64url encoded without padding 
        Standard Base64 requires encoded string must be a multiple of 4 - otherwise it is padded with "="
        """
        assert isinstance(session_token, str) 
        assert session_token 
        assert session_token.count(".") == 2

        payload_part = session_token.split(".")[1]
        padded = payload_part + "=" * (-len(payload_part) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded))

        assert "exp" in payload
        assert payload["exp"] > time.time()

    @pytest.mark.integration
    def test_login_on_real_submission_api_endpoint(self): 
        with SubmissionAPISession() as session: 
            self.validate_jwt_token(session.access_token)
            self.validate_jwt_token(session.refresh_token)

    @pytest.mark.integration 
    def test_refresh_on_real_submission_api_endpoint(self): 
        with SubmissionAPISession() as session: 
            access_token_before_refresh = session.access_token
            refresh_token_before_refresh = session.refresh_token 

            session._refresh()

            access_token_post_refresh = session.access_token
            refresh_token_post_refresh = session.refresh_token 

            self.validate_jwt_token(session.access_token)
            self.validate_jwt_token(session.refresh_token)
            assert access_token_before_refresh != access_token_post_refresh 
            assert refresh_token_before_refresh != refresh_token_post_refresh 

    @pytest.mark.integration 
    def test_minio_credentials_are_successfully_fetched(self): 
        with SubmissionAPISession() as token_session: 
            client = MinIOClient(token_session)
            creds = client._exchange_token_for_credentials()
            session_token = creds["session_token"]

            assert "access_key" in creds 
            assert "secret_key" in creds 
            assert "session_token" in creds
            
            self.validate_jwt_token(session_token)
    
    @pytest.mark.integration 
    def test_minio_credentials_refresh_after_token_invalidation(self):
        with SubmissionAPISession() as token_session: 
            client = MinIOClient(token_session)

            client._get_client()
            minio_access_key_before = client._credentials["access_key"]
            minio_secret_key_before = client._credentials["secret_key"]

            client._client = None 
            client.refresh_credentials()
            token_session._access_token = 'invalid'

            client._get_client()
            minio_access_key_after = client._credentials["access_key"]
            minio_secret_key_after = client._credentials["secret_key"]

            assert minio_access_key_after != minio_access_key_before
            assert minio_secret_key_after != minio_secret_key_before
            self.validate_jwt_token(token_session.access_token)
    
    @pytest.mark.integration 
    def test_minio_client_list_buckets_after_token_invalidation(self):
        """
        Important to test that the Minio Credentials are refreshed properly upon 
        token expiry - easiest way to do this is to manually corrupt the session token 
        and then test that the simple list_buckets method is sucessfully called afterwards. 
        """
        with SubmissionAPISession() as token_session: 
            client = MinIOClient(token_session)

            buckets = client.list_buckets()
            minio_access_key_before = client._credentials["access_key"]
            minio_secret_key_before = client._credentials["secret_key"]
            original_access = token_session.access_token

            assert isinstance(buckets, list)

            client._client = None 
            client.refresh_credentials()
            token_session._access_token = "invalid"
            
            buckets = client.list_buckets()
            minio_access_key_after = client._credentials["access_key"]
            minio_secret_key_after = client._credentials["secret_key"]

            assert isinstance(buckets, list)
            assert minio_access_key_after != minio_access_key_before
            assert minio_secret_key_after != minio_secret_key_before
            self.validate_jwt_token(token_session.access_token)
            token_session.access_token != original_access 
    